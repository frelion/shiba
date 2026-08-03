#!/usr/bin/env bash
# M15.4 static SQL binding/registration dependency and authority contract.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

python3 - <<'PY'
import pathlib
import re
import sys


def normalized(name):
    return name.replace("_", "-").lower()


def dependencies(path, production_only=False):
    names = set()
    section = ""
    table_alias = None
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        header = re.fullmatch(r"\[([^]]+)]", line)
        if header:
            section = header.group(1).strip()
            table = re.fullmatch(
                r"(?:target\..+\.)?((?:dev-|build-)?dependencies)(?:\.([A-Za-z0-9_-]+))?",
                section,
            )
            kind = table.group(1) if table else ""
            table_alias = table.group(2) if table else None
            allowed = bool(table) and (not production_only or kind == "dependencies")
            if allowed and table_alias:
                names.add(normalized(table_alias))
            continue
        dependency_section = re.fullmatch(r"(?:target\..+\.)?dependencies", section)
        any_dependency_section = re.fullmatch(
            r"(?:target\..+\.)?(?:dev-|build-)?dependencies", section
        )
        allowed_section = dependency_section if production_only else any_dependency_section
        if allowed_section and "=" in line:
            declared, specification = line.split("=", 1)
            names.add(normalized(declared.strip().strip('"\'')))
            package = re.search(r'''\bpackage\s*=\s*["']([^"']+)["']''', specification)
            if package:
                names.add(normalized(package.group(1)))
        elif table_alias:
            table_header = re.fullmatch(
                r"(?:target\..+\.)?((?:dev-|build-)?dependencies)\.[A-Za-z0-9_-]+",
                section,
            )
            allowed = bool(table_header) and (
                not production_only or table_header.group(1) == "dependencies"
            )
            package = re.fullmatch(r'''package\s*=\s*["']([^"']+)["']''', line)
            if allowed and package:
                names.add(normalized(package.group(1)))
    return names


root_manifest = pathlib.Path("Cargo.toml")
workspace = root_manifest.read_text()
members_match = re.search(r"(?ms)^members\s*=\s*\[(.*?)^]", workspace)
members = re.findall(r'''["']([^"']+)["']''', members_match.group(1)) if members_match else []
registration_member = "crates/shiba-sql-registration"
if registration_member not in members:
    raise SystemExit("M15.4 SQL registration crate is not enrolled in the workspace")

crate_manifests = sorted(pathlib.Path("crates").glob("*/Cargo.toml"))
manifests = {path.parent.name: path for path in crate_manifests}
required_crates = {
    "shiba-sql-frontend",
    "shiba-sql-registration",
    "shiba-runtime",
    "shiba-ingress",
    "shiba-operator",
}
missing = sorted(required_crates - manifests.keys())
if missing:
    raise SystemExit(f"M15.4 required crates are missing: {missing}")

frontend_forbidden = {
    "postgres",
    "postgres-types",
    "tokio-postgres",
    "pgrx",
    "shiba-runtime",
    "shiba-catalog",
    "shiba-ingress",
}
frontend_dependencies = dependencies(manifests["shiba-sql-frontend"])
found = sorted(frontend_dependencies & frontend_forbidden)
if found:
    raise SystemExit(f"M15.4 pure frontend crosses the database/runtime boundary: {found}")
if not {"shiba-compiler", "shiba-protocol"}.issubset(frontend_dependencies):
    raise SystemExit("M15.4 frontend must bind through shiba-compiler and shiba-protocol")

parser_dependencies = {"shiba-sql-frontend", "sqlparser"}
for crate in ("shiba-runtime", "shiba-ingress", "shiba-operator"):
    found = sorted(dependencies(manifests[crate], production_only=True) & parser_dependencies)
    if found:
        raise SystemExit(f"M15.4 {crate} production dependency leaks SQL frontend/parser: {found}")

registration_dependencies = dependencies(
    manifests["shiba-sql-registration"], production_only=True
)
required_registration = {"shiba-sql-frontend", "postgres", "shiba-runtime"}
if not required_registration.issubset(registration_dependencies):
    absent = sorted(required_registration - registration_dependencies)
    raise SystemExit(f"M15.4 control-plane registration dependencies are incomplete: {absent}")
for crate, manifest in manifests.items():
    if crate == "shiba-sql-registration":
        continue
    direct = dependencies(manifest, production_only=True)
    if "shiba-sql-frontend" in direct and ({"postgres", "shiba-runtime"} & direct):
        raise SystemExit(
            f"M15.4 only shiba-sql-registration may join frontend to postgres/runtime: {crate}"
        )


def source_text(crate):
    paths = sorted((pathlib.Path("crates") / crate / "src").rglob("*.rs"))
    if not paths:
        raise SystemExit(f"M15.4 crate has no production sources: {crate}")
    return paths, "\n".join(path.read_text() for path in paths)


frontend_paths, frontend_source = source_text("shiba-sql-frontend")
registration_paths, registration_source = source_text("shiba-sql-registration")
_, runtime_source = source_text("shiba-runtime")
_, ingress_source = source_text("shiba-ingress")

markers = {
    "pure Binder API": (frontend_source, r"\bpub\s+fn\s+bind_query\s*\("),
    "resolved exact source descriptor": (
        frontend_source,
        r"(?s)\bpub\s+struct\s+ResolvedSource\s*\{[^}]*\bSourceDescriptor\b[^}]*\bIdentityIndexDescriptor\b",
    ),
    "SQL control-plane entry": (
        registration_source,
        r"\bpub\s+fn\s+compile_sql_and_register\s*\(",
    ),
    "transaction-local Runtime entry": (
        runtime_source,
        r"\bpub\s+fn\s+compile_and_register_in_transaction\s*\(\s*transaction\s*:\s*&mut\s+Transaction\b",
    ),
}
absent = [name for name, (text, pattern) in markers.items() if not re.search(pattern, text)]
if absent:
    raise SystemExit(f"M15.4 registration contract markers are missing: {absent}")

unsafe_construct = re.compile(r"\bunsafe\s+(?:fn|impl|trait|extern)\b|\bunsafe\s*\{")
for crate in manifests:
    _, text = source_text(crate)
    if unsafe_construct.search(text):
        raise SystemExit(f"M15.4 {crate} production code contains an unsafe construct")

production_paths = sorted(path for crate in manifests for path in (pathlib.Path("crates") / crate / "src").rglob("*.rs"))
for path in production_paths:
    lines = len(path.read_text().splitlines())
    if lines > 400:
        raise SystemExit(f"M15.4 production file exceeds 400 lines: {path} ({lines})")
    if lines > 300:
        print(
            f"warning: M15.4 production file exceeds 300 lines: {path} ({lines})",
            file=sys.stderr,
        )

for crate, text in (("shiba-runtime", runtime_source), ("shiba-ingress", ingress_source)):
    leaks = [
        marker
        for marker in ("parse_sql", "UnboundQuery", "sqlparser")
        if re.search(rf"\b{re.escape(marker)}\b", text)
    ]
    if leaks:
        raise SystemExit(f"M15.4 {crate} production source knows SQL/parser AST: {leaks}")

raw_sql_authority = re.compile(
    r"\b(?:raw_sql|sql_text|query_sql|query_text|statement_sql|statement_text)\b",
    re.IGNORECASE,
)
for path in sorted(pathlib.Path("sql").rglob("*.sql")):
    if raw_sql_authority.search(path.read_text()):
        raise SystemExit(f"M15.4 Catalog schema contains a raw SQL authority field: {path}")
PY

echo "M15.4 SQL binding, registration, dependency, and authority gate passed"
