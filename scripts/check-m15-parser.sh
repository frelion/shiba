#!/usr/bin/env bash
# M15.3 static SQL frontend dependency, safety, and resource-bound contract.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

python3 - <<'PY'
import pathlib
import re
import sys

root_manifest = pathlib.Path("Cargo.toml")
frontend_dir = pathlib.Path("crates/shiba-sql-frontend")
frontend_manifest = frontend_dir / "Cargo.toml"

if not frontend_manifest.is_file():
    raise SystemExit("M15.3 frontend crate is missing: crates/shiba-sql-frontend")

workspace_text = root_manifest.read_text()
members_match = re.search(r"(?ms)^members\s*=\s*\[(.*?)^\]", workspace_text)
members = re.findall(r'''["']([^"']+)["']''', members_match.group(1)) if members_match else []
if "crates/shiba-sql-frontend" not in members:
    raise SystemExit("M15.3 frontend crate is not enrolled as a workspace member")


def normalized(name):
    return name.replace("_", "-").lower()


def dependency_names(path):
    names = set()
    section = ""
    table_alias = None
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        header = re.fullmatch(r"\[([^]]+)\]", line)
        if header:
            section = header.group(1).strip()
            table = re.fullmatch(
                r"(?:target\..+\.)?(?:dev-|build-)?dependencies(?:\.([A-Za-z0-9_-]+))?",
                section,
            )
            table_alias = table.group(1) if table else None
            if table_alias:
                names.add(normalized(table_alias))
            continue
        dependency_section = re.fullmatch(
            r"(?:target\..+\.)?(?:dev-|build-)?dependencies", section
        )
        if dependency_section and "=" in line:
            declared, specification = line.split("=", 1)
            names.add(normalized(declared.strip().strip('"\'')))
            package = re.search(r'''\bpackage\s*=\s*["']([^"']+)["']''', specification)
            if package:
                names.add(normalized(package.group(1)))
        elif table_alias:
            package = re.fullmatch(r'''package\s*=\s*["']([^"']+)["']''', line)
            if package:
                names.add(normalized(package.group(1)))
    return names


frontend_text = frontend_manifest.read_text()
sqlparser = re.search(
    r'''(?m)^sqlparser\s*=\s*\{([^}]*)\}\s*(?:#.*)?$''', frontend_text
)
if not sqlparser:
    raise SystemExit("M15.3 frontend must declare sqlparser with an explicit inline dependency table")
sqlparser_fields = sqlparser.group(1)
if not re.search(r'''\bversion\s*=\s*["']=0\.62\.0["']''', sqlparser_fields):
    raise SystemExit("M15.3 sqlparser version must be pinned exactly to =0.62.0")
if not re.search(r"\bdefault-features\s*=\s*false\b", sqlparser_fields):
    raise SystemExit("M15.3 sqlparser must set default-features = false")

frontend_forbidden = {
    "shiba-runtime",
    "shiba-ingress",
    "shiba-catalog",
    "postgres",
    "postgres-types",
    "tokio-postgres",
    "pgrx",
}
found = sorted(dependency_names(frontend_manifest) & frontend_forbidden)
if found:
    raise SystemExit(f"M15.3 frontend crosses the database/runtime boundary: {found}")

downstream_forbidden = {"shiba-sql-frontend", "sqlparser"}
for crate in ("shiba-runtime", "shiba-ingress", "shiba-operator"):
    manifest_path = pathlib.Path("crates") / crate / "Cargo.toml"
    found = sorted(dependency_names(manifest_path) & downstream_forbidden)
    if found:
        raise SystemExit(f"M15.3 {crate} must remain frontend/parser-independent: {found}")

sources = sorted((frontend_dir / "src").rglob("*.rs"))
if not sources:
    raise SystemExit("M15.3 frontend has no production Rust source files")

combined = "\n".join(path.read_text() for path in sources)
for path in sources:
    lines = len(path.read_text().splitlines())
    if lines > 400:
        raise SystemExit(f"M15.3 production file exceeds 400 lines: {path} ({lines})")
    if lines > 300:
        print(f"warning: M15.3 production file exceeds 300 lines: {path} ({lines})", file=sys.stderr)

if re.search(r"\bunsafe\b", combined):
    raise SystemExit("M15.3 frontend production code contains an unsafe construct")

required_markers = {
    "database-independent declaration": r"\bpub\s+struct\s+UnboundQuery\b",
    "stable byte span": r"\bpub\s+struct\s+(?:ByteSpan|Span)\b",
    "closed stable error codes": r"\bpub\s+enum\s+ErrorCode\b",
    "error code field": r"\bpub\s+code\s*:\s*ErrorCode\b",
    "error byte span field": r"\bpub\s+span\s*:\s*(?:ByteSpan|Span)\b",
    "64 KiB SQL bound": r"\bMAX_SQL_BYTES\s*:\s*usize\s*=\s*(?:64\s*\*\s*1024|65_?536)\s*;",
    "4096-token bound": r"\bMAX_TOKENS\s*:\s*usize\s*=\s*4_?096\s*;",
    "2048-AST-node bound": r"\bMAX_AST_NODES\s*:\s*usize\s*=\s*2_?048\s*;",
    "256-expression-node bound": r"\bMAX_EXPRESSION_NODES\s*:\s*usize\s*=\s*256\s*;",
    "32-expression-depth bound": r"\bMAX_EXPRESSION_DEPTH\s*:\s*usize\s*=\s*32\s*;",
}
missing = [name for name, pattern in required_markers.items() if not re.search(pattern, combined)]
if missing:
    raise SystemExit(f"M15.3 frontend is missing frozen contract markers: {missing}")
PY

echo "M15.3 SQL frontend dependency, safety, bounds, and complexity gate passed"
