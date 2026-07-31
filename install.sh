#!/usr/bin/env bash

set -Eeuo pipefail

readonly REPOSITORY="frelion/shiba"
readonly RELEASES_URL="https://github.com/${REPOSITORY}/releases"

version="latest"
pg_config=""

usage() {
  cat <<'EOF'
Install the pre-built Shiba PostgreSQL 17 or 18 extension from GitHub Releases.

Usage:
  ./install.sh [--version VERSION] [--pg-config PATH]

Options:
  --version VERSION    Release tag to install, for example v0.1.0.
                       Defaults to the latest GitHub Release.
  --pg-config PATH     PostgreSQL 17 or 18 pg_config to install against.
  --help               Show this help.

This installer currently supports Linux x86_64 with the Debian/Ubuntu
PostgreSQL 17/18 Debian/Ubuntu directory layout. It installs the extension
files only; it does not change postgresql.conf or restart PostgreSQL.
EOF
}

die() {
  printf 'error: %s\n' "$1" >&2
  exit 1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

download() {
  local url="$1"
  local destination="$2"

  if command_exists curl; then
    curl --fail --location --retry 3 --silent --show-error \
      --proto '=https' --tlsv1.2 "$url" --output "$destination"
  elif command_exists wget; then
    wget --https-only --tries=3 --output-document="$destination" "$url"
  else
    die 'curl or wget is required'
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      [[ $# -ge 2 ]] || die '--version requires a value'
      version="$2"
      shift 2
      ;;
    --pg-config)
      [[ $# -ge 2 ]] || die '--pg-config requires a path'
      pg_config="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

[[ "$(uname -s)" == "Linux" ]] || die 'the pre-built release currently supports Linux only; build from source on macOS or Windows'
[[ "$(uname -m)" == "x86_64" ]] || die 'the pre-built release currently supports x86_64 only; build from source on another architecture'

if [[ -z "$pg_config" ]]; then
  if command_exists pg_config; then
    pg_config="$(command -v pg_config)"
  elif [[ -x /usr/lib/postgresql/18/bin/pg_config ]]; then
    pg_config=/usr/lib/postgresql/18/bin/pg_config
  elif [[ -x /usr/lib/postgresql/17/bin/pg_config ]]; then
    pg_config=/usr/lib/postgresql/17/bin/pg_config
  else
    die 'PostgreSQL 17 or 18 pg_config was not found; pass --pg-config /path/to/pg_config'
  fi
fi

[[ -x "$pg_config" ]] || die "pg_config is not executable: $pg_config"
pg_version="$("$pg_config" --version)"
pg_major="${pg_version#PostgreSQL }"
pg_major="${pg_major%%.*}"
case "$pg_major" in
  17|18)
    ;;
  *)
    die "Shiba requires PostgreSQL 17 or 18, found: $pg_version"
    ;;
esac

case "$version" in
  latest)
    command_exists curl || die 'curl is required when --version is omitted'
    latest_url="$(
      curl --fail --location --retry 3 --silent --show-error \
        --proto '=https' --tlsv1.2 --output /dev/null --write-out '%{url_effective}' \
        "${RELEASES_URL}/latest"
    )"
    version="${latest_url##*/}"
    [[ "$version" == v* ]] || die "could not determine the latest release tag from $latest_url"
    ;;
  v*)
    ;;
  *)
    version="v${version}"
    ;;
esac

asset="shiba-${version}-postgresql${pg_major}.tar.gz"
checksum="${asset}.sha256"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/shiba-install.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

download "https://github.com/${REPOSITORY}/releases/download/${version}/${asset}" "${tmp_dir}/${asset}"
download "https://github.com/${REPOSITORY}/releases/download/${version}/${checksum}" "${tmp_dir}/${checksum}"

(
  cd "$tmp_dir"
  if command_exists sha256sum; then
    sha256sum --check "$checksum"
  elif command_exists shasum; then
    shasum --algorithm 256 --check "$checksum"
  else
    die 'sha256sum or shasum is required to verify the release'
  fi
)

pkglibdir="$("$pg_config" --pkglibdir)"
sharedir="$("$pg_config" --sharedir)"
case "$pkglibdir:$sharedir" in
  "/usr/lib/postgresql/${pg_major}/lib:/usr/share/postgresql/${pg_major}")
    ;;
  *)
    die "the pre-built package uses Debian/Ubuntu PostgreSQL ${pg_major} paths, but pg_config reports pkglibdir=$pkglibdir sharedir=$sharedir"
    ;;
esac

if [[ "$(id -u)" -eq 0 ]]; then
  sudo_cmd=()
else
  command_exists sudo || die 'root privileges are required; install sudo or run this script as root'
  sudo_cmd=(sudo)
fi

"${sudo_cmd[@]}" tar --extract --gzip --file="${tmp_dir}/${asset}" --directory=/

printf '\nInstalled Shiba %s for %s.\n' "$version" "$pg_version"
cat <<'EOF'

The installer did not modify PostgreSQL configuration. Add these settings to
postgresql.conf, then restart PostgreSQL:

  session_preload_libraries = 'shiba'
  wal_level = logical
  max_replication_slots = 4
  shiba.replication_conninfo = 'host=/var/run/postgresql dbname=my_database user=postgres'

Use peer, certificate, or passfile authentication for the replication
connection; do not put a password in shiba.replication_conninfo.

Then connect to the target database and run:

  CREATE EXTENSION shiba;
  SELECT shiba.activate();
EOF
