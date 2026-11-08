#!/usr/bin/env bash

# Automated build script for the BigBlueButton fork of Hasura GraphQL Engine v2.
#
# Builds the community edition graphql-engine executable from source using the
# toolchain the upstream repository declares (see cabal.project and
# server/VERSIONS.json) and the unmodified upstream freeze file
# (cabal.project.freeze), so the dependency solve is reproducible.
#
# - installs system build dependencies (Debian/Ubuntu) when desired
# - ensures GHC + cabal-install via a pinned, checksum-verified ghcup
# - writes server/CURRENT_VERSION from HASURA_VERSION (used by /v1/version)
# - builds the graphql-engine executable in release mode (the root
#   cabal.project already sets optimization: 2 and -split-sections)
#
# Usage (modes):
#   ./scripts/build-v2.sh                # same as "all"
#   ./scripts/build-v2.sh deps           # install toolchain only
#   ./scripts/build-v2.sh build          # build only (assumes deps present)
#   ./scripts/build-v2.sh all            # deps + build
#   ./scripts/build-v2.sh clean          # clean dist-newstyle + CURRENT_VERSION
#
# Flags:
#   --no-system-deps        # skip apt-get (toolchain assumed present)
#   --no-cabal-update       # skip 'cabal update'
#   --ghc VERSION           # override GHC version
#   --jobs N                # override CABAL_JOBS (default: env or 2)
#
# Environment:
#   HASURA_VERSION          # version string written to server/CURRENT_VERSION
#                           # (e.g. v2.50.7-bbb1); required, so that a fork
#                           # build never presents itself with an upstream
#                           # version string

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"
cd "$ROOT_DIR"

export PATH="$HOME/.ghcup/bin:$PATH"

# Defaults (can be overridden by flags or env). These must match the versions
# the upstream tag declares in cabal.project / server/VERSIONS.json.
GHC_VERSION_DEFAULT="9.14.1"
GHC_VERSION="${GHC_VERSION:-$GHC_VERSION_DEFAULT}"
CABAL_VERSION_DEFAULT="3.16.1.0"
CABAL_VERSION="${CABAL_VERSION:-$CABAL_VERSION_DEFAULT}"
CABAL_JOBS="${CABAL_JOBS:-2}"

# Pinned ghcup binary, verified against the SHA256SUMS published next to it at
# https://downloads.haskell.org/~ghcup/<version>/
GHCUP_VERSION="0.1.50.2"
GHCUP_SHA256="ff6288df9758211372d8242fe830d8e6be6a8365d9406f1c9bde144b7e744143"

MODE="all"
INSTALL_SYSTEM_DEPS=1
RUN_CABAL_UPDATE=1

# Run apt-get through sudo only when not root: CI containers run as root and
# have no sudo installed.
SUDO="sudo"
if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=""
fi

# ---------- Argument parsing ----------

print_usage() {
  cat <<EOF
Usage: $0 [MODE] [FLAGS]

Modes (one of):
  deps           Install toolchain (no build)
  build          Build graphql-engine (no deps)
  all            deps + build (default)
  clean          Remove dist-newstyle and server/CURRENT_VERSION

Backwards-compatible aliases:
  --skip-deps    Same as "build"

Flags:
  --no-system-deps       Do not run apt-get (toolchain assumed present)
  --no-cabal-update      Do not run 'cabal update'
  --ghc VERSION          Use specific GHC version (default: ${GHC_VERSION_DEFAULT})
  --jobs N               Override CABAL_JOBS (default: ${CABAL_JOBS})

Examples:
  $0 deps --no-system-deps
  $0 build --jobs 4
  $0 all --ghc ${GHC_VERSION_DEFAULT}
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    deps|build|all|clean)
      MODE="$1"
      shift
      ;;
    --skip-deps)
      MODE="build"
      shift
      ;;
    --no-system-deps)
      INSTALL_SYSTEM_DEPS=0
      shift
      ;;
    --no-cabal-update)
      RUN_CABAL_UPDATE=0
      shift
      ;;
    --ghc)
      GHC_VERSION="${2:?--ghc requires a version}"
      shift 2
      ;;
    --jobs)
      CABAL_JOBS="${2:?--jobs requires a number}"
      shift 2
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      print_usage
      exit 1
      ;;
  esac
done

# ---------- Helpers ----------

log() {
  echo "==> $*"
}

install_deps() {
  log "Installing build dependencies"

  if [[ $INSTALL_SYSTEM_DEPS -eq 1 ]]; then
    if command -v apt-get >/dev/null 2>&1; then
      log "Installing system packages via apt-get"
      $SUDO env DEBIAN_FRONTEND=noninteractive apt-get update -y
      $SUDO env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential git curl libpq-dev libssl-dev zlib1g-dev unixodbc-dev \
        ca-certificates libgmp-dev libffi-dev libncurses-dev libtinfo6 pkg-config \
        xz-utils
    else
      echo "apt-get not found; please install build tools, libpq-dev, libssl-dev, zlib1g-dev, unixodbc-dev, etc manually." >&2
    fi
  else
    log "Skipping system package installation (--no-system-deps)"
  fi

  if ! command -v ghcup >/dev/null 2>&1; then
    log "Installing ghcup ${GHCUP_VERSION} (pinned, checksum-verified)"
    mkdir -p "$HOME/.ghcup/bin"
    curl --proto '=https' --tlsv1.2 -fsSL \
      -o "$HOME/.ghcup/bin/ghcup" \
      "https://downloads.haskell.org/~ghcup/${GHCUP_VERSION}/x86_64-linux-ghcup-${GHCUP_VERSION}"
    echo "${GHCUP_SHA256}  $HOME/.ghcup/bin/ghcup" | sha256sum -c -
    chmod +x "$HOME/.ghcup/bin/ghcup"
  fi

  log "Ensuring GHC ${GHC_VERSION}"
  if ! ghcup whereis ghc "${GHC_VERSION}" >/dev/null 2>&1; then
    ghcup install ghc "${GHC_VERSION}"
  fi
  ghcup set ghc "${GHC_VERSION}"

  log "Ensuring cabal-install ${CABAL_VERSION}"
  if [[ "$(cabal --numeric-version 2>/dev/null || true)" != "${CABAL_VERSION}" ]]; then
    ghcup install cabal "${CABAL_VERSION}"
    ghcup set cabal "${CABAL_VERSION}"
  fi

  if [[ $RUN_CABAL_UPDATE -eq 1 ]]; then
    log "Updating cabal index"
    cabal update
  else
    log "Skipping 'cabal update' (--no-cabal-update)"
  fi

  log "Dependencies installed successfully"
}

ensure_version_file() {
  log "Ensuring server/CURRENT_VERSION"
  local version_file="$ROOT_DIR/server/CURRENT_VERSION"
  if [[ -n "${HASURA_VERSION:-}" ]]; then
    echo "$HASURA_VERSION" > "$version_file"
  elif [[ ! -f "$version_file" ]]; then
    echo "HASURA_VERSION is not set. Set it (e.g. HASURA_VERSION=v2.50.7-bbb1)" >&2
    echo "so server/CURRENT_VERSION can be written; there is no fallback, so a" >&2
    echo "fork build never presents itself with an upstream version string." >&2
    exit 1
  fi
  log "server/CURRENT_VERSION = $(cat "$version_file")"
}

do_build() {
  log "Starting build (jobs: ${CABAL_JOBS}, ghc: $(ghc --numeric-version 2>/dev/null || echo '?'), cabal: $(cabal --numeric-version 2>/dev/null || echo '?'))"

  ensure_version_file

  # The root cabal.project + cabal.project.freeze from upstream are used as-is:
  # no --project-file override, no freeze edits.
  cabal build -j"${CABAL_JOBS}" graphql-engine

  local engine_bin
  engine_bin=$(find dist-newstyle/build -type f -name graphql-engine -path '*/x/graphql-engine/*' | head -n1 || true)
  if [[ -z "$engine_bin" ]]; then
    echo "graphql-engine binary not found under dist-newstyle/build after the build." >&2
    exit 1
  fi

  echo "Build complete."
  echo "Engine binary: ${engine_bin}"
}

do_clean() {
  log "Cleaning build artifacts"
  rm -rf dist-newstyle
  rm -f server/CURRENT_VERSION
  log "Clean complete"
}

# ---------- Main dispatch ----------

case "$MODE" in
  deps)
    install_deps
    ;;
  build)
    log "Build mode (skipping dependency installation)"
    do_build
    ;;
  all)
    install_deps
    do_build
    ;;
  clean)
    do_clean
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    print_usage
    exit 1
    ;;
esac
