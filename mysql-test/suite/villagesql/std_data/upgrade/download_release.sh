#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Download a published VillageSQL server release and unpack it, ready to run,
# in a temporary directory.
#
# Useful for upgrade testing: unpack an older release, run its mysqld to
# initialize a datadir (see create_datadir_zip.sh), then upgrade that datadir
# with the server built from this tree.
#
# The unpacked package directory is printed on stdout as the last line, so a
# caller can capture it:
#
#   PKG=$(./download_release.sh --version 0.0.6 | tail -1)
#   "$PKG/bin/mysqld" --version
#
# Nothing is cleaned up automatically; remove the directory when you are done.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$SOURCE_DIR/villagesql/bld_tools/build_info.sh"

# Repository holding the releases. Override for a fork.
REPO="${VSQL_RELEASE_REPO:-villagesql/villagesql-server}"

VERSION=""
CODE_BASE=""
PLATFORM_ARCH=""
DEST_DIR=""
TAG=""
KEEP_TARBALL=0

usage() {
  cat <<EOF
Usage: $0 --version <VERSION> [options]

Required:
  --version <X.Y.Z>     Release to download (e.g. 0.0.6)

Options:
  --codebase <NAME>     Codebase of the package (default: this tree's
                        VSQL_CODE_BASE, e.g. mysql-8.4)
  --platform <NAME>     Package platform, <os>-<arch> (default: this machine,
                        e.g. linux-x86_64, linux-aarch64, macos-arm64)
  --dir <PATH>          Unpack here instead of a fresh temporary directory.
                        Created if missing; must not already hold the package.
  --tag <TAG>           Release tag to download from. By default both
                        "<VERSION>" and "release/<VERSION>" are tried, since
                        both tagging schemes are in use.
  --keep-tarball        Leave the downloaded tarball next to the package.
  --help                Show this message.

Environment:
  VSQL_RELEASE_REPO     GitHub repository to download from
                        (default: villagesql/villagesql-server)
EOF
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --version)
      VERSION="$2"
      shift 2
      ;;
    --codebase)
      CODE_BASE="$2"
      shift 2
      ;;
    --platform)
      PLATFORM_ARCH="$2"
      shift 2
      ;;
    --dir)
      DEST_DIR="$2"
      shift 2
      ;;
    --tag)
      TAG="$2"
      shift 2
      ;;
    --keep-tarball)
      KEEP_TARBALL=1
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      log_error "Unknown option: $1"
      usage >&2
      exit 1
      ;;
  esac
done

[[ -n "$VERSION" ]] || { log_error "--version is required"; usage >&2; exit 1; }

# Defaults come from the source tree and the current machine, so that an
# unadorned run produces a package this tree can upgrade from, on this host.
vsql_parse_version "$SOURCE_DIR"
vsql_platform_info

[[ -n "$CODE_BASE" ]] || CODE_BASE="$VSQL_CODE_BASE"
[[ -n "$PLATFORM_ARCH" ]] || PLATFORM_ARCH="${PLATFORM}-${ARCH}"

PACKAGE_NAME="villagesql-dev-server-${CODE_BASE}_${VERSION}-${PLATFORM_ARCH}"
TARBALL_NAME="${PACKAGE_NAME}.tar.gz"

# Both tagging schemes appear in the release history: 0.0.5 is tagged "0.0.5",
# 0.0.6 is tagged "release/0.0.6".
if [[ -n "$TAG" ]]; then
  CANDIDATE_TAGS=("$TAG")
else
  CANDIDATE_TAGS=("$VERSION" "release/$VERSION")
fi

if [[ -n "$DEST_DIR" ]]; then
  mkdir -p "$DEST_DIR"
  DEST_DIR="$(cd "$DEST_DIR" && pwd)"
  if [[ -e "$DEST_DIR/$PACKAGE_NAME" ]]; then
    die "$DEST_DIR/$PACKAGE_NAME already exists; remove it or pick another --dir"
  fi
else
  TMP_ROOT="${TMPDIR:-/tmp}"
  DEST_DIR=$(mktemp -d "${TMP_ROOT%/}/villagesql_release_XXXXXX") \
    || die "Cannot create a temporary directory"
  # Ours to remove: drop it again if we bail out before the package is ready,
  # rather than littering /tmp with empty directories.
  CREATED_DEST_DIR="$DEST_DIR"
  trap 'if [[ -n "${CREATED_DEST_DIR:-}" ]]; then rm -rf "$CREATED_DEST_DIR"; fi' EXIT
fi

log_info "Repository: $REPO"
log_info "Version: $VERSION"
log_info "Codebase: $CODE_BASE"
log_info "Platform: $PLATFORM_ARCH"
log_info "Package: $TARBALL_NAME"
log_info "Directory: $DEST_DIR"
echo ""

# Download the tarball into DEST_DIR, trying each candidate tag in turn.
# Prefers the gh CLI when it is installed and logged in, so that private or
# rate-limited releases work; otherwise falls back to a plain HTTPS download.
log_step "Downloading release..."

download_with_gh() {
  local tag="$1"
  gh release download "$tag" \
    --repo "$REPO" \
    --pattern "$TARBALL_NAME" \
    --dir "$DEST_DIR" \
    >/dev/null 2>&1
}

download_with_curl() {
  local tag="$1"
  curl -fsSL --retry 3 \
    -o "$DEST_DIR/$TARBALL_NAME" \
    "https://github.com/$REPO/releases/download/$tag/$TARBALL_NAME" \
    >/dev/null 2>&1
}

if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
  DOWNLOAD=download_with_gh
elif command -v curl >/dev/null 2>&1; then
  DOWNLOAD=download_with_curl
else
  die "Neither the gh CLI nor curl is available to download the release"
fi

DOWNLOADED=0
for tag in "${CANDIDATE_TAGS[@]}"; do
  log_info "Trying tag: $tag"
  if "$DOWNLOAD" "$tag"; then
    log_info "Downloaded from tag: $tag"
    DOWNLOADED=1
    break
  fi
  rm -f "$DEST_DIR/$TARBALL_NAME"
done

if [[ "$DOWNLOADED" -ne 1 ]]; then
  log_error "Could not download $TARBALL_NAME from $REPO"
  log_error "Tried tags: ${CANDIDATE_TAGS[*]}"
  log_error "Check the version, --codebase and --platform against the"
  log_error "assets listed at https://github.com/$REPO/releases"
  exit 1
fi

log_step "Unpacking..."
tar xzf "$DEST_DIR/$TARBALL_NAME" -C "$DEST_DIR" \
  || die "Failed to unpack $DEST_DIR/$TARBALL_NAME"

PACKAGE_DIR="$DEST_DIR/$PACKAGE_NAME"
[[ -d "$PACKAGE_DIR" ]] \
  || die "Tarball did not contain the expected directory: $PACKAGE_NAME"
[[ -x "$PACKAGE_DIR/bin/mysqld" ]] \
  || die "Unpacked package has no executable bin/mysqld"

if [[ "$KEEP_TARBALL" -eq 0 ]]; then
  rm -f "$DEST_DIR/$TARBALL_NAME"
fi

# The package is unpacked and intact; from here on it is the caller's to keep.
CREATED_DEST_DIR=""

echo ""
log_step "Ready."
echo ""
echo "  Initialize a datadir:  $PACKAGE_DIR/villagesql init"
echo "  Start the server:      $PACKAGE_DIR/villagesql start"
echo "  Run the server direct: $PACKAGE_DIR/bin/mysqld --version"
echo "  Read first:            $PACKAGE_DIR/QUICKSTART.md"
echo ""
echo "  Remove when done:      rm -rf $DEST_DIR"
echo ""
echo "$PACKAGE_DIR"
