#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Build the VillageSQL development server from this source tree and install it
# into a directory of the caller's choosing, laid out exactly as a downloaded
# villagesql-dev-server-* tarball unpacks: bin/, lib/veb/, mysql-test/, the
# villagesql control script, QUICKSTART.md, and VERSION. The extension SDK is
# placed alongside, as the install script does for a prebuilt download.
#
# This is the from-source counterpart of downloading a prebuilt dev server: a
# caller such as the install script ends up with the same tree either way.
# Optionally the bundled extensions (villagesql/dev_server/bundled_extensions.txt)
# are cloned, built against the SDK, and folded into lib/veb.
#
# The work is delegated to the other scripts in this directory, in the same
# order the release workflow runs them:
#   build_server.sh               configure and compile the server
#   checkout_bundled_extensions.sh clone the bundled extensions   (optional)
#   build_bundled_extensions.sh   build each one into a .veb       (optional)
#   stage_dev_server.sh           assemble the tree
#
# Usage: build_dev_server.sh --dest <dir> [options]
#
# Run with --help for the options.

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<USAGE
Usage: $(basename "$0") --dest <dir> [options]

Build the VillageSQL dev server and install it as a directory tree, the same
tree a villagesql-dev-server-* tarball unpacks to.

Required:
  --dest <dir>            Directory to install the dev server into. Must not
                          exist, or be empty, unless --clean is given.

Build options:
  --build-dir <dir>       CMake build directory (default: <source>/../build).
                          Reused across runs; an incremental rebuild is fast.
  --source-dir <dir>      Source tree to build (default: the tree this script
                          lives in).
  --build-type <type>     release (default) or debug.
  --jobs <n>              Parallel build jobs (default: every core).
  --cmake-flags "<flags>" Extra cmake flags for the server configure, appended
                          verbatim (e.g. "-DWITH_FOO=1 -DWITH_BAR=0").
  --release-build         Strip the -dev suffix from the version, as a release
                          build does.
  --skip-build            Do not build the server; stage from an existing
                          build in --build-dir.

Content options:
  --bundled-extensions    Clone and build the bundled extensions and include
                          them in lib/veb. Needs the extension build
                          dependencies (scripts/setup_linux_ext_deps.sh or
                          scripts/setup_macos_ext_deps.sh) and jq.
  --ext-dir <dir>         Where the bundled extensions are cloned
                          (default: <build-dir>/bundled_extensions). Wiped and
                          re-cloned on every run.

Other:
  --clean                 Replace --dest if it already exists.
  -h, --help              Show this help.

Environment:
  The build is configured by build_server.sh and build_ci.sh, so the variables
  they read (CMAKE_EXTRA_FLAGS, PARALLEL_JOBS, VSQL_PRE_RELEASE_VERSION) apply
  here too; the options above set them.

Examples:
  # Server only, into ~/.villagesql/prebuilt, building in ~/.villagesql/build
  $(basename "$0") --dest ~/.villagesql/prebuilt --build-dir ~/.villagesql/build

  # Server plus the bundled extensions, replacing an earlier install
  $(basename "$0") --dest ~/.villagesql/prebuilt --bundled-extensions --clean
USAGE
}

DEST=""
BUILD_DIR=""
SOURCE_DIR_ARG=""
BUILD_TYPE="release"
JOBS=""
CMAKE_FLAGS_ARG=""
RELEASE_BUILD=0
SKIP_BUILD=0
BUNDLED_EXTENSIONS=0
EXT_DIR=""
CLEAN=0

# Return the value of an option that takes an argument, from either
# "--opt value" or "--opt=value". Sets OPT_VALUE and OPT_SHIFT.
take_value() {
    local opt="$1" next="${2-}"
    if [[ "$opt" == *=* ]]; then
        OPT_VALUE="${opt#*=}"
        OPT_SHIFT=1
    else
        [[ $# -ge 2 ]] || { echo "Error: $opt requires a value" >&2; usage >&2; exit 2; }
        OPT_VALUE="$next"
        OPT_SHIFT=2
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dest|--dest=*)               take_value "$@"; DEST="$OPT_VALUE";            shift "$OPT_SHIFT" ;;
        --build-dir|--build-dir=*)     take_value "$@"; BUILD_DIR="$OPT_VALUE";       shift "$OPT_SHIFT" ;;
        --source-dir|--source-dir=*)   take_value "$@"; SOURCE_DIR_ARG="$OPT_VALUE";  shift "$OPT_SHIFT" ;;
        --build-type|--build-type=*)   take_value "$@"; BUILD_TYPE="$OPT_VALUE";      shift "$OPT_SHIFT" ;;
        --jobs|--jobs=*)               take_value "$@"; JOBS="$OPT_VALUE";            shift "$OPT_SHIFT" ;;
        --cmake-flags|--cmake-flags=*) take_value "$@"; CMAKE_FLAGS_ARG="$OPT_VALUE"; shift "$OPT_SHIFT" ;;
        --ext-dir|--ext-dir=*)         take_value "$@"; EXT_DIR="$OPT_VALUE";         shift "$OPT_SHIFT" ;;
        --release-build)               RELEASE_BUILD=1;      shift ;;
        --skip-build)                  SKIP_BUILD=1;         shift ;;
        --bundled-extensions)          BUNDLED_EXTENSIONS=1; shift ;;
        --clean)                       CLEAN=1;              shift ;;
        -h|--help)                     usage; exit 0 ;;
        *)
            echo "Error: Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "$DEST" ]]; then
    echo "Error: --dest is required" >&2
    usage >&2
    exit 2
fi

case "$BUILD_TYPE" in
    release|debug) ;;
    *) echo "Error: --build-type must be release or debug, not '$BUILD_TYPE'" >&2; exit 2 ;;
esac

if [[ -n "$JOBS" && ! "$JOBS" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --jobs must be a positive integer, not '$JOBS'" >&2
    exit 2
fi

# Resolve the trees. SOURCE_DIR defaults to the checkout this script is part
# of, so a caller that cloned the repo and ran the script in place needs no
# --source-dir.
if [[ -n "$SOURCE_DIR_ARG" ]]; then
    [[ -d "$SOURCE_DIR_ARG" ]] || { echo "Error: --source-dir not found: $SOURCE_DIR_ARG" >&2; exit 2; }
    SOURCE_DIR="$(cd "$SOURCE_DIR_ARG" && pwd)"
else
    SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"
fi
[[ -f "$SOURCE_DIR/CMakeLists.txt" ]] \
    || { echo "Error: not a VillageSQL source tree (no CMakeLists.txt): $SOURCE_DIR" >&2; exit 2; }

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

# The scripts delegated to are the ones in the tree being built, not this
# script's own copies, so a --source-dir at another revision builds as that
# revision expects.
BLD_TOOLS="$SOURCE_DIR/villagesql/bld_tools"

if [[ -z "$BUILD_DIR" ]]; then
    BUILD_DIR="$(cd "$SOURCE_DIR/.." && pwd)/build"
fi
mkdir -p "$BUILD_DIR"
BUILD_DIR="$(cd "$BUILD_DIR" && pwd)"

if [[ -z "$EXT_DIR" ]]; then
    EXT_DIR="$BUILD_DIR/bundled_extensions"
fi

# Resolve --dest to an absolute path and check it before spending the build
# time. The tree is assembled next to it and moved into place at the end, so a
# failed run never leaves a half-populated destination behind.
mkdir -p "$(dirname "$DEST")"
DEST_PARENT="$(cd "$(dirname "$DEST")" && pwd)"
DEST="$DEST_PARENT/$(basename "$DEST")"
if [[ -e "$DEST" ]]; then
    if [[ -d "$DEST" && -z "$(ls -A "$DEST")" ]]; then
        : # empty directory; replaced at the end
    elif [[ "$CLEAN" == "1" ]]; then
        log_warn "Destination exists and will be replaced: $DEST"
    else
        die "Destination exists and is not empty: $DEST (use --clean to replace it)"
    fi
fi

for tool in cmake cpack make; do
    command -v "$tool" >/dev/null 2>&1 || die "'$tool' not found; install the build toolchain first ($BLD_TOOLS/setup_build_env.sh)"
done
if [[ "$BUNDLED_EXTENSIONS" == "1" ]]; then
    for tool in git jq; do
        command -v "$tool" >/dev/null 2>&1 || die "'$tool' is needed for --bundled-extensions"
    done
fi

STAGE_DIR=""
cleanup() {
    if [[ -n "$STAGE_DIR" && -d "$STAGE_DIR" ]]; then
        rm -rf "$STAGE_DIR"
    fi
    return 0
}
trap cleanup EXIT

# Settings every delegated script reads from the environment.
export SOURCE_DIR BUILD_DIR BUILD_TYPE
[[ -n "$JOBS" ]] && export PARALLEL_JOBS="$JOBS"
[[ -n "$CMAKE_FLAGS_ARG" ]] && export CMAKE_EXTRA_FLAGS="$CMAKE_FLAGS_ARG"
if [[ "$RELEASE_BUILD" == "1" ]]; then
    # Set, and empty, means "no pre-release suffix". build_server.sh forwards
    # it to cmake and build_info.sh reads it when naming the SDK and VERSION.
    export VSQL_PRE_RELEASE_VERSION=""
fi
# An install has no use for the unit test binaries.
export BUILD_UNIT_TESTS=0

log_step "VillageSQL dev server build"
log_info "Source:        $SOURCE_DIR"
log_info "Build dir:     $BUILD_DIR"
log_info "Destination:   $DEST"
log_info "Build type:    $BUILD_TYPE"
log_info "Build server:  $([[ "$SKIP_BUILD" == "1" ]] && echo "no (--skip-build)" || echo "yes")"
log_info "Extensions:    $([[ "$BUNDLED_EXTENSIONS" == "1" ]] && echo "bundled (clones in $EXT_DIR)" || echo "examples only")"
echo ""

if [[ "$SKIP_BUILD" == "1" ]]; then
    log_step "Skipping server build (--skip-build)"
    [[ -x "$BUILD_DIR/runtime_output_directory/mysqld" ]] \
        || die "No server build in $BUILD_DIR (drop --skip-build to build one)"
else
    log_step "Building the server"
    "$BLD_TOOLS/build_server.sh"
fi

if [[ "$BUNDLED_EXTENSIONS" == "1" ]]; then
    log_step "Checking out the bundled extensions"
    # Stale clones from an earlier run would be built as they are, so start
    # from the manifest every time.
    rm -rf "$EXT_DIR"
    mkdir -p "$EXT_DIR"
    "$BLD_TOOLS/checkout_bundled_extensions.sh" "$EXT_DIR"

    log_step "Building the bundled extensions"
    SDK_DIR="$("$BLD_TOOLS/get_sdk.sh" "$BUILD_DIR")"
    if ! "$BLD_TOOLS/build_bundled_extensions.sh" \
            "$EXT_DIR" "$SDK_DIR" "$BUILD_DIR/veb_output_directory"; then
        log_error "An extension failed to build. The bundled extensions need"
        log_error "their own dependencies; install them with one of:"
        log_error "  sudo $SOURCE_DIR/scripts/setup_linux_ext_deps.sh"
        log_error "  $SOURCE_DIR/scripts/setup_macos_ext_deps.sh"
        die "Bundled extension build failed"
    fi
fi

log_step "Assembling the dev server tree"
STAGE_DIR="$(mktemp -d "$DEST_PARENT/.$(basename "$DEST").stage.XXXXXX")"
BUILD_BUNDLED_EXTENSIONS="$BUNDLED_EXTENSIONS" \
INCLUDE_SDK=1 \
    "$BLD_TOOLS/stage_dev_server.sh" "$STAGE_DIR"

log_step "Installing into $DEST"
if [[ -e "$DEST" ]]; then
    rm -rf "$DEST"
fi
mv "$STAGE_DIR" "$DEST"
STAGE_DIR=""

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  📦 Dev server installed: $DEST"
echo "  🔧 Build directory:      $BUILD_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "To use:"
echo "  1. Read: cat $DEST/QUICKSTART.md"
echo "  2. Initialize: $DEST/villagesql init"
echo "  3. Start: $DEST/villagesql start"
echo "  4. Connect: $DEST/villagesql connect"
echo ""
