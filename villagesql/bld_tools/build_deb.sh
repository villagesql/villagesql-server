#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Build VillageSQL Debian packages from a source tree.
#
# Mirrors build_rpm.sh: stages the source, materializes debian/ from the
# templates in packaging/villagesql-deb, and runs dpkg-buildpackage. Like the
# RPM driver it does NOT require a prior cmake configure -- every substitution
# comes from VSQL_VERSION and MYSQL_VERSION.
#
# Environment:
#   SOURCE_DIR   Source root (default: two levels up from this script)
#   OUTPUT_DIR   Where .deb files are copied (default: $PWD/debs)
#   WORK_DIR     Staging root (default: a mktemp dir)
#   VSQL_PRE_RELEASE_VERSION
#                Override the pre-release suffix; set empty for a release build
#
# Usage:
#   villagesql/bld_tools/build_deb.sh [--worktree] [--source-only]

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${SOURCE_DIR:-$(cd "$TOOLS_DIR/../.." && pwd)}"

# vsql_script_utils.sh provides log_*/die; vsql_parse_version() and friends
# live in build_info.sh, which expects the utilities to be sourced first.
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$SOURCE_DIR/villagesql/bld_tools/build_info.sh"

OUTPUT_DIR="${OUTPUT_DIR:-$PWD/debs}"

USE_WORKTREE=0
STAGE_ONLY=0
SOURCE_ONLY=0
BINARY_ONLY=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --worktree)    USE_WORKTREE=1; shift ;;
        --stage-only)  STAGE_ONLY=1; shift ;;
        --source-only) SOURCE_ONLY=1; shift ;;
        --binary-only) BINARY_ONLY=1; shift ;;
        -h|--help)
            sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
            exit 0
            ;;
        *) die "Unknown option: $1" ;;
    esac
done

[[ -f "$SOURCE_DIR/CMakeLists.txt" ]] || die "Not a source tree: $SOURCE_DIR"

log_step "VillageSQL DEB Build"

vsql_parse_version "$SOURCE_DIR"

# Debian orders '~' before everything, so 0.0.6~dev < 0.0.6. Epoch 1 is
# required for the same reason as in the RPM spec: VillageSQL versions as
# 0.0.x while MySQL is at 8.4.x, and without an epoch the Breaks/Replaces
# against mysql-server would never be considered an upgrade.
DEB_UPSTREAM="${VSQL_MAJOR}.${VSQL_MINOR}.${VSQL_PATCH}"
if [[ -n "$VSQL_PRE" ]]; then
    DEB_UPSTREAM="${DEB_UPSTREAM}~${VSQL_PRE}"
fi
DEB_VERSION="1:${DEB_UPSTREAM}"

mysql_version_value() {
    grep "^$1=" "$SOURCE_DIR/MYSQL_VERSION" | cut -d'=' -f2
}
MYSQL_BASE_VERSION="$(mysql_version_value MYSQL_VERSION_MAJOR).$(mysql_version_value MYSQL_VERSION_MINOR)"

# Reproducible-ish: honour SOURCE_DATE_EPOCH when the caller sets it.
if [[ -n "${SOURCE_DATE_EPOCH:-}" ]]; then
    DEB_DATE="$(date -u -d "@${SOURCE_DATE_EPOCH}" -R)"
else
    DEB_DATE="$(date -R)"
fi

log_info "VillageSQL version:  $VSQL_VERSION"
log_info "DEB version:         $DEB_VERSION"
log_info "Upstream code base:  $VSQL_CODE_BASE"
log_info "MySQL base:          $MYSQL_BASE_VERSION"
log_info "Source directory:    $SOURCE_DIR"

DEB_TEMPLATE_DIR="$SOURCE_DIR/packaging/villagesql-deb"
[[ -d "$DEB_TEMPLATE_DIR" ]] || die "Missing $DEB_TEMPLATE_DIR"

WORK_DIR="${WORK_DIR:-$(mktemp -d)}"
WORK_DIR="$(realpath -m "$WORK_DIR")"

# The staging copy below reads SOURCE_DIR recursively, so a WORK_DIR inside the
# source tree makes tar copy its own output ("file changed as we read it") and
# silently produces a corrupt tree. Refuse it rather than emit a confusing tar
# warning halfway through a build.
case "$WORK_DIR/" in
    "$(realpath -m "$SOURCE_DIR")"/*)
        die "WORK_DIR must be outside SOURCE_DIR (got $WORK_DIR)" ;;
esac

STAGE="$WORK_DIR/villagesql-${DEB_UPSTREAM}"
trap 'if [[ -z "${KEEP_WORK:-}" ]]; then rm -rf "$WORK_DIR"; fi' EXIT
log_info "Staging root:        $STAGE"

log_step "Staging source"
mkdir -p "$STAGE"
GIT="git -c safe.directory=$SOURCE_DIR -C $SOURCE_DIR"
if [[ $USE_WORKTREE -eq 0 ]] && $GIT rev-parse --git-dir >/dev/null 2>&1; then
    if ! $GIT diff-index --quiet HEAD -- 2>/dev/null || \
       [[ -n "$($GIT ls-files --others --exclude-standard 2>/dev/null | head -1)" ]]; then
        log_warn "Working tree has uncommitted changes; packaging HEAD anyway."
        log_warn "Use --worktree to package what is on disk instead."
    fi
    $GIT archive --format=tar HEAD | (cd "$STAGE" && tar xf -)
else
    log_info "Copying working tree"
    # Excludes are anchored with './' on purpose: an unanchored 'build*' also
    # matches cmake/build_configurations/ and silently breaks the build.
    tar -C "$SOURCE_DIR" \
        --exclude='./.git' \
        --exclude='./build' \
        --exclude='./build-*' \
        --exclude='./debs' \
        --exclude='./rpms' \
        --exclude='*.deb' \
        --exclude='*.rpm' \
        -cf - . | tar -C "$STAGE" -xf -
fi

# Fail fast on a staged tree missing build inputs. Without this, an over-broad
# exclude surfaces as an unrelated-looking cmake error minutes into the build.
# Keep this list in step with the one in build_rpm.sh.
REQUIRED_PATHS=(
    "CMakeLists.txt"
    "cmake/build_configurations/compiler_options.cmake"
    "cmake/build_configurations/mysql_release.cmake"
    "cmake/install_layout.cmake"
    "extra/boost/boost_1_84_0/boost/version.hpp"
    "sql/mysqld.cc"
    "villagesql/CMakeLists.txt"
    "packaging/villagesql-deb/control.in"
    "VSQL_VERSION"
    "MYSQL_VERSION"
)
MISSING=0
for p in "${REQUIRED_PATHS[@]}"; do
    [[ -e "$STAGE/$p" ]] || { log_error "Missing from staged source: $p"; MISSING=1; }
done
[[ $MISSING -eq 0 ]] || die "Staged source tree is incomplete; check the tar excludes"
log_info "Staged source tree passed completeness check"

log_step "Materializing debian/"
mkdir -p "$STAGE/debian"
# Copy every template except the .in files, which are substituted below.
for f in "$DEB_TEMPLATE_DIR"/*; do
    base="$(basename "$f")"
    case "$base" in
        *.in) continue ;;
        source) cp -r "$f" "$STAGE/debian/" ;;
        *) cp "$f" "$STAGE/debian/$base" ;;
    esac
done

sed -e "s|@MYSQL_BASE_VERSION@|${MYSQL_BASE_VERSION}|g" \
    -e "s|@VSQL_CODE_BASE@|${VSQL_CODE_BASE}|g" \
    "$DEB_TEMPLATE_DIR/control.in" > "$STAGE/debian/control"
sed -e "s|@DEB_VERSION@|${DEB_VERSION}|g" \
    -e "s|@DEB_DATE@|${DEB_DATE}|g" \
    "$DEB_TEMPLATE_DIR/changelog.in" > "$STAGE/debian/changelog"

for f in "$STAGE/debian/control" "$STAGE/debian/changelog"; do
    if grep -q '@[A-Z_]*@' "$f"; then
        log_error "Unsubstituted placeholders in $(basename "$f"):"
        grep -n '@[A-Z_]*@' "$f" >&2
        die "Refusing to build with an incomplete debian/"
    fi
done

# debian/copyright is a DEP-5 template copied with the other debian/ files
# above; assert it landed rather than silently shipping without one.
[[ -f "$STAGE/debian/copyright" ]] || die "debian/copyright missing from $DEB_TEMPLATE_DIR"
chmod +x "$STAGE/debian/rules"
chmod +x "$STAGE"/debian/*.postinst "$STAGE"/debian/*.prerm "$STAGE"/debian/*.postrm 2>/dev/null || true
log_info "debian/ materialized in $STAGE"

if [[ $STAGE_ONLY -eq 1 ]]; then
    KEEP_WORK=1
    log_info "--stage-only: staged tree kept at $STAGE"
    exit 0
fi

# Default to a full build (-F): source package (.dsc + .tar.xz) AND binaries.
# The source package is what Launchpad PPAs, OBS and sbuild consume, and it is
# the only way to get builds for an architecture we do not build on directly.
if [[ $SOURCE_ONLY -eq 1 ]]; then
    DPKG_MODE="-S"
    log_step "Building source package only"
elif [[ $BINARY_ONLY -eq 1 ]]; then
    DPKG_MODE="-b"
    log_step "Building binary packages only"
else
    DPKG_MODE="-F"
    log_step "Building source and binary packages"
fi

cd "$STAGE"
dpkg-buildpackage -us -uc "$DPKG_MODE" -j"$(getconf _NPROCESSORS_ONLN)"

mkdir -p "$OUTPUT_DIR"
find "$WORK_DIR" -maxdepth 1 \
    \( -name '*.deb' -o -name '*.dsc' -o -name '*.tar.xz' -o -name '*.tar.gz' \
       -o -name '*.buildinfo' -o -name '*.changes' \) \
    -exec cp {} "$OUTPUT_DIR/" \;

log_step "Done"
log_info "Packages in $OUTPUT_DIR:"
ls -1 "$OUTPUT_DIR"/*.deb | while read -r f; do log_info "  $(basename "$f")"; done
