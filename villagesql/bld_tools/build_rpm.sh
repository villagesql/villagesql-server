#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Build VillageSQL RPM packages from a source tree.
#
# Generates the spec from packaging/villagesql-rpm/villagesql.spec.in, rolls a
# source tarball, and runs rpmbuild. Deliberately does NOT require a prior
# cmake configure: the spec substitutions are all derived from VSQL_VERSION and
# MYSQL_VERSION, so this runs on a bare source checkout inside a build
# container.
#
# Environment:
#   SOURCE_DIR   Source root (default: two levels up from this script)
#   OUTPUT_DIR   Where RPMs are copied (default: $PWD/rpms)
#   RPM_TOPDIR   rpmbuild tree (default: $HOME/rpmbuild)
#   VSQL_PRE_RELEASE_VERSION
#                Override the pre-release suffix; set empty for a release build
#
# Usage:
#   villagesql/bld_tools/build_rpm.sh [--spec-only] [--srpm]

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${SOURCE_DIR:-$(cd "$TOOLS_DIR/../.." && pwd)}"

# vsql_script_utils.sh provides log_*/die; vsql_parse_version() and friends
# live in build_info.sh, which expects the utilities to be sourced first.
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$SOURCE_DIR/villagesql/bld_tools/build_info.sh"

OUTPUT_DIR="${OUTPUT_DIR:-$PWD/rpms}"
RPM_TOPDIR="${RPM_TOPDIR:-$HOME/rpmbuild}"

SPEC_ONLY=0
SRPM_ONLY=0
BINARY_ONLY=0
USE_WORKTREE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --spec-only)   SPEC_ONLY=1; shift ;;
        --srpm)        SRPM_ONLY=1; shift ;;
        --binary-only) BINARY_ONLY=1; shift ;;
        --worktree)    USE_WORKTREE=1; shift ;;
        -h|--help)
            sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
            exit 0
            ;;
        *) die "Unknown option: $1" ;;
    esac
done

[[ -f "$SOURCE_DIR/CMakeLists.txt" ]] || die "Not a source tree: $SOURCE_DIR"

log_step "VillageSQL RPM Build"

vsql_parse_version "$SOURCE_DIR"

# RPM Version cannot contain '-'. '~' sorts BEFORE the plain version, which is
# what a pre-release needs (0.0.6~dev < 0.0.6). A '-dev' or '.dev' suffix would
# sort AFTER 0.0.6 and make the real release un-upgradable.
VSQL_RPM_VERSION="${VSQL_MAJOR}.${VSQL_MINOR}.${VSQL_PATCH}"
if [[ -n "$VSQL_PRE" ]]; then
    VSQL_RPM_VERSION="${VSQL_RPM_VERSION}~${VSQL_PRE}"
fi

mysql_version_value() {
    grep "^$1=" "$SOURCE_DIR/MYSQL_VERSION" | cut -d'=' -f2
}
MYSQL_MAJOR="$(mysql_version_value MYSQL_VERSION_MAJOR)"
MYSQL_MINOR="$(mysql_version_value MYSQL_VERSION_MINOR)"
MYSQL_PATCH="$(mysql_version_value MYSQL_VERSION_PATCH)"
MYSQL_BASE_VERSION="${MYSQL_MAJOR}.${MYSQL_MINOR}"
MYSQL_NO_DASH_VERSION="${MYSQL_MAJOR}.${MYSQL_MINOR}.${MYSQL_PATCH}"

VSQL_SRC_DIR="villagesql-server-${VSQL_RPM_VERSION}"

log_info "VillageSQL version:  $VSQL_VERSION"
log_info "RPM version:         $VSQL_RPM_VERSION"
log_info "MySQL base:          $MYSQL_NO_DASH_VERSION"
log_info "Source directory:    $SOURCE_DIR"
log_info "Tarball root:        $VSQL_SRC_DIR"

SPEC_IN="$SOURCE_DIR/packaging/villagesql-rpm/villagesql.spec.in"
[[ -f "$SPEC_IN" ]] || die "Spec template not found: $SPEC_IN"

RPM_TOPDIR="$(realpath -m "$RPM_TOPDIR")"

# Same reasoning as build_deb.sh: the source tarball is written under
# RPM_TOPDIR while the staging copy reads SOURCE_DIR, so an RPM_TOPDIR inside
# the source tree would fold partially written artifacts back into the tarball.
case "$RPM_TOPDIR/" in
    "$(realpath -m "$SOURCE_DIR")"/*)
        die "RPM_TOPDIR must be outside SOURCE_DIR (got $RPM_TOPDIR)" ;;
esac

mkdir -p "$RPM_TOPDIR"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
SPEC_OUT="$RPM_TOPDIR/SPECS/villagesql.spec"

log_step "Generating spec"
sed -e "s|@VSQL_RPM_VERSION@|${VSQL_RPM_VERSION}|g" \
    -e "s|@VSQL_SRC_DIR@|${VSQL_SRC_DIR}|g" \
    -e "s|@VSQL_CODE_BASE@|${VSQL_CODE_BASE}|g" \
    -e "s|@MYSQL_BASE_VERSION@|${MYSQL_BASE_VERSION}|g" \
    -e "s|@MYSQL_NO_DASH_VERSION@|${MYSQL_NO_DASH_VERSION}|g" \
    "$SPEC_IN" > "$SPEC_OUT"

if grep -q '@[A-Z_]*@' "$SPEC_OUT"; then
    log_error "Unsubstituted placeholders remain in the spec:"
    grep -n '@[A-Z_]*@' "$SPEC_OUT" >&2
    die "Refusing to build with an incomplete spec"
fi

# rpm expands macros inside '#' comments. A stray %cmake or %files in a comment
# silently executes -- %cmake in particular runs a whole second cmake with
# distro defaults, so the spec's own cmake invocation becomes dead code and
# every -D flag it sets is quietly discarded. Comments must escape '%' as '%%'.
if grep -nE '^[[:space:]]*#.*[^%]%[a-zA-Z{]' "$SPEC_OUT" >/dev/null; then
    log_error "Unescaped RPM macro(s) in spec comments -- write '%%' not '%':"
    grep -nE '^[[:space:]]*#.*[^%]%[a-zA-Z{]' "$SPEC_OUT" >&2
    die "Refusing to build: a macro in a comment will execute"
fi
log_info "Spec passed macro-in-comment lint"
log_info "Spec written to $SPEC_OUT"

cp "$SOURCE_DIR/packaging/villagesql-rpm/villagesql.logrotate" \
   "$RPM_TOPDIR/SOURCES/villagesql.logrotate"

if [[ $SPEC_ONLY -eq 1 ]]; then
    log_info "--spec-only: stopping after spec generation"
    exit 0
fi

log_step "Creating source tarball"
TARBALL="$RPM_TOPDIR/SOURCES/${VSQL_SRC_DIR}.tar.gz"
STAGING="$(mktemp -d)"
trap 'rm -rf "$STAGING"' EXIT

# Default to git archive HEAD so a package is reproducible from a commit.
# --worktree packages what is on disk instead, for iterating on packaging
# changes before they are committed.
GIT="git -c safe.directory=$SOURCE_DIR -C $SOURCE_DIR"
if [[ $USE_WORKTREE -eq 0 ]] && $GIT rev-parse --git-dir >/dev/null 2>&1; then
    if ! $GIT diff-index --quiet HEAD -- 2>/dev/null || \
       [[ -n "$($GIT ls-files --others --exclude-standard 2>/dev/null | head -1)" ]]; then
        log_warn "Working tree has uncommitted changes; packaging HEAD anyway."
        log_warn "Use --worktree to package what is on disk instead."
    fi
    log_info "Exporting via git archive (HEAD)"
    $GIT archive --format=tar --prefix="${VSQL_SRC_DIR}/" HEAD \
        | (cd "$STAGING" && tar xf -)
else
    if [[ $USE_WORKTREE -eq 1 ]]; then
        log_info "Copying working tree (--worktree)"
    else
        log_info "Copying source tree (not a git checkout)"
    fi
    mkdir -p "$STAGING/$VSQL_SRC_DIR"
    # Excludes MUST be anchored with './'. tar matches --exclude globs against
    # every path component, so a bare 'build*' also drops
    # cmake/build_configurations/, which the build needs -- and the resulting
    # tarball fails deep inside cmake with a confusing missing-include error.
    tar -C "$SOURCE_DIR" \
        --exclude='./.git' \
        --exclude='./build' \
        --exclude='./build-*' \
        --exclude='./rpms' \
        --exclude='*.rpm' \
        -cf - . | tar -C "$STAGING/$VSQL_SRC_DIR" -xf -
fi

# Fail fast on a tarball that is missing build inputs. Without this, an
# over-broad exclude surfaces as an unrelated-looking cmake error several
# minutes into %build.
REQUIRED_PATHS=(
    "CMakeLists.txt"
    "cmake/build_configurations/compiler_options.cmake"
    "cmake/build_configurations/mysql_release.cmake"
    "cmake/install_layout.cmake"
    "extra/boost/boost_1_84_0/boost/version.hpp"
    "sql/mysqld.cc"
    "villagesql/CMakeLists.txt"
    "packaging/villagesql-rpm/villagesql.spec.in"
    "VSQL_VERSION"
    "MYSQL_VERSION"
)
MISSING=0
for p in "${REQUIRED_PATHS[@]}"; do
    if [[ ! -e "$STAGING/$VSQL_SRC_DIR/$p" ]]; then
        log_error "Missing from staged source: $p"
        MISSING=1
    fi
done
[[ $MISSING -eq 0 ]] || die "Staged source tree is incomplete; check the tar excludes"
log_info "Staged source tree passed completeness check"

tar -C "$STAGING" -czf "$TARBALL" "$VSQL_SRC_DIR"
log_info "Tarball: $TARBALL ($(du -h "$TARBALL" | cut -f1))"

if [[ $SRPM_ONLY -eq 1 ]]; then
    log_step "Building SRPM"
    rpmbuild -bs --define "_topdir $RPM_TOPDIR" "$SPEC_OUT"
    exit 0
fi

# Default to -ba (source + binary). The SRPM is what COPR, OBS, Koji and mock
# consume, and it is the only artifact that lets someone rebuild these packages
# for an architecture we do not build on. --binary-only skips it.
if [[ $BINARY_ONLY -eq 1 ]]; then
    RPMBUILD_MODE="-bb"
    log_step "Building binary RPMs (no SRPM)"
else
    RPMBUILD_MODE="-ba"
    log_step "Building source and binary RPMs"
fi

NPROC="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
rpmbuild "$RPMBUILD_MODE" \
    --define "_topdir $RPM_TOPDIR" \
    --define "_smp_mflags -j${NPROC}" \
    "$SPEC_OUT"

mkdir -p "$OUTPUT_DIR"
find "$RPM_TOPDIR/RPMS" -name '*.rpm' -exec cp {} "$OUTPUT_DIR/" \;
if [[ $BINARY_ONLY -eq 0 ]]; then
    find "$RPM_TOPDIR/SRPMS" -name '*.src.rpm' -exec cp {} "$OUTPUT_DIR/" \;
fi

log_step "Done"
log_info "Packages in $OUTPUT_DIR:"
ls -1 "$OUTPUT_DIR"/*.rpm | while read -r f; do log_info "  $(basename "$f")"; done
