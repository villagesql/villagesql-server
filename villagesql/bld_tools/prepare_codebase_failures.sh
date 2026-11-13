#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Build a vendor MySQL tag, run its full MTR suite, and write the tests that
# fail to mysql-test/collections/disabled-<tag>-fails.def in this source tree.
# With --include-unstable, tests that failed but passed on retry are listed
# too.
#
# With --use-result-file, skip the build and MTR run and write the file from
# an existing MTR XML result file.
#
# Regenerate the file each time the baseline MySQL codebase is updated.
# The vendor clone must be a separate checkout of Oracle MySQL, since
# codebase_build_and_test.sh checks TAG out in it.
#
# Usage: prepare_codebase_failures.sh <tag> <vendor-src-dir> [options]
#
# Other options are passed through to codebase_build_and_test.sh (e.g.
# --build-dir, --build-type, --jobs, --skip-build, --clean).

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"

source "$TOOLS_DIR/../scripts/vsql_script_utils.sh"

usage() {
    cat <<EOF
Usage: $(basename "$0") <tag> <vendor-src-dir> [options]

Build TAG in the vendor MySQL clone VENDOR-SRC-DIR, run the full MTR suite,
and write the failing tests to
  mysql-test/collections/disabled-<tag>-fails.def

Options:
  --include-unstable  Also list unstable tests (failed, then passed on retry).
  --use-result-file FILE
                      Skip the build and MTR run; write the file from FILE, an
                      existing MTR XML result file. VENDOR-SRC-DIR is then only
                      recorded in the header, and passthrough options other
                      than --build-type (also recorded there) are ignored.

Other options are passed through to codebase_build_and_test.sh; run it with
--help to list them.

Example:
  $(basename "$0") mysql-8.4.11 ~/Projects/mysql/mysql-8.4
EOF
}

# Print the --build-type value from the passthrough options, or debug, the
# codebase_build_and_test.sh default.
# Args: passthrough options...
find_build_type() {
    local build_type="debug"
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == "--build-type" && $# -gt 1 ]]; then
            build_type="$2"
            shift
        fi
        shift
    done
    echo "$build_type"
}

# List tests with status from result_file. An empty list is not an error;
# xmllint exits 10 when nothing matches.
# Args: status result_file
extract_tests() {
    local status="$1"
    local result_file="$2"
    local rc=0

    "$TOOLS_DIR/mtr_extract_tests.sh" "$status" "$result_file" 2>/dev/null || rc=$?
    if [[ $rc -ne 0 && $rc -ne 10 ]]; then
        die "mtr_extract_tests.sh $status failed (exit $rc) on $result_file"
    fi
}

# Print the .def header comment.
# Args: tag def_name build_type vendor_src include_unstable
write_header() {
    local tag="$1"
    local def_name="$2"
    local build_type="$3"
    local vendor_src="$4"
    local include_unstable="$5"
    local version="${tag#mysql-}"
    # Keep the committed file free of this user's home path.
    local tilde="~"
    vendor_src="${vendor_src/#$HOME/$tilde}"
    local unstable_desc=""
    if [[ $include_unstable -eq 1 ]]; then
        unstable_desc=" or are unstable"
    fi

    cat <<EOF
##############################################################################
#
# Test cases that fail$unstable_desc running vanilla MySQL $version.
#
# A new instance of this disabled-mysql-Maj.Min.Pat-fails.def should be created
# each time the baseline code for the codebase is updated.
#
# This $def_name was generated $(date '+%d-%b-%Y at %l:%M%p %Z').
# This disabled test file is based on a $build_type build of the $tag tag.
#
# This file should be generated from a new vendor clone, using the
# villagesql/bld_tools/prepare_codebase_failures.sh script
#
# villagesql/bld_tools/prepare_codebase_failures.sh $tag $vendor_src
#
##############################################################################
EOF
}

# Print one disabled-test line per test name read from stdin.
# Args: reason
format_entries() {
    local reason="$1"
    awk -v reason="$reason" 'NF { print $1 " : BUG#0 " reason }'
}

# Build tag in vendor_src and run the full MTR suite, passing extra options
# through to codebase_build_and_test.sh. Build output goes to stderr; the
# path of the MTR XML result file is printed on stdout.
# Args: tag vendor_src [codebase_build_and_test.sh options...]
build_and_test() {
    local tag="$1"
    local vendor_src="$2"
    shift 2

    local run_log="$(mktemp -t prepare_codebase_failures.XXXXXX)"

    # MTR exits non-zero when tests fail, which is expected here. A missing
    # result file is the real failure signal.
    local rc=0
    "$TOOLS_DIR/codebase_build_and_test.sh" \
        --tag "$tag" --src-dir "$vendor_src" "$@" \
        2>&1 | tee "$run_log" >&2 || rc=${PIPESTATUS[0]}

    local result_file="$(sed -n 's/^result-file=//p' "$run_log" | tail -1)"
    rm -f "$run_log"
    if [[ -z "$result_file" || ! -r "$result_file" ]]; then
        die "No MTR result file from codebase_build_and_test.sh (exit $rc)"
    fi
    echo "$result_file"
}

# Write the disabled-<tag>-fails.def file from result_file.
# Args: tag vendor_src build_type include_unstable result_file
write_disabled_file() {
    local tag="$1"
    local vendor_src="$2"
    local build_type="$3"
    local include_unstable="$4"
    local result_file="$5"

    local def_name="disabled-${tag}-fails.def"
    local def_file="$SOURCE_DIR/mysql-test/collections/$def_name"
    local version="${tag#mysql-}"

    log_info "MTR result file: $result_file"

    local unstable=""
    local failed="$(extract_tests fail "$result_file" | LC_ALL=C sort -u)"
    if [[ $include_unstable -eq 1 ]]; then
        unstable="$(extract_tests unstable "$result_file" | LC_ALL=C sort -u)"
    fi

    local tmp_def="$def_file.tmp"
    {
        write_header "$tag" "$def_name" "$build_type" "$vendor_src" \
            "$include_unstable"
        {
            printf '%s\n' "$failed" |
                format_entries "Known failure in MySQL $version"
            printf '%s\n' "$unstable" |
                format_entries "Known unstable test in MySQL $version"
        } | LC_ALL=C sort
    } > "$tmp_def"
    mv "$tmp_def" "$def_file"

    log_info "Wrote $def_file"
    log_info "  failed:   $(printf '%s' "$failed" | grep -c . || true)"
    if [[ $include_unstable -eq 1 ]]; then
        log_info "  unstable: $(printf '%s' "$unstable" | grep -c . || true)"
    fi
}

main() {
    if [[ $# -ge 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
        usage
        return 0
    fi
    if [[ $# -lt 2 ]]; then
        usage >&2
        return 2
    fi

    local tag="$1"
    local vendor_src="$2"
    shift 2

    local include_unstable=0
    local result_file=""
    local passthrough=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --include-unstable) include_unstable=1 ;;
            --use-result-file)
                result_file="${2:?--use-result-file requires a value}"
                shift
                ;;
            *) passthrough+=("$1") ;;
        esac
        shift
    done
    set -- ${passthrough[@]+"${passthrough[@]}"}

    command -v xmllint >/dev/null || die "Missing dependency: xmllint"

    local build_type="$(find_build_type "$@")"

    if [[ -n "$result_file" ]]; then
        [[ -r "$result_file" ]] || die "Cannot read result file: $result_file"
    else
        [[ -d "$vendor_src" ]] || die "No such vendor source directory: $vendor_src"
        result_file="$(build_and_test "$tag" "$vendor_src" "$@")"
    fi

    write_disabled_file "$tag" "$vendor_src" "$build_type" \
        "$include_unstable" "$result_file"
}

main "$@"
