#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Advance the source tree to a new VillageSQL version and commit the result.
#
# Rewrites the three version numbers in VSQL_VERSION, updates the test result
# files that embed the version string, and commits just those files.
#
# The result files come from the header comment of VSQL_VERSION, so that list
# lives in one place.  Only the first group is used, the files listed under
# "When changing the version".  The second group tracks codebase changes and is
# not this script's concern.
#
# Usage:
#   villagesql/bld_tools/prepare_release_update.sh <major>.<minor>.<patch>

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$SOURCE_DIR/villagesql/bld_tools/build_info.sh"

NEW_VERSION="${1:?Usage: $0 <major>.<minor>.<patch>}"
IFS='.' read -r NEW_MAJOR NEW_MINOR NEW_PATCH <<<"$NEW_VERSION"

VERSION_FILE="$SOURCE_DIR/VSQL_VERSION"

# The version as it appears in a test result, e.g. "mysql-8.4_0.0.7": the code
# base, then the version with no pre-release suffix.  Any suffix in the file
# trails this and is left alone.  Matching on the code base as well keeps the
# replacement away from the extension versions that also appear in these files.
#
# The code base is the one field build_info.sh has no accessor for, as the
# version strings it computes leave it out.
VSQL_CODE_BASE="$("$SOURCE_DIR/villagesql/bld_matrix/json_version.sh" \
    | jq -r '.code_base')"
OLD_PATTERN="${VSQL_CODE_BASE}_$(vsql_json_version "$SOURCE_DIR" "")"

# Replace every occurrence of $1 with $2 in the file $3, and print the number of
# lines that changed.  A temp file and mv keeps this portable, as the -i flag of
# sed differs between the BSD and GNU versions.
replace_in_file() {
    local from="$1" to="$2" file="$3"
    local tmp changed
    tmp="$(mktemp)"
    sed "s|${from}|${to}|g" "$file" >"$tmp"
    changed="$(grep -c '^<' < <(diff "$file" "$tmp") || true)"
    mv "$tmp" "$file"
    echo "$changed"
}

# The result files named in the first group of the VSQL_VERSION header.  Collect
# the indented comment lines that follow that group's heading, stopping at the
# first line that is not one of them.
read_result_files() {
    awk '
        /^# When changing the version/ { collecting = 1; next }
        !collecting                    { next }
        /^#   [^ ]/                    { print $2; next }
                                       { exit }
    ' "$VERSION_FILE"
}

log_step "Advancing $SOURCE_DIR to $NEW_VERSION"

RESULT_FILES=()
while IFS= read -r line; do
    RESULT_FILES+=("$line")
done < <(read_result_files)
[[ ${#RESULT_FILES[@]} -gt 0 ]] \
    || die "VSQL_VERSION names no test result files to update"

replace_in_file "^VSQL_MAJOR_VERSION=.*" "VSQL_MAJOR_VERSION=$NEW_MAJOR" \
    "$VERSION_FILE" >/dev/null
replace_in_file "^VSQL_MINOR_VERSION=.*" "VSQL_MINOR_VERSION=$NEW_MINOR" \
    "$VERSION_FILE" >/dev/null
replace_in_file "^VSQL_PATCH_VERSION=.*" "VSQL_PATCH_VERSION=$NEW_PATCH" \
    "$VERSION_FILE" >/dev/null

# Read the new version back out of the rewritten file, rather than assembling it
# from the argument.  That confirms the rewrite took, and names the version the
# way the rest of the build names it.  It serves both the test results and the
# commit message.
COMMIT_VERSION="$(vsql_json_version "$SOURCE_DIR" "")"
NEW_PATTERN="${VSQL_CODE_BASE}_${COMMIT_VERSION}"
log_info "VSQL_VERSION: $OLD_PATTERN -> $NEW_PATTERN"

for result in "${RESULT_FILES[@]}"; do
    changed="$(replace_in_file "$OLD_PATTERN" "$NEW_PATTERN" \
        "$SOURCE_DIR/$result")"
    if [[ "$changed" -eq 0 ]]; then
        log_warn "$result: no $OLD_PATTERN to replace, is the list stale?"
    else
        log_info "$result: $changed line(s)"
    fi
done

# Name the files on the command line, so that unrelated work in the tree, staged
# or not, stays out of the commit.
git -C "$SOURCE_DIR" commit \
    -m "Advance the main branch to Release $COMMIT_VERSION" \
    -- VSQL_VERSION "${RESULT_FILES[@]}"
