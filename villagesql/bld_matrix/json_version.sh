#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print the VSQL_VERSION file as a compact JSON object, one record.
#
# This is the parsed form of the file; it applies no filtering and computes no
# version strings. Callers assemble the strings they want from the fields.
#
# The record:
#   code_base    — named fork the version belongs to, e.g. "mysql-8.4"
#   major        — major version, a number
#   minor        — minor version, a number
#   patch        — patch version, a number
#   pre_release  — pre-release suffix, "" when the file names none
#
# A line is a KEY=value pair with the value ending at the first whitespace,
# matching how cmake/vsql_version.cmake reads the same file. Anything else,
# comments included, is ignored. code_base and the three numbers must all be
# present, and the numbers must parse as numbers; a file missing any of them
# is an error. pre_release is the only optional field.
#
# Inputs (environment variables, all optional):
#   VSQL_VERSION_FILE — path to VSQL_VERSION
#                       (default: the copy in this script's own source tree)
#
# Testable locally:
#   VSQL_VERSION_FILE=/tmp/version villagesql/bld_matrix/json_version.sh
#   villagesql/bld_matrix/json_version.sh | jq .

set -euo pipefail

MATRIX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$MATRIX_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

VSQL_VERSION_FILE="${VSQL_VERSION_FILE:-$SOURCE_DIR/VSQL_VERSION}"

if [[ ! -f "$VSQL_VERSION_FILE" ]]; then
    die "VSQL_VERSION file not found: $VSQL_VERSION_FILE"
fi

jq -Rcn '
    (reduce (inputs | capture("^\\s*(?<k>[A-Za-z_]+)=(?<v>\\S*)")) as $e
       ({}; .[$e.k] = $e.v)) as $file
    | def number($key):
        $file[$key] | tonumber? // error("VSQL_VERSION names no numeric \($key)");
      if ($file.VSQL_CODE_BASE // "") == "" then
        error("VSQL_VERSION names no VSQL_CODE_BASE")
      else . end
    | {
        code_base:   $file.VSQL_CODE_BASE,
        major:       number("VSQL_MAJOR_VERSION"),
        minor:       number("VSQL_MINOR_VERSION"),
        patch:       number("VSQL_PATCH_VERSION"),
        pre_release: ($file.VSQL_PRE_RELEASE_VERSION // "")
      }' "$VSQL_VERSION_FILE"
