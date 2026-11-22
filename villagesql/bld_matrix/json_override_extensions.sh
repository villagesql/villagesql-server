#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Apply extension_ref_overrides.txt to extension test rows for one server ref.
#
# Reads a JSON array of rows, as json_test_extensions.sh prints them, on stdin
# and prints the same array with the overrides for `server_ref` applied: a
# branch-or-tag override replaces the row's branch, and a skip override drops
# the row. Rows for extensions with no override pass through unchanged.
#
# Usage: json_override_extensions.sh <server_ref>
#
# Inputs (environment variables, all optional):
#   OVERRIDES_FILE — path to extension_ref_overrides.txt
#                    (default: the copy in this script's own source tree)
#
# Testable locally:
#   villagesql/bld_matrix/json_test_extensions.sh linux-x86_64 \
#     | villagesql/bld_matrix/json_override_extensions.sh release/0.0.7 | jq .

set -euo pipefail

MATRIX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$MATRIX_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

SERVER_REF="${1:-}"
[[ -n "$SERVER_REF" ]] || die "Usage: $(basename "$0") <server_ref>"

OVERRIDES_FILE="${OVERRIDES_FILE:-$SOURCE_DIR/villagesql/dev_server/extension_ref_overrides.txt}"

if [[ ! -f "$OVERRIDES_FILE" ]]; then
    die "Overrides list not found: $OVERRIDES_FILE"
fi

# Map of extension name to its override for SERVER_REF.
OVERRIDES=$(jq -Rcn --arg ref "$SERVER_REF" '
    [
      inputs
      | gsub("^\\s+|\\s+$"; "")
      | select(length > 0 and (startswith("#") | not))
      | [splits("\\s+")] as $f
      | if ($f | length) != 3 then
          error("override needs <server-ref> <extension> <branch-or-tag|skip>: \(.)")
        else . end
      | select($f[0] == $ref)
      | {key: $f[1], value: $f[2]}
    ] | from_entries' "$OVERRIDES_FILE")

jq -c --argjson overrides "$OVERRIDES" '
    [
      .[]
      | ($overrides[.extension] // null) as $o
      | if $o == null then .
        elif $o == "skip" then empty
        else .branch = $o end
    ]'
