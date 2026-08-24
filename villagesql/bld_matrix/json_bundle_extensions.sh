#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print the extensions that belong in one build channel, as a compact JSON
# array. This selects from json_extensions.sh; it is not itself a data table.
#
# Usage: json_bundle_extensions.sh [channel] [extension]
#
# [channel]:   Which build channel to select for. Default "release".
#                release — bundle=true only. What a release artifact ships.
#                dev     — bundle=true and bundle=dev. What a pre-release
#                          artifact ships: the -dev tarball, the dev image.
#                all     — every entry, bundle=false included. What the
#                          sanitizer and compat suites build, since they test
#                          extensions that no artifact ships.
#              For compatibility with the include_unbundled argument these
#              scripts used to take, "" and 0/no mean release, and 1/yes means
#              all.
# [extension]: Optional extension name to select just that one. Pass "" to
#              keep every extension the channel allows.
#
# A channel is a widening sequence: release ⊂ dev ⊂ all. Nothing selects
# bundle=dev without also selecting bundle=true, because an extension held
# back from release still depends on the ones that are not.
#
# Inputs (environment variables, all optional):
#   EXTENSIONS_FILE — passed through to json_extensions.sh
#
# Testable locally:
#   villagesql/bld_matrix/json_bundle_extensions.sh dev | jq .
#   villagesql/bld_matrix/json_bundle_extensions.sh all vsql-ai | jq .

set -euo pipefail

MATRIX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$MATRIX_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

CHANNEL="${1:-release}"
EXTENSION_FILTER="${2:-}"

case "$CHANNEL" in
    release|0|no|"") CHANNEL=release ;;
    dev)             CHANNEL=dev ;;
    all|1|yes)       CHANNEL=all ;;
    *) die "Invalid channel: $CHANNEL (expected release, dev, or all)" ;;
esac

"$MATRIX_DIR/json_extensions.sh" | jq -c \
    --arg channel "$CHANNEL" \
    --arg filter "$EXTENSION_FILTER" '
      [ { release: ["release"],
          dev:     ["release", "dev"],
          all:     ["release", "dev", "none"] }[$channel] ] as $keep
      | map(select((.bundle | IN($keep[][]))
                   and ($filter == "" or .extension == $filter)))'
