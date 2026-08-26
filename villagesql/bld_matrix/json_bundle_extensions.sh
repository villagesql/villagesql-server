#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print the extensions that belong in one build channel, as a compact JSON
# array. This selects from json_extensions.sh; it is not itself a data table.
#
# Usage: json_bundle_extensions.sh [channel] [extension]
#
# [channel]:   Which build channel to select for. Default "release".
#                release — bundle=all only. What a release artifact ships.
#                dev     — bundle=all and bundle=dev. What a pre-release
#                          artifact ships: the -dev tarball, the dev image.
#                test    — every entry, bundle=none included. What the
#                          sanitizer and compat suites build, since they test
#                          extensions that no artifact ships.
#              A channel names who the build is for, which is why the widest
#              one is "test" and not "all": bundle=all already means an
#              extension ships in every artifact, and reusing the word for a
#              set that also holds the extensions shipping nowhere would read
#              as its opposite. For compatibility with the include_unbundled
#              argument these scripts used to take, "" and 0/no mean release,
#              and 1/yes/all mean test.
# [extension]: Optional extension name to select just that one. Pass "" to
#              keep every extension the channel allows.
#
# A channel is a widening sequence: release ⊂ dev ⊂ test. Nothing selects
# bundle=dev without also selecting bundle=all, because an extension held
# back from release still depends on the ones that are not.
#
# Inputs (environment variables, all optional):
#   EXTENSIONS_FILE — passed through to json_extensions.sh
#
# Testable locally:
#   villagesql/bld_matrix/json_bundle_extensions.sh dev | jq .
#   villagesql/bld_matrix/json_bundle_extensions.sh test vsql-ai | jq .

set -euo pipefail

MATRIX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$MATRIX_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

CHANNEL="${1:-release}"
EXTENSION_FILTER="${2:-}"

case "$CHANNEL" in
    release|0|no|"")  CHANNEL=release ;;
    dev)              CHANNEL=dev ;;
    test|all|1|yes)   CHANNEL=test ;;
    *) die "Invalid channel: $CHANNEL (expected release, dev, or test)" ;;
esac

"$MATRIX_DIR/json_extensions.sh" | jq -c \
    --arg channel "$CHANNEL" \
    --arg filter "$EXTENSION_FILTER" '
      [ { release: ["release"],
          dev:     ["release", "dev"],
          test:    ["release", "dev", "none"] }[$channel] ] as $keep
      | map(select((.bundle | IN($keep[][]))
                   and ($filter == "" or .extension == $filter)))'
