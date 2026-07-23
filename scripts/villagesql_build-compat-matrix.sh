#!/usr/bin/env bash
# Builds the JSON matrices used by extension-compat-suite.yml.
#
# TODO(villagesql-beta): this and the other Actions-only scripts (e.g.
# should_run_all_tests.sh) should move into scripts/ci_helpers/ alongside the
# slash-command/extension-compat helpers; left here for now to keep this PR's
# diff focused. Update the workflow reference when moved.
#
# Outputs two JSON values to stdout (one per line):
#   1. build-matrix  — platforms for the build-server job (no abi dimension;
#                      the server binary is ABI-agnostic)
#   2. test-matrix   — full (platform, extension, abi) triples for test-extension
#
# Inputs (environment variables, all optional):
#   PLATFORM_FILTER   — limit to a single platform  (e.g. linux-x86_64)
#   EXTENSION_FILTER  — limit to a single extension (e.g. vsql-ai)
#   ABI_FILTER        — limit to a single ABI        (e.g. stable)
#   EXTENSIONS_FILE   — path to bundled_extensions.txt
#                       (default: villagesql/dev_server/bundled_extensions.txt)
#
# Usage (from repo root):
#   ./scripts/villagesql_build-compat-matrix.sh
#   PLATFORM_FILTER=macos-arm64 ABI_FILTER=stable ./scripts/villagesql_build-compat-matrix.sh
#   EXTENSION_FILTER=vsql-ai ./scripts/villagesql_build-compat-matrix.sh

set -euo pipefail

EXTENSIONS_FILE="${EXTENSIONS_FILE:-villagesql/dev_server/bundled_extensions.txt}"

# ---------------------------------------------------------------------------
# Parse extensions
# ---------------------------------------------------------------------------
EXTS_JSON=$(grep -v '^[[:space:]]*#\|^[[:space:]]*$' "$EXTENSIONS_FILE" \
  | jq -R '
      split(" ") |
      . as $f |
      {
        url:       ($f[0] | rtrimstr("/")),
        branch:    ($f[1] // ""),
        extension: ($f[0] | rtrimstr("/") | split("/") | last),
        abis:      (($f[2:] | map(select(startswith("abi=")) | ltrimstr("abi=")) | .[0]) as $a |
                   if $a then [$a] else ["stable","dev"] end)
      }' \
  | jq -sc .)

if [ -n "${EXTENSION_FILTER:-}" ]; then
  EXTS_JSON=$(echo "$EXTS_JSON" \
    | jq --arg f "$EXTENSION_FILTER" '[.[] | select(.extension == $f)]')
fi

# ---------------------------------------------------------------------------
# Platforms
# ---------------------------------------------------------------------------
ALL_PLATFORMS='[
  {"platform":"linux-x86_64","runner":["self-hosted","linux","x86_64"],"os":"linux"},
  {"platform":"linux-aarch64","runner":"ubuntu-24.04-arm","os":"linux"},
  {"platform":"macos-arm64","runner":["self-hosted","macOS","ARM64"],"os":"macos"}
]'

if [ -n "${PLATFORM_FILTER:-}" ]; then
  PLATFORMS=$(echo "$ALL_PLATFORMS" \
    | jq --arg f "$PLATFORM_FILTER" '[.[] | select(.platform == $f)]')
else
  PLATFORMS="$ALL_PLATFORMS"
fi

ABI_FILTER="${ABI_FILTER:-}"

# ---------------------------------------------------------------------------
# Emit empty sentinel when nothing matches
# ---------------------------------------------------------------------------
EXT_COUNT=$(echo "$EXTS_JSON" | jq 'length')
PLAT_COUNT=$(echo "$PLATFORMS" | jq 'length')

if [ "$EXT_COUNT" -eq 0 ] || [ "$PLAT_COUNT" -eq 0 ]; then
  echo '{"include":[]}'  # build-matrix
  echo '{"include":[]}'  # test-matrix
  exit 0
fi

# ---------------------------------------------------------------------------
# build-matrix: platforms only — the server is ABI-agnostic
# ---------------------------------------------------------------------------
BUILD_MATRIX=$(jq -cn \
  --argjson platforms "$PLATFORMS" \
  '{include: [$platforms[]]}')

# ---------------------------------------------------------------------------
# test-matrix: full (platform, extension, abi) triples
# ---------------------------------------------------------------------------
TEST_MATRIX=$(jq -cn \
  --argjson platforms "$PLATFORMS" \
  --argjson extensions "$EXTS_JSON" \
  --arg abi_filter "$ABI_FILTER" \
  '{include: [
    $platforms[] as $p |
    $extensions[] as $e |
    ($e.abis | if $abi_filter != "" then map(select(. == $abi_filter)) else . end)[] as $a |
    ($p + {url: $e.url, branch: $e.branch, extension: $e.extension} + {abi: $a})
  ]}')

echo "$BUILD_MATRIX"
echo "$TEST_MATRIX"
