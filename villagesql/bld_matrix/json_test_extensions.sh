#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print the extension tests to run, as a compact JSON array.
#
# One row per (platform, extension, abi) triple: every platform tests every
# extension against every ABI that extension supports. Each row carries what a
# test job needs to check the extension out, build it, and run it.
#
# The rows are a bare array, not a GitHub Actions matrix. Wrap them in
# {"include": ...} at the call site if that is what the caller wants.
#
# Each object holds the platform's fields — platform, runner, os — plus:
#   url        — extension repository to clone
#   branch     — branch or tag to check out
#   build      — build tool, "cmake" or "cargo"
#   path       — subdirectory holding the extension, "" for the repo root
#   extension  — extension name
#   abi        — the single ABI this row tests
#
# Usage: json_test_extensions.sh [platform_filter] [extension_filter] [abi_filter]
#   Each filter keeps only the value of that name; omit or pass "" to keep all.
#   The platform and extension tables come from json_platforms.sh and
#   json_extensions.sh, which this script calls and then filters.
#
# Testable locally:
#   villagesql/bld_matrix/json_test_extensions.sh | jq .
#   villagesql/bld_matrix/json_test_extensions.sh macos-arm64 vsql-ai stable

set -euo pipefail

MATRIX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PLATFORM_FILTER="${1:-}"
EXTENSION_FILTER="${2:-}"
ABI_FILTER="${3:-}"

PLATFORMS=$("$MATRIX_DIR/json_platforms.sh" \
  | jq -c --arg f "$PLATFORM_FILTER" '[.[] | select($f == "" or .platform == $f)]')

# An empty side leaves nothing to cross-multiply, so stop before doing the
# work the other side would cost.
if [ "$(jq 'length' <<<"$PLATFORMS")" -eq 0 ]; then
    echo '[]'
    exit 0
fi

EXTENSIONS=$("$MATRIX_DIR/json_extensions.sh" \
  | jq -c --arg f "$EXTENSION_FILTER" '[.[] | select($f == "" or .extension == $f)]')

if [ "$(jq 'length' <<<"$EXTENSIONS")" -eq 0 ]; then
    echo '[]'
    exit 0
fi

jq -cn \
  --argjson platforms "$PLATFORMS" \
  --argjson extensions "$EXTENSIONS" \
  --arg abi_filter "$ABI_FILTER" \
  '[
    $platforms[] as $p |
    $extensions[] as $e |
    ($e.abis | if $abi_filter != "" then map(select(. == $abi_filter)) else . end)[] as $a |
    ($p + {url: $e.url, branch: $e.branch, extension: $e.extension,
           build: $e.build, path: $e.path} + {abi: $a})
  ]'
