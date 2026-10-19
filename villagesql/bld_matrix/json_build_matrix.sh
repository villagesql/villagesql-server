#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print the build-server matrix as a compact JSON object.
#
# The server binary is ABI-agnostic and carries no extension, so the matrix is
# one row per platform: {"include": [<platform>, ...]}. The rows are the
# platform objects themselves, unchanged.
#
# The {"include": [...]} shape is a GitHub Actions strategy matrix, so a job
# consumes this output directly:
#
#   strategy:
#     matrix: ${{ fromJson(needs.prepare.outputs.build-matrix) }}
#
# Each row's keys become matrix.<key> in that job — matrix.platform,
# matrix.runner, matrix.os.
#
# Usage: json_build_matrix.sh [platform_filter]
#   The filter keeps only the platform of that name; omit or pass "" to keep
#   all. The platform table comes from json_platforms.sh, which this script
#   calls and then filters.
#
# Testable locally:
#   villagesql/bld_matrix/json_build_matrix.sh | jq .
#   villagesql/bld_matrix/json_build_matrix.sh macos-arm64

set -euo pipefail

MATRIX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PLATFORM_FILTER="${1:-}"

"$MATRIX_DIR/json_platforms.sh" \
  | jq -c --arg f "$PLATFORM_FILTER" \
      '{include: [.[] | select($f == "" or .platform == $f)]}'
