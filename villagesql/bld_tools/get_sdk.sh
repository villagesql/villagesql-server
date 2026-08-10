#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Identify the path for the SDK

set -euo pipefail

BUILD_DIR="${1:?Usage: $0 <build_dir>}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"

# Because build_info expects die() to be available.
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$SOURCE_DIR/villagesql/bld_tools/build_info.sh"
vsql_parse_version "$SOURCE_DIR"

echo "$BUILD_DIR/villagesql-extension-sdk-${VSQL_VERSION}"
