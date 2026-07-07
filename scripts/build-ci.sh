#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Configure and build the VillageSQL server for CI.

# Delegate to the reference implementation in the villagesql/bld_tools directory
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPTS_DIR/.." && pwd)"

"$SOURCE_DIR/villagesql/bld_tools/build_ci.sh" "$@"
