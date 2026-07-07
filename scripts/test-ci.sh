#!/bin/bash

# VillageSQL CI Test Script

# Delegate to the reference implementation in the villagesql/bld_tools directory
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPTS_DIR/.." && pwd)"

"$SOURCE_DIR/villagesql/bld_tools/test_ci.sh" "$@"
