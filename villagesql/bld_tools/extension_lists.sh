#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Registry mapping extension build tools to their list files, plus
# the resolver used by the bundled-extension bld_tools scripts
#
# Source this after vsql_script_utils.sh (it uses die()).
#
# To add a build tool later: create a new
# villagesql/dev_server/bundled_extensions_<x>.text file, add a case
# arm below, and list it under "all".

# Directory holding the list files, relative to this script.
_EXT_LISTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../dev_server" && pwd)"

# resolve_extension_lists selector, where selector is a runtime token
# (cpp, rust) or "all". An unknown selector is fatal.
# Echoes one absolute list-file path per line.
resolve_extension_lists() {
    local selector="${1:?resolve_extension_lists: selector required (cpp|rust|all)}"
    case "$selector" in
        cpp) echo "$_EXT_LISTS_DIR/bundled_extensions.txt" ;;
        rust) echo "$_EXT_LISTS_DIR/bundled_extensions_rust.txt" ;;
        all)
            resolve_extension_lists cpp
            resolve_extension_lists rust
            ;;
        *) die "Unknown EXTENSION_RUNTIME '$selector' (expected: cpp, rust, all)" ;;
    esac
}
