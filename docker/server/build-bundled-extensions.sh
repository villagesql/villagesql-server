#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Build every bundled VillageSQL extension during a Docker image build.
#
# Usage: build-bundled-extensions.sh [channel]
#
# [channel]: Build channel to build for: release (default) or dev. "dev" adds
#            the bundle=dev extensions, which ship only in pre-release images.
#            Taken from BUNDLE_CHANNEL when the argument is omitted.
#
# The extensions themselves are still defined by the manifest,
# villagesql/dev_server/bundled_extensions.txt. This script no longer parses it
# directly: villagesql/bld_matrix/json_bundle_extensions.sh does, so the image
# and CI agree on what a manifest entry means. That matters for bundle=dev,
# which the parser this replaced would have shipped in release images.
#
# Inputs (environment variables, all optional):
#   SOURCE_DIR      — the source tree holding the scripts and the manifest
#                     (default: /source, where the Dockerfile copies it)
#   EXTENSIONS_FILE — a manifest somewhere other than the one in SOURCE_DIR.
#                     Read by json_extensions.sh; passed straight through.
#
# jq comes from setup_linux_build_env.sh, run earlier in the builder stage.
#
# Each extension is handed to build-bundled-extension.sh, which does the clone
# and the cmake build. The url and branch travel with it so a manifest entry
# pointing at a fork or a pinned tag is honoured here, not just in CI.

set -euo pipefail

SOURCE_DIR="${SOURCE_DIR:-/source}"
BUNDLE_CHANNEL="${1:-${BUNDLE_CHANNEL:-release}}"

SELECT="$SOURCE_DIR/villagesql/bld_matrix/json_bundle_extensions.sh"
[[ -x "$SELECT" ]] || { echo "error: not found: $SELECT" >&2; exit 1; }

echo "==> Bundled extensions: $BUNDLE_CHANNEL channel"

# Collected up front, rather than piped into the loop, so a manifest error
# stops the script before anything is built.
ENTRIES="$("$SELECT" "$BUNDLE_CHANNEL" \
    | jq -r '.[] | [.extension, .url, .branch, .build] | @tsv')"

while IFS=$'\t' read -r EXTENSION URL BRANCH BUILD_TOOL; do
    [[ -z "$EXTENSION" ]] && continue

    # This image builds with cmake. Skip entries that use another build tool
    # such as cargo, matching villagesql/bld_tools/build_bundled_extensions.sh.
    # TODO(villagesql-rust): support cargo extensions in the image too.
    if [[ "$BUILD_TOOL" != "cmake" ]]; then
        echo "Skipping $EXTENSION (build=$BUILD_TOOL not supported here)"
        continue
    fi

    EXT_URL="$URL" EXT_BRANCH="$BRANCH" build-bundled-extension.sh "$EXTENSION"
done <<< "$ENTRIES"
