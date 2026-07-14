#!/usr/bin/env bash
# Copyright (c) 2026 VillageSQL Contributors
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <https://www.gnu.org/licenses/>.

# Build and publish multi-arch VillageSQL Server images to a registry
# (Docker Hub by default).
#
# Builds each architecture serially under its own arch-specific tag
# (e.g. villagesql/server:0.0.5-arm64), then stitches the shared tags
# (the version, plus "latest" and "stable") into a single multi-arch
# manifest list that references both arch images.
#
# Because the image compiles the server from source, building an arch
# that does not match the host runs under QEMU emulation and is slow.
# Prefer running the matching arch on native hardware.
#
# Usage:
#   release_docker.sh [options]
#
# Options:
#   -v, --version VER    Version tag to publish (required)
#   -r, --repo REPO      Image repository (default: villagesql/server)
#   -p, --platforms LIST Comma-separated platforms to build
#                        (default: linux/amd64,linux/arm64)
#   -t, --tags LIST      Comma-separated shared tags applied to the
#                        multi-arch manifest, in addition to the version
#                        (default: latest,stable)
#       --image-only     Build and push arch images only; skip the manifest
#       --manifest-only  Skip builds; only (re)create the shared manifest
#                        from arch images already in the registry
#   -n, --dry-run        Print the commands without running them
#   -h, --help           Show this help and exit
#
# Examples:
#   # Full release of 0.0.5 for both arches, tagged latest + stable
#   release_docker.sh --version 0.0.5
#
#   # Build just the native arch now; add the other and the manifest later
#   release_docker.sh --version 0.0.5 --platforms linux/arm64 --image-only
#   release_docker.sh --version 0.0.5 --platforms linux/amd64 --image-only
#   release_docker.sh --version 0.0.5 --manifest-only

set -euo pipefail

VERSION=""
REPO="villagesql/server"
PLATFORMS="linux/amd64,linux/arm64"
SHARED_TAGS="latest,stable"
BUILD=1
MANIFEST=1
DRY_RUN=0

# Build args forwarded to docker build. Override via the environment, e.g.
#   VSQL_PRE_RELEASE_VERSION="" VSQL_DEV_ABI=OFF release_docker.sh ...
# Default to a release build (empty pre-release suffix).
VSQL_PRE_RELEASE_VERSION="${VSQL_PRE_RELEASE_VERSION-}"
VSQL_DEV_ABI="${VSQL_DEV_ABI:-ON}"

# Resolve paths relative to this script so it can be run from anywhere.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DOCKERFILE="$SCRIPT_DIR/Dockerfile"

usage() {
    # Print the leading documentation comment block: the run of comment
    # lines that follows the license header, stopping at the first line of
    # actual code. Robust to edits above/below so it never leaks code.
    awk '
        NR == 1 && /^#!/ { next }
        !past_license { if ($0 == "") past_license = 1; next }
        /^#/ { sub(/^# ?/, ""); print; next }
        { exit }
    ' "${BASH_SOURCE[0]}"
}

# Print a command, and run it unless in dry-run mode.
run() {
    echo "==> $*"
    if [ "$DRY_RUN" -eq 0 ]; then
        "$@"
    fi
}

# Map a docker platform (linux/amd64) to the arch suffix we tag with (amd64).
arch_suffix() {
    echo "${1##*/}"
}

# Phase 1: build and push a single architecture under its arch-specific tag
# (e.g. villagesql/server:0.0.5-arm64). Takes one docker platform argument.
build_arch_image() {
    local platform="$1"
    local arch_tag="$REPO:$VERSION-$(arch_suffix "$platform")"
    echo "--- Building $platform -> $arch_tag ---"
    run docker buildx build \
        --platform "$platform" \
        -f "$DOCKERFILE" \
        --build-arg "VSQL_PRE_RELEASE_VERSION=$VSQL_PRE_RELEASE_VERSION" \
        --build-arg "VSQL_DEV_ABI=$VSQL_DEV_ABI" \
        -t "$arch_tag" \
        --push \
        "$REPO_ROOT"
    echo ""
}

# Phase 2: stitch the arch images into one multi-arch manifest list, tagged
# with the version plus every shared tag, then verify the result.
create_shared_manifest() {
    echo "--- Creating shared manifest tags ---"
    local manifest_args=()
    manifest_args+=(-t "$REPO:$VERSION")
    local tag
    IFS=',' read -ra TAG_LIST <<< "$SHARED_TAGS"
    for tag in "${TAG_LIST[@]}"; do
        [ -n "$tag" ] && manifest_args+=(-t "$REPO:$tag")
    done

    run docker buildx imagetools create "${manifest_args[@]}" "${ARCH_TAGS[@]}"
    echo ""

    echo "--- Verifying $REPO:$VERSION ---"
    run docker buildx imagetools inspect "$REPO:$VERSION"
}

main() {
    while [ $# -gt 0 ]; do
        case "$1" in
            -v|--version)   VERSION="$2"; shift 2 ;;
            -r|--repo)      REPO="$2"; shift 2 ;;
            -p|--platforms) PLATFORMS="$2"; shift 2 ;;
            -t|--tags)      SHARED_TAGS="$2"; shift 2 ;;
            --image-only)   MANIFEST=0; shift ;;
            --manifest-only) BUILD=0; shift ;;
            -n|--dry-run)   DRY_RUN=1; shift ;;
            -h|--help)      usage; exit 0 ;;
            *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
        esac
    done

    if [ -z "$VERSION" ]; then
        echo "error: --version is required" >&2
        usage >&2
        exit 2
    fi

    if ! command -v docker >/dev/null 2>&1; then
        echo "error: docker not found on PATH" >&2
        exit 1
    fi

    echo "Repository : $REPO"
    echo "Version    : $VERSION"
    echo "Platforms  : $PLATFORMS"
    echo "Shared tags: $VERSION $(echo "$SHARED_TAGS" | tr ',' ' ')"
    echo "Build args : VSQL_PRE_RELEASE_VERSION='$VSQL_PRE_RELEASE_VERSION' VSQL_DEV_ABI=$VSQL_DEV_ABI"
    [ "$DRY_RUN" -eq 1 ] && echo "(dry run: commands will be printed, not executed)"
    echo ""

    # Collect the arch-specific tags so the manifest step can reference them,
    # whether or not we built them in this invocation.
    ARCH_TAGS=()
    IFS=',' read -ra PLATFORM_LIST <<< "$PLATFORMS"
    local platform
    for platform in "${PLATFORM_LIST[@]}"; do
        ARCH_TAGS+=("$REPO:$VERSION-$(arch_suffix "$platform")")
    done

    if [ "$BUILD" -eq 1 ]; then
        for platform in "${PLATFORM_LIST[@]}"; do
            build_arch_image "$platform"
        done
    fi

    if [ "$MANIFEST" -eq 1 ]; then
        create_shared_manifest
    fi

    echo ""
    echo "=== Done ==="
}

main "$@"
