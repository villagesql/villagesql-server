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

# Build one platform-specific VillageSQL Server image under its arch-specific
# tag (e.g. villagesql/server:mysql-8.4_0.0.6-arm64), and optionally publish
# it to a registry (Docker Hub by default).
#
# By default the image is built and loaded into the local docker image store,
# where test-image.sh can smoke test it before anything is published. Pass
# --push to publish instead.
#
# Because the image compiles the server from source, building an arch that
# does not match the host runs under QEMU emulation and is slow. Two to
# three hours on a 2026 MacBook Pro M5.  Prefer running the matching arch
# on native hardware.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=docker_release_lib.sh
source "$SCRIPT_DIR/docker_release_lib.sh"

REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DOCKERFILE="$SCRIPT_DIR/Dockerfile"

TAG=""
REPO="$DOCKER_REPO"
PLATFORM=""
PUSH=0

usage() {
    cat <<EOF
Build one platform-specific VillageSQL Server image, and optionally publish it.

Usage:
  publish_image.sh --tag TAG --platform PLATFORM [options]

Options:
  -t, --tag TAG          Tag to build.  For releases, use the full release
                         identifier with codebase and version (required)
  -p, --platform PLAT    Single docker platform, e.g. linux/arm64 (required)
  -r, --repo REPO        Image repository (default: $DOCKER_REPO)
      --push             Publish the image to the registry. Without this the
                         image is only built, and loaded locally for testing.
  -n, --dry-run          Print the commands without running them
  -h, --help             Show this help and exit

The image is tagged REPO:TAG-ARCH, where ARCH is derived from the
platform (linux/amd64 -> amd64). publish_manifest.sh stitches those
arch tags into the multi-arch tags.

Build args are taken from the environment:
  VSQL_PRE_RELEASE_VERSION  pre-release suffix (default: empty, a release)
  VSQL_DEV_ABI              expose the development ABI (default: ON)

Examples:
  # Build the native arch and smoke test it before publishing anything
  ./publish_image.sh --tag mysql-8.4_0.0.6 --platform linux/arm64
  ./test-image.sh villagesql/server:mysql-8.4_0.0.6-arm64

  # Build and publish one arch
  ./publish_image.sh --tag mysql-8.4_0.0.6 --platform linux/arm64 --push
EOF
}

main() {
    while [ $# -gt 0 ]; do
        case "$1" in
            -t|--tag)      TAG="$2"; shift 2 ;;
            -p|--platform) PLATFORM="$2"; shift 2 ;;
            -r|--repo)     REPO="$2"; shift 2 ;;
            --push)        PUSH=1; shift ;;
            -n|--dry-run)  DRY_RUN=1; shift ;;
            -h|--help)     usage; exit 0 ;;
            *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
        esac
    done

    [ -n "$TAG" ] || { echo "error: --tag is required" >&2; usage >&2; exit 2; }
    [ -n "$PLATFORM" ] || { echo "error: --platform is required" >&2; usage >&2; exit 2; }
    case "$PLATFORM" in
        *,*) echo "error: --platform takes a single platform; run once per arch" >&2; exit 2 ;;
    esac
    require_docker

    local image_tag output
    image_tag="$(arch_tag "$REPO" "$TAG" "$PLATFORM")"
    if [ "$PUSH" -eq 1 ]; then
        output="--push"
    else
        output="--load"
    fi

    echo "--- Building $PLATFORM -> $image_tag ($output) ---"
    echo "Build args : VSQL_PRE_RELEASE_VERSION='$VSQL_PRE_RELEASE_VERSION' VSQL_DEV_ABI=$VSQL_DEV_ABI"
    [ "$DRY_RUN" -eq 1 ] && echo "(dry run: commands will be printed, not executed)"

    run docker buildx build \
        --platform "$PLATFORM" \
        -f "$DOCKERFILE" \
        --build-arg "VSQL_PRE_RELEASE_VERSION=$VSQL_PRE_RELEASE_VERSION" \
        --build-arg "VSQL_DEV_ABI=$VSQL_DEV_ABI" \
        -t "$image_tag" \
        "$output" \
        "$REPO_ROOT"

    echo ""
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "Dry run: nothing was built or published."
    elif [ "$PUSH" -eq 1 ]; then
        echo "Published $image_tag"
    else
        echo "Built $image_tag (local only; smoke test with ./test-image.sh $image_tag)"
    fi
}

main "$@"
