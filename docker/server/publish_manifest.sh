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

# Stitch the per-arch VillageSQL Server images already in the registry into
# one multi-arch manifest list, published under the primary tag plus tags
# for any version suffix (e.g. latest) that are provided.
#
# Use --dry-run to see what would be published.
# The arch images it references are verified before anything is written, which
# catches an arch that was never pushed.
#
# Run publish_image.sh --push once per platform first.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=docker_release_lib.sh
source "$SCRIPT_DIR/docker_release_lib.sh"

TAG=""
REPO="$DOCKER_REPO"
PLATFORMS="$DEFAULT_PLATFORMS"
VERSION_LABELS=""
DEFINE_TAGS=""

usage() {
    cat <<EOF
Publishes a manifest list to the registry.

Publish the multi-arch manifest for VillageSQL Server images.

Usage:
  publish_manifest.sh --tag TAG [options]

Options:
  -t, --tag TAG          Primary tag to publish.  For releases, use the full release
                         identifier with codebase and version (required)
  -r, --repo REPO        Image repository (default: $DOCKER_REPO)
  -p, --platforms LIST   Comma-separated platforms whose arch images make up
                         the manifest (default: $DEFAULT_PLATFORMS)
  -v, --version-labels LIST
                         Comma-separated version labels for version suffix tags
                         in addition to the primary tag. Leave empty for none.
  -d, --define-tags LIST
                         Comma-separated tags to publish as well, each used as
                         given rather than combined with the codebase. Optional.
  -n, --dry-run          Print the commands without running them. The arch
                         images are still checked, and a missing one is a
                         warning rather than an error.
  -h, --help             Show this help and exit
EOF
}

# Confirm every arch image the manifest will reference is in the registry.
# Read-only, so it runs in dry-run too, where a miss only warns.
verify_arch_images() {
    local image_tag missing=0
    echo "--- Verifying arch images ---"
    for image_tag in "${ARCH_TAGS[@]}"; do
        if docker buildx imagetools inspect "$image_tag" >/dev/null 2>&1; then
            echo "ok      $image_tag"
        else
            echo "MISSING $image_tag"
            missing=$((missing + 1))
        fi
    done
    echo ""

    if [ "$missing" -gt 0 ]; then
        if [ "$DRY_RUN" -eq 1 ]; then
            echo "warning: $missing arch image(s) not in the registry;" \
                 "publish_image.sh --push has not run for them yet" >&2
            echo ""
        else
            die "$missing arch image(s) not in the registry; run" \
                "publish_image.sh --push for them first"
        fi
    fi
}

publish_manifest() {
    local manifest_args=() version_label base_tag define_tag
    manifest_args+=(-t "$REPO:$TAG")
    # Guarded by the count: bash 3.2, and so macOS, treats "${arr[@]}" on an
    # empty array as an unbound variable under set -u.
    if [ "${#VERSION_LABELS_LIST[@]}" -gt 0 ]; then
        # Strip the version to leave the codebase, or use as is for tests
        base_tag=${TAG%_*}
        for version_label in "${VERSION_LABELS_LIST[@]}"; do
            manifest_args+=(-t "$REPO:${base_tag}_${version_label}")
        done
    fi
    if [ "${#DEFINE_TAGS_LIST[@]}" -gt 0 ]; then
        for define_tag in "${DEFINE_TAGS_LIST[@]}"; do
            manifest_args+=(-t "$REPO:${define_tag}")
        done
    fi

    echo "--- Publishing manifest tags ---"
    run docker buildx imagetools create "${manifest_args[@]}" "${ARCH_TAGS[@]}"
    echo ""

    echo "--- Inspecting $REPO:$TAG ---"
    run docker buildx imagetools inspect "$REPO:$TAG"
}

main() {
    while [ $# -gt 0 ]; do
        case "$1" in
            -t|--tag)          TAG="$2"; shift 2 ;;
            -r|--repo)      REPO="$2"; shift 2 ;;
            -p|--platforms) PLATFORMS="$2"; shift 2 ;;
            -v|--version-labels)      VERSION_LABELS="$2"; shift 2 ;;
            -d|--define-tags)      DEFINE_TAGS="$2"; shift 2 ;;
            -n|--dry-run)   DRY_RUN=1; shift ;;
            -h|--help)      usage; exit 0 ;;
            *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
        esac
    done

    [ -n "$TAG" ] || { echo "error: --tag is required" >&2; usage >&2; exit 2; }
    require_docker

    local platform
    ARCH_TAGS=()
    split_list "$PLATFORMS"
    [ "${#SPLIT_RESULT[@]}" -gt 0 ] || die "--platforms is empty"
    for platform in "${SPLIT_RESULT[@]}"; do
        ARCH_TAGS+=("$(arch_tag "$REPO" "$TAG" "$platform")")
    done

    split_list "$VERSION_LABELS"
    VERSION_LABELS_LIST=("${SPLIT_RESULT[@]+"${SPLIT_RESULT[@]}"}")

    split_list "$DEFINE_TAGS"
    DEFINE_TAGS_LIST=("${SPLIT_RESULT[@]+"${SPLIT_RESULT[@]}"}")

    echo "Repository     : $REPO"
    echo "Arch images    : ${ARCH_TAGS[*]}"
    echo "Image tag      : $TAG"
    echo "Version labels : ${VERSION_LABELS_LIST[*]+${VERSION_LABELS_LIST[*]}}"
    echo "Defined tags   : ${DEFINE_TAGS_LIST[*]+${DEFINE_TAGS_LIST[*]}}"
    [ "$DRY_RUN" -eq 1 ] && echo "(dry run: commands will be printed, not executed)"
    echo ""

    verify_arch_images
    publish_manifest
}

main "$@"
