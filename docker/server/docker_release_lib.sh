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

# Shared helpers for the VillageSQL Server Docker release scripts.
# Sourced, never executed. Defaults live here.

# The defaults below are intended forvi scripts that source this file,
# a use which shellcheck cannot see.
# shellcheck disable=SC2034
DOCKER_REPO="villagesql/server"
DOCKER_SHARED_TAGS="latest,stable"
DEFAULT_PLATFORMS="linux/amd64,linux/arm64"

# Where pre-release images go: Docker Hub carries the releases, and an
# unreleased build has no business on a public registry. This is the
# villagesql-server-dev repository declared in the vcloud-cell/dev-cluster
# Terraform (artifact-registry.tf), which also grants this GitHub repo push
# access.
#
# One whole string rather than parts assembled at run time. Publishing
# somewhere else means passing that path to --repo, which every script and
# workflow already accepts, so there is nothing here to reconfigure.
GAR_DEV_REPO="us-central1-docker.pkg.dev/villagesql-vcloud-pelican-dev/villagesql-server-dev/server"

# Build args forwarded to docker build. Override via the environment, e.g.
#   VSQL_PRE_RELEASE_VERSION="" VSQL_DEV_ABI=OFF publish_image.sh ...
# Default to a release build (empty pre-release suffix).
VSQL_PRE_RELEASE_VERSION="${VSQL_PRE_RELEASE_VERSION-}"
VSQL_DEV_ABI="${VSQL_DEV_ABI:-ON}"

DRY_RUN="${DRY_RUN:-0}"

die() {
    echo "error: $*" >&2
    exit 1
}

# Print a command, and run it unless in dry-run mode.
run() {
    echo "==> $*"
    if [ "$DRY_RUN" -eq 0 ]; then
        "$@"
    fi
}

require_docker() {
    command -v docker >/dev/null 2>&1 || die "docker not found on PATH"
}

# The registry host a repository points at, or empty for Docker Hub. This is
# docker's own rule for reading an image reference: the first path segment is a
# hostname only when it looks like one, meaning it carries a dot or a port.
# Deriving it means a repository is the single thing anyone has to supply — the
# login target follows from it, with no second flag to keep in agreement.
registry_host() {
    local first="${1%%/*}"
    case "$first" in
        *.*|*:*) echo "$first" ;;
        *)       echo "" ;;
    esac
}

# Map a docker platform to the arch suffix we tag with: linux/amd64 -> amd64,
# linux/arm/v7 -> arm-v7.
arch_suffix() {
    local arch="${1#*/}"
    echo "${arch//\//-}"
}

# The arch-specific tag for one platform. This naming is the contract between
# publish_image.sh (which creates these tags) and publish_manifest.sh (which
# stitches them together), so both must derive it here.
arch_tag() {
    local repo="$1" tag="$2" platform="$3"
    echo "$repo:$tag-$(arch_suffix "$platform")"
}

# Split a comma-separated list into the SPLIT_RESULT array, dropping empty
# entries. A fixed global rather than a nameref, which needs bash 4.3 and so
# would not run under the bash 3.2 that ships with macOS.
SPLIT_RESULT=()
split_list() {
    local raw="$1"
    local item
    SPLIT_RESULT=()
    # An empty list leaves _split_raw unset rather than empty, which "$@" on
    # it treats as an unbound variable under bash 3.2 and so macOS. Return
    # before read rather than relying on the bash 4.4 behaviour.
    [ -n "$raw" ] || return 0
    IFS=',' read -ra _split_raw <<< "$raw"
    for item in "${_split_raw[@]}"; do
        [ -n "$item" ] && SPLIT_RESULT+=("$item")
    done
}
