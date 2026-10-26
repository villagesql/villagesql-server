#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Build VillageSQL Debian packages inside a Debian or Ubuntu container.
#
# build_deb.sh needs a Debian-family host: dpkg-buildpackage, the build
# dependencies from packaging/villagesql-deb/control.in, and GNU coreutils.
# This wrapper supplies all three from a stock distro image, so the packages
# can be built from any host with Docker (macOS included) and are built against
# the distro they will be installed on.
#
# The source tree is mounted read-only. Packages land in OUTPUT_DIR on the
# host. Every argument is passed through to build_deb.sh.
#
# Environment:
#   IMAGE           Distro image to build in (default: ubuntu:24.04)
#   DOCKER_PLATFORM Docker --platform, e.g. linux/arm64 (default: the host's)
#   SOURCE_DIR      Source root (default: two levels up from this script)
#   OUTPUT_DIR      Where packages are copied (default: $PWD/debs)
#   VSQL_PRE_RELEASE_VERSION, SOURCE_DATE_EPOCH
#                   Passed through to build_deb.sh when set
#
# Usage:
#   villagesql/bld_tools/build_deb_docker.sh [build_deb.sh options]
#   IMAGE=debian:12 villagesql/bld_tools/build_deb_docker.sh --binary-only

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${SOURCE_DIR:-$(cd "$TOOLS_DIR/../.." && pwd)}"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

# Inside the container: install the toolchain and build dependencies, then hand
# off to build_deb.sh. Not meant to be called directly.
if [[ "${1:-}" == "--in-container" ]]; then
    shift
    export DEBIAN_FRONTEND=noninteractive
    log_step "Installing build dependencies"
    apt-get update -qq
    apt-get install -y -qq --no-install-recommends \
        ca-certificates git dpkg-dev devscripts equivs >/dev/null
    # mk-build-deps reads Build-Depends straight from the template; the
    # placeholders left in it are only in package descriptions.
    DEPS_DIR="$(mktemp -d)"
    cp /src/packaging/villagesql-deb/control.in "$DEPS_DIR/control"
    (cd "$DEPS_DIR" && mk-build-deps --install --remove \
        --tool 'apt-get -y -qq --no-install-recommends' control)
    rm -rf "$DEPS_DIR"

    SOURCE_DIR=/src OUTPUT_DIR=/out /src/villagesql/bld_tools/build_deb.sh "$@"
    if [[ -n "${HOST_UID:-}" ]]; then
        chown -R "$HOST_UID:$HOST_GID" /out
    fi
    exit 0
fi

command -v docker >/dev/null || die "docker not found"

IMAGE="${IMAGE:-ubuntu:24.04}"
OUTPUT_DIR="${OUTPUT_DIR:-$PWD/debs}"
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

DOCKER_ARGS=(--rm
    -v "$SOURCE_DIR:/src:ro"
    -v "$OUTPUT_DIR:/out"
    -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)")
if [[ -n "${DOCKER_PLATFORM:-}" ]]; then
    DOCKER_ARGS+=(--platform "$DOCKER_PLATFORM")
fi
# Pass these only when set: build_deb.sh tells unset VSQL_PRE_RELEASE_VERSION
# (use the VSQL_VERSION file) apart from empty (a release build).
for v in VSQL_PRE_RELEASE_VERSION SOURCE_DATE_EPOCH; do
    if [[ "${!v+set}" == "set" ]]; then
        DOCKER_ARGS+=(-e "$v=${!v}")
    fi
done

log_step "Building DEBs in $IMAGE"
log_info "Source directory:    $SOURCE_DIR"
log_info "Output directory:    $OUTPUT_DIR"
docker run "${DOCKER_ARGS[@]}" "$IMAGE" \
    /src/villagesql/bld_tools/build_deb_docker.sh --in-container "$@"
