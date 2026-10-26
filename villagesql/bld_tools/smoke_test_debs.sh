#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Install freshly built VillageSQL DEBs into a clean container and check that
# the server initializes, starts and answers a query.
#
# This catches what dpkg-buildpackage cannot: a Depends that does not resolve
# on the target distro, a missing shared library, or a server that will not
# start from the packaged layout.
#
# Environment:
#   IMAGE           Distro image to install into; use the one the DEBs were
#                   built in (default: ubuntu:24.04)
#   DOCKER_PLATFORM Docker --platform, e.g. linux/arm64 (default: the host's)
#   SOURCE_DIR      Source root (default: two levels up from this script)
#
# Usage:
#   villagesql/bld_tools/smoke_test_debs.sh <deb-dir>

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${SOURCE_DIR:-$(cd "$TOOLS_DIR/../.." && pwd)}"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

if [[ "${1:-}" == "--in-container" ]]; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    # The dbgsym and test packages are not needed to run the server.
    DEBS=()
    for f in /debs/*.deb; do
        case "$(basename "$f")" in
            *-dbgsym_*|villagesql-test_*) ;;
            *) DEBS+=("$f") ;;
        esac
    done
    apt-get install -y -qq --no-install-recommends "${DEBS[@]}"

    mysqld --version
    mysqld --initialize-insecure --user=mysql
    mysqld --user=mysql --daemonize --log-error=/var/log/mysql/error.log
    for _ in $(seq 1 30); do
        mysqladmin -uroot ping >/dev/null 2>&1 && break
        sleep 1
    done
    if ! mysql -uroot -e "SELECT VERSION()"; then
        cat /var/log/mysql/error.log
        exit 1
    fi
    mysqladmin -uroot shutdown
    exit 0
fi

[[ $# -eq 1 ]] || die "Usage: $0 <deb-dir>"
DEB_DIR="$(cd "$1" && pwd)"
ls "$DEB_DIR"/villagesql-server_*.deb >/dev/null 2>&1 || \
    die "No villagesql-server DEB in $DEB_DIR"

IMAGE="${IMAGE:-ubuntu:24.04}"
DOCKER_ARGS=(--rm -v "$SOURCE_DIR:/src:ro" -v "$DEB_DIR:/debs:ro")
if [[ -n "${DOCKER_PLATFORM:-}" ]]; then
    DOCKER_ARGS+=(--platform "$DOCKER_PLATFORM")
fi

log_step "Smoke testing DEBs in $IMAGE"
docker run "${DOCKER_ARGS[@]}" "$IMAGE" \
    /src/villagesql/bld_tools/smoke_test_debs.sh --in-container
