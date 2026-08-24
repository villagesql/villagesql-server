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

# Build a bundled VillageSQL extension during Docker image build.
#
# Usage:
#   build-bundled-extension.sh <extension-name>
#
# The extension name should be like "vsql-complex", "vsql-uuid", etc.
#
# For in-tree extensions (vsql-complex), the source is taken from
# /source/villagesql/examples/<name>. For external extensions, the repo is
# cloned from EXT_URL at EXT_BRANCH when build-bundled-extensions.sh supplies
# them from the manifest, and from github.com/villagesql/<name> otherwise.

set -eo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: build-bundled-extension.sh <extension-name>" >&2
    exit 1
fi

EXT_NAME="$1"
BUILD_DIR="/build"
VEB_INSTALL_DIR="/install/usr/lib/veb"
SDK_DIR=
for d in "${BUILD_DIR}"/villagesql-extension-sdk-*/; do
    if [ -z "$SDK_DIR" ]; then
        SDK_DIR="${d%/}"
    else
        echo "Error: multiple SDK directories found in ${BUILD_DIR}" >&2
        exit 1
    fi
done
if [ -z "$SDK_DIR" ] || [ ! -d "$SDK_DIR" ]; then
    echo "Error: no SDK directory found in ${BUILD_DIR}" >&2
    exit 1
fi
echo "==> Using SDK: ${SDK_DIR}"

# Determine source directory
IN_TREE_DIR="/source/villagesql/examples/${EXT_NAME}"
if [ -d "$IN_TREE_DIR" ]; then
    EXT_SRC="$IN_TREE_DIR"
    echo "==> Building in-tree extension: ${EXT_NAME}"
else
    EXT_SRC="/ext-src-${EXT_NAME}"
    CLONE_URL="${EXT_URL:-https://github.com/villagesql/${EXT_NAME}.git}"
    echo "==> Cloning extension: ${EXT_NAME} (${CLONE_URL}${EXT_BRANCH:+ @ ${EXT_BRANCH}})"
    if [ ! -f /etc/ssl/certs/ca-certificates.crt ]; then
        apt-get update -qq && apt-get install -y -qq ca-certificates >/dev/null
    fi
    git clone --depth=1 ${EXT_BRANCH:+--branch "$EXT_BRANCH"} \
        "$CLONE_URL" "$EXT_SRC"
fi

EXT_BUILD="/ext-build-${EXT_NAME}"
mkdir -p "$EXT_BUILD"
cd "$EXT_BUILD"

# In-tree extensions use find_package(VillageSQLExtensionFramework) directly
# and need CMAKE_PREFIX_PATH. External extensions use find_package(VillageSQL)
# with their bundled FindVillageSQL.cmake and need VillageSQL_BUILD_DIR.
CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
    -DVillageSQL_VEB_INSTALL_DIR="$VEB_INSTALL_DIR"
    -DVSQL_USE_DEV_ABI="${VSQL_DEV_ABI:-ON}"
)

if [ "$EXT_SRC" = "$IN_TREE_DIR" ]; then
    CMAKE_ARGS+=(-DCMAKE_PREFIX_PATH="$SDK_DIR")
else
    CMAKE_ARGS+=(-DVillageSQL_BUILD_DIR="$BUILD_DIR")
fi

echo "==> Configuring ${EXT_NAME}..."
cmake "$EXT_SRC" "${CMAKE_ARGS[@]}"

echo "==> Building ${EXT_NAME}..."
make -j"$(nproc)"

echo "==> Installing ${EXT_NAME}..."
make install

echo "==> Done: ${EXT_NAME}"
