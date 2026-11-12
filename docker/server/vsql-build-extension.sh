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

# Build a VillageSQL extension from source and install the .veb file.
#
# Usage:
#   vsql-build-extension.sh /ext/my-extension
#
# The argument is the path to the extension source directory (containing
# CMakeLists.txt). The script runs cmake + make, then copies the resulting
# .veb file into the server's VEB directory (/usr/lib/veb/).

set -eo pipefail

VEB_INSTALL_DIR="/usr/lib/veb"

if [ $# -ne 1 ]; then
    echo "Usage: vsql-build-extension.sh <extension-source-dir>" >&2
    exit 1
fi

EXT_SRC="$1"

if [ ! -d "$EXT_SRC" ]; then
    echo "Error: directory not found: $EXT_SRC" >&2
    exit 1
fi

if [ ! -f "$EXT_SRC/CMakeLists.txt" ]; then
    echo "Error: no CMakeLists.txt found in $EXT_SRC" >&2
    exit 1
fi

BUILD_DIR="$EXT_SRC/_docker_build"
mkdir -p "$BUILD_DIR"

echo "==> Configuring extension..."
cd "$BUILD_DIR"
cmake "$EXT_SRC" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DVillageSQL_VEB_INSTALL_DIR="$VEB_INSTALL_DIR"

echo "==> Building extension..."
make -j"$(nproc)"

echo "==> Installing extension to $VEB_INSTALL_DIR/..."
make install

echo "==> Waiting for server to be ready..."
for i in $(seq 1 30); do
    if mysqladmin ping --silent 2>/dev/null; then
        echo "==> Done. Use INSTALL EXTENSION to load the extension."
        exit 0
    fi
    sleep 1
done

echo "Warning: server did not become ready within 30 seconds" >&2
echo "==> Done. Use INSTALL EXTENSION to load the extension."
