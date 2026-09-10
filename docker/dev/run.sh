#!/bin/bash
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

# Run the VillageSQL development Docker container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Check if image exists
if ! docker image inspect villagesql-dev:latest >/dev/null 2>&1; then
  echo "Image 'villagesql-dev:latest' not found. Building..."
  "$SCRIPT_DIR/build.sh"
fi

# Should be the same user who built the image
USER="$(id -un)"
echo "Starting VillageSQL development container..."
docker run -it --rm \
  -v "$REPO_ROOT:/home/$USER/villagesql-server" \
  -v villagesql-build:/home/$USER/build/villagesql \
  -v villagesql-ccache:/home/$USER/build/villagesql/.ccache \
  -v villagesql-datadir:/home/$USER/mysql-data/data \
  --cap-add SYS_NICE \
  -p 3306:3306 \
  villagesql-dev:latest
