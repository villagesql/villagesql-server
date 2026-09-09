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

# Build the VillageSQL development Docker image

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Building VillageSQL development Docker image..."

# Match the docker user to have the same uid, user-name, gid and group-name
# as the host user. This is to make mysqld run as a non-root user and to enable # the docker user write back to the host without permission mess. The machine
# on which the image was built has to be the machine on which the corresponding # container runs. For devs, that's a fair enough assumption.
docker build \
	--build-arg USER_ID=$(id -u) \
	--build-arg GROUP_ID=$(id -g) \
	--build-arg USER_NAME=$(id -un) \
	--build-arg GROUP_NAME=$(id -gn) \
	-t villagesql-dev:latest \
	-f "$SCRIPT_DIR/Dockerfile" \
	"$REPO_ROOT"

echo ""
echo "Build complete! Run with: $SCRIPT_DIR/run.sh"
