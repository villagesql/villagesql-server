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

# Dockerfile for VillageSQL Server development
# Provides build environment and test infrastructure

FROM ubuntu:24.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies using setup script
COPY villagesql/bld_tools/setup_linux_build_env.sh /tmp/
RUN /tmp/setup_linux_build_env.sh && rm -rf /var/lib/apt/lists/* && rm /tmp/setup_linux_build_env.sh

# Set environment variables
ENV SOURCE_DIR=/source
ENV BUILD_DIR=/build
ENV MYSQLTEST_VARDIR=${BUILD_DIR}/mysql-test/var
ENV CCACHE_DIR=/root/.ccache
ENV PATH="/usr/lib/ccache:${PATH}"
# Disable NUMA for Docker containers (NUMA syscalls not available)
ENV CMAKE_EXTRA_FLAGS="-DHAVE_LIBNUMA=no"

# Create build directory
RUN mkdir -p ${BUILD_DIR}

# Set working directory
WORKDIR ${BUILD_DIR}

# Expose MySQL port
EXPOSE 3306

# Default command drops you into a shell for interactive work
CMD ["/bin/bash"]
