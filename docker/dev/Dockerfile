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

# Add a non-root user. Use build-args when provided, default otherwise
ARG USER_ID=1000
ARG GROUP_ID=1000
ARG USER_NAME=dev_user
ARG GROUP_NAME=dev_group

COPY villagesql/bld_tools/add_non_root_docker_user.sh /tmp/
RUN /tmp/add_non_root_docker_user.sh && rm /tmp/add_non_root_docker_user.sh

# Switch to the custom user
USER ${USER_NAME}

# Set the workdir
WORKDIR /home/${USER_NAME}

# Set environment variables
ENV HOME=/home/${USER_NAME}
ENV SOURCE_DIR=${HOME}/villagesql-server
ENV BUILD_DIR=${HOME}/build/villagesql
ENV DATA_DIR=${HOME}/mysql-data/data
ENV MYSQLTEST_VARDIR=${BUILD_DIR}/mysql-test/var
ENV CCACHE_DIR=${HOME}/build/villagesql/.ccache
ENV PATH="/usr/lib/ccache:${PATH}"
# Disable NUMA for Docker containers (NUMA syscalls not available)
ENV CMAKE_EXTRA_FLAGS="-DHAVE_LIBNUMA=no"

# Create directories
RUN mkdir -p ${BUILD_DIR}
RUN mkdir -p ${DATA_DIR}
RUN mkdir -p ${CCACHE_DIR}

# Set working directory
WORKDIR ${BUILD_DIR}

# Expose MySQL port
EXPOSE 3306

# Default command drops you into a shell for interactive work
CMD ["/bin/bash"]
