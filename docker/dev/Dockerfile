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

# Use build-args when provided, default otherwise
ARG USER_ID=1000
ARG GROUP_ID=1000
ARG USER_NAME=dev_user
ARG GROUP_NAME=dev_group

RUN \
    # Ubuntu images come prebuilt with user(ubuntu, 1000) and
    # group(ubuntu, 1000). Delete existing user(if any) on USER_ID
    if getent passwd ${USER_ID} >/dev/null; then \
        userdel $(getent passwd ${USER_ID} | cut -d: -f1); \
    fi && \
    \
    # Delete existing group(if any) on GROUP_ID
    if getent group ${GROUP_ID} >/dev/null; then \
        groupdel $(getent group ${GROUP_ID} | cut -d: -f1); \
    fi && \
    \
    # USER_NAME might already exist on a different uid other than USER_ID
    # Delete it if so
    if getent passwd ${USER_NAME} >/dev/null; then \
        userdel ${USER_NAME}; \
    fi && \
    \
    # GROUP_NAME might already exist on a different gid other than GROUP_ID
    # Delete it if so
    if getent group ${GROUP_NAME} >/dev/null; then \
        groupdel ${GROUP_NAME}; \
    fi && \
    \
    # Add (GROUP_ID, GROUP_NAME)
    echo "Case 5"; \
    groupadd -g ${GROUP_ID} ${GROUP_NAME}; \
    \
    # Add (USER_ID, USER_NAME) within GROUP_ID
    echo "Case 6"; \
    useradd -l -u ${USER_ID} -g ${GROUP_ID} -m -s /bin/bash ${USER_NAME}; \
    \
    # Grant sudo privileges to USER_NAME
    usermod -a -G sudo ${USER_NAME}; \
    \
    # Enable all sudoers to use sudo without password
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers;

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
