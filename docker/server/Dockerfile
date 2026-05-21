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

# Multi-stage Dockerfile for VillageSQL Server
# Build from the repository root:
#   docker build -f docker/server/Dockerfile -t villagesql/server:latest .

# ---------------------------------------------------------------------------
# Stage 1: Build VillageSQL from source and bundled extensions
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
COPY scripts/setup_linux_build_env.sh /tmp/
RUN /tmp/setup_linux_build_env.sh && rm -rf /var/lib/apt/lists/*

# Copy source tree (excluding docker/ so script changes don't invalidate the
# expensive server build cache)
COPY --exclude=docker . /source

# Build and install
# VSQL_PRE_RELEASE_VERSION defaults to "dev"; set to "" for release builds
ARG VSQL_PRE_RELEASE_VERSION=dev
RUN mkdir /build && cd /build && \
    cmake /source \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DBUILD_CONFIG=mysql_release \
        -DINSTALL_MYSQLTESTDIR="" \
        -DWITH_ROUTER=OFF \
        -DWITH_NUMA=OFF \
        -DMYSQL_DATADIR=/var/lib/mysql \
        -DMYSQL_UNIX_ADDR=/var/run/mysqld/mysqld.sock \
        -DMYSQLX_UNIX_ADDR=/var/run/mysqld/mysqlx.sock \
        -DVSQL_PRE_RELEASE_VERSION="$VSQL_PRE_RELEASE_VERSION" \
    && make -j"$(nproc)" \
    && make install DESTDIR=/install

# Build bundled extensions and install .veb files to /install/usr/lib/veb
ARG VSQL_DEV_ABI=ON
COPY docker/server/build-bundled-extension.sh /usr/local/bin/
COPY docker/server/build-bundled-extensions.sh /usr/local/bin/
RUN mkdir -p /install/usr/lib/veb

# In-tree extensions track the dev API, always use dev ABI
RUN VSQL_DEV_ABI=ON build-bundled-extension.sh vsql-complex

# Out-of-tree extensions use the configured ABI (stable for release builds)
# and have additional dependencies
COPY scripts/setup_linux_ext_deps.sh /tmp/
RUN /tmp/setup_linux_ext_deps.sh && rm -rf /var/lib/apt/lists/*
RUN VSQL_DEV_ABI=$VSQL_DEV_ABI build-bundled-extensions.sh

# ---------------------------------------------------------------------------
# Stage 2: Runtime image
# ---------------------------------------------------------------------------
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Create mysql user/group
RUN groupadd -r mysql && useradd -r -g mysql mysql

# Install runtime dependencies and extension build toolchain
RUN apt-get update && apt-get install -y --no-install-recommends \
        gosu \
        libaio1t64 \
        libncurses6 \
        libssl3 \
        libtirpc3t64 \
        openssl \
        perl \
        tzdata \
        zlib1g \
        build-essential \
        cmake \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy built server and SDK from builder stage
COPY --from=builder /install /

# Set up directories and permissions
RUN mkdir -p /var/lib/mysql /var/run/mysqld /docker-entrypoint-initdb.d /usr/lib/veb \
    && chown -R mysql:mysql /var/lib/mysql /var/run/mysqld /usr/lib/veb \
    && chmod 1777 /var/lib/mysql /var/run/mysqld

VOLUME /var/lib/mysql

COPY docker/server/docker-entrypoint.sh /usr/local/bin/
COPY docker/server/vsql-build-extension.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

EXPOSE 3306 33060
CMD ["mysqld"]
