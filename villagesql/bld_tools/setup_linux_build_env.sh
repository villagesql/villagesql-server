#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Install the packages required to build VillageSQL Server on Linux
# (Debian/Ubuntu, apt-based). Self-elevates with sudo when not run as root.

set -e

# apt-get needs root. Self-elevate with sudo when not already root so callers
# (and the CI action) don't have to special-case privilege per OS.
SUDO=""
if [[ "$(id -u)" -ne 0 ]]; then
    SUDO="sudo"
fi

APT_OPTS=(-o Acquire::Retries=5)
$SUDO apt-get "${APT_OPTS[@]}" update
$SUDO apt-get "${APT_OPTS[@]}" install -y --no-install-recommends \
    build-essential \
    cmake \
    libssl-dev \
    pkg-config \
    bison \
    libncurses5-dev \
    libaio-dev \
    libmecab-dev \
    libnuma-dev \
    libjson-perl \
    libz-dev \
    g++ \
    make \
    git \
    curl \
    bash \
    valgrind \
    libtirpc-dev \
    ccache \
    perl \
    openssl \
    libdbd-mysql-perl \
    zip \
    unzip
