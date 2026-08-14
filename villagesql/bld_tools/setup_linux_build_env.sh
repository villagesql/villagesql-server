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
    bash \
    bison \
    build-essential \
    ccache \
    cmake \
    curl \
    g++ \
    git \
    jq \
    libaio-dev \
    libcurl4-openssl-dev \
    libdbd-mysql-perl \
    libjson-perl \
    libmecab-dev \
    libncurses5-dev \
    libnuma-dev \
    libssl-dev \
    libtirpc-dev \
    libz-dev \
    make \
    openssl \
    perl \
    pkg-config \
    unzip \
    valgrind \
    zip
