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
# Add packages to the list lexicographically
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
    libdbd-mysql-perl \
    libjson-perl \
    libmecab-dev \
    libncurses5-dev \
    libnuma-dev \
    libssl-dev \
    libtirpc-dev \
    libz-dev \
    llvm \
    make \
    mold \
    openssl \
    perl \
    pkg-config \
    sudo \
    unzip \
    valgrind \
    zip

# Sanitizers symbolize through an unversioned llvm-symbolizer on PATH. It does
# not help with dlopen'd plugins -- the tsan workflow resolves those itself.
if ! command -v llvm-symbolizer >/dev/null 2>&1; then
    VERSIONED=$(ls -1 /usr/bin/llvm-symbolizer-* 2>/dev/null | sort -V | tail -1)
    if [[ -n "$VERSIONED" ]]; then
        $SUDO ln -sf "$VERSIONED" /usr/bin/llvm-symbolizer
    fi
fi

command -v llvm-symbolizer || echo "WARNING: no llvm-symbolizer; sanitizer reports will be less readable"
