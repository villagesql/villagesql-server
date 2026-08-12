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

# The LDAP/SASL/Kerberos and curl -dev packages are not optional for us.
#
# Oracle gates WITH_AUTHENTICATION_LDAP and WITH_AUTHENTICATION_KERBEROS behind
# WITH_INTERNAL (Enterprise builds only), so they default OFF for a plain fork
# and their system libraries are never needed. Percona removed that gate in
# PS-7811 (882b59b173c, 2021) precisely because a non-Enterprise fork would
# otherwise silently not build the auth plugins -- so from the Percona 8.4.10
# merge onward these default ON here, and CMake hard-fails at configure time if
# the libraries are absent:
#
#   -DWITH_AUTHENTICATION_LDAP=ON, but missing system libraries
#
# WITH_CURL likewise defaults to "system" (build_ci.sh passes -DWITH_SSL=system
# and nothing that would switch curl to bundled), so libcurl needs its headers.
# To drop any of these instead, turn the corresponding option off explicitly in
# build_ci.sh rather than removing the package and rediscovering this.
APT_OPTS=(-o Acquire::Retries=5)
$SUDO apt-get "${APT_OPTS[@]}" update
$SUDO apt-get "${APT_OPTS[@]}" install -y --no-install-recommends \
    build-essential \
    cmake \
    libssl-dev \
    libldap-dev \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    libkrb5-dev \
    libcurl4-openssl-dev \
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
