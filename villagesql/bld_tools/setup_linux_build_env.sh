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
#
# libxml-parser-perl is a test dependency rather than a build one: the
# component_audit_log_filter suite validates its XML output through a Perl
# helper (validate_logs_format.inc) that uses XML::Parser. The suite has no
# guard for the module being absent, so without the package every test in it
# fails outright instead of skipping.
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
    libkrb5-dev \
    libldap-dev \
    libmecab-dev \
    libncurses5-dev \
    libnuma-dev \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    libssl-dev \
    libtirpc-dev \
    libxml-parser-perl \
    libz-dev \
    make \
    openssl \
    perl \
    pkg-config \
    unzip \
    valgrind \
    zip
