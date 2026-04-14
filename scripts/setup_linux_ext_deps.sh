#!/bin/bash

# Installs the minimal build dependencies needed to compile a VillageSQL
# extension VEB on a Linux host. Intentionally lighter than
# setup_linux_build_env.sh, which pulls in server-only tools (valgrind, bison,
# Perl libs, etc.) that extensions do not need.

set -e

apt-get update
apt-get install -y --no-install-recommends \
  build-essential \
  cmake \
  libssl-dev \
  libcurl4-openssl-dev \
  pkg-config
