#!/bin/bash

# Installs the minimal build dependencies needed to compile a VillageSQL
# extension VEB on a macOS host. Mirrors setup_linux_ext_deps.sh for macOS;
# Xcode Command Line Tools (build-essential equivalent) are pre-installed on
# GitHub Actions runners.

set -e

brew install cmake openssl curl pkg-config
