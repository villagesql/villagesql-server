#!/bin/bash

# Installs the build dependencies needed to compile a VillageSQL extension VEB
# on a macOS host. Mirrors setup_linux_ext_deps.sh for macOS; Xcode Command Line
# Tools (build-essential equivalent) are pre-installed on GitHub Actions
# runners.
#
# curl is not a general extension requirement. It is here because this script
# also prepares the bundled-extension build, and vsql_http links libcurl. An
# extension with its own third-party dependency installs it in its own CI, not
# here.

# openssl@3 is pinned, not the unversioned `openssl` alias: Homebrew
# retargeted that alias to openssl@4, which MySQL 8.4 does not build against.

set -e

brew install cmake openssl@3 curl pkg-config
