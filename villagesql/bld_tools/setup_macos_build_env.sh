#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Install the packages required to build VillageSQL Server on macOS via
# Homebrew. The Xcode Command Line Tools (the build-essential equivalent) are
# assumed to be present; on GitHub Actions runners they are pre-installed.

# openssl@3 is pinned, not the unversioned `openssl` alias: Homebrew
# retargeted that alias to openssl@4, which MySQL 8.4 does not build against.

set -e

brew install bison cmake jq openssl@3
