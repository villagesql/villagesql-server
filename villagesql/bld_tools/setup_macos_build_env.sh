#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Install the packages required to build VillageSQL Server on macOS via
# Homebrew. The Xcode Command Line Tools (the build-essential equivalent) are
# assumed to be present; on GitHub Actions runners they are pre-installed.

set -e

brew install bison cmake openssl jq
