#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print the platforms VillageSQL builds and tests on, as a compact JSON array.
#
# This is the whole set; it applies no filtering. Callers select the platforms
# they want from the array.
#
# Each object:
#   platform  — platform identifier, used to name artifacts
#   runner    — GitHub Actions runs-on value: a label array for self-hosted
#               runners, a plain string for a GitHub-hosted one
#   os        — operating-system family, "linux" or "macos"
#
# The objects are written out by hand below, one per line, and jq assembles
# them into the array. Add a platform by adding a line.
#
# Testable locally:
#   villagesql/bld_matrix/json_platforms.sh | jq .

set -euo pipefail

jq -sc . <<'PLATFORMS'
{"platform":"linux-x86_64","runner":["self-hosted","linux","x86_64"],"os":"linux"}
{"platform":"linux-aarch64","runner":"ubuntu-24.04-arm","os":"linux"}
{"platform":"macos-arm64","runner":["self-hosted","macOS","ARM64"],"os":"macos"}
PLATFORMS
