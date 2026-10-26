#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print the Debian package targets VillageSQL publishes, as a compact JSON
# array.
#
# This is the whole set; it applies no filtering. Callers select the rows they
# want from the array.
#
# Each object:
#   distro  — distro label, used to name artifacts, e.g. ubuntu24.04
#   image   — Docker image the packages are built and tested in
#   arch    — Debian architecture of the packages, e.g. amd64
#   runner  — GitHub Actions runs-on value: a label array for a self-hosted
#             runner, a plain string for a GitHub-hosted one
#
# The runner must be native to arch and have docker. The packages are always
# built natively, never under QEMU emulation, because compiling the server
# under emulation takes hours.
#
# Packages are built per distro because their shared-library Depends name the
# distro's own packages (libssl3 vs libssl3t64, for example), so a DEB built on
# one release does not install on another.
#
# The objects are written out by hand below, one per line, and jq assembles
# them into the array. Add a target by adding a line.
#
# Testable locally:
#   villagesql/bld_matrix/json_deb.sh | jq .

set -euo pipefail

jq -sc . <<'TARGETS'
{"distro":"ubuntu24.04","image":"ubuntu:24.04","arch":"amd64","runner":"ubuntu-24.04"}
{"distro":"ubuntu24.04","image":"ubuntu:24.04","arch":"arm64","runner":"ubuntu-24.04-arm"}
{"distro":"debian12","image":"debian:12","arch":"amd64","runner":"ubuntu-24.04"}
{"distro":"debian12","image":"debian:12","arch":"arm64","runner":"ubuntu-24.04-arm"}
TARGETS
