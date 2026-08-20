#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print the Docker images VillageSQL publishes, as a compact JSON array.
#
# This is the whole set; it applies no filtering. Callers select the rows they
# want from the array.
#
# Each object:
#   docker_platform — docker platform to build, e.g. linux/amd64
#   runner          — GitHub Actions runs-on value: a label array for a
#                     self-hosted runner, a plain string for a GitHub-hosted one
#
# The runner must be native to docker_platform. The image is always built
# natively, never under QEMU emulation, because compiling the server under
# emulation takes hours.
#
# This table is deliberately separate from json_platforms.sh. The platforms we
# build a server for and the platforms we publish an image for are different
# questions with different answers: macOS is a build target and will never be a
# Docker target, and the runners differ too, because the self-hosted Linux
# runners have no docker.
#
# The objects are written out by hand below, one per line, and jq assembles
# them into the array. Add an image by adding a line.
#
# Testable locally:
#   villagesql/bld_matrix/json_docker.sh | jq .

set -euo pipefail

jq -sc . <<'IMAGES'
{"docker_platform":"linux/amd64","runner":"ubuntu-latest"}
{"docker_platform":"linux/arm64","runner":"ubuntu-24.04-arm"}
IMAGES
