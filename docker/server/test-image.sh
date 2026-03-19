#!/usr/bin/env bash
# Copyright (c) 2026 VillageSQL Contributors
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <https://www.gnu.org/licenses/>.

# Smoke test a VillageSQL Server Docker image.
#
# Usage:
#   test-image.sh [image]
#
# Default image: villagesql/server:latest

set -eo pipefail

IMAGE="${1:-villagesql/server:latest}"
CONTAINER="vsql-test-$$"
PORT=13306

cleanup() {
    echo "==> Cleaning up..."
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mysql_cmd() {
    mysql -h 127.0.0.1 -P "$PORT" -u root --skip-column-names -e "$1"
}

echo "==> Starting container from $IMAGE..."
docker run -d --name "$CONTAINER" -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -p "$PORT:3306" "$IMAGE" >/dev/null

echo "==> Waiting for server to be ready..."
for i in $(seq 1 60); do
    if mysqladmin ping -h 127.0.0.1 -P "$PORT" --silent 2>/dev/null; then
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "FAIL: server not ready after 60 seconds"
        docker logs "$CONTAINER" | tail -20
        exit 1
    fi
    sleep 1
done
echo "    Server ready."

echo "==> Creating test database..."
mysql_cmd "CREATE DATABASE vsql_test;"

echo "==> Installing vsql_complex extension..."
mysql_cmd "INSTALL EXTENSION vsql_complex;"

echo "==> Testing COMPLEX type (create, insert, read)..."
RESULT=$(mysql_cmd "
    USE vsql_test;
    CREATE TABLE test_complex (id INT PRIMARY KEY, val COMPLEX NOT NULL);
    INSERT INTO test_complex VALUES (1, '(3.0,4.0)');
    SELECT val FROM test_complex WHERE id = 1;
    DROP TABLE test_complex;
")
if [ "$RESULT" != "(3,4)" ]; then
    echo "FAIL: expected '(3,4)', got '$RESULT'"
    exit 1
fi
echo "    PASS: read back (3,4)"

echo "==> Uninstalling vsql_complex..."
mysql_cmd "UNINSTALL EXTENSION vsql_complex;"

echo "==> Installing remaining extensions..."
for ext in vsql_uuid vsql_network_address vsql_ai vsql_crypto; do
    echo "    $ext..."
    mysql_cmd "INSTALL EXTENSION $ext;"
done
echo "    All extensions installed."

echo "==> Uninstalling all extensions..."
for ext in vsql_uuid vsql_network_address vsql_ai vsql_crypto; do
    mysql_cmd "UNINSTALL EXTENSION $ext;"
done

echo ""
echo "=== All tests passed ==="
