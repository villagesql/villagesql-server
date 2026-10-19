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

# Extra containers/volumes created by the init-path tests below.
INIT_CIDS=()
INIT_VOLS=()

cleanup() {
    echo "==> Cleaning up..."
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    for c in "${INIT_CIDS[@]:-}"; do docker rm -f "$c" >/dev/null 2>&1 || true; done
    for v in "${INIT_VOLS[@]:-}"; do docker volume rm "$v" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

mysql_cmd() {
    mysql -h 127.0.0.1 -P "$PORT" -u root --skip-column-names -e "$1"
}

# Start a throwaway init-test container (tracked for cleanup). It talks over its
# own socket via `docker exec`.
init_run() {
    local name=$1; shift
    INIT_CIDS+=("$name")
    docker run -d --name "$name" "$@" >/dev/null
}

# Wait until root (with the given auth args) can run a query. Returns 0 when
# ready, 2 if the container exited first, 1 on timeout.
init_wait_ready() {
    local name=$1 timeout=$2; shift 2
    local i status
    for i in $(seq 1 "$timeout"); do
        status=$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null || echo gone)
        [ "$status" = exited ] && return 2
        if docker exec "$name" mysql "$@" -N -e 'SELECT 1' >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
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

# --- First-time initialization regression tests -------------------------
# Guard the init logic in docker-entrypoint.sh: the caching_sha2_password hash
# path (including the remote root user), the fail-fast temporary-server
# lifecycle, and init idempotency on an already-populated datadir.

echo ""
echo "==> [init] caching_sha2_password hash path..."
# Generate a real hash from the image, then feed it back in via the hash env.
GEN="${CONTAINER}-gen"
init_run "$GEN" -e MYSQL_ROOT_PASSWORD=hashpw "$IMAGE"
HASHHEX=
if init_wait_ready "$GEN" 90 -uroot -phashpw; then
    HASHHEX=$(docker exec "$GEN" mysql -uroot -phashpw -N \
        -e "SELECT HEX(authentication_string) FROM mysql.user WHERE user='root' AND host='localhost'" 2>/dev/null)
fi
docker rm -f "$GEN" >/dev/null 2>&1 || true
if [ -z "$HASHHEX" ]; then
    echo "FAIL: could not generate a caching_sha2_password hash from the image"
    exit 1
fi
HASHC="${CONTAINER}-hash"
init_run "$HASHC" -e MYSQL_ROOT_PASSWORD_CACHING_SHA2_HASH_HEX="$HASHHEX" "$IMAGE"
if ! init_wait_ready "$HASHC" 90 -uroot -phashpw; then
    echo "FAIL: root login with the caching_sha2 hash did not work"
    docker logs "$HASHC" 2>&1 | tail -20
    exit 1
fi
# The remote root user must carry the same hash (it once got an empty
# credential in hash mode).
RHASH=$(docker exec "$HASHC" mysql -uroot -phashpw -N \
    -e "SELECT HEX(authentication_string) FROM mysql.user WHERE user='root' AND host='%' AND plugin='caching_sha2_password'" 2>/dev/null || true)
if [ "$RHASH" != "$HASHHEX" ]; then
    echo "FAIL: root@% not created with the supplied caching_sha2 hash"
    exit 1
fi
# The temp-server log watcher tail process must not outlive init.
#if docker exec "$HASHC" sh -c 'ps -eo comm 2>/dev/null | grep -qx tail'; then
if docker exec "$HASHC" sh -c 'pgrep tail > /dev/null'; then
    echo "FAIL: a 'tail' process lingers after init"
    exit 1
fi
docker rm -f "$HASHC" >/dev/null 2>&1 || true
echo "    PASS: hash applied to root@localhost and root@%, watcher cleaned up"

echo "==> [init] temporary-server startup failure is fail-fast (no hang)..."
# Generate a 115-character unix socket filename (which is longer than sun_path
# (~107)).
# --initialize doesn't open the socket, so so the datadir bootstraps fine; the
# temporary server then fails to bind and exits during startup, exercising the
# wait -n / kill -0 crash-detection branch in docker-entrypoint.sh.
FAILC="${CONTAINER}-fail"
LONGSOCK="/var/lib/mysql/$(printf '%03d' $(seq 1 34)).sock"
init_run "$FAILC" -e MYSQL_ROOT_PASSWORD=x "$IMAGE" mysqld --socket="$LONGSOCK"
RC=
for i in $(seq 1 90); do
    st=$(docker inspect -f '{{.State.Status}}' "$FAILC" 2>/dev/null || echo gone)
    if [ "$st" = exited ]; then RC=$(docker inspect -f '{{.State.ExitCode}}' "$FAILC"); break; fi
    sleep 1
done
if [ -z "$RC" ]; then
    echo "FAIL: container hung (did not exit) on temporary-server startup failure"
    docker logs "$FAILC" 2>&1 | tail -20
    exit 1
fi
if [ "$RC" = 0 ]; then
    echo "FAIL: expected a non-zero exit on startup failure, got 0"
    exit 1
fi
if ! docker logs "$FAILC" 2>&1 | grep -q 'exited during startup before it became ready'; then
    echo "FAIL: expected 'exited during startup' message not found in logs"
    docker logs "$FAILC" 2>&1 | tail -20
    exit 1
fi
docker rm -f "$FAILC" >/dev/null 2>&1 || true
echo "    PASS: failed fast with exit code $RC"

echo "==> [init] initialization is skipped on an already-populated datadir..."
IVOL="${CONTAINER}-vol"
INIT_VOLS+=("$IVOL")
docker volume create "$IVOL" >/dev/null
I1="${CONTAINER}-init1"
init_run "$I1" -v "$IVOL":/var/lib/mysql -e MYSQL_ROOT_PASSWORD=secret1 "$IMAGE"
if ! init_wait_ready "$I1" 90 -uroot -psecret1; then
    echo "FAIL: first init run did not become ready"
    docker logs "$I1" 2>&1 | tail -20
    exit 1
fi
docker rm -f "$I1" >/dev/null 2>&1 || true
# Second run on the populated volume: init must be skipped, the new
# MYSQL_ROOT_PASSWORD ignored, and 'secret1' still in effect.
I2="${CONTAINER}-init2"
init_run "$I2" -v "$IVOL":/var/lib/mysql -e MYSQL_ROOT_PASSWORD=ignored2 "$IMAGE"
if ! init_wait_ready "$I2" 90 -uroot -psecret1; then
    echo "FAIL: original password not preserved on restart (init not skipped?)"
    docker logs "$I2" 2>&1 | tail -20
    exit 1
fi
if docker logs "$I2" 2>&1 | grep -qi 'Initializing database'; then
    echo "FAIL: datadir was re-initialized on the second run"
    exit 1
fi
docker rm -f "$I2" >/dev/null 2>&1 || true
echo "    PASS: existing datadir left intact, new password env ignored"

echo ""
echo "=== All tests passed ==="
