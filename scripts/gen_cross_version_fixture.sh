#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Generate a canned datadir used by the cross-version mtr test
# (villagesql/startup/cross_version_pending_apply). Produces a zip that
# lives at mysql-test/std_data/villagesql_0_0_5_pending_apply.zip.
#
# Meant to be re-run whenever the fixture needs to be regenerated (new
# baseline version, updated bundled extensions, etc.). The generated zip
# is committed to git so the test doesn't require a two-stage build.
#
# Prerequisites:
#   - A built mysqld at $BUILD_DIR/bin/mysqld that corresponds to the
#     baseline VillageSQL server version being captured (e.g. cut from
#     the 0.0.5 tag when generating the 0.0.5 fixture).
#   - The bundled test extensions built as .veb files under
#     $BUILD_DIR/veb_output_directory/.
#
# BUILD_DIR and TARGET_REPO usually point at DIFFERENT checkouts:
#   - BUILD_DIR = a build of the baseline version being captured (e.g. a
#                 build of a 0.0.5 checkout).
#   - TARGET_REPO = the repo where the zip should land, i.e. the tree
#                   whose mtr test consumes the fixture (typically your
#                   current working checkout, one version newer).
# TARGET_REPO defaults to the repo the script itself lives in.
#
# Usage:
#   BUILD_DIR=/path/to/baseline-build \
#     [TARGET_REPO=/path/to/current-repo] \
#     ./scripts/gen_cross_version_fixture.sh
#
# The output is:
#   $TARGET_REPO/mysql-test/std_data/villagesql_0_0_5_pending_apply.zip

set -euo pipefail

# Default TARGET_REPO to the checkout containing this script.
_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_DEFAULT_TARGET_REPO="$(cd "$_SCRIPT_DIR/.." && pwd)"

BUILD_DIR="${BUILD_DIR:?BUILD_DIR must point at the baseline mysqld build, e.g. a build-debug of a 0.0.5 checkout}"
TARGET_REPO="${TARGET_REPO:-$_DEFAULT_TARGET_REPO}"

MYSQLD="$BUILD_DIR/bin/mysqld"
VEB_DIR="$BUILD_DIR/veb_output_directory"
FIXTURE_NAME="villagesql_0_0_5_pending_apply"
OUTPUT_ZIP="$TARGET_REPO/mysql-test/std_data/${FIXTURE_NAME}.zip"

# Working area under /tmp so a partial run does not touch the source tree.
WORK_DIR="$(mktemp -d -t vsql_fixture.XXXXXX)"
DATADIR="$WORK_DIR/data"
FIXTURE_DIR="$WORK_DIR/$FIXTURE_NAME"
SOCKET="$WORK_DIR/mysqld.sock"
PIDFILE="$WORK_DIR/mysqld.pid"
ERRLOG="$WORK_DIR/mysqld.err"

cleanup() {
  if [ -f "$PIDFILE" ]; then
    local pid
    pid="$(cat "$PIDFILE" 2>/dev/null || true)"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "Shutting down mysqld (pid $pid)..."
      kill "$pid" || true
      # Wait up to 30s for clean shutdown.
      local i
      for i in $(seq 1 30); do
        if ! kill -0 "$pid" 2>/dev/null; then break; fi
        sleep 1
      done
    fi
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# Sanity check inputs.
if [ ! -x "$MYSQLD" ]; then
  echo "error: mysqld not executable at $MYSQLD" >&2
  exit 1
fi
if [ ! -d "$VEB_DIR" ]; then
  echo "error: VEB directory missing at $VEB_DIR" >&2
  exit 1
fi
if [ ! -f "$VEB_DIR/update_test-1.0.0.veb" ] ||
   [ ! -f "$VEB_DIR/update_test-2.0.0.veb" ]; then
  echo "error: expected update_test 1.0.0 and 2.0.0 VEBs in $VEB_DIR" >&2
  exit 1
fi

# Report the baseline server version we're capturing.
echo "=== Generating cross-version fixture ==="
echo "mysqld     : $MYSQLD (baseline)"
"$MYSQLD" --version
echo "veb dir    : $VEB_DIR"
echo "target repo: $TARGET_REPO"
echo "workdir    : $WORK_DIR"
echo "output     : $OUTPUT_ZIP"
echo

# 1. Initialize a fresh datadir with the baseline mysqld.
#    Force lower_case_table_names=1 so the DD is portable across case-
#    sensitive (Linux CI, default lctn=0) and case-insensitive (macOS
#    dev, default lctn=2) filesystems. lctn=1 works on both. Without
#    this, macOS init picks lctn=2 and the DD refuses to open on Linux.
echo "--- Initializing datadir ---"
mkdir -p "$DATADIR"
"$MYSQLD" --no-defaults \
          --initialize-insecure \
          --datadir="$DATADIR" \
          --veb-dir="$VEB_DIR" \
          --lower_case_table_names=1 \
          > "$ERRLOG" 2>&1

# 2. Start the server pointing at that datadir, log to $ERRLOG.
#    --vsql_allow_preview_extensions=ON: the bundled update_test extension
#    exposes a preview sys var, so it needs the preview gate open. The test
#    that consumes this fixture must set the same flag on startup.
echo "--- Starting mysqld ---"
"$MYSQLD" --no-defaults \
          --datadir="$DATADIR" \
          --veb-dir="$VEB_DIR" \
          --lower_case_table_names=1 \
          --vsql_allow_preview_extensions=ON \
          --socket="$SOCKET" \
          --pid-file="$PIDFILE" \
          --skip-networking \
          --log-error="$ERRLOG" \
          --daemonize

# Wait for the server to accept connections on the socket.
for i in $(seq 1 30); do
  if [ -S "$SOCKET" ] && "$BUILD_DIR/bin/mysql" --socket="$SOCKET" \
       -uroot -e "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! [ -S "$SOCKET" ]; then
  echo "error: mysqld did not create socket at $SOCKET; see $ERRLOG" >&2
  cat "$ERRLOG" >&2 || true
  exit 1
fi

# 3. Install the baseline extension version and stage a pending update.
#    Choose update_test 1.0.0 -> 2.0.0 as the cross-version scenario: both
#    versions share the COUNTER on-disk layout (only the to_string / VDF
#    semantics change), so restart-time precheck is trivially green and the
#    pending apply always succeeds.
echo "--- Installing 1.0.0 and staging pending 2.0.0 ---"
"$BUILD_DIR/bin/mysql" --socket="$SOCKET" -uroot <<'SQL'
CREATE DATABASE IF NOT EXISTS test;
INSTALL EXTENSION update_test VERSION '1.0.0';
CREATE TABLE test.t (id INT PRIMARY KEY, val update_test.COUNTER);
INSERT INTO test.t VALUES (1, '7'), (2, '42');
ALTER EXTENSION update_test VERSION '2.0.0' AT RESTART;
SELECT extension_name, extension_version, pending_version, pending_last_error
  FROM INFORMATION_SCHEMA.EXTENSIONS
  WHERE extension_name = 'update_test';
SQL

# 4. Clean shutdown -- pending action stays on disk, no restart-apply here.
echo "--- Shutting down cleanly ---"
"$BUILD_DIR/bin/mysqladmin" --socket="$SOCKET" -uroot shutdown

# Wait for pid to disappear.
if [ -f "$PIDFILE" ]; then
  pid="$(cat "$PIDFILE")"
  for i in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "error: mysqld did not shut down cleanly" >&2
    exit 1
  fi
fi

# 5. Package: rename data/ to the fixture name, capture the VEBs the
#    pending action was recorded against, and zip. The mtr test extracts
#    into $MYSQL_TMP_DIR/$FIXTURE_NAME, so the zip must contain a
#    top-level $FIXTURE_NAME/ directory.
echo "--- Packaging ---"
mv "$DATADIR" "$FIXTURE_DIR"

# Capture the exact update_test VEBs that were current when the pending
# action was recorded. The pending row stores each target's sha256; if
# the test-side server sees a different sha at restart, the apply is
# rejected. Shipping the VEBs inside the fixture makes the fixture
# self-contained and reproducible across rebuilds of the current tree.
mkdir -p "$FIXTURE_DIR/veb"
cp "$VEB_DIR/update_test-1.0.0.veb" "$FIXTURE_DIR/veb/"
cp "$VEB_DIR/update_test-2.0.0.veb" "$FIXTURE_DIR/veb/"

mkdir -p "$(dirname "$OUTPUT_ZIP")"
rm -f "$OUTPUT_ZIP"

# Strip mtr-noise before zipping: sockets, temporary files, error log.
# We keep the datadir contents themselves (mysql/, sys/, undo files,
# ibdata, redo logs, binlogs, villagesql/) so the server can boot clean.
find "$FIXTURE_DIR" -name '*.sock' -delete
find "$FIXTURE_DIR" -name '*.pid' -delete
rm -f "$FIXTURE_DIR/mysqld.err"

(cd "$WORK_DIR" && zip -qr "$OUTPUT_ZIP" "$FIXTURE_NAME")

echo
echo "=== Done ==="
echo "Wrote $OUTPUT_ZIP"
ls -lh "$OUTPUT_ZIP"
