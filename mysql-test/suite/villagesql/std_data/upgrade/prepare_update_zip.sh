#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Create a zipped datadir for upgrade testing from an existing MySQL or
# VillageSQL installation.
#
# This script runs an unpacked installation, initializes a datadir, optionally
# runs SQL to configurate it, and zips the result.
#
# The zip is named <label>_lctn<N>.zip and contains a single directory, named
# after --data-dir. Tests unzip it and reference that inner directory by name,
# so follow the existing convention and call it villagesql_<label>_test:
#
#   ./prepare_update_zip.sh \
#       --install-dir /tmp/mysql-8.4.6-macos-arm64 \
#       --data-dir /tmp/villagesql_mysql_8.4.6_test \
#       --label mysql_8.4.6 \
#       --lower-case-table-names 0
#
# The datadir is removed once it has been zipped.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

INSTALL_DIR=""
DATADIR=""
LABEL=""
LOWER_CASE_TABLE_NAMES=""
OUTPUT_DIR=""
SQL_FILE=""
SERVER_OPTIONS=()

usage() {
  cat <<EOF
Usage: $0 --install-dir <DIR> --data-dir <DIR> --label <LABEL> \\
          --lower-case-table-names <0|1|2> [--sql-file <FILE>] \\
          [--server-option <OPT>]... [--output-dir <DIR>]

Required:
  --install-dir <DIR>   Unpacked MySQL or VillageSQL installation to
                        initialize with; must contain bin/mysqld
  --data-dir <DIR>      Where to create the database; must not exist yet.
                        Its name becomes the directory inside the zip.
  --label <LABEL>       Names the zip, and so selects it in the tests that
                        unzip it. Usually built around a version, e.g.
                        mysql_8.4.6 or veb_upgrade_0.0.5, but the whole label
                        is what a test refers to.
  --lower-case-table-names <0|1|2>
                        Value to initialize the datadir with

Options:
  --sql-file <FILE>     SQL to run against the started server before the
                        datadir is captured, e.g. installing an extension and
                        staging an update for the next startup. The file
                        selects its own database.
  --server-option <OPT> Extra option for the server that --sql-file runs
                        against, e.g. --vsql_allow_preview_extensions=ON.
                        Repeat for more than one. These apply to the running
                        server only, not to --initialize, so they can enable
                        SQL the datadir is meant to capture without changing
                        how the datadir is created.
  --output-dir <DIR>    Where to write the zip (default: the parent of
                        --data-dir)
  --help                Show this message
EOF
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --install-dir)
      INSTALL_DIR="$2"
      shift 2
      ;;
    --data-dir)
      DATADIR="$2"
      shift 2
      ;;
    --label)
      LABEL="$2"
      shift 2
      ;;
    --lower-case-table-names)
      LOWER_CASE_TABLE_NAMES="$2"
      shift 2
      ;;
    --sql-file)
      SQL_FILE="$2"
      shift 2
      ;;
    --server-option)
      SERVER_OPTIONS+=("$2")
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      log_error "Unknown option: $1"
      usage >&2
      exit 1
      ;;
  esac
done

[[ -n "$INSTALL_DIR" ]] || { log_error "--install-dir is required"; usage >&2; exit 1; }
[[ -n "$DATADIR" ]] || { log_error "--data-dir is required"; usage >&2; exit 1; }
[[ -n "$LABEL" ]] || { log_error "--label is required"; usage >&2; exit 1; }
if [[ -z "$LOWER_CASE_TABLE_NAMES" ]]; then
  log_error "--lower-case-table-names is required"
  usage >&2
  exit 1
fi
if [[ ! "$LOWER_CASE_TABLE_NAMES" =~ ^[0-2]$ ]]; then
  die "--lower-case-table-names must be 0, 1, or 2"
fi

# The installation stands in for the build directory: all we need from it is a
# server to initialize with, and a client to run --sql-file through.
[[ -d "$INSTALL_DIR" ]] || die "Install directory not found: $INSTALL_DIR"
INSTALL_DIR="$(cd "$INSTALL_DIR" && pwd)"
MYSQLD="$INSTALL_DIR/bin/mysqld"
MYSQL="$INSTALL_DIR/bin/mysql"
MYSQLADMIN="$INSTALL_DIR/bin/mysqladmin"
[[ -x "$MYSQLD" ]] || die "No executable mysqld at $MYSQLD"

if [[ -n "$SQL_FILE" ]]; then
  [[ -f "$SQL_FILE" ]] || die "SQL file not found: $SQL_FILE"
  SQL_FILE="$(cd "$(dirname "$SQL_FILE")" && pwd)/$(basename "$SQL_FILE")"
  [[ -x "$MYSQL" ]] || die "No executable mysql client at $MYSQL"
  [[ -x "$MYSQLADMIN" ]] || die "No executable mysqladmin at $MYSQLADMIN"
fi

# Resolve the datadir to an absolute path without creating it, so that mysqld
# does not interpret it relative to some directory of its own choosing.
mkdir -p "$(dirname "$DATADIR")"
DATADIR="$(cd "$(dirname "$DATADIR")" && pwd)/$(basename "$DATADIR")"

if [[ -e "$DATADIR" ]]; then
  log_error "$DATADIR already exists!"
  log_error "Please remove it manually first:"
  log_error "  rm -rf $DATADIR"
  exit 1
fi

[[ -n "$OUTPUT_DIR" ]] || OUTPUT_DIR="$(dirname "$DATADIR")"
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

ZIP_NAME="${LABEL}_lctn${LOWER_CASE_TABLE_NAMES}.zip"
ZIP_PATH="$OUTPUT_DIR/$ZIP_NAME"

# zip adds to an existing archive rather than replacing it, which would quietly
# leave stale entries from an earlier run in the result.
[[ ! -e "$ZIP_PATH" ]] || die "$ZIP_PATH already exists; remove it first"

log_info "Install directory: $INSTALL_DIR"
log_info "Server: $($MYSQLD --version)"
log_info "Datadir: $DATADIR"
log_info "lower_case_table_names: $LOWER_CASE_TABLE_NAMES"
[[ -z "$SQL_FILE" ]] || log_info "SQL file: $SQL_FILE"
[[ ${#SERVER_OPTIONS[@]} -eq 0 ]] || log_info "Server options: ${SERVER_OPTIONS[*]}"
log_info "Zip: $ZIP_PATH"
echo ""

SERVER_PID=""
RUNTIME_DIR=""

# Stop a server this script started, and wait for it to be gone. Only a clean
# shutdown leaves a datadir worth capturing, so a server that will not stop is
# an error rather than something to kill and carry on from.
stop_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  local pid="$SERVER_PID"
  SERVER_PID=""
  "$MYSQLADMIN" --no-defaults --socket="$RUNTIME_DIR/mysqld.sock" \
    --user=root shutdown >/dev/null 2>&1 || true
  wait "$pid" 2>/dev/null || true
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" 2>/dev/null || true
    return 1
  fi
  return 0
}

cleanup() {
  stop_server || true
  rm -f "${BOOTSTRAP_FILE:-}"
  [[ -z "$RUNTIME_DIR" ]] || rm -rf "$RUNTIME_DIR"
  return 0
}

trap cleanup EXIT

# Abandon a datadir that never reached a state worth zipping. Leaving it behind
# would block the next attempt on the "already exists" check above, hiding the
# real failure behind a stale one.
die_discarding_datadir() {
  stop_server || true
  rm -rf "$DATADIR"
  die "$* ($DATADIR discarded)"
}

# Bootstrap file, creating the test database the test framework expects.
BOOTSTRAP_FILE=$(mktemp "${TMPDIR:-/tmp}/vsql_bootstrap_XXXXXX.sql")
cat > "$BOOTSTRAP_FILE" <<'EOF'
CREATE DATABASE test;
EOF

log_step "Initializing datadir..."
# A failed --initialize leaves a datadir mysqld itself calls unusable.
if ! "$MYSQLD" --no-defaults --initialize-insecure \
  --datadir="$DATADIR" \
  --basedir="$INSTALL_DIR" \
  --lower_case_table_names="$LOWER_CASE_TABLE_NAMES" \
  --init-file="$BOOTSTRAP_FILE"; then
  die_discarding_datadir "mysqld --initialize-insecure failed"
fi

log_info "Datadir initialized successfully."

if [[ -n "$SQL_FILE" ]]; then
  # Keep the socket, pid file and error log out of the datadir: the zip is
  # supposed to hold a cleanly shut down database and nothing else. /tmp keeps
  # the socket path inside the ~100 character limit imposed on unix sockets,
  # which a long --data-dir would blow past.
  RUNTIME_DIR=$(mktemp -d /tmp/vsql_prepare_XXXXXX)
  SOCKET="$RUNTIME_DIR/mysqld.sock"
  ERROR_LOG="$RUNTIME_DIR/mysqld.err"

  log_step "Starting the server..."

  # --skip-log-bin keeps binlog files out of the captured datadir, and
  # --skip-networking with --mysqlx=OFF keeps this server off every port, so it
  # cannot collide with a server already running on this machine.
  "$MYSQLD" --no-defaults \
    --datadir="$DATADIR" \
    --basedir="$INSTALL_DIR" \
    --lower_case_table_names="$LOWER_CASE_TABLE_NAMES" \
    --socket="$SOCKET" \
    --pid-file="$RUNTIME_DIR/mysqld.pid" \
    --log-error="$ERROR_LOG" \
    --skip-log-bin \
    --skip-networking \
    --mysqlx=OFF \
    ${SERVER_OPTIONS[@]+"${SERVER_OPTIONS[@]}"} &
  SERVER_PID=$!

  WAITED=0
  until "$MYSQLADMIN" --no-defaults --socket="$SOCKET" --user=root ping \
      >/dev/null 2>&1; do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      log_error "The server exited during startup. Error log:"
      cat "$ERROR_LOG" >&2 || true
      SERVER_PID=""
      die_discarding_datadir "Server failed to start"
    fi
    if [[ "$WAITED" -ge 60 ]]; then
      log_error "Last lines of the error log:"
      tail -20 "$ERROR_LOG" >&2 || true
      die_discarding_datadir "Server did not become ready within 60 seconds"
    fi
    sleep 1
    WAITED=$((WAITED + 1))
  done

  log_step "Running $SQL_FILE..."
  if ! "$MYSQL" --no-defaults --socket="$SOCKET" --user=root \
      --table < "$SQL_FILE"; then
    log_error "Last lines of the error log:"
    tail -20 "$ERROR_LOG" >&2 || true
    die_discarding_datadir "SQL from $SQL_FILE failed"
  fi

  # No restart from here on: anything the SQL staged for the next startup, such
  # as ALTER EXTENSION ... AT RESTART, has to still be pending when the datadir
  # is zipped.
  log_step "Shutting the server down..."
  stop_server || die_discarding_datadir "Server did not shut down cleanly"
fi

# Check for runtime files (indicates unclean shutdown)
if ls "$DATADIR"/*.{log,err,pid,sock} "$DATADIR"/binlog.* 2>/dev/null; then
  log_error "Runtime files found in datadir! Shutdown was not clean."
  log_error "Do not continue. Clean up and try again."
  exit 1
fi

log_step "Creating zip file..."
(cd "$(dirname "$DATADIR")" && zip -r "$ZIP_PATH" "$(basename "$DATADIR")")

# Verify the zip doesn't contain runtime files
if unzip -l "$ZIP_PATH" | grep -qE "\.log|\.err|\.pid|binlog\."; then
  log_error "Zip contains runtime files (.log, .err, .pid, binlog)!"
  log_error "This should not happen. Something went wrong."
  exit 1
fi

log_step "Cleaning up datadir..."
rm -rf "$DATADIR"

echo ""
log_step "Success! Created: $ZIP_PATH"
echo ""
echo "To use in tests, copy it into the upgrade std_data directory:"
echo "  cp $ZIP_PATH $SCRIPT_DIR/"
echo ""
