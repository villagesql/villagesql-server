#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# VillageSQL Development Database Setup Script
# This script automates the MySQL initialization and user setup process

set -e  # Exit on error

# Configuration
DATADIR="${MYSQL_DATADIR:-$HOME/villagesql_data}"
SOCKET="${MYSQL_SOCKET:-/tmp/mysql_villagesql.sock}"
PORT="${MYSQL_PORT:-3307}"
BUILD_DIR="${BUILD_DIR:-$HOME/build}"
ROOT_PASS="root123"
DEV_USER="${VILLAGESQL_DEV_USERNAME:-$USER}"
DEV_PASS="${VILLAGESQL_DEV_PASSWORD:-password}"

echo "=== VillageSQL MySQL Development Setup ==="
echo "Data directory: $DATADIR"
echo "Socket: $SOCKET"
echo "Port: $PORT"
echo "Build directory: $BUILD_DIR"
echo

# Check if mysqld exists
if [ ! -x "$BUILD_DIR/bin/mysqld" ]; then
    echo "Error: mysqld not found at $BUILD_DIR/bin/mysqld"
    echo "Please build MySQL first or set BUILD_DIR environment variable"
    exit 1
fi

# Check if data directory already exists
if [ -d "$DATADIR" ]; then
    echo "Warning: Data directory already exists at $DATADIR"
    read -p "Remove and recreate? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf "$DATADIR"
    else
        echo "Aborting..."
        exit 1
    fi
fi

# 1. Initialize without password
echo "Step 1: Initializing MySQL with VillageSQL extensions..."
"$BUILD_DIR/bin/mysqld" --initialize-insecure --datadir="$DATADIR" --basedir="$BUILD_DIR" --log-error-verbosity=3

# 2. Start MySQL in background
echo "Step 2: Starting MySQL server..."
"$BUILD_DIR/bin/mysqld" \
    --datadir="$DATADIR" \
    --socket="$SOCKET" \
    --port="$PORT" \
    --basedir="$BUILD_DIR" \
    --log-error="$DATADIR/error.log" \
    --log-error-verbosity=3 \
    --pid-file="$DATADIR/mysql.pid" &
MYSQL_PID=$!

# 3. Wait for MySQL to be ready
echo "Step 3: Waiting for MySQL to start..."
for i in {1..30}; do
    # Check if process is still alive
    if ! kill -0 $MYSQL_PID 2>/dev/null; then
        echo
        echo "=== Last 250 lines of error log ==="
        tail -250 "$DATADIR/error.log"
        echo
        echo "=== Context around the failure ==="
        # Look for key error indicators and show context
        grep -B5 -A5 -E "(ERROR|FATAL|Assertion failed|signal|crash|abort)" "$DATADIR/error.log" | tail -50
        echo
        echo "=== ERROR: MySQL process died ==="
        echo "Check $DATADIR/error.log for complete details"
        echo "Build directory: $BUILD_DIR"
        echo "Data directory: $DATADIR"
        exit 1
    fi

    # Check if MySQL is accepting connections
    if "$BUILD_DIR/bin/mysqladmin" --socket="$SOCKET" ping >/dev/null 2>&1; then
        echo " MySQL is ready!"
        break
    fi

    if [ $i -eq 30 ]; then
        echo
        echo "=== Last 250 lines of error log ==="
        tail -250 "$DATADIR/error.log"
        echo
        echo "=== Context around the failure ==="
        # Look for key error indicators and show context
        grep -B5 -A5 -E "(ERROR|FATAL|Assertion failed|signal|crash|abort)" "$DATADIR/error.log" | tail -50
        echo
        echo "=== ERROR: MySQL failed to start within 30 seconds ==="
        echo "Check $DATADIR/error.log for complete details"
        echo "Build directory: $BUILD_DIR"
        echo "Data directory: $DATADIR"
        exit 1
    fi
    echo -n "."
    sleep 1
done

# 4. Run setup commands
echo "Step 4: Configuring users and databases..."
"$BUILD_DIR/bin/mysql" --socket="$SOCKET" -u root << EOF
-- Set root password
ALTER USER 'root'@'localhost' IDENTIFIED BY '${ROOT_PASS}';

-- Create development user with full privileges
CREATE USER '${DEV_USER}'@'localhost' IDENTIFIED BY '${DEV_PASS}';
CREATE USER '${DEV_USER}'@'%' IDENTIFIED BY '${DEV_PASS}';
GRANT ALL PRIVILEGES ON *.* TO '${DEV_USER}'@'localhost' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON *.* TO '${DEV_USER}'@'%' WITH GRANT OPTION;

-- Create test databases
CREATE DATABASE IF NOT EXISTS vsql_test;
CREATE DATABASE IF NOT EXISTS vsql_dev;

-- Verify VillageSQL schema was created
SELECT 'VillageSQL tables:' as '';
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'villagesql'
ORDER BY TABLE_NAME;

FLUSH PRIVILEGES;
EOF

# 5. Create connection helper script
cat > "$DATADIR/connect.sh" << EOF
#!/bin/bash
# Helper script to connect to the VillageSQL development database

echo "Connecting to VillageSQL MySQL..."
echo "User: $DEV_USER"
echo "Password: $DEV_PASS"
echo

"$BUILD_DIR/bin/mysql" --socket="$SOCKET" -u "$DEV_USER" -p"$DEV_PASS" "\$@"
EOF
chmod 0700 "$DATADIR/connect.sh"

# 6. Create shutdown script
cat > "$DATADIR/shutdown.sh" << EOF
#!/bin/bash
# Shutdown the VillageSQL development database

echo "Shutting down VillageSQL MySQL..."
"$BUILD_DIR/bin/mysqladmin" --socket="$SOCKET" -u root -p"$ROOT_PASS" shutdown
EOF
chmod 0700 "$DATADIR/shutdown.sh"

echo
echo "=== Setup Complete! ==="
echo
echo "Connection details:"
echo "  Socket: $SOCKET"
echo "  Port: $PORT"
echo "  Root password: $ROOT_PASS"
echo "  Dev user: $DEV_USER / $DEV_PASS"
echo
echo "Helper scripts created:"
echo "  Connect: $DATADIR/connect.sh"
echo "  Shutdown: $DATADIR/shutdown.sh"
echo
echo "To connect as developer:"
echo "  $DATADIR/connect.sh"
echo
echo "To connect as root:"
echo "  $BUILD_DIR/bin/mysql --socket=$SOCKET -u root -p$ROOT_PASS"
echo
echo "To shutdown:"
echo "  $DATADIR/shutdown.sh"
