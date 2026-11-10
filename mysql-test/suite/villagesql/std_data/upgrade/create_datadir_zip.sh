#!/bin/bash
set -e

# Script to create a zipped MySQL datadir for upgrade testing
# Run from a build directory after building the target version

VERSION=""
LOWER_CASE_TABLE_NAMES=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --version)
      VERSION="$2"
      shift 2
      ;;
    --lower_case_table_names)
      LOWER_CASE_TABLE_NAMES="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 --version <VERSION> --lower_case_table_names <0|1|2>"
      exit 1
      ;;
  esac
done

# Validate arguments
if [ -z "$VERSION" ]; then
  echo "ERROR: --version is required"
  echo "Usage: $0 --version <VERSION> --lower_case_table_names <0|1|2>"
  exit 1
fi

if [ -z "$LOWER_CASE_TABLE_NAMES" ]; then
  echo "ERROR: --lower_case_table_names is required"
  echo "Usage: $0 --version <VERSION> --lower_case_table_names <0|1|2>"
  exit 1
fi

if [[ ! "$LOWER_CASE_TABLE_NAMES" =~ ^[0-2]$ ]]; then
  echo "ERROR: --lower_case_table_names must be 0, 1, or 2"
  exit 1
fi

# Check we're in a build directory
if [ ! -f "./CMakeCache.txt" ]; then
  echo "ERROR: CMakeCache.txt not found. Are you in a build directory?"
  exit 1
fi

# Check if datadir already exists (before building!)
if [ -d "/tmp/villagesql_${VERSION}_test" ]; then
  echo "ERROR: /tmp/villagesql_${VERSION}_test already exists!"
  echo "Please remove it manually first:"
  echo "  rm -rf /tmp/villagesql_${VERSION}_test"
  exit 1
fi

# Build
make

echo "Creating datadir zip for version: $VERSION"

# Create bootstrap file (creates test database - required by test framework)
BOOTSTRAP_FILE="$(pwd)/bootstrap.sql"
cat > "$BOOTSTRAP_FILE" <<'EOF'
CREATE DATABASE test;
EOF

# Initialize the datadir
echo "Initializing datadir..."
./bin/mysqld --no-defaults --initialize-insecure \
  --datadir=/tmp/villagesql_${VERSION}_test \
  --basedir=. \
  --lower_case_table_names=${LOWER_CASE_TABLE_NAMES} \
  --init-file="$BOOTSTRAP_FILE"

# Check for runtime files (indicates unclean shutdown)
if ls /tmp/villagesql_${VERSION}_test/*.{log,err,pid,sock} 2>/dev/null; then
  echo "ERROR: Runtime files found in datadir! Shutdown was not clean."
  echo "Do not continue. Clean up and try again."
  exit 1
fi

echo "Datadir initialized successfully."

# Create the zip file
echo "Creating zip file..."
(cd /tmp && zip -r ${VERSION}_lctn${LOWER_CASE_TABLE_NAMES}.zip villagesql_${VERSION}_test/)

# Verify the zip doesn't contain runtime files
if unzip -l /tmp/${VERSION}_lctn${LOWER_CASE_TABLE_NAMES}.zip | grep -qE "\.log|\.err|\.pid"; then
  echo "ERROR: Zip contains runtime files (.log, .err, .pid)!"
  echo "This should not happen. Something went wrong."
  exit 1
fi

# Clean up the datadir
echo "Cleaning up datadir..."
rm -rf /tmp/villagesql_${VERSION}_test

# Success!
echo ""
echo "✓ Success! Created: /tmp/${VERSION}_lctn${LOWER_CASE_TABLE_NAMES}.zip"
echo ""
echo "To use in tests, run from source directory:"
echo "  cp /tmp/${VERSION}_lctn${LOWER_CASE_TABLE_NAMES}.zip mysql-test/suite/villagesql/std_data/upgrade/"
echo ""
