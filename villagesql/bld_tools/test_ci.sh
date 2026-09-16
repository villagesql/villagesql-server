#!/bin/bash

# VillageSQL CI Test Script
# Runs unit tests and integration tests for CI environments
# Configurable via command-line flags

set -e

# Show usage
show_usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Run VillageSQL CI tests with configurable options.

OPTIONS:
  --unit-tests              Run unit tests (default: true)
  --no-unit-tests           Skip unit tests
  --integration-tests       Run integration tests (default: true)
  --no-integration-tests    Skip integration tests
  --big-tests               Include big tests (default: false)
  --all-suites              Run all test suites (default: false)
  --suite=SUITE             Specify test suite to run (default: village)
  --skip-suite=SUITE        Skip specific test suite (requires --all-suites)
  --mysql-test-extra-flags="FLAGS"  Additional flags for mysql-test-run.pl
  --valgrind                Run unit and integration tests under Valgrind
                            memcheck (default: false). Requires a build
                            configured with -DWITH_VALGRIND=1.
  --help                    Show this help message

ENVIRONMENT VARIABLES:
  SOURCE_DIR    Path to source directory (default: auto-detect)
  BUILD_DIR     Path to build directory (default: SOURCE_DIR/../build)

EXAMPLES:
  # Run default tests (unit + village integration)
  $0

  # Run big tests only
  $0 --no-unit-tests --big-tests

  # Run all suites except village
  $0 --no-unit-tests --all-suites --skip-suite=village

  # Run specific suite with extra flags
  $0 --suite=innodb --mysql-test-extra-flags="--verbose"

  # Run everything under Valgrind memcheck
  $0 --valgrind
EOF
}

# Default values
RUN_UNIT_TESTS=true
RUN_INTEGRATION_TESTS=true
RUN_BIG_TESTS=false
RUN_ALL_SUITES=false
TEST_SUITE=""
SKIP_SUITE=""
MYSQL_TEST_EXTRA_FLAGS=""
RUN_VALGRIND=false

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --unit-tests)
      RUN_UNIT_TESTS=true
      shift
      ;;
    --no-unit-tests)
      RUN_UNIT_TESTS=false
      shift
      ;;
    --integration-tests)
      RUN_INTEGRATION_TESTS=true
      shift
      ;;
    --no-integration-tests)
      RUN_INTEGRATION_TESTS=false
      shift
      ;;
    --big-tests)
      RUN_BIG_TESTS=true
      shift
      ;;
    --all-suites)
      RUN_ALL_SUITES=true
      shift
      ;;
    --suite=*)
      TEST_SUITE="${1#*=}"
      shift
      ;;
    --skip-suite=*)
      SKIP_SUITE="${1#*=}"
      shift
      ;;
    --mysql-test-extra-flags=*)
      MYSQL_TEST_EXTRA_FLAGS="${1#*=}"
      shift
      ;;
    --valgrind)
      RUN_VALGRIND=true
      shift
      ;;
    --help)
      show_usage
      exit 0
      ;;
    *)
      echo "ERROR: Unknown option: $1"
      echo "Run '$0 --help' for usage information"
      exit 1
      ;;
  esac
done

# Apply default for TEST_SUITE if not specified
TEST_SUITE="${TEST_SUITE:-village}"

# Use SOURCE_DIR/BUILD_DIR from environment, or auto-detect
SOURCE_DIR="${SOURCE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
BUILD_DIR="${BUILD_DIR:-${SOURCE_DIR}/../build}"

# Validate suite selection options
if [ "$RUN_ALL_SUITES" = "true" ] && [ "$TEST_SUITE" != "village" ]; then
  echo "ERROR: Cannot specify both --all-suites and --suite"
  echo "  Either use --all-suites (optionally with --skip-suite)"
  echo "  Or use --suite=<suite-name>"
  exit 1
fi

if [ -n "$SKIP_SUITE" ] && [ "$RUN_ALL_SUITES" != "true" ]; then
  echo "ERROR: --skip-suite can only be used with --all-suites"
  exit 1
fi

echo "=== VillageSQL CI Tests ==="
echo "Working directory: $(pwd)"

echo "Build directory: ${BUILD_DIR}"
echo "Test configuration:"
echo "  RUN_UNIT_TESTS: ${RUN_UNIT_TESTS}"
echo "  RUN_INTEGRATION_TESTS: ${RUN_INTEGRATION_TESTS}"
echo "  RUN_BIG_TESTS: ${RUN_BIG_TESTS}"
echo "  RUN_ALL_SUITES: ${RUN_ALL_SUITES}"
echo "  TEST_SUITE: ${TEST_SUITE}"
echo "  SKIP_SUITE: ${SKIP_SUITE}"
echo "  MYSQL_TEST_EXTRA_FLAGS: ${MYSQL_TEST_EXTRA_FLAGS}"
echo "  RUN_VALGRIND: ${RUN_VALGRIND}"

# Locate valgrind before running anything, so a missing binary fails fast.
if [ "$RUN_VALGRIND" = "true" ]; then
  VALGRIND_BIN="$(command -v valgrind || true)"
  if [ -z "$VALGRIND_BIN" ]; then
    echo "ERROR: --valgrind requested but valgrind is not on PATH"
    exit 1
  fi
  VALGRIND_SUPP="${SOURCE_DIR}/mysql-test/valgrind.supp"
  if [ ! -f "$VALGRIND_SUPP" ]; then
    echo "WARNING: valgrind.supp not found; unit tests will run unsuppressed"
    VALGRIND_SUPP=""
  fi
  echo "  VALGRIND_BIN: ${VALGRIND_BIN}"
  echo "  VALGRIND_SUPP: ${VALGRIND_SUPP:-<none>}"
fi

cd "$BUILD_DIR"

# Run VillageSQL unit tests
if [ "$RUN_UNIT_TESTS" = "true" ]; then
  echo "=== Running VillageSQL Unit Tests ==="
  CTEST_ARGS=(-L villagesql --output-on-failure)

  # --error-exitcode makes valgrind fail the test process itself, so ctest's
  # exit code reflects memcheck defects. Its own defect counter does not.
  if [ "$RUN_VALGRIND" = "true" ]; then
    CTEST_ARGS+=(-T memcheck
      --overwrite "MemoryCheckCommand=${VALGRIND_BIN}"
      --overwrite "MemoryCheckCommandOptions=--tool=memcheck --leak-check=full --track-origins=yes --num-callers=16 --error-exitcode=1")
    if [ -n "$VALGRIND_SUPP" ]; then
      CTEST_ARGS+=(--overwrite "MemoryCheckSuppressionFile=${VALGRIND_SUPP}")
    fi
  fi

  ctest "${CTEST_ARGS[@]}"
fi

# Run VillageSQL integration tests
if [ "$RUN_INTEGRATION_TESTS" = "true" ]; then
  echo "=== Running VillageSQL Integration Tests ==="

  # Build mysql-test command with appropriate flags
  XML_REPORT_FILE="${BUILD_DIR}/mysql-test-report.xml"
  MYSQL_TEST_CMD="./mysql-test/mysql-test-run.pl"
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --mem"
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --report-unstable-tests"
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --parallel=auto"
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --nounit-tests"
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --force"
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --max-test-fail=0"
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --xml-report=${XML_REPORT_FILE}"

  # Add suite selection
  if [ "$RUN_ALL_SUITES" = "true" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --suite=all"
  else
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --do-suite=${TEST_SUITE}"
  fi

  # Add skip suite if specified
  if [ -n "$SKIP_SUITE" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --skip-suite=${SKIP_SUITE}"
  fi

  # Add big test flag if requested
  if [ "$RUN_BIG_TESTS" = "true" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --only-big-test"
  fi

  # Add valgrind flags if requested. This has to be --valgrind (all
  # executables) rather than --valgrind-mysqld: mysql-test-run.pl escalates to
  # all-executables as soon as any --valgrind-option is given, and only that
  # branch scales the start/suite timeouts mysqld needs under memcheck.
  if [ "$RUN_VALGRIND" = "true" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --valgrind"
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --valgrind-option=--track-origins=yes"
  fi

  # Add any extra flags
  if [ -n "$MYSQL_TEST_EXTRA_FLAGS" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD $MYSQL_TEST_EXTRA_FLAGS"
  fi

  echo "Running: $MYSQL_TEST_CMD"
  eval $MYSQL_TEST_CMD
fi

echo "=== All Tests Passed ==="
