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
SOURCE_DIR="${SOURCE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
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

# TEMPORARY: repeat one test many times, to characterise a flaky failure.
#
# Not long-term infrastructure. Same lever as the six-way sharding: the workflow
# file .github/workflows/full-test-suite.yml is read from the ref the run is
# DISPATCHED on, not the ref being tested, so it cannot be changed from this
# branch. It exposes only ref/build-type/artifact-prefix, and artifact-prefix is
# the one input that reaches this script -- as $ARTIFACT_PREFIX -- so it doubles
# as the mode selector. Dispatch once:
#
#   gh workflow run full-test-suite.yml --ref main \
#     -f ref=<branch> -f artifact-prefix=repeat100
#
# The point is to amortise the ~20 minute build over many executions of a single
# test, rather than paying that build for one data point.
#
# The workflow calls this script three times. In repeat mode those three calls
# become two experiment slots plus a no-op, which is what lets one build answer
# both questions that matter for a timing-sensitive test:
#
#   call 1  (unit + --do-suite=village)          -> SERIAL     --parallel=1
#   call 2  (--only-big-test)                    -> CONCURRENT --parallel=auto
#   call 3  (--suite=all --skip-suite=village)   -> skipped, exits 0
#
# Serial on an otherwise idle runner is the clean baseline: if the test fails
# there, machine load is not the explanation. Concurrent runs the same repeats
# across all mtr workers, so many copies of the test compete for CPU, disk and
# scheduler the way they do in a wide run -- which is the condition the original
# failure was observed under. Their logs land in separate artifacts already,
# because the workflow gives each call its own artifact name.
#
# repeat mode deliberately does NOT fail the job on a test failure. We are
# collecting a pass/fail distribution, not gating a merge, and exiting non-zero
# on call 1 would make the workflow skip call 2 and throw away the comparison.
# Read the tally line, and the per-execution results, from the step log.
#
# Any other artifact-prefix, including the default 'full-test' and an unset
# value, leaves this script behaving exactly as before. ARTIFACT_PREFIX is set
# only by full-test-suite.yml, so nightly.yml, sanitizer.yml, valgrind.yml,
# build.yml and build-server-branch.yml are unaffected.
# Overridable so the lever can be pointed at a different test without editing this
# file, and so its failure path can be exercised locally.
REPEAT_TEST="${REPEAT_TEST:-main.show_check_cs main.show_check_cs_myisam main.show_check_ci main.show_check_ci_myisam}"
if [[ "${ARTIFACT_PREFIX:-}" =~ ^repeat([0-9]*)$ ]]; then
  REPEAT_N="${BASH_REMATCH[1]}"
  REPEAT_N="${REPEAT_N:-100}"

  # Identify which of the workflow's three calls this is, from the flags it
  # passed, and pick the mode. Call 3 is the only one with --all-suites.
  if [ "$RUN_ALL_SUITES" = "true" ]; then
    echo "=== repeat mode (${ARTIFACT_PREFIX}): nothing to do for the all-suites call ==="
    exit 0
  elif [ "$RUN_BIG_TESTS" = "true" ]; then
    REPEAT_MODE="concurrent"
    REPEAT_PARALLEL="auto"
  else
    REPEAT_MODE="serial"
    REPEAT_PARALLEL="1"
  fi

  cd "$BUILD_DIR"

  # --force and --max-test-fail=0 so every execution runs and we see the whole
  # distribution instead of stopping at the first failure. --mem matches what
  # the wide run used, since vardir on tmpfs changes I/O timing and this test is
  # timing-sensitive. No --skip-test-list: it must not be able to skip the very
  # test we are here to run.
  REPEAT_CMD="./mysql-test/mysql-test-run.pl"
  REPEAT_CMD="$REPEAT_CMD --mem"
  REPEAT_CMD="$REPEAT_CMD --parallel=${REPEAT_PARALLEL}"
  REPEAT_CMD="$REPEAT_CMD --nounit-tests"
  REPEAT_CMD="$REPEAT_CMD --force"
  REPEAT_CMD="$REPEAT_CMD --max-test-fail=0"
  REPEAT_CMD="$REPEAT_CMD --repeat=${REPEAT_N}"
  REPEAT_CMD="$REPEAT_CMD --xml-report=${BUILD_DIR}/mysql-test-report.xml"
  REPEAT_CMD="$REPEAT_CMD ${REPEAT_TEST}"

  echo "=== repeat mode (${ARTIFACT_PREFIX}) ==="
  echo "  test:        ${REPEAT_TEST}"
  echo "  executions:  ${REPEAT_N}"
  echo "  mode:        ${REPEAT_MODE} (--parallel=${REPEAT_PARALLEL})"
  echo "Running: $REPEAT_CMD"

  set +e
  eval $REPEAT_CMD
  REPEAT_RC=$?
  set -e

  echo "=== repeat mode (${ARTIFACT_PREFIX}) finished: mode=${REPEAT_MODE}" \
       "executions=${REPEAT_N} mtr_exit=${REPEAT_RC} ==="
  echo "mtr_exit 0 means every execution passed. Non-zero means at least one did" \
       "not; grep the log above for '[ fail ]' and for 'interpolated' to see the" \
       "asserted values, and note repeat mode does not fail the job."
  exit 0
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

cd "$BUILD_DIR"

# Run VillageSQL unit tests
if [ "$RUN_UNIT_TESTS" = "true" ]; then
  echo "=== Running VillageSQL Unit Tests ==="
  ctest -L villagesql --output-on-failure
fi

# Run VillageSQL integration tests
if [ "$RUN_INTEGRATION_TESTS" = "true" ]; then
  echo "=== Running VillageSQL Integration Tests ==="

  # Build mysql-test command with appropriate flags
  XML_REPORT_FILE="${BUILD_DIR}/mysql-test-report.xml"
  MYSQL_TEST_CMD="./mysql-test/mysql-test-run.pl"
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --mem"
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

  # Add any extra flags
  if [ -n "$MYSQL_TEST_EXTRA_FLAGS" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD $MYSQL_TEST_EXTRA_FLAGS"
  fi

  echo "Running: $MYSQL_TEST_CMD"
  eval $MYSQL_TEST_CMD
fi

echo "=== All Tests Passed ==="
