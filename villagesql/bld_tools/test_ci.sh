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
  --dry-run                 Print what would run, then exit without running
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
DRY_RUN=false

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
    --dry-run)
      DRY_RUN=true
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

# TEMPORARY: manual sharding of the full test suite across parallel dispatches.
#
# full-test-suite.yml is read from the ref the run is DISPATCHED on, not the ref
# being tested, and it exposes only ref/build-type/artifact-prefix.
# artifact-prefix is therefore the one input that reaches this script, arriving
# as $ARTIFACT_PREFIX, so it doubles as the shard selector:
#
#   for n in 1 2 3 4 5; do
#     gh workflow run full-test-suite.yml --ref main \
#       -f ref=dbentley/shard_9_7_1031_candidate \
#       -f artifact-prefix=shard${n}of5
#   done
#
# Any other artifact-prefix, including the default 'full-test' and an unset
# value, leaves this script behaving exactly as before. ARTIFACT_PREFIX is set
# only by full-test-suite.yml, so nightly.yml, sanitizer.yml, valgrind.yml,
# build.yml and build-server-branch.yml are unaffected.
#
# Shards 1-4 carry explicit suite lists. Shard 5 is the REMAINDER: --suite=all
# minus village and minus everything shards 1-4 claim. That matters because no
# full-test-suite run has ever tested the 9.7 chain, so several of its suites
# have no measured weight at all -- classic_hashing, component_connection_control,
# component_telemetry, jdv, router, thread_pool, thread_pool_noload,
# thread_pool_rpl, about 332 test files between them -- plus the ndb suites,
# which collect nothing in this build. Naming them explicitly would mean
# guessing both their names and their cost, and a wrong name is a run-time
# error while a missing one is silently lost coverage. A remainder shard can be
# neither wrong nor incomplete.
#
# Shard 5 also owns phases 1 and 2, village and the unit tests.
#
# The four explicit groups are balanced on weights from run #107 on main, using
# the cost model fitted to a measured six-shard round: the max of
# test-seconds/(32*0.55), test-count/57, and the group's longest single test.
# Balancing on summed test-seconds alone was tried there and did not work --
# groups within 14 percent on test-seconds ranged 24.2 to 63.5 minutes of wall
# time -- because wall time is set by how many tests must be scheduled through
# 32 workers, not by their summed duration. main's count is scaled 1385 -> 1541
# for this branch. Shard 5 is given a 13.8 minute allowance for village plus
# the unmeasured suites, so the five come out at 33.4-33.8 minutes each.
#
# Suite lists use --suite (exact comma-separated names) and not --do-suite,
# which init_pattern() in mysql-test/lib/mtr_cases.pm turns into an unanchored
# regex. Shard 5's --skip-suite value is likewise a Perl REGEX and not a list: a
# comma-separated value there matches no suite name at all and would silently
# skip nothing, so alternation is spelled '|' with each name anchored ^...$, and
# the whole value single-quoted because MYSQL_TEST_CMD goes through eval, where
# a bare '|' would be read as a shell pipe.
SHARD=""
SHARD_SUITES=""
SHARD_SKIP_RE=""
VILLAGE_SHARD=5
REMAINDER_SHARD=5

if [[ "${ARTIFACT_PREFIX:-}" =~ shard([0-9]+)of([0-9]+) ]]; then
  SHARD="${BASH_REMATCH[1]}"
  SHARD_TOTAL="${BASH_REMATCH[2]}"

  if [ "$SHARD_TOTAL" != "5" ]; then
    echo "ERROR: only a 5-way split is defined; got shard${SHARD}of${SHARD_TOTAL}"
    exit 1
  fi

  case "$SHARD" in
    1) SHARD_SUITES="audit_null,binlog_gtid,binlog_nogtid,gcol,information_schema,innodb_stress,innodb_zip,interactive_utilities,main,max_parts,network_namespace,secondary_engine,service_udf_registration,special,stress,test_services" ;;
    2) SHARD_SUITES="auth_sec,collations,component_keyring_file,engines/funcs,federated,innodb,jp,json,query_rewrite_plugins,test_service_sql_api" ;;
    3) SHARD_SUITES="funcs_2,gis,innodb_fts,innodb_gis,parts,rpl,rpl_nogtid,x" ;;
    4) SHARD_SUITES="binlog,connection_control,innodb_undo,large_tests,lock_order,parts/special_tests,perfschema,service_status_var_registration,service_sys_var_registration,sys_vars,sysschema" ;;
    5) SHARD_SKIP_RE='^village|^(audit_null|auth_sec|binlog|binlog_gtid|binlog_nogtid|collations|component_keyring_file|connection_control|engines/funcs|federated|funcs_2|gcol|gis|information_schema|innodb|innodb_fts|innodb_gis|innodb_stress|innodb_undo|innodb_zip|interactive_utilities|jp|json|large_tests|lock_order|main|max_parts|network_namespace|parts|parts/special_tests|perfschema|query_rewrite_plugins|rpl|rpl_nogtid|secondary_engine|service_status_var_registration|service_sys_var_registration|service_udf_registration|special|stress|sys_vars|sysschema|test_service_sql_api|test_services|x)$' ;;
    *)
      echo "ERROR: shard $SHARD is outside the 1..5 range"
      exit 1
      ;;
  esac

  echo "=== Shard ${SHARD} of ${SHARD_TOTAL} (from ARTIFACT_PREFIX='${ARTIFACT_PREFIX}') ==="

  # Phases 1 and 2 do not pass --all-suites. Only the village shard runs them.
  if [ "$RUN_ALL_SUITES" != "true" ] && [ "$SHARD" != "$VILLAGE_SHARD" ]; then
    echo "Shard ${SHARD} skips village/big/unit tests (shard ${VILLAGE_SHARD} owns them)"
    exit 0
  fi

  # The shard selection stands in for --suite=all only. Phases 1 and 2 (only the
  # village shard reaches here) keep their own --do-suite=village.
  if [ "$RUN_ALL_SUITES" != "true" ]; then
    SHARD_SUITES=""
    SHARD_SKIP_RE=""
  fi
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
echo "  SHARD: ${SHARD:-none}"
echo "  SHARD_SUITES: ${SHARD_SUITES:-none}"
echo "  SHARD_SKIP_RE: ${SHARD_SKIP_RE:-none}"

cd "$BUILD_DIR"

# Run VillageSQL unit tests
if [ "$RUN_UNIT_TESTS" = "true" ]; then
  echo "=== Running VillageSQL Unit Tests ==="
  if [ "$DRY_RUN" = "true" ]; then
    echo "DRY RUN: ctest -L villagesql --output-on-failure"
  else
    ctest -L villagesql --output-on-failure
  fi
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
  if [ -n "$SHARD_SUITES" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --suite=${SHARD_SUITES}"
  elif [ "$RUN_ALL_SUITES" = "true" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --suite=all"
  else
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --do-suite=${TEST_SUITE}"
  fi

  # Add skip suite if specified. The remainder shard replaces the plain
  # 'village' value with a regex excluding village and everything shards 1-4
  # already claim.
  if [ -n "$SHARD_SKIP_RE" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --skip-suite='${SHARD_SKIP_RE}'"
  elif [ -n "$SKIP_SUITE" ] && [ -z "$SHARD_SUITES" ]; then
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
  if [ "$DRY_RUN" != "true" ]; then
    eval $MYSQL_TEST_CMD
  fi
fi

echo "=== All Tests Passed ==="
