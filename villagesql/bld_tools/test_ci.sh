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

# Tests that cannot pass against a build_ci.sh binary because the feature they
# exercise is compiled out, and that have no guard of their own to notice.
# Resolved to an absolute path from this script's own location: MTR searches
# several directories for a bare filename, and callers may point SOURCE_DIR
# somewhere else entirely.
SKIP_TEST_LIST="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/mysql-test/collections/disabled-villagesql-ci.list"

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
#       -f ref=dbentley/shard_percona_1031_candidate \
#       -f artifact-prefix=shard${n}of5
#   done
#
# Any other artifact-prefix, including the default 'full-test' and an unset
# value, leaves this script behaving exactly as before. ARTIFACT_PREFIX is set
# only by full-test-suite.yml, so nightly.yml, sanitizer.yml, valgrind.yml,
# build.yml and build-server-branch.yml are unaffected.
#
# The workflow calls this script three times and the shards divide that work:
#   phase 1  unit tests + --do-suite=village          only the village shard
#   phase 2  --do-suite=village --only-big-test       only the village shard
#   phase 3  --suite=all --skip-suite='^village|^junit'   split five ways below
#
# Groups are balanced on a cost model fitted to a measured six-shard round
# (runs 32499112824..32499120064). Summed test-seconds was tried first and is
# the wrong quantity: those groups came out within 14 percent of each other on
# test-seconds, 27.3k-31.1k, yet their wall times ranged 24.2 to 63.5 minutes.
# Test count predicts wall time far better -- the four shards that ran clean all
# landed between 45 and 63 tests per minute, mean 57 -- because wall time is set
# by how many tests must be scheduled through 32 workers rather than by their
# summed duration. A group's cost is the max of three bounds:
# test-seconds/(32*0.55), test-count/57, and its longest single test.
#
# Five shards, not six: six comes out at 29.5 minutes a shard against 35.4 here,
# so this trades about six minutes per shard for one fewer runner. main is no
# longer alone on a shard at this width.
#
# Phases 1 and 2 cost 9 minutes of nearly incompressible wall time -- 668 tests
# but a 321s longest -- so the village shard carries a lighter phase-3 group.
#
# max_parts and percona_innodb are deliberately on different shards. In the
# measured round both landed on shard 5 and nine of their tests each hung for
# the full 900s timeout, which alone turned a ~26 minute shard into 63.5. That
# splits the exposure; it does not fix the hangs.
#
# Suite lists use --suite (exact comma-separated names) and not --do-suite,
# which init_pattern() in mysql-test/lib/mtr_cases.pm turns into an unanchored
# regex. On the shard path --skip-suite is dropped entirely, so the junit
# exclusion comes from these lists simply not naming it. Keep it that way:
# junit's tests crash, hang and abort on purpose. Never make a shard a
# --suite=all catch-all here.
SHARD=""
SHARD_SUITES=""
VILLAGE_SHARD=5

if [[ "${ARTIFACT_PREFIX:-}" =~ shard([0-9]+)of([0-9]+) ]]; then
  SHARD="${BASH_REMATCH[1]}"
  SHARD_TOTAL="${BASH_REMATCH[2]}"

  if [ "$SHARD_TOTAL" != "5" ]; then
    echo "ERROR: only a 5-way split is defined; got shard${SHARD}of${SHARD_TOTAL}"
    exit 1
  fi

  case "$SHARD" in
    1) SHARD_SUITES="clone,component_keyring_kms,federated,funcs_1,information_schema,main,parts,service_sys_var_registration,x" ;;
    2) SHARD_SUITES="audit_null,binlog,binlog_gtid,collations,component_js_lang,component_masking_functions,component_percona_telemetry,connection_control,encryption,gcol,innodb,innodb_gis,innodb_stress,innodb_zip,interactive_utilities,json,lock_order,max_parts,network_namespace,parts/special_tests,percona,percona_rpl,query_rewrite_plugins,rocksdb_clone,rocksdb_stress,rpl_nogtid,service_status_var_registration,service_udf_registration,test_services" ;;
    3) SHARD_SUITES="binlog_nogtid,component_audit_log_filter,component_encryption_udf,group_replication,percona_innodb,rocksdb_rpl,rpl" ;;
    4) SHARD_SUITES="component_keyring_kmip,gis,opt_trace,percona_rpl_gtid,perfschema,rocksdb_sys_vars,rpl_encryption,sys_vars,sysschema" ;;
    5) SHARD_SUITES="auth_sec,component_keyring_file,component_keyring_vault,engines/funcs,engines/iuds,funcs_2,innodb_fts,innodb_undo,jp,large_tests,percona_binlog,rocksdb,rpl_gtid,secondary_engine,stress,test_service_sql_api" ;;
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

  # The shard's suite list stands in for --suite=all only. Phases 1 and 2 (only
  # the village shard reaches here) keep their own --do-suite=village.
  if [ "$RUN_ALL_SUITES" != "true" ]; then
    SHARD_SUITES=""
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
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --skip-test-list=${SKIP_TEST_LIST}"
  MYSQL_TEST_CMD="$MYSQL_TEST_CMD --xml-report=${XML_REPORT_FILE}"

  # Add suite selection
  if [ -n "$SHARD_SUITES" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --suite=${SHARD_SUITES}"
  elif [ "$RUN_ALL_SUITES" = "true" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --suite=all"
  else
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --do-suite=${TEST_SUITE}"
  fi

  # Add skip suite if specified, and always exclude the junit suites.
  #
  # Done here rather than by passing a different skip-suite from
  # .github/workflows/full-test-suite.yml, because that workflow file is read
  # from the ref the run is DISPATCHED on (main), not from the ref being tested.
  # Editing it on this branch would have no effect. This script and
  # .github/actions/run-tests are both taken from the checked-out source, so a
  # change here does take effect.
  #
  # Two traps in the value itself:
  #   - --skip-suite takes a Perl REGEX, not a comma-separated list.
  #     "village,junit" matches no suite name and silently skips NOTHING --
  #     including re-enabling village. Alternation is spelled '|'.
  #   - MYSQL_TEST_CMD is run through `eval` below, where a bare '|' would be
  #     read as a shell pipe. Hence the single quotes.
  # Anchored with ^ so each alternative matches a suite name prefix; '^junit'
  # also covers junit_combinations.
  #
  # Why junit: its tests crash, hang, mismatch and leak ON PURPOSE, to exercise
  # MTR's own junit XML reporting -- the server carries matching self-destruct
  # hooks (DBUG_SUICIDE, DBUG_ABORT, trigger_buffer_overrun) inside #ifndef
  # NDEBUG. They cannot pass in a wide run and never should have been in one.
  # The suite comes from Percona and does not exist on origin/main.
  if [ -n "$SKIP_SUITE" ] && [ -z "$SHARD_SUITES" ]; then
    MYSQL_TEST_CMD="$MYSQL_TEST_CMD --skip-suite='^${SKIP_SUITE}|^junit'"
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
