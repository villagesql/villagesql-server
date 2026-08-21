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
#   for n in 1 2 3 4 5 6 7; do
#     gh workflow run full-test-suite.yml --ref main \
#       -f ref=dbentley/shard_mysql_9_7 \
#       -f artifact-prefix=shard${n}of7
#   done
#
# Any other artifact-prefix, including the default 'full-test' and an unset
# value, leaves this script behaving exactly as before. ARTIFACT_PREFIX is set
# only by full-test-suite.yml, so nightly.yml, sanitizer.yml, valgrind.yml,
# build.yml and build-server-branch.yml are unaffected.
#
# Seven shards, not six, because this branch has no measured baseline: no
# full-test-suite run has ever tested it. Shards 1-5 carry the suites whose
# weights are known from run #107 on main, balanced on max of
# test-seconds/(32*0.55), test-count/57 and longest-test -- the model fitted to
# a measured 6-shard round, where balancing on test-seconds alone produced a
# 24-63 minute spread. main's count is scaled 1385 -> 1541 for this branch.
#
# Shard 6 is a CATCH-ALL: --suite=all minus shards 1-5 and village. It exists
# because 9.7 adds suites that have never been measured here -- classic_hashing,
# component_connection_control, component_telemetry, jdv, router, thread_pool,
# thread_pool_noload, thread_pool_rpl, around 332 test files between them, plus
# the ndb suites that collect nothing in this build. Listing them explicitly
# would mean guessing both their names and their cost; a catch-all cannot lose
# coverage and cannot name a suite that does not exist. Its wall time is
# genuinely unknown for this first round.
#
# Shard 7 owns phases 1 and 2 alone -- village, big tests and unit tests -- and
# runs no phase-3 suites. In the measured percona round the village shard also
# carried a suite group, and when village failed the group was skipped, losing
# its coverage entirely. Isolating village costs one runner and roughly nothing
# in wall time, since village is about 8 minutes against a ~30 minute shard.
#
# Suite lists use --suite (exact comma-separated names) and not --do-suite,
# which init_pattern() in mysql-test/lib/mtr_cases.pm turns into an unanchored
# regex. The catch-all's --skip-suite value is a Perl REGEX, not a list: a
# comma-separated value there matches nothing at all and would silently skip
# nothing, so alternation is spelled '|' and each name anchored with ^...$. It
# is single-quoted because MYSQL_TEST_CMD is run through eval, where a bare '|'
# would be read as a shell pipe.
SHARD=""
SHARD_SUITES=""
SHARD_SKIP_RE=""
VILLAGE_SHARD=7
CATCHALL_SHARD=6

if [[ "${ARTIFACT_PREFIX:-}" =~ shard([0-9]+)of([0-9]+) ]]; then
  SHARD="${BASH_REMATCH[1]}"
  SHARD_TOTAL="${BASH_REMATCH[2]}"

  if [ "$SHARD_TOTAL" != "7" ]; then
    echo "ERROR: only a 7-way split is defined; got shard${SHARD}of${SHARD_TOTAL}"
    exit 1
  fi

  case "$SHARD" in
    1) SHARD_SUITES="audit_null,connection_control,information_schema,innodb_stress,interactive_utilities,lock_order,main,max_parts,network_namespace,parts/special_tests,service_status_var_registration,service_sys_var_registration,service_udf_registration,special,test_services" ;;
    2) SHARD_SUITES="auth_sec,clone,collations,encryption,gcol,innodb,jp,opt_trace,test_service_sql_api,x" ;;
    3) SHARD_SUITES="binlog,engines/funcs,funcs_1,rpl" ;;
    4) SHARD_SUITES="component_keyring_file,federated,funcs_2,innodb_fts,innodb_gis,innodb_zip,json,large_tests,parts,perfschema,query_rewrite_plugins,rpl_gtid,rpl_nogtid,secondary_engine" ;;
    5) SHARD_SUITES="binlog_gtid,binlog_nogtid,engines/iuds,gis,group_replication,innodb_undo,stress,sys_vars,sysschema" ;;
    6) SHARD_SKIP_RE='^village|^(audit_null|auth_sec|binlog|binlog_gtid|binlog_nogtid|clone|collations|component_keyring_file|connection_control|encryption|engines/funcs|engines/iuds|federated|funcs_1|funcs_2|gcol|gis|group_replication|information_schema|innodb|innodb_fts|innodb_gis|innodb_stress|innodb_undo|innodb_zip|interactive_utilities|jp|json|large_tests|lock_order|main|max_parts|network_namespace|opt_trace|parts|parts/special_tests|perfschema|query_rewrite_plugins|rpl|rpl_gtid|rpl_nogtid|secondary_engine|service_status_var_registration|service_sys_var_registration|service_udf_registration|special|stress|sys_vars|sysschema|test_service_sql_api|test_services|x)$' ;;
    7) : ;;
    *)
      echo "ERROR: shard $SHARD is outside the 1..7 range"
      exit 1
      ;;
  esac

  echo "=== Shard ${SHARD} of ${SHARD_TOTAL} (from ARTIFACT_PREFIX='${ARTIFACT_PREFIX}') ==="

  # Phases 1 and 2 do not pass --all-suites. Only the village shard runs them.
  if [ "$RUN_ALL_SUITES" != "true" ] && [ "$SHARD" != "$VILLAGE_SHARD" ]; then
    echo "Shard ${SHARD} skips village/big/unit tests (shard ${VILLAGE_SHARD} owns them)"
    exit 0
  fi

  # Conversely the village shard runs no phase-3 suites.
  if [ "$RUN_ALL_SUITES" = "true" ] && [ "$SHARD" = "$VILLAGE_SHARD" ]; then
    echo "Shard ${SHARD} runs village only; skipping the phase-3 suites"
    exit 0
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

  # Add skip suite if specified. The catch-all shard replaces the plain
  # 'village' value with a regex excluding village and every suite already
  # claimed by shards 1-5.
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
