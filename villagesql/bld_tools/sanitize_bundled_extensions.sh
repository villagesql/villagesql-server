#!/usr/bin/env bash
# Build every bundled extension with sanitizers and run its MTR suite under
# --sanitize, against an already-built sanitized server.
#
# Used only by .github/workflows/sanitizer.yml, so the sanitizer specifics stay
# out of the shared build/test scripts and reusable workflows. Differences from
# the shipping bundled build (build_bundled_extensions.sh):
#   - Does NOT skip bundle=false extensions; vsql-vector and other compat-only
#     extensions get sanitized too.
#   - Builds each VEB with the sanitizer flags below so ASan/UBSan see the
#     extension's own code once the server dlopen's it (an uninstrumented .so
#     gives UBSan no coverage and loses ASan's stack/global checks).
#   - Runs MTR with --sanitize.
#
# The flag set lives in SAN_CFLAGS/SAN_LDFLAGS; repoint them (e.g. at tsan) to
# reuse this driver for a different sanitizer.
#
# Usage: sanitize_bundled_extensions.sh <build_dir> [extension]
#   <build_dir>: VillageSQL build dir. mysqld (runtime_output_directory/mysqld)
#                and the SDK staging dir must already exist from a sanitized
#                build (build_ci.sh with -DWITH_ASAN=1 -DWITH_UBSAN=1).
#   [extension]: Optional repo name to limit to one extension (e.g. vsql-vector).

set -uo pipefail

BUILD_DIR="${1:?Usage: $0 <build_dir> [extension]}"
EXTENSION_FILTER="${2:-}"

# Match sanitizer.yml's server build (ASan + UBSan). The extension links the
# runtime already loaded by the instrumented server, so no -shared-libasan.
SAN_CFLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer"
SAN_LDFLAGS="-fsanitize=address,undefined"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"
source "$SOURCE_DIR/scripts/vsql_script_utils.sh"

EXTENSIONS_LIST="$SOURCE_DIR/villagesql/dev_server/bundled_extensions.txt"
[[ -f "$EXTENSIONS_LIST" ]] || die "Extensions list not found: $EXTENSIONS_LIST"

MYSQLD="$BUILD_DIR/runtime_output_directory/mysqld"
[[ -x "$MYSQLD" ]] || die "mysqld not found at $MYSQLD (build the server first)"

SDK_DIR="$("$SOURCE_DIR/villagesql/bld_tools/get_sdk.sh" "$BUILD_DIR")"
[[ -d "$SDK_DIR" ]] || die "Sanitized SDK not found at $SDK_DIR"

# MTR auto-copies *.veb from <bindir>/veb_output_directory into each test
# server, so dropping the sanitized VEBs here is all that's needed to load them.
VEB_OUT="$BUILD_DIR/veb_output_directory"
mkdir -p "$VEB_OUT"

# Suites are staged into the source tree's mysql-test/suite (which also holds
# lsan.supp / asan.supp that --sanitize needs); clean up only what we add.
MTR_SUITE_DIR="$SOURCE_DIR/mysql-test/suite"
CLONES_DIR="$(mktemp -d)"
STAGED=()
cleanup() {
  for name in "${STAGED[@]:-}"; do
    [[ -n "$name" ]] && rm -rf "$MTR_SUITE_DIR/$name"
  done
  rm -rf "$CLONES_DIR"
}
trap cleanup EXIT

NCORES=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo "4")
SUITES=""
BUILT=0
FAILED=0

while IFS= read -r line; do
  [[ "$line" =~ ^[[:space:]]*# ]] && continue
  [[ -z "${line// }" ]] && continue

  # Parse "url [branch-or-tag] [key=value ...]"; a key=value where a branch
  # would sit (no branch given) must not be mistaken for a branch.
  read -ra FIELDS <<< "$line"
  SOURCE="${FIELDS[0]%/}"
  BRANCH="${FIELDS[1]:-}"
  [[ "$BRANCH" == *=* ]] && BRANCH=""
  REPO_NAME="${SOURCE##*/}"

  if [[ -n "$EXTENSION_FILTER" && "$REPO_NAME" != "$EXTENSION_FILTER" ]]; then
    continue
  fi

  log_step "Building $REPO_NAME ($SOURCE${BRANCH:+ @ $BRANCH}) with sanitizers"

  CLONE_DIR="$CLONES_DIR/$REPO_NAME"
  CLONE_ARGS=(--depth=1)
  [[ -n "$BRANCH" ]] && CLONE_ARGS+=(--branch "$BRANCH")
  if ! git clone "${CLONE_ARGS[@]}" "$SOURCE" "$CLONE_DIR" 2>&1; then
    log_error "Clone failed for $REPO_NAME"
    FAILED=$((FAILED + 1))
    continue
  fi

  # Debug (not Release) so optimization doesn't fold away the checks and asserts
  # stay live. Each extension's CMakeLists self-selects its ABI (dev vs stable),
  # matching the shipping bundled build.
  if ! cmake -S "$CLONE_DIR" -B "$CLONE_DIR/build" \
        -DCMAKE_PREFIX_PATH="$SDK_DIR" \
        -DVillageSQL_SDK_DIR="$SDK_DIR" \
        -DCMAKE_BUILD_TYPE=Debug \
        -DCMAKE_C_FLAGS="$SAN_CFLAGS" \
        -DCMAKE_CXX_FLAGS="$SAN_CFLAGS" \
        -DCMAKE_SHARED_LINKER_FLAGS="$SAN_LDFLAGS" \
        -DCMAKE_EXE_LINKER_FLAGS="$SAN_LDFLAGS" 2>&1; then
    log_error "CMake configure failed for $REPO_NAME"
    FAILED=$((FAILED + 1))
    continue
  fi

  if ! cmake --build "$CLONE_DIR/build" --parallel "$NCORES" 2>&1; then
    log_error "Build failed for $REPO_NAME"
    FAILED=$((FAILED + 1))
    continue
  fi

  VEB_COUNT=0
  while IFS= read -r veb; do
    cp "$veb" "$VEB_OUT/"
    log_info "$(basename "$veb")"
    VEB_COUNT=$((VEB_COUNT + 1))
  done < <(find "$CLONE_DIR/build" -maxdepth 1 -name "*.veb")
  if [[ $VEB_COUNT -eq 0 ]]; then
    log_error "No .veb produced for $REPO_NAME"
    FAILED=$((FAILED + 1))
    continue
  fi

  if [[ -d "$CLONE_DIR/mysql-test" ]]; then
    cp -r "$CLONE_DIR/mysql-test" "$MTR_SUITE_DIR/$REPO_NAME"
    STAGED+=("$REPO_NAME")
    SUITES="${SUITES:+$SUITES,}$REPO_NAME"
  else
    log_warn "$REPO_NAME: no mysql-test/ directory, tests skipped"
  fi

  BUILT=$((BUILT + 1))
done < "$EXTENSIONS_LIST"

echo ""
log_info "Extensions built: $BUILT, failed: $FAILED"

if [[ -n "$EXTENSION_FILTER" && $BUILT -eq 0 && $FAILED -eq 0 ]]; then
  die "'$EXTENSION_FILTER' not found in $EXTENSIONS_LIST"
fi

if [[ -z "$SUITES" ]]; then
  log_warn "No extension test suites staged; nothing to run."
  [[ $FAILED -eq 0 ]] || exit 1
  exit 0
fi

log_step "Running extension MTR suites under --sanitize: $SUITES"
cd "$SOURCE_DIR/mysql-test"
MTR_EXIT=0
MTR_BINDIR="$BUILD_DIR" perl mysql-test-run.pl \
  --suite="$SUITES" \
  --nounit-tests \
  --parallel="$NCORES" \
  --sanitize \
  --force \
  --retry=0 \
  || MTR_EXIT=$?

if [[ $FAILED -ne 0 || $MTR_EXIT -ne 0 ]]; then
  die "Sanitizer extension run failed (build failures: $FAILED, MTR exit: $MTR_EXIT)"
fi
log_info "All sanitized extension suites passed"
