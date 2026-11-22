#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Regression tests for check_retired_protocols.sh.
#
# Each test builds commits in a scratch git repository with its own types.h,
# so the tests do not depend on the real VEF_PROTOCOL_CURRENT and need no
# change when a protocol rolls.
#
# Run locally:
#   ./scripts/regtest_check_retired_protocols.sh
#
# Exits non-zero if any check fails.

set -uo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/check_retired_protocols.sh"
TYPES_H="villagesql/sdk/include/villagesql/abi/types.h"

REPO="$(mktemp -d)"
trap 'rm -rf "$REPO"' EXIT
cd "$REPO" || exit 1
git -c init.defaultBranch=main init -q
git config user.name test
git config user.email test@example.com
git config commit.gpgsign false

fail=0
check() {
  local label="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "ok   - $label"
  else
    echo "FAIL - $label"
    echo "       expected: $expected"
    echo "       actual:   $actual"
    fail=1
  fi
}

# Writes a types.h whose enum runs up to <current>.
write_types() {
  local current="$1" v
  mkdir -p "$(dirname "$TYPES_H")"
  {
    echo "typedef enum {"
    for ((v = 1; v <= current; v++)); do
      echo "  VEF_PROTOCOL_$v,"
    done
    echo "} vef_protocol_t;"
    echo "#define VEF_PROTOCOL_CURRENT VEF_PROTOCOL_$current"
  } >"$TYPES_H"
}

# Writes <file> with the remaining arguments as its lines.
write() {
  local file="$1"
  shift
  mkdir -p "$(dirname "$file")"
  printf '%s\n' "$@" >"$file"
}

# Commits everything and prints the new commit.
commit() {
  git add -A
  git commit -q --allow-empty -m "$1"
  git rev-parse HEAD
}

# Runs the check and prints "exit=<status>" then each reported "path:line",
# joined with spaces.
run() {
  local out status
  out=$(bash "$SCRIPT" "$@" 2>&1)
  status=$?
  printf 'exit=%s' "$status"
  printf '%s\n' "$out" | grep -E '^[^:]+:[0-9]+:' |
    sed -E 's/^([^:]+:[0-9]+):.*/ \1/' | tr -d '\n'
}

# Runs the check and prints its first output line.
first_line() {
  bash "$SCRIPT" "$@" 2>&1 | head -1
}

# 1. With protocol 2 current, no version is retired yet.
write_types 2
c_v2=$(commit "protocol 2 is current")
check "nothing retired below 2" \
  "No retired protocol versions below VEF_PROTOCOL_2" "$(first_line "$c_v2")"

# 2. The enum definition in types.h may name a retired version. Later cases
# build on this commit.
write_types 4
write a.cc "int a = 0;" "int b = 0;" "int c = 0;" "int d = 0;" "int e = 0;"
base=$(commit "protocol 4 is current")
check "enum definition is exempt" "exit=0" "$(run "$base")"

# 3. References in two hunks of one file and in a new file are reported at
# their line numbers, in both modes. An enum-style line outside types.h is not
# exempt. Deleting the references passes.
write a.cc "// VEF_PROTOCOL_2" "int a = 0;" "int b = 0;" "int c = 0;" \
  "int d = 0;" "if (p >= VEF_PROTOCOL_2) {}" "int e = 0;"
write new.h "line 1" "  VEF_PROTOCOL_2,"
head=$(commit "add references")
check "added lines, with line numbers" \
  "exit=1 a.cc:1 a.cc:6 new.h:2" "$(run --added "$base" "$head" "$head")"
check "whole tree at a commit" \
  "exit=1 a.cc:1 a.cc:6 new.h:2" "$(run "$head")"
check "deleting references passes" \
  "exit=0" "$(run --added "$head" "$base" "$head")"

# 4. A name that only contains a retired version is not a reference.
git checkout -q "$base"
write a.cc "int x = VEF_PROTOCOL_22 + VEF_PROTOCOL_2X + MY_VEF_PROTOCOL_2;"
head=$(commit "names that only contain a retired version")
check "whole-word matches only" "exit=0" "$(run --added "$base" "$head" "$head")"

# 5. Code, tests and CMake files are checked, including paths with a space or
# non-ASCII characters. Markdown, other files and stable_sdk/ are not.
git checkout -q "$base"
write t/x.test "# VEF_PROTOCOL_2"
write include/x.inc "# VEF_PROTOCOL_2"
write cmake/x.cmake "# VEF_PROTOCOL_2"
write sub/dir/CMakeLists.txt "# VEF_PROTOCOL_2"
write "dir with space/x.cc" "// VEF_PROTOCOL_2"
write café.cc "// VEF_PROTOCOL_2"
write README.md "VEF_PROTOCOL_2"
write tool.py "# VEF_PROTOCOL_2"
write villagesql/stable_sdk/v1/x.h "// VEF_PROTOCOL_2"
head=$(commit "references in each kind of file")
expected="exit=1 café.cc:1 cmake/x.cmake:1 dir with space/x.cc:1"
expected+=" include/x.inc:1 sub/dir/CMakeLists.txt:1 t/x.test:1"
check "checked and unchecked files, added lines" \
  "$expected" "$(run --added "$base" "$head" "$head")"
check "checked and unchecked files, whole tree" "$expected" "$(run "$head")"

# 6. The <commit> argument sets which versions are retired. A roll from 4 to 6
# retires 4, and the roll's own enum lines pass.
git checkout -q "$base"
write a.cc "if (p >= VEF_PROTOCOL_4) {}"
head=$(commit "reference to protocol 4")
write_types 6
rolled=$(commit "protocol 6 is current")
check "4 is current on the base" \
  "exit=0" "$(run --added "$base" "$head" "$head")"
check "4 is retired once 6 is current" \
  "exit=1 a.cc:1" "$(run --added "$base" "$head" "$rolled")"
check "roll lists every retired version" \
  "ERROR: code references a retired protocol version (retired: 2, 4; current: 6)." \
  "$(first_line --added "$base" "$head" "$rolled")"
check "roll's enum lines are exempt" \
  "exit=0" "$(run --added "$head" "$rolled" "$rolled")"

# 7. With no commit, the working tree is checked.
git checkout -q "$base"
write a.cc "// VEF_PROTOCOL_2"
check "working tree" "exit=1 a.cc:1" "$(run)"
git checkout -q -- a.cc

# 8. Bad revisions and missing arguments fail.
check "bad base" "ERROR: not a commit: nosuch" \
  "$(first_line --added nosuch "$base" "$base")"
check "bad head" "ERROR: not a commit: nosuch" \
  "$(first_line --added "$base" nosuch "$base")"
check "bad commit" "ERROR: not a commit: nosuch" "$(first_line nosuch)"
check "missing arguments" \
  "Usage: check_retired_protocols.sh --added <base> <head> <commit>" \
  "$(first_line --added "$base")"

# 9. A types.h without VEF_PROTOCOL_CURRENT fails.
write "$TYPES_H" "typedef enum { VEF_PROTOCOL_1 } vef_protocol_t;"
no_current=$(commit "no VEF_PROTOCOL_CURRENT")
check "missing VEF_PROTOCOL_CURRENT" \
  "ERROR: could not find VEF_PROTOCOL_CURRENT in $TYPES_H" \
  "$(first_line "$no_current")"

# 10. A git failure with valid revisions, here an unreadable file, must fail
# the check rather than leave it nothing to check. This corrupts the
# repository, so it runs last.
git checkout -q "$base"
write unreadable.cc "int x = 0;"
head=$(commit "file whose contents will be lost")
blob=$(git rev-parse "$head:unreadable.cc")
rm -f ".git/objects/${blob:0:2}/${blob:2}"
check "unreadable file, added lines" "exit=1" \
  "$(run --added "$base" "$head" "$base")"
check "unreadable file, whole tree" "exit=1" "$(run "$head")"

exit $fail
