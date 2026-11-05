#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Fails if C/C++ code references a retired VEF protocol version.
#
# Even protocol versions are unstable. When one is promoted to the next odd
# (stable) version, the server rejects it and every gate on it is retargeted to
# the stable version. A gate left behind still works, but only because the
# version is rejected, so this check keeps any from lingering, including ones
# that arrive from a branch opened before the roll.
#
# The retired versions are every even version below VEF_PROTOCOL_CURRENT, so a
# roll extends this check without editing it. The enumerators themselves, and
# the frozen stable_sdk/ snapshots, are allowed.
#
# Usage:
#   check_retired_protocols.sh [<commit>]
#     Checks every line in <commit>'s tree, or in the working tree when
#     omitted.
#   check_retired_protocols.sh --added <base> <head> <commit>
#     Checks only the lines <head> adds relative to <base>, against the
#     VEF_PROTOCOL_CURRENT in <commit>. PR validation passes the PR's merge
#     base, its head and its merge commit, so a PR fails only for references it
#     adds, judged by the protocol version of the main it merges into.

types_h="villagesql/sdk/include/villagesql/abi/types.h"
pathspecs=('*.c' '*.cc' '*.cpp' '*.h' '*.hpp' '*.h.in'
  ':!villagesql/stable_sdk/')

if [ "$1" = "--added" ]; then
  if [ $# -ne 4 ]; then
    echo "Usage: check_retired_protocols.sh --added <base> <head> <commit>"
    exit 1
  fi
  base="$2"
  head="$3"
  rev="$4"
else
  rev="$1"
fi

cd "$(git rev-parse --show-toplevel)" || exit 1

if [ -n "$rev" ]; then
  types_src=$(git show "$rev:$types_h") || exit 1
else
  types_src=$(cat "$types_h") || exit 1
fi

current=$(printf '%s\n' "$types_src" |
  sed -n -E 's/^#define VEF_PROTOCOL_CURRENT VEF_PROTOCOL_([0-9]+).*/\1/p')
if [ -z "$current" ]; then
  echo "ERROR: could not find VEF_PROTOCOL_CURRENT in $types_h"
  exit 1
fi

retired=""
for ((v = 2; v < current; v += 2)); do
  retired="${retired:+$retired|}$v"
done
if [ -z "$retired" ]; then
  echo "No retired protocol versions below VEF_PROTOCOL_$current"
  exit 0
fi

# Both branches print candidate lines as "path:line:text" so the allowlist
# below applies to either.
if [ -n "$head" ]; then
  # Number each added line from the hunk headers of a zero-context diff.
  candidates=$(git diff -U0 --no-color "$base" "$head" -- "${pathspecs[@]}" |
    awk '
      /^diff --git / { in_header = 1; next }
      in_header && /^\+\+\+ / { path = substr($0, 7); next }
      /^@@ / {
        in_header = 0
        split($3, hunk, ",")
        line = substr(hunk[1], 2)
        next
      }
      !in_header && /^\+/ { print path ":" line ":" substr($0, 2); line++ }
    ')
else
  # git grep prefixes each match with "<rev>:" when given a commit.
  candidates=$(git grep -n -w -E "VEF_PROTOCOL_($retired)" $rev -- \
    "${pathspecs[@]}" | sed "s|^${rev:+$rev:}||")
fi

matches=$(printf '%s\n' "$candidates" |
  grep -w -E "VEF_PROTOCOL_($retired)" |
  grep -v -E "^$types_h:[0-9]+:[[:space:]]*VEF_PROTOCOL_[0-9]+,")

retired_list=$(echo "$retired" | sed 's/|/, /g')
if [ -n "$matches" ]; then
  echo "ERROR: code references a retired protocol version" \
    "(retired: $retired_list; current: $current)."
  echo ""
  echo "$matches"
  echo ""
  echo "Each retired version N was promoted to stable version N+1. Retarget"
  echo "each reference by what it gates:"
  echo "  - A feature that shipped in a stable version: use that version,"
  echo "    e.g. VEF_PROTOCOL_$((current - 2)) -> VEF_PROTOCOL_$((current - 1))."
  echo "  - New work that is not in a stable version yet: use the current"
  echo "    unstable version, VEF_PROTOCOL_$current."
  exit 1
fi

echo "No references to retired protocol versions" \
  "(retired: $retired_list; current: $current)"
