#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Fails if C/C++ code references a retired VEF protocol version.
#
# Even protocol versions are unstable. When one is promoted to the next odd
# (stable) version, the server rejects it and every check on it is retargeted
# to the stable version. A check left behind still works, but only because the
# version is rejected. This script keeps any from lingering, including ones that
# arrive from a branch opened before the roll.
#
# The retired versions are every even version below VEF_PROTOCOL_CURRENT, so a
# roll extends this script without editing it. The enum definition and the
# frozen stable_sdk/ snapshots may name retired versions.
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

# A git failure inside a pipeline must fail the check, not leave it with
# nothing to check.
set -o pipefail

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

# A bad revision would otherwise leave the check with nothing to check.
for r in "$base" "$head" "$rev"; do
  if [ -n "$r" ] &&
    ! git rev-parse --verify --quiet "$r^{commit}" >/dev/null; then
    echo "ERROR: not a commit: $r"
    exit 1
  fi
done

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

# Retired versions are the even (unstable) versions below the current one.
retired=()
for ((v = 2; v < current; v += 2)); do
  retired+=("$v")
done
if [ ${#retired[@]} -eq 0 ]; then
  echo "No retired protocol versions below VEF_PROTOCOL_$current"
  exit 0
fi
retired_pattern="VEF_PROTOCOL_($(IFS='|'; echo "${retired[*]}"))"
retired_list="${retired[*]}"
retired_list="${retired_list// /, }"

# Prints "path:line:text" for each line that <head> adds relative to <base>.
# A zero-context diff has no unchanged lines, so the hunk headers give the
# line number of each added line.
added_lines() {
  git diff --unified=0 --no-color "$base" "$head" -- "${pathspecs[@]}" |
    awk '
      # "diff --git" starts a file. Its header lines are not content.
      /^diff --git / { in_header = 1; next }

      # "+++ b/<path>" names the file.
      in_header && /^\+\+\+ / { path = substr($0, 7); next }

      # "@@ -a,b +c,d @@": the hunk adds lines starting at line c.
      /^@@ / {
        in_header = 0
        split($3, new_range, ",")
        line = substr(new_range[1], 2)
        next
      }

      !in_header && /^\+/ { print path ":" line ":" substr($0, 2); line++ }
    '
}

# Prints "path:line:text" for each line in the tree that names a retired
# version.
tree_lines() {
  if [ -n "$rev" ]; then
    # Given a commit, git grep prefixes each match with "<commit>:".
    git grep -n -w -E "$retired_pattern" "$rev" -- "${pathspecs[@]}" |
      sed "s|^$rev:||"
  else
    git grep -n -w -E "$retired_pattern" -- "${pathspecs[@]}"
  fi
}

if [ -n "$head" ]; then
  candidates=$(added_lines) || exit 1
else
  candidates=$(tree_lines)
  # git grep exits 1 when nothing matches, and above 1 on an error.
  if [ $? -gt 1 ]; then
    exit 1
  fi
fi

# Keep the lines that name a retired version, except the enum definition in
# types.h.
enum_line="^$types_h:[0-9]+:[[:space:]]*VEF_PROTOCOL_[0-9]+,"
matches=$(printf '%s\n' "$candidates" |
  grep -w -E "$retired_pattern" |
  grep -v -E "$enum_line")
if [ -n "$matches" ]; then
  echo "ERROR: code references a retired protocol version" \
    "(retired: $retired_list; current: $current)."
  echo ""
  echo "$matches"
  echo ""
  echo "Each retired version N was promoted to stable version N+1. Retarget"
  echo "each reference by what it checks:"
  echo "  - A feature that shipped in a stable version: use that version,"
  echo "    e.g. VEF_PROTOCOL_$((current - 2)) -> VEF_PROTOCOL_$((current - 1))."
  echo "  - New work that is not in a stable version yet: use the current"
  echo "    unstable version, VEF_PROTOCOL_$current."
  exit 1
fi

echo "No references to retired protocol versions" \
  "(retired: $retired_list; current: $current)"
