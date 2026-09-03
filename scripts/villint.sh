#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Option Parsing ---
LINT_C_FULL=false
LINT_OTHER_FULL=false
FIX_EOF=true  # Default to fixing EOF newlines
FIX_COPYRIGHT=true  # Default to fixing copyrights
COMMIT_ISH=""  # Will be computed if not specified
CMDLINE_IGNORE_PATTERNS=()  # Array to store command-line ignore patterns
PRINT_CLANG_FORMAT_VERSION=false  # Print the pinned clang-format version and exit

usage() {
  echo "Usage: $(basename "$0") [-c] [-o] [--fixeof|--no-fixeof] [--fixcopyright|--no-fixcopyright] [--commit <commit-ish>] [--ignore <pattern>]"
  echo "  Lints files changed in this PR/branch."
  echo
  echo "  -c:                 Lint entire C/C++ files instead of just changed ranges."
  echo "  -o:                 Lint entire non-C/C++ files (this is the default behavior)."
  echo "  --fixeof:           Fix missing newline at the end of C/C++ files (default: on)."
  echo "  --no-fixeof:        Skip EOF newline fixing."
  echo "  --fixcopyright:     Add/update VillageSQL copyright notices (default: on)."
  echo "  --no-fixcopyright:  Skip copyright fixing."
  echo "  --commit <ref>:     Specify the commit/revision to diff against."
  echo "                      Git default: merge-base with origin/main"
  echo "                      jj default: main@<remote> where <remote> is from git.push config"
  echo "                        Set with: jj config set git.push github"
  echo "                        Defaults to 'origin' if not set"
  echo "  --ignore <pattern>: Ignore files matching pattern (can be used multiple times)."
  echo "                      Supports exact paths, directory recursion, and glob patterns."
  echo "  --clang-format-version:"
  echo "                      Print the pinned clang-format version and exit."
  echo "  -h, --help:         Show this help message."
  exit 1
}

# Parse options
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -c)
      LINT_C_FULL=true
      shift # past argument
      ;;
    -o)
      LINT_OTHER_FULL=true
      shift # past argument
      ;;
    --fixeof)
      FIX_EOF=true
      shift # past argument
      ;;
    --no-fixeof)
      FIX_EOF=false
      shift # past argument
      ;;
    --fixcopyright)
      FIX_COPYRIGHT=true
      shift # past argument
      ;;
    --no-fixcopyright)
      FIX_COPYRIGHT=false
      shift # past argument
      ;;
    --commit)
      if [[ -z "$2" || "$2" == -* ]]; then
        echo "Error: --commit option requires an argument." >&2
        usage
      fi
      COMMIT_ISH="$2"
      shift # past argument
      shift # past value
      ;;
    --ignore)
      if [[ -z "$2" || "$2" == -* ]]; then
        echo "Error: --ignore option requires an argument." >&2
        usage
      fi
      CMDLINE_IGNORE_PATTERNS+=("$2")
      shift # past argument
      shift # past value
      ;;
    --clang-format-version)
      PRINT_CLANG_FORMAT_VERSION=true
      shift # past argument
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

# --- Helper Functions ---

# Apply changes to a file only if the content actually changed
# This preserves mtime when no changes are made, avoiding unnecessary rebuilds
apply_if_changed() {
  local original="$1"
  local modified="$2"
  local message="$3"

  if cmp -s "$original" "$modified"; then
    # Files are identical - no changes needed
    rm "$modified"
    return 1  # Return 1 to indicate "no change"
  else
    # Files differ - apply changes
    mv "$modified" "$original"
    echo "  $message"
    return 0  # Return 0 to indicate "changed"
  fi
}

# Check if we're in a jj workspace
is_jj_workspace() {
  [ -d ".jj" ]
}

# Get ignore patterns from commit messages in the given range
get_ignored_patterns() {
  local commit_range="$1"
  # Check if we're in a jj workspace or git repo
  if is_jj_workspace; then
    # The caller passes commit_range in git's `X..HEAD` form.  Translate to
    # jj's equivalent revset (`X..@`) and use it directly.  Previously this
    # wrapped the range in `::@ ~ ::$commit_range`, which produced an
    # invalid jj revset like `::@ ~ ::main@origin..HEAD` (the `..` infix
    # cannot follow `::` in jj) -- jj returned a parse error that was
    # silently swallowed by `2>/dev/null`, leaving no ignore directives
    # visible locally.
    local jj_range="${commit_range/..HEAD/..@}"
    jj log -r "$jj_range" --no-graph -T description 2>/dev/null | \
      grep -i "^villint-ignore:" | \
      sed 's/^villint-ignore://i' | \
      tr ',' '\n' | \
      sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | \
      grep -v '^$' | \
      sort -u
  else
    git log "$commit_range" --format="%B" 2>/dev/null | \
      grep -i "^villint-ignore:" | \
      sed 's/^villint-ignore://i' | \
      tr ',' '\n' | \
      sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | \
      grep -v '^$' | \
      sort -u
  fi
}

# Check if a file should be ignored based on ignore patterns
is_ignored() {
  local file="$1"

  for pattern in "${IGNORED_PATTERNS[@]}"; do
    case "$file" in
      # Try exact match, prefix match (for directories), and glob match
      "$pattern"|"$pattern"/*|$pattern)
        return 0
        ;;
    esac
  done

  return 1
}

get_villint_dir() {
  local script_dir="$(dirname "${BASH_SOURCE[0]}")"
  local source_dir="$(cd "$script_dir/.." && pwd)"
  echo "$source_dir/villagesql/villint"
}

get_allowed_list() {
  echo "$(get_villint_dir)/regtest_villint_data/allowlist_nonstandard_copyrights.txt"
}

get_clang_format_version() {
  cat "$(get_villint_dir)/clang-format-version"
}

# Whitelist of allowed TODO tags for VillageSQL code
ALLOWED_TODO_TAGS=(
  "villagesql"
  "villagesql-back-to-mysql"
  "villagesql-beta"
  "villagesql-blob"
  "villagesql-charset"
  "villagesql-crash"
  "villagesql-general"
  "villagesql-indexing"
  "villagesql-performance"
  "villagesql-preview"
  "villagesql-production"
  "villagesql-rebase"
  "villagesql-rust"
  "villagesql-windows"
)

INVALID_TODO_TAGS_FOUND=0

# Arguments: file path
check_todo_tags() {
  local file="$1"

  # Get only the added lines from the diff (lines starting with +, excluding +++ header)
  local added_lines
  if is_jj_workspace; then
    if jj file show -r "$COMMIT_ISH" "$file" >/dev/null 2>&1; then
      added_lines=$(jj diff --from "$COMMIT_ISH" --git "$file" --context=0 2>/dev/null | grep '^+' | grep -v '^+++' | sed 's/^+//')
    else
      # New file - all lines are added
      added_lines=$(cat "$file")
    fi
  else
    if git ls-files --error-unmatch "$file" >/dev/null 2>&1; then
      added_lines=$(git diff --unified=0 "$COMMIT_ISH" -- "$file" | grep '^+' | grep -v '^+++' | sed 's/^+//')
    else
      # New untracked file - all lines are added
      added_lines=$(cat "$file")
    fi
  fi

  # Find TODO(tag) patterns in added lines and validate tags
  while IFS= read -r line; do
    [ -z "$line" ] && continue

    # Extract all TODOs
    local todo_matches=$(echo "$line" | grep -oE 'TODO\([^)]+\)')
    [ -z "$todo_matches" ] && continue

    local todo_count=$(echo "$todo_matches" | wc -l)
    if [ "$todo_count" -gt 1 ]; then
      echo "  ERROR: $file has multiple TODOs on one line"
      echo "    Line: $line"
      INVALID_TODO_TAGS_FOUND=$((INVALID_TODO_TAGS_FOUND + 1))
      continue
    fi

    # Extract the tag from the match
    local tag=$(echo "$todo_matches" | sed -E 's/TODO\(([^)]+)\)/\1/')

    # Check if tag is in allowed list
    local tag_valid=false
    for allowed in "${ALLOWED_TODO_TAGS[@]}"; do
      if [ "$tag" = "$allowed" ]; then
        tag_valid=true
        break
      fi
    done

    if [ "$tag_valid" = false ]; then
      echo "  ERROR: $file has invalid TODO tag '$tag'"
      echo "    Line: $line"
      INVALID_TODO_TAGS_FOUND=$((INVALID_TODO_TAGS_FOUND + 1))
    fi
  done <<< "$added_lines"
}

# Fix copyright block in a C/C++ file
fix_copyright() {
  local file="$1"

  # Check if file already has a copyright block
  if head -20 "$file" | grep -qi "Copyright"; then
    # File has a copyright block - check if VillageSQL is already mentioned
    if head -30 "$file" | grep -q "VillageSQL Contributors"; then
      echo "  Copyright already has VillageSQL Contributors"
      return 0
    fi

    # Check if this is an Oracle/MySQL copyright that we can augment
    if head -30 "$file" | grep -q "Copyright.*Oracle"; then
      # Add VillageSQL Contributors line after the first Copyright line
      # Use perl for in-place editing with proper line insertion
      # Extract the indentation of the existing Copyright line to match it
      perl -i -pe 'if (!$done && /Copyright.*Oracle/) {
        # Extract everything before "Copyright" to determine formatting
        my $prefix = "";
        if (/^(.*?)Copyright/) {
          $prefix = $1;
        }

        # If prefix contains comment start (/*), replace it with proper comment continuation
        if ($prefix =~ /\/\*\s*$/) {
          # Replace /* with proper comment line prefix
          $prefix =~ s/\/\*\s*$/   /;
        }

        $_ .= "${prefix}Copyright (c) 2026 VillageSQL Contributors\n";
        $done = 1;
      }' "$file"
      echo "  Added VillageSQL Contributors to existing copyright"
    else
      # Non-standard copyright - check if it's in the allowlist
      local allowlist=$(get_allowed_list)
      # Check for exact match or prefix match (allows directory patterns)
      if [ -f "$allowlist" ]; then
        if grep -qxF "$file" "$allowlist"; then
          echo "  Non-standard copyright (allowlisted): $file"
          return 0
        fi
        # Check for directory prefix matches (e.g., extra/libarchive/ matches extra/libarchive/*)
        while IFS= read -r pattern; do
          # Skip comments and empty lines
          [[ "$pattern" =~ ^#.*$ || -z "$pattern" ]] && continue
          # Check if file starts with the pattern
          if [[ "$file" == "$pattern"* ]]; then
            echo "  Non-standard copyright (allowlisted by prefix): $file"
            return 0
          fi
        done < "$allowlist"
      fi
      # Not in allowlist
      echo "ERROR: Non-standard copyright in $file" >&2
      echo "  This file has a copyright notice but it's not the standard Oracle/MySQL format." >&2
      echo "  Either update the copyright to match the standard format, or add this file to:" >&2
      echo "  $allowlist" >&2
      return 1
    fi
  else
    # No copyright block - add full VillageSQL copyright
    cat > /tmp/villagesql_copyright.txt << 'EOF'
/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

EOF
    cat /tmp/villagesql_copyright.txt "$file" > /tmp/villint_tmp && mv /tmp/villint_tmp "$file"
    echo "  Added full VillageSQL copyright block"
  fi
}

# Fix copyright block in a CMakeLists.txt file
fix_copyright_cmake() {
  local file="$1"

  # Check if file already has a copyright block
  if head -20 "$file" | grep -qi "Copyright"; then
    # File has a copyright block - check if VillageSQL is already mentioned
    if head -30 "$file" | grep -q "VillageSQL Contributors"; then
      echo "  Copyright already has VillageSQL Contributors"
      return 0
    fi

    # Check if this is an Oracle/MySQL copyright that we can augment
    if head -30 "$file" | grep -q "Copyright.*Oracle"; then
      # Add VillageSQL Contributors line after the first Copyright line
      # Use perl for in-place editing with proper line insertion
      perl -i -pe 'if (!$done && /Copyright.*Oracle/) {
        $_ .= "#    Copyright (c) 2026 VillageSQL Contributors\n";
        $done = 1;
      }' "$file"
      echo "  Added VillageSQL Contributors to existing copyright"
    else
      # Non-standard copyright - check if it's in the allowlist
      local allowlist=$(get_allowed_list)
      # Check for exact match or prefix match (allows directory patterns)
      if [ -f "$allowlist" ]; then
        if grep -qxF "$file" "$allowlist"; then
          echo "  Non-standard copyright (allowlisted): $file"
          return 0
        fi
        # Check for directory prefix matches (e.g., extra/libarchive/ matches extra/libarchive/*)
        while IFS= read -r pattern; do
          # Skip comments and empty lines
          [[ "$pattern" =~ ^#.*$ || -z "$pattern" ]] && continue
          # Check if file starts with the pattern
          if [[ "$file" == "$pattern"* ]]; then
            echo "  Non-standard copyright (allowlisted by prefix): $file"
            return 0
          fi
        done < "$allowlist"
      fi
      # Not in allowlist
      echo "ERROR: Non-standard copyright in $file" >&2
      echo "  This file has a copyright notice but it's not the standard Oracle/MySQL format." >&2
      echo "  Either update the copyright to match the standard format, or add this file to:" >&2
      echo "  scripts/regtest_villint_data/allowlist_nonstandard_copyrights.txt" >&2
      return 1
    fi
  else
    # No copyright block - add full VillageSQL copyright
    cat > /tmp/villagesql_copyright_cmake.txt << 'EOF'
# Copyright (c) 2026 VillageSQL Contributors
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <https://www.gnu.org/licenses/>.

EOF
    cat /tmp/villagesql_copyright_cmake.txt "$file" > /tmp/villint_tmp && mv /tmp/villint_tmp "$file"
    echo "  Added full VillageSQL copyright block"
  fi
}

# --- Main Logic ---

# Check for required tools
REQUIRED_CLANG_FORMAT_VERSION=$(get_clang_format_version)

# Handle introspection requests first
if [ "$PRINT_CLANG_FORMAT_VERSION" = true ]; then
  echo "$REQUIRED_CLANG_FORMAT_VERSION"
  exit 0
fi

die_clang_format() {
  echo "Error: $1" >&2
  echo "Please install clang-format $REQUIRED_CLANG_FORMAT_VERSION:" >&2
  echo "  Linux: pip install clang-format==$REQUIRED_CLANG_FORMAT_VERSION" >&2
  echo "  macOS: brew install clang-format" >&2
  echo "         (then verify 'clang-format --version' matches; brew tracks" >&2
  echo "         LLVM's latest stable so a future bump may drift away)" >&2
  exit 1
}

# Validate the pin's shape: two or three dot-separated numeric components and
# nothing else.  A trailing newline does NOT matter -- $() strips it whether
# present or not, on Linux CI and macOS alike -- but $() does NOT strip a
# stray trailing space or carriage return (e.g. a CRLF checkout), and either
# would silently corrupt both this check and the "pip install ...==<pin>.*"
# step in .github/workflows/validate-pr.yml.  Reject anything malformed here
# with a clear message instead.
clang_format_version_pattern='^[0-9]+\.[0-9]+(\.[0-9]+)?$'
if ! [[ "$REQUIRED_CLANG_FORMAT_VERSION" =~ $clang_format_version_pattern ]]; then
  die_clang_format "clang-format-version pin '$REQUIRED_CLANG_FORMAT_VERSION' is malformed; expected two or three numeric components like 22.1 or 22.1.8"
fi

if ! command -v clang-format >/dev/null 2>&1; then
  die_clang_format "clang-format is not installed or not in PATH"
fi
# Match at the precision of the pin in clang-format-version: a two-component
# pin like "22.1" accepts any 22.1.x patch release, while a three-component
# pin like "22.1.8" requires that exact patch.  We normally pin to two
# components so devs (e.g. brew tracking LLVM's latest stable) don't have to
# update lockstep on every patch; but patch releases are not policy-bound to
# keep formatting stable, so if a release changes output we pin the full
# three-component version to force everyone onto the same build.  We have
# been burned by drift before (see the bug history of this script's revset
# and ranges logic for the investigation that surfaced this).
# The CI install in .github/workflows/validate-pr.yml mirrors this by
# appending ".*" to the pin, so pip honors the same two-or-three precision.
CLANG_FORMAT_VERSION=$(clang-format --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
# Truncate the detected version to the same number of dotted components as
# the pin so the comparison honors the pin's precision.
num_components=$(printf '%s' "$REQUIRED_CLANG_FORMAT_VERSION" | awk -F. '{print NF}')
CLANG_FORMAT_VERSION_PREFIX=$(printf '%s' "$CLANG_FORMAT_VERSION" | cut -d. -f1-"$num_components")
if [ "$CLANG_FORMAT_VERSION_PREFIX" != "$REQUIRED_CLANG_FORMAT_VERSION" ]; then
  die_clang_format "clang-format version $CLANG_FORMAT_VERSION found, expected $REQUIRED_CLANG_FORMAT_VERSION"
fi

# Determine comparison point if not specified
if [ -z "$COMMIT_ISH" ]; then
  # Check if we're in a jj workspace or git repo
  if is_jj_workspace; then
    # In jj workspace, use git.push config or default to "origin"
    # User can set: jj config set git.push "github"
    # We diff against the fork point with main@<remote>, not the tip — so when
    # upstream main is ahead of our branch's base, upstream commits do not
    # appear as "reverse-modified" files in our changed-file set (which would
    # otherwise cause villint to lint unrelated upstream-only files).
    VILLINT_REMOTE=$(jj config get git.push 2>/dev/null || echo "origin")
    COMMIT_ISH="fork_point(@ | main@$VILLINT_REMOTE)"
    echo "Using jj workspace, comparing against: $COMMIT_ISH"
  else
    # Default: compare against merge-base with origin/main
    COMMIT_ISH=$(git merge-base HEAD origin/main)
    echo "Using merge-base with origin/main: $COMMIT_ISH"
  fi
fi

# Build ignore patterns from commit messages and command line
IGNORE_COMMIT_RANGE="$COMMIT_ISH..HEAD"
IGNORED_PATTERNS=()

# Add command-line ignore patterns first
for pattern in "${CMDLINE_IGNORE_PATTERNS[@]}"; do
  IGNORED_PATTERNS+=("$pattern")
done

# Add patterns from commit messages
while IFS= read -r pattern; do
  if [ -n "$pattern" ]; then
    IGNORED_PATTERNS+=("$pattern")
  fi
done < <(get_ignored_patterns "$IGNORE_COMMIT_RANGE")

if [ ${#IGNORED_PATTERNS[@]} -gt 0 ]; then
  echo "Found ignore directives:"
  for pattern in "${IGNORED_PATTERNS[@]}"; do
    echo "  - $pattern"
  done
fi

# Get lists of changed files.
# Check if we're in a jj workspace or git repo
if is_jj_workspace; then
  # Use jj commands to get all changed files (including new ones)
  # Filter out deleted files (D) since we don't need to lint them
  # Temporarily disable set -e to capture errors properly
  set +e
  JJ_DIFF_OUTPUT=$(jj diff --from "$COMMIT_ISH" --summary 2>&1)
  JJ_EXIT_CODE=$?
  set -e
  if [ $JJ_EXIT_CODE -ne 0 ]; then
    echo "Error: Failed to get diff from jj. Check that '$COMMIT_ISH' is a valid revision." >&2
    echo "$JJ_DIFF_OUTPUT" >&2
    exit 1
  fi
  ALL_FILES=$(echo "$JJ_DIFF_OUTPUT" | awk '$1 != "D" {print $NF}')
else
  # Tracked files that have been modified/added since the specified commit.
  TRACKED_FILES=$(git diff --name-only --diff-filter=d "$COMMIT_ISH")
  # New untracked files.
  UNTRACKED_FILES=$(git ls-files --others --exclude-standard)

  ALL_FILES="$TRACKED_FILES
$UNTRACKED_FILES"
fi

if [ -z "$ALL_FILES" ]; then
  echo "No changes to lint."
  exit 0
fi

# Separate files into C/C++, CMakeLists.txt, and others
C_FILES=""
CMAKE_FILES=""
OTHER_FILES=""

for file in $ALL_FILES; do
  # Ensure the path points to an existing, regular file before processing.
  if [ ! -f "$file" ]; then
    continue
  fi

  case "$file" in
    *.c|*.cc|*.h|*.cpp|*.hpp)
      C_FILES="$C_FILES $file"
      ;;
    */CMakeLists.txt|CMakeLists.txt)
      CMAKE_FILES="$CMAKE_FILES $file"
      ;;
    *)
      OTHER_FILES="$OTHER_FILES $file"
      ;;
  esac
done

# --- Lint C/C++ Files ---
for file in $C_FILES; do
  # Check if file should be ignored
  if is_ignored "$file"; then
    echo "Skipping ignored file: $file"
    continue
  fi

  if [ "$FIX_COPYRIGHT" = true ]; then
    echo "Fixing copyright: $file"
    fix_copyright "$file"
  fi

  if [ "$FIX_EOF" = true ]; then
    # Ensure there is a single newline at the end of the file.
    if [ -n "$(tail -c1 "$file")" ]; then
      echo "" >> "$file"
    fi
  fi

  if [ "$LINT_C_FULL" = true ]; then
    echo "Linting C/C++ (full file): $file"
    clang-format -i -style=file "$file"
  else
    echo "Linting C/C++ (changed ranges or new file): $file"
    # Get diff and extract hunk info for added/modified lines (+).
    # The format is `+<start_line>,<number_of_lines>`.
    # We convert this to `start:end` for clang-format's --lines option.

    # Check if we're in a jj workspace or git repo
    if is_jj_workspace; then
      # `jj diff --from "$COMMIT_ISH"` works whether or not the file
      # existed at COMMIT_ISH: for a brand-new file it produces a
      # full-file-added diff, matching what the git path does for
      # untracked files.  Previously we branched on existence and the
      # "new file" arm dropped --from, which made jj diff against the
      # working-copy parent (empty for a clean checkout) and silently
      # skip the file entirely.
      ranges_str=$(jj diff --from "$COMMIT_ISH" --git "$file" --context=0 2>/dev/null | grep -E '^@@' | sed -E 's/^@@.* \+([0-9]+),?([0-9]*).*/\1,\2/')
    else
      # Check if the file is tracked by git to determine the correct diff command.
      if git ls-files --error-unmatch "$file" >/dev/null 2>&1; then
        # File is tracked: diff against the specified commit-ish.
        ranges_str=$(git diff --unified=0 "$COMMIT_ISH" -- "$file" | grep -E '^@@' | sed -E 's/^@@.* \+([0-9]+),?([0-9]*).*/\1,\2/')
      else
        # File is untracked: diff against an empty tree to get all lines.
        ranges_str=$(git diff --unified=0 --no-index /dev/null "$file" | grep -E '^@@' | sed -E 's/^@@.* \+([0-9]+),?([0-9]*).*/\1,\2/')
      fi
    fi

    cf_ranges=()
    # Only process ranges if ranges_str is not empty
    if [[ -n "$ranges_str" ]]; then
      while IFS=, read -r start len; do
        # If len is empty, it's a single line addition.
        if [[ -z "$len" ]]; then
          len=1
        fi
        # If len is 0, it's a pure deletion, so no lines to format.
        if [[ "$len" -gt 0 ]]; then
          end=$((start + len - 1))
          cf_ranges+=("--lines=$start:$end")
        fi
      done <<< "$ranges_str"
    fi

    if [ ${#cf_ranges[@]} -gt 0 ]; then
      # Pass all ranges for the file at once to clang-format.
      clang-format -i -style=file "${cf_ranges[@]}" "$file"
    else
      echo "  (No added/modified lines to format in $file)"
    fi
  fi

  check_todo_tags "$file"
done

# --- Lint CMakeLists.txt Files ---
for file in $CMAKE_FILES; do
  # Check if file should be ignored
  if is_ignored "$file"; then
    echo "Skipping ignored file: $file"
    continue
  fi

  if [ "$FIX_COPYRIGHT" = true ]; then
    echo "Fixing copyright: $file"
    fix_copyright_cmake "$file"
  fi

  echo "Linting CMakeLists.txt (whitespace/newline): $file"

  # Use temp file to preserve mtime if no changes needed
  temp=$(mktemp)
  cp "$file" "$temp"

  # Remove trailing whitespace (spaces, tabs, carriage returns) but not newlines.
  perl -pi -e 's/[ \t\r]+$//' "$temp"

  # Ensure there is a single newline at the end of the file.
  if [ -n "$(tail -c1 "$temp")" ]; then
    echo "" >> "$temp"
  fi

  apply_if_changed "$file" "$temp" "Fixed whitespace/newline in $file" || true

  check_todo_tags "$file"
done

# --- Lint Other Files ---
for file in $OTHER_FILES; do
  # Check if file should be ignored
  if is_ignored "$file"; then
    echo "Skipping ignored file: $file"
    continue
  fi

  # Skip .result files - they are auto-generated test output where trailing
  # whitespace may be intentional/required for test matching
  # Skip binary files - text operations don't apply to them
  case "$file" in
    *.result)
      echo "Skipping .result file (auto-generated): $file"
      continue
      ;;
    *.zip|*.veb)
      echo "Skipping binary file: $file"
      continue
      ;;
  esac

  echo "Linting other (whitespace/newline): $file"

  # Use temp file to preserve mtime if no changes needed
  temp=$(mktemp)
  cp "$file" "$temp"

  # Remove trailing whitespace (spaces, tabs, carriage returns) but not newlines.
  perl -pi -e 's/[ \t\r]+$//' "$temp"

  # Ensure there is a single newline at the end of the file.
  if [ -n "$(tail -c1 "$temp")" ]; then
    echo "" >> "$temp"
  fi

  apply_if_changed "$file" "$temp" "Fixed whitespace/newline in $file" || true

  check_todo_tags "$file"
done

# --- Check for Accidental Laptop Strings ---
# Check VillageSQL test files for laptop-specific strings (usernames, paths, etc.)
LAPTOP_STRINGS_FOUND=0

# List of strings that shouldn't appear in test files
accidental_laptop_strings=(
  "dbentley"
)

# Filter files to only VillageSQL test files
VILLAGESQL_TEST_FILES=""
for file in $ALL_FILES; do
  if [ ! -f "$file" ]; then
    continue
  fi
  case "$file" in
    mysql-test/suite/villagesql/*)
      VILLAGESQL_TEST_FILES="$VILLAGESQL_TEST_FILES $file"
      ;;
  esac
done

if [ -n "$VILLAGESQL_TEST_FILES" ]; then
  echo "Checking VillageSQL test files for accidental laptop strings..."

  for file in $VILLAGESQL_TEST_FILES; do
    for forbidden_string in "${accidental_laptop_strings[@]}"; do
      # Use grep to find lines containing the forbidden string
      while IFS=: read -r line_num line_content; do
        if [ -n "$line_num" ] && [ -n "$line_content" ]; then
          echo "  ERROR: $file:$line_num contains forbidden string '$forbidden_string'"
          echo "    $line_content"
          LAPTOP_STRINGS_FOUND=$((LAPTOP_STRINGS_FOUND + 1))
        fi
      done < <(grep -n "$forbidden_string" "$file" 2>/dev/null || true)
    done
  done
fi

# --- Check for raw VSQL enum indexing into com_stat arrays ---
# com_stat arrays use compact indexing. VSQL commands (>= SQLCOM_VSQL_FIRST)
# must be wrapped in sqlcom_compact_index(). Auto-derives VSQL command names
# from the enum header so the check stays current as new commands are added.
COMPACT_INDEX_ERRORS=0

# Extract VSQL command names from the enum (between SQLCOM_VSQL_FIRST and SQLCOM_END)
VSQL_COMMANDS=$(sed -n '/SQLCOM_VSQL_FIRST/,/SQLCOM_END/p' include/my_sqlcommand.h | \
  grep -oE 'SQLCOM_[A-Z_]+' | \
  grep -v 'SQLCOM_VSQL_FIRST\|SQLCOM_END\|SQLCOM_MYSQL_COUNT\|SQLCOM_COMPACT_COUNT' | \
  sort -u)

if [ -n "$VSQL_COMMANDS" ]; then
  for file in $C_FILES; do
    if is_ignored "$file"; then
      continue
    fi
    # Skip the header that defines the enum itself
    case "$file" in
      include/my_sqlcommand.h|include/mysql/plugin_audit.h.pp) continue ;;
    esac
    for vsql_cmd in $VSQL_COMMANDS; do
      # Find lines with com_stat[...VSQL_CMD...] but NOT sqlcom_compact_index
      while IFS=: read -r line_num line_content; do
        if [ -n "$line_num" ] && [ -n "$line_content" ]; then
          if ! echo "$line_content" | grep -q 'sqlcom_compact_index'; then
            echo "  ERROR: $file:$line_num uses $vsql_cmd in com_stat without sqlcom_compact_index()"
            echo "    $line_content"
            COMPACT_INDEX_ERRORS=$((COMPACT_INDEX_ERRORS + 1))
          fi
        fi
      done < <(grep -n "com_stat\[.*${vsql_cmd}" "$file" 2>/dev/null || true)
    done
  done
fi

echo "Linting complete."

# Exit with error if laptop strings were found
if [ $LAPTOP_STRINGS_FOUND -gt 0 ]; then
  echo ""
  echo "ERROR: Found $LAPTOP_STRINGS_FOUND forbidden laptop string(s) in test files."
  echo "Remove these before committing."
  exit 1
fi

if [ $INVALID_TODO_TAGS_FOUND -gt 0 ]; then
  echo ""
  echo "ERROR: Found $INVALID_TODO_TAGS_FOUND invalid TODO tag(s)."
  echo "Allowed tags: ${ALLOWED_TODO_TAGS[*]}"
  echo "To allow a new tag, edit ALLOWED_TODO_TAGS in: scripts/villint.sh"
  exit 1
fi

if [ $COMPACT_INDEX_ERRORS -gt 0 ]; then
  echo ""
  echo "ERROR: Found $COMPACT_INDEX_ERRORS com_stat access(es) using VSQL enum values without sqlcom_compact_index()."
  echo "VSQL commands (>= SQLCOM_VSQL_FIRST) must use sqlcom_compact_index() when indexing com_stat arrays."
  echo "Example: com_stat[sqlcom_compact_index((uint)SQLCOM_INSTALL_EXTENSION)]"
  exit 1
fi
