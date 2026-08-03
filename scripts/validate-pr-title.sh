#!/bin/bash
# Copyright (c) 2026, VillageSQL. All rights reserved.
#
# Validates a PR title for length and ending character.
# Usage: validate-pr-title.sh "PR title to validate"
#   or:  PR_TITLE="PR title" validate-pr-title.sh
#
# When a PR contains a single commit, GitHub overwrites the PR title with that
# commit's title on merge, and the commit title never goes through this check.
# Pass the single commit's title so it is validated too:
#   validate-pr-title.sh "PR title" "single commit title"
#   or: SINGLE_COMMIT_TITLE="commit title" validate-pr-title.sh

title="${1:-$PR_TITLE}"
single_commit_title="${2:-$SINGLE_COMMIT_TITLE}"

if [ -z "$title" ]; then
  echo "Usage: validate-pr-title.sh \"PR title to validate\""
  echo "   or: PR_TITLE=\"PR title\" validate-pr-title.sh"
  exit 1
fi

errors=()

# Validates one title against the length and ending-punctuation rules,
# prefixing each error with $1 (e.g. "PR title", "commit title") so the
# failing title is clear when both are checked.
validate_title() {
  local label="$1" value="$2"
  if [ ${#value} -gt 41 ]; then
    errors+=("$label is ${#value} characters long (max 41 to allow growth to 50 with gh issue numbers)")
  fi
  local last_char="${value: -1}"
  if [[ "$last_char" =~ [.,\;:!] ]]; then
    errors+=("$label ends with '$last_char' (must not end with punctuation: . , ; : !)")
  fi
}

validate_title "PR title" "$title"

# A single-commit PR merges with the commit title as the PR title, so that
# title must satisfy the same rules.
if [ -n "$single_commit_title" ]; then
  validate_title "commit title for a single-commit PR (GitHub uses it as the merged PR title)" "$single_commit_title"
fi

if [ ${#errors[@]} -gt 0 ]; then
  echo "ERROR: PR title validation failed:"
  for err in "${errors[@]}"; do
    echo "  - $err"
  done
  echo ""
  echo "PR title: \"$title\""
  if [ -n "$single_commit_title" ]; then
    echo "Commit title: \"$single_commit_title\""
  fi
  exit 1
fi
echo "PR title is valid: \"$title\""
