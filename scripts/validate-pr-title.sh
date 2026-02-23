#!/bin/bash
# Copyright (c) 2026, VillageSQL. All rights reserved.
#
# Validates a PR title for length and ending character.
# Usage: validate-pr-title.sh "PR title to validate"
#   or:  PR_TITLE="PR title" validate-pr-title.sh

title="${1:-$PR_TITLE}"

if [ -z "$title" ]; then
  echo "Usage: validate-pr-title.sh \"PR title to validate\""
  echo "   or: PR_TITLE=\"PR title\" validate-pr-title.sh"
  exit 1
fi

errors=()

if [ ${#title} -gt 41 ]; then
  errors+=("PR title is ${#title} characters long (max 41 to allow growth to 50 with gh issue numbers)")
fi

last_char="${title: -1}"
if ! [[ "$last_char" =~ [a-zA-Z0-9\)] ]]; then
  errors+=("PR title ends with '$last_char' (must end with alphanumeric or closing paren)")
fi

if [ ${#errors[@]} -gt 0 ]; then
  echo "ERROR: PR title validation failed:"
  for err in "${errors[@]}"; do
    echo "  - $err"
  done
  echo ""
  echo "Title: \"$title\""
  exit 1
fi
echo "PR title is valid: \"$title\""
