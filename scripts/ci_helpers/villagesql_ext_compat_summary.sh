#!/usr/bin/env bash
# Builds the Markdown summary posted as a PR comment by /testextensions.
#
# Reads the per-extension result JSONs written by villagesql_ext_record_result.sh
# and prints a Markdown table to stdout. Pure (no GitHub API), so it's testable:
#   villagesql_ext_record_result.sh vsql-ai dev linux-x86_64 success failure /tmp/r
#   villagesql_ext_compat_summary.sh /tmp/r success 'https://example/run/1'
#
# Usage:
#   villagesql_ext_compat_summary.sh <results_dir> <build_result> <run_url>

# Backticks in single-quoted printf strings are intentional literal Markdown
# code-spans, not command substitution.
# shellcheck disable=SC2016
set -euo pipefail
shopt -s nullglob

results_dir="${1:?usage: $0 <results_dir> <build_result> <run_url>}"
build_result="${2:-}"
run_url="${3:-}"

printf '## 🧩 `/testextensions` results\n\n'

if [ "$build_result" != "success" ]; then
  printf '⚠️ Server/SDK build was `%s` — extension results may be incomplete.\n\n' \
    "$build_result"
fi

# Gather rows, sorted by extension then abi. Missing/empty dir => no rows.
files=("$results_dir"/*.json)
if [ "${#files[@]}" -eq 0 ]; then
  printf 'No extension results were produced. See the [run](%s).\n' "$run_url"
  exit 0
fi
rows=$(jq -s 'sort_by(.extension, .abi)' "${files[@]}")
count=${#files[@]}

printf '| Extension | ABI | Result |\n|---|---|---|\n'
printf '%s' "$rows" | jq -r '.[]
  | "| `\(.extension)` | \(.abi) | " +
    (if .status == "success" then "✅ success" else "❌ \(.status)" end) + " |"'

failed=$(printf '%s' "$rows" | jq '[.[] | select(.status != "success")] | length')
if [ "$failed" -eq 0 ]; then
  printf '\n✅ All extensions passed.'
else
  printf '\n❌ %s of %s failed.' "$failed" "$count"
fi
printf '\n\n[View run](%s)\n' "$run_url"
