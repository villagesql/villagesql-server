#!/usr/bin/env bash
# Builds the Slack message body listing every extension test result.
#
# Reads the per-extension result JSONs written by villagesql_ext_record_result.sh
# and prints a flat, Slack-mrkdwn list to stdout — one line per test, passes and
# failures alike. Pure (no GitHub API), so it's testable locally:
#   villagesql_ext_record_result.sh vsql-ai dev linux-x86_64 success failure /tmp/r
#   villagesql_ext_slack_summary.sh /tmp/r
#
# Usage:
#   villagesql_ext_slack_summary.sh <results_dir>

# shellcheck disable=SC2016
set -euo pipefail
shopt -s nullglob

results_dir="${1:?usage: $0 <results_dir>}"

files=("$results_dir"/*.json)
if [ "${#files[@]}" -eq 0 ]; then
  printf 'No extension results were produced.'
  exit 0
fi

jq -rs 'sort_by(.extension, .abi, .platform)[]
  | (if .status == "success" then "✅" else "❌" end)
    + " `\(.extension)` (\(.abi), \(.platform))"' "${files[@]}"