#!/usr/bin/env bash
# Finds an already-running Slash Commands run for a PR (the duplicate guard).
#
# Reads the GitHub "list workflow runs" JSON on stdin and prints the first
# in-flight run (status queued/in_progress) whose display title starts with the
# given prefix, excluding the current run. Prints nothing if none.
#
# issue_comment runs all report the default branch, so we can't filter by
# branch; we match on the run-name prefix ("PR #<n>:") instead.
#
# Pure (no GitHub API), so it's testable:
#   echo "$RUNS_JSON" | villagesql_slash_find_running.sh 'PR #5:' 123
#
# Usage:
#   gh api ".../workflows/slash-commands.yml/runs?per_page=50" \
#     | villagesql_slash_find_running.sh "<prefix>" "<self_run_id>"
#
# Output: "<run_number>\t<html_url>" of the matching run, or empty.

set -euo pipefail

prefix="${1:?usage: $0 <prefix> <self_run_id>}"
self_id="${2:?usage: $0 <prefix> <self_run_id>}"

jq -r \
  --arg prefix "$prefix" \
  --argjson self "$self_id" '
    .workflow_runs // []
    | map(select(
        (.status == "in_progress" or .status == "queued")
        and .id != $self
        and ((.display_title // .name // "") | startswith($prefix))))
    | if length > 0 then "\(.[0].run_number)\t\(.[0].html_url)" else "" end'
