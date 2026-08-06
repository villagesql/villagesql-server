#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Decide whether a workflow run can skip the mandatory build + unit-test gate.
#
# Usage:
#   workflow_skips_build.sh <event_name> <base_ref>
#
#   event_name - the GitHub Actions event name (github.event_name)
#   base_ref   - the target branch of a pull request (github.base_ref);
#                unused for non-pull_request events
#
# Only "pull_request" events are eligible to skip; every other event (e.g. a
# push to main) always builds. For a pull request, the script fetches the base
# ref into the existing checkout and diffs it against HEAD, then matches the
# changed paths against the exempt-pattern list file. It never clones -- it
# operates on the working directory it is run from.
#
# Prints exactly one token to stdout -- never blank:
#   skip   - pull_request whose every changed path matches an exempt pattern;
#            the build/test gate can be safely skipped.
#   build  - anything else, INCLUDING every error or uncertain case.
#
# This gates a required status check, so it fails SAFE: a non-PR event, a
# missing base ref, git errors, an empty or missing pattern file, an empty
# diff, or a grep failure all resolve to "build". "skip" is emitted only when
# we positively confirm every changed path is exempt.
#
# Human-readable logging goes to stderr; stdout carries only the token so the
# caller can capture it cleanly (mirrors discover_build_env.sh).
#
# Env vars:
#   EXEMPT_FILE - path to the exempt-pattern list (default: workflow_skip_paths.txt
#                 next to this script)

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$TOOLS_DIR/../scripts" && pwd)"
source "$SCRIPTS_DIR/vsql_script_utils.sh"

EXEMPT_FILE="${EXEMPT_FILE:-$TOOLS_DIR/workflow_skip_paths.txt}"
EVENT_NAME="${1:-}"
BASE_REF="${2:-}"

# stdout is reserved for the verdict token so the caller can capture it cleanly.
# The log_* helpers write to stdout, so redirect fd 1 to stderr for the whole
# script and keep the original stdout on fd 3 for emit() alone.
exec 3>&1 1>&2

# Print the verdict token on the reserved stdout and exit.
emit() {
    echo "$1" >&3
    exit 0
}

# Only pull requests are eligible to skip; every other event always builds.
if [[ "$EVENT_NAME" != "pull_request" ]]; then
    log_info "Event '${EVENT_NAME:-<none>}' is not a pull request; build required"
    emit build
fi

if [[ -z "$BASE_REF" ]]; then
    log_warn "No base ref given for pull request; build required (fail-safe)"
    emit build
fi

if [[ ! -r "$EXEMPT_FILE" ]]; then
    log_warn "Exempt-pattern file not readable ($EXEMPT_FILE); build required"
    emit build
fi

# Load exempt patterns, dropping blank lines and '#' comments. A stray blank
# line would match every path in a `grep -f`, so it must never reach grep.
PATTERNS="$(grep -vE '^[[:space:]]*(#|$)' "$EXEMPT_FILE" || true)"
if [[ -z "$PATTERNS" ]]; then
    log_warn "No exempt patterns in $EXEMPT_FILE; build required"
    emit build
fi

# Fetch the base ref into the existing checkout so we can diff against it. Any
# git failure is treated as "build" -- we must not skip on an unknown diff.
if ! git fetch origin "$BASE_REF" --depth=50; then
    log_warn "git fetch of base ref '$BASE_REF' failed; build required"
    emit build
fi

set +e
CHANGED="$(git diff --name-only FETCH_HEAD...HEAD)"
git_status=$?
set -e
if [[ $git_status -ne 0 ]]; then
    log_warn "git diff failed (status $git_status); build required"
    emit build
fi

CHANGED="$(printf '%s\n' "$CHANGED" | grep -vE '^[[:space:]]*$' || true)"
if [[ -z "$CHANGED" ]]; then
    log_warn "No changed files in diff; nothing to check"
    emit skip
fi

# Find changed paths matching NONE of the exempt patterns. grep exits 0 when it
# prints such paths, 1 when there are none (everything is exempt), and >1 on a
# real error -- distinguish these explicitly rather than letting the status
# propagate under `set -euo pipefail`.
set +e
UNMATCHED="$(printf '%s\n' "$CHANGED" | grep -vE -f <(printf '%s\n' "$PATTERNS"))"
grep_status=$?
set -e

if [[ $grep_status -gt 1 ]]; then
    log_warn "grep failed (status $grep_status) matching exempt patterns; build required"
    emit build
fi

changed_count="$(printf '%s\n' "$CHANGED" | wc -l | tr -d ' ')"

if [[ -z "$UNMATCHED" ]]; then
    log_info "All $changed_count changed file(s) match exempt patterns -- build can be skipped"
    emit skip
fi

log_info "Non-exempt changed file(s) present -- build required:"
while IFS= read -r f; do
    [[ -n "$f" ]] && log_info "  $f"
done <<< "$UNMATCHED"
emit build
