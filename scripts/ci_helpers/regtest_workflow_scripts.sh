#!/usr/bin/env bash
# Regression tests for the slash-command / extension-compat helper scripts.
#
# Run locally:
#   ./scripts/regtest_workflow_scripts.sh
#
# Exits non-zero on the first failure.

set -uo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLASSIFY="$DIR/villagesql_slash_classify.sh"
FIND_RUNNING="$DIR/villagesql_slash_find_running.sh"
RECORD="$DIR/villagesql_ext_record_result.sh"
SUMMARY="$DIR/villagesql_ext_compat_summary.sh"

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

# --- classify ---------------------------------------------------------------
out() { BODY="$1" AUTHOR_ASSOCIATION="$2" "$CLASSIFY" | tr '\n' ',' ; }

check "classify: member /testall"        "kind=run,command=testall,"        "$(out '/testall' MEMBER)"
check "classify: owner /testextensions"  "kind=run,command=testextensions," "$(out '/testextensions' OWNER)"
check "classify: non-member /testall"    "kind=unauthorized,command=testall," "$(out '/testall' NONE)"
check "classify: unknown command"        "kind=unknown,command=deploy,"     "$(out '/deploy' MEMBER)"
check "classify: help"                   "kind=help,"                       "$(out '/help' NONE)"
check "classify: not a command"          "kind=ignore,"                     "$(out 'looks good to me' MEMBER)"
check "classify: trailing args ignored"  "kind=run,command=testall,"        "$(out '/testall please' MEMBER)"
check "classify: case-insensitive"       "kind=run,command=testall,"        "$(out '/TestAll' MEMBER)"
check "classify: empty body"             "kind=ignore,"                     "$(out '' MEMBER)"

# --- find_running -----------------------------------------------------------
RUNS='{"workflow_runs":[
  {"id":111,"run_number":7,"display_title":"PR #5: /testall","status":"in_progress","html_url":"http://x/111"},
  {"id":222,"run_number":8,"display_title":"PR #9: /testall","status":"in_progress","html_url":"http://x/222"},
  {"id":333,"run_number":9,"display_title":"PR #5: /testall","status":"completed","html_url":"http://x/333"}
]}'
check "find_running: other in-flight for PR" "$(printf '7\thttp://x/111')" "$(printf '%s' "$RUNS" | "$FIND_RUNNING" 'PR #5:' 999)"
check "find_running: excludes self"          ""                            "$(printf '%s' "$RUNS" | "$FIND_RUNNING" 'PR #5:' 111)"
check "find_running: none for other PR"      ""                            "$(printf '%s' "$RUNS" | "$FIND_RUNNING" 'PR #42:' 999)"
check "find_running: empty list"             ""                            "$(echo '{"workflow_runs":[]}' | "$FIND_RUNNING" 'PR #5:' 999)"

# --- record_result + summary ------------------------------------------------
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
"$RECORD" vsql-ai     dev    linux-x86_64 success success "$TMP" >/dev/null
"$RECORD" vsql-ai     stable linux-x86_64 success failure "$TMP" >/dev/null
"$RECORD" vsql-crypto stable linux-x86_64 failure skipped "$TMP" >/dev/null

check "record: success json" '{"extension":"vsql-ai","abi":"dev","platform":"linux-x86_64","status":"success"}' \
  "$(cat "$TMP/vsql-ai-linux-x86_64-dev.json")"
check "record: failed mtr => failure" '{"extension":"vsql-ai","abi":"stable","platform":"linux-x86_64","status":"failure"}' \
  "$(cat "$TMP/vsql-ai-linux-x86_64-stable.json")"

SUM="$("$SUMMARY" "$TMP" success 'http://run/1')"
check "summary: counts failures" "❌ 2 of 3 failed." \
  "$(printf '%s' "$SUM" | grep -o '❌ [0-9]* of [0-9]* failed.')"
check "summary: has table header" "| Extension | ABI | Result |" \
  "$(printf '%s' "$SUM" | grep -F '| Extension | ABI | Result |')"

EMPTY="$(mktemp -d)"
check "summary: empty results" "No extension results were produced. See the [run](http://run/1)." \
  "$("$SUMMARY" "$EMPTY" success 'http://run/1' | grep -F 'No extension results')"
rm -rf "$EMPTY"

SUM_OK="$("$SUMMARY" "$TMP" failure 'http://run/1')"
check "summary: build-failure note" "⚠️ Server/SDK build was \`failure\` — extension results may be incomplete." \
  "$(printf '%s' "$SUM_OK" | grep -F 'Server/SDK build was')"

echo
if [ "$fail" -ne 0 ]; then
  echo "REGTEST FAILED"
  exit 1
fi
echo "All workflow-script regtests passed."
