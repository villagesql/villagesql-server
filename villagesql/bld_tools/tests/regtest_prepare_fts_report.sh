#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Regression tests for prepare_fts_report.sh.
#
# Run locally:
#   villagesql/bld_tools/tests/regtest_prepare_fts_report.sh
#
# Needs jq and xmllint.  Exits non-zero if any check fails.

# Backticks in single quotes are Slack mono, not command substitution.
# shellcheck disable=SC2016
set -uo pipefail

TESTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPORT="$TESTS_DIR/../prepare_fts_report.sh"

fail=0
check() {
    local label="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        echo "ok   - $label"
    else
        echo "FAIL - $label"
        echo "       expected: $expected"
        echo "       actual:   $actual"
        fail=1
    fi
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Write an MTR XML result file with the given number of failed tests, plus one
# passing and one unstable test, laid out as mysql-test-run.pl writes it.
# Args: file fail_count
write_xml() {
    local file="$1"
    local fail_count="$2"
    local i
    {
        echo '<?xml version="1.0" encoding="UTF-8"?>'
        echo '<testsuites>'
        echo '  <testsuite name="report">'
        echo '    <testcase name="alias" status="pass" time="1" suitename="main" />'
        echo '    <testcase name="flaky" status="unstable" time="1" suitename="main" >'
        echo '    </testcase>'
        for ((i = 1; i <= fail_count; i++)); do
            printf '    <testcase name="t%03d" status="fail" time="1" suitename="innodb" >\n' "$i"
            echo '    </testcase>'
        done
        echo '  </testsuite>'
        echo '</testsuites>'
    } > "$file"
}

write_xml "$TMP/two.xml" 2
write_xml "$TMP/many.xml" 53
write_xml "$TMP/none.xml" 0

COMMON=(--platform linux-x86_64 --build-type debug
        --run-url http://run/1 --branch main --commit abc123
        --trigger workflow_dispatch)

# Print the ladder text, or the failed-test text (empty when absent), from a
# payload on stdin.
ladder() { jq -r '.attachments[0].text'; }
failed() { jq -r '.attachments[] | select(.title == "Failed tests") | .text'; }

# --- all stages pass --------------------------------------------------------
OUT="$("$REPORT" --status success "${COMMON[@]}" \
    --stage "Checkout code=success" \
    --stage "Build VillageSQL=success" \
    --test-stage "Run big tests=success" \
    --xml "$TMP/two.xml")"
check "pass: header" "✅ Full Test Suite passed (linux-x86_64, debug)" \
    "$(jq -r .text <<<"$OUT")"
check "pass: ladder" "$(printf '%s\n' \
    '✅ `Checkout code`: success' \
    '✅ `Build VillageSQL`: success' \
    '✅ `Run big tests`: success')" "$(ladder <<<"$OUT")"
check "pass: no failed-test section" "" "$(failed <<<"$OUT")"
check "pass: color" "#36a64f" "$(jq -r '.attachments[0].color' <<<"$OUT")"
check "pass: footer fields" "Run=<http://run/1|View details>,Branch=main,Commit=abc123,Triggered by=workflow_dispatch" \
    "$(jq -r '[.attachments[-1].fields[] | "\(.title)=\(.value)"] | join(",")' <<<"$OUT")"

# --- build fails: later stages are not listed, no tests -------------------
OUT="$("$REPORT" --status failure "${COMMON[@]}" \
    --stage "Checkout code=success" \
    --stage "Build VillageSQL=failure" \
    --test-stage "Run big tests=" \
    --xml "$TMP/two.xml")"
check "build fail: header" "❌ Full Test Suite failed (linux-x86_64, debug)" \
    "$(jq -r .text <<<"$OUT")"
check "build fail: ladder stops" "$(printf '%s\n' \
    '✅ `Checkout code`: success' \
    '❌ `Build VillageSQL`: fail')" "$(ladder <<<"$OUT")"
check "build fail: no failed-test section" "" "$(failed <<<"$OUT")"

# --- test stage fails: failed tests listed ---------------------------------
OUT="$("$REPORT" --status failure "${COMMON[@]}" \
    --stage "Build VillageSQL=success" \
    --test-stage "Run big tests=failure" \
    --test-stage "Run all test suites=skipped" \
    --xml "$TMP/two.xml")"
check "test fail: ladder stops" "$(printf '%s\n' \
    '✅ `Build VillageSQL`: success' \
    '❌ `Run big tests`: fail')" "$(ladder <<<"$OUT")"
check "test fail: failed tests, unstable ignored" "$(printf '%s\n' \
    '❌ `innodb.t001`: fail' \
    '❌ `innodb.t002`: fail')" "$(failed <<<"$OUT")"
check "test fail: footer is last" "4" \
    "$(jq '.attachments[-1].fields | length' <<<"$OUT")"

# --- many failures are capped ----------------------------------------------
OUT="$("$REPORT" --status failure "${COMMON[@]}" \
    --test-stage "Run big tests=failure" --xml "$TMP/many.xml")"
check "cap: 50 tests listed" "50" "$(failed <<<"$OUT" | grep -c '^❌')"
check "cap: remainder noted" "…and 3 more" "$(failed <<<"$OUT" | tail -1)"
OUT="$("$REPORT" --status failure "${COMMON[@]}" --max-failures 5 \
    --test-stage "Run big tests=failure" --xml "$TMP/many.xml")"
check "cap: --max-failures" "…and 48 more" "$(failed <<<"$OUT" | tail -1)"

# --- a failed test stage with no recorded failures -------------------------
NO_FAILS="No MTR test failures recorded; see the run logs."
OUT="$("$REPORT" --status failure "${COMMON[@]}" \
    --test-stage "Run standard tests=failure" --xml "$TMP/none.xml")"
check "no fails in xml: pointer to logs" "$NO_FAILS" "$(failed <<<"$OUT")"
OUT="$("$REPORT" --status failure "${COMMON[@]}" \
    --test-stage "Run standard tests=failure" --xml "$TMP/missing.xml")"
check "missing xml: pointer to logs" "$NO_FAILS" "$(failed <<<"$OUT")"

# --- cancelled -------------------------------------------------------------
OUT="$("$REPORT" --status cancelled "${COMMON[@]}" \
    --stage "Build VillageSQL=success" \
    --test-stage "Run big tests=cancelled" --xml "$TMP/two.xml")"
check "cancelled: header" "⚠️ Full Test Suite cancelled (linux-x86_64, debug)" \
    "$(jq -r .text <<<"$OUT")"
check "cancelled: ladder" "$(printf '%s\n' \
    '✅ `Build VillageSQL`: success' \
    '⚠️ `Run big tests`: cancelled')" "$(ladder <<<"$OUT")"
check "cancelled: no failed-test section" "" "$(failed <<<"$OUT")"
check "cancelled: color" "#daa520" "$(jq -r '.attachments[0].color' <<<"$OUT")"

# --- the job never ran -----------------------------------------------------
OUT="$("$REPORT" --status skipped --stage "Checkout code=")"
check "never ran: header" "❌ Full Test Suite failed" "$(jq -r .text <<<"$OUT")"
check "never ran: ladder" "No stages ran." "$(ladder <<<"$OUT")"
check "never ran: unknown footer" "unknown,unknown,unknown,unknown" \
    "$(jq -r '[.attachments[-1].fields[].value] | join(",")' <<<"$OUT")"

# --- bad arguments ---------------------------------------------------------
"$REPORT" --stage "Build" >/dev/null 2>&1
check "args: stage without outcome rejected" "1" "$?"
"$REPORT" --stage "Build=success" >/dev/null 2>&1
check "args: --status required" "1" "$?"

echo
if [[ "$fail" -ne 0 ]]; then
    echo "REGTEST FAILED"
    exit 1
fi
echo "All prepare_fts_report.sh regtests passed."
