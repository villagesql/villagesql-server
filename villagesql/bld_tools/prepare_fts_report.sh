#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print the Slack incoming-webhook payload (JSON) that reports a Full Test
# Suite run.
#
# The header gives the overall result, platform, and build type.  The first
# section is the stage ladder: one line per stage, in order, ending at the
# first stage that did not succeed.  Stages after it never ran and are not
# listed.  When that stage is a test stage that failed, a second section lists
# the failed MTR tests from the MTR XML result file, up to a cap.  The last
# section carries the Run, Branch, Commit, and Triggered by fields.
#
# Usage: prepare_fts_report.sh [options]
#
# Stage outcomes are GitHub step outcomes (success, failure, cancelled,
# skipped); an empty outcome means the step never ran.

# Backticks in single quotes are Slack mono, not command substitution.
# shellcheck disable=SC2016
set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$TOOLS_DIR/../scripts/vsql_script_utils.sh"

TITLE="Full Test Suite"
DEFAULT_MAX_FAILURES=50

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Print the Slack payload (JSON) that reports a Full Test Suite run.

Options:
  --status RESULT         Overall result of the run: success, failure,
                          cancelled, or skipped.  Any other value is
                          reported as given.
  --platform NAME         Platform tested, e.g. linux-x86_64.
  --build-type TYPE       Build type tested, e.g. debug.
  --stage LABEL=OUTCOME   A build stage, in ladder order.  Repeatable.
  --test-stage LABEL=OUTCOME
                          A test stage, in ladder order.  Repeatable.  If it
                          is the stage that failed, the failed tests in the
                          --xml file are listed.
  --xml FILE              MTR XML result file (mysql-test-run.pl --xml-report).
  --max-failures N        Most failed tests to list (default $DEFAULT_MAX_FAILURES).
  --run-url URL           Link for the Run field.
  --branch REF            Value for the Branch field.
  --commit SHA            Value for the Commit field.
  --trigger EVENT         Value for the Triggered by field.

Example:
  $(basename "$0") --status failure --platform linux-x86_64 \\
      --build-type debug --stage "Build VillageSQL=success" \\
      --test-stage "Run big tests=failure" --xml mysql-test-report.xml
EOF
}

# Set icon, word, label, and color for a GitHub result.  A stage line uses
# word; the header uses label.  An unknown result keeps its own value as its
# word and label.
# Args: result
outcome_attrs() {
    case "$1" in
        success)   icon="✅"; word=success;   label=passed;    color="#36a64f" ;;
        failure)   icon="❌"; word=fail;      label=failed;    color="#cc0000" ;;
        cancelled) icon="⚠️"; word=cancelled; label=cancelled; color="#daa520" ;;
        *)         icon="⚠️"; word="$1";      label="$1";      color="#daa520" ;;
    esac
}

# Print one report line: icon, label in mono, and outcome.
# Args: label outcome
item_line() {
    local icon word label color
    outcome_attrs "$2"
    printf '%s `%s`: %s\n' "$icon" "$1" "$word"
}

# Print the failed tests in xml_file, one item line each, up to max.  A
# missing file or an empty list prints a pointer to the run logs instead.
# Args: xml_file max
failed_test_lines() {
    local xml_file="$1"
    local max="$2"
    local tests=""
    local rc=0

    if [[ -n "$xml_file" && -r "$xml_file" ]]; then
        # xmllint exits 10 when nothing matches, which is not an error here.
        tests="$("$TOOLS_DIR/mtr_extract_tests.sh" fail "$xml_file" \
            2>/dev/null)" || rc=$?
        if [[ $rc -ne 0 && $rc -ne 10 ]]; then
            die "mtr_extract_tests.sh failed (exit $rc) on $xml_file"
        fi
    fi

    tests="$(printf '%s\n' "$tests" | grep -v '^$' | LC_ALL=C sort -u || true)"
    if [[ -z "$tests" ]]; then
        echo "No MTR test failures recorded; see the run logs."
        return 0
    fi

    local count="$(printf '%s\n' "$tests" | wc -l | tr -d ' ')"
    local name
    printf '%s\n' "$tests" | head -n "$max" | while IFS= read -r name; do
        item_line "$name" failure
    done
    if [[ $count -gt $max ]]; then
        echo "…and $((count - max)) more"
    fi
}

main() {
    local status=""
    local platform=""
    local build_type=""
    local xml_file=""
    local max_failures="$DEFAULT_MAX_FAILURES"
    local run_url=""
    local branch=""
    local commit=""
    local trigger=""
    local stages=()
    local stage_kinds=()

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help) usage; return 0 ;;
            --status) status="${2:?--status requires a value}"; shift ;;
            --platform) platform="${2:?--platform requires a value}"; shift ;;
            --build-type)
                build_type="${2:?--build-type requires a value}"
                shift
                ;;
            --stage|--test-stage)
                [[ $# -gt 1 && "$2" == *=* ]] ||
                    die "$1 requires a LABEL=OUTCOME value"
                stages+=("$2")
                stage_kinds+=("${1#--}")
                shift
                ;;
            --xml) xml_file="${2:?--xml requires a value}"; shift ;;
            --max-failures)
                max_failures="${2:?--max-failures requires a value}"
                [[ "$max_failures" =~ ^[0-9]+$ ]] ||
                    die "--max-failures must be a number: $max_failures"
                shift
                ;;
            --run-url) run_url="${2-}"; shift ;;
            --branch) branch="${2-}"; shift ;;
            --commit) commit="${2-}"; shift ;;
            --trigger) trigger="${2-}"; shift ;;
            *) usage >&2; return 2 ;;
        esac
        shift
    done
    [[ -n "$status" ]] || die "--status is required"

    command -v jq >/dev/null || die "Missing dependency: jq"

    local icon word label color
    outcome_attrs "$status"

    local header="$icon $TITLE $label"
    if [[ -n "$platform" || -n "$build_type" ]]; then
        local detail="$platform${platform:+${build_type:+, }}$build_type"
        header="$header ($detail)"
    fi

    # The ladder ends at the first stage that did not succeed.  A stage that
    # was skipped or never ran is not listed.
    local ladder=""
    local failed_tests=""
    local i
    for i in "${!stages[@]}"; do
        local stage_label="${stages[$i]%=*}"
        local outcome="${stages[$i]##*=}"
        if [[ -z "$outcome" || "$outcome" == "skipped" ]]; then
            break
        fi
        ladder+="$(item_line "$stage_label" "$outcome")"$'\n'
        if [[ "$outcome" == "failure" &&
                "${stage_kinds[$i]}" == "test-stage" ]]; then
            failed_tests="$(failed_test_lines "$xml_file" "$max_failures")"
        fi
        if [[ "$outcome" != "success" ]]; then
            break
        fi
    done
    if [[ -z "$ladder" ]]; then
        ladder="No stages ran."
    fi

    jq -n \
        --arg header "$header" \
        --arg color "$color" \
        --arg ladder "${ladder%$'\n'}" \
        --arg failed_tests "$failed_tests" \
        --arg run_url "$run_url" \
        --arg branch "${branch:-unknown}" \
        --arg commit "${commit:-unknown}" \
        --arg trigger "${trigger:-unknown}" \
        '{
          text: $header,
          attachments: (
            [{color: $color, text: $ladder}]
            + (if $failed_tests == "" then []
               else [{color: $color, title: "Failed tests",
                      text: $failed_tests}]
               end)
            + [{color: $color,
                fields: [
                  {title: "Run",
                   value: (if $run_url == "" then "unknown"
                           else "<\($run_url)|View details>" end),
                   short: true},
                  {title: "Branch", value: $branch, short: true},
                  {title: "Commit", value: $commit, short: true},
                  {title: "Triggered by", value: $trigger, short: true}
                ]}]
          )
        }'
}

main "$@"
