#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# List tests with a given status from an MTR XML result file (as written by
# mysql-test-run.pl --xml-report), one suite.testname per line.
#
# Usage: mtr_extract_tests.sh <fail|unstable> <result.xml>

set -euo pipefail

usage() {
  cat <<EOF
Usage: $(basename "$0") <fail|unstable> <result.xml>

List tests with the given status from an MTR XML result file,
one suite.testname per line.
EOF
}

# Convert xmllint attribute output (name="t" then suitename="s" for each
# testcase, in document order) to suite.testname lines.
xml_to_suite_test() {
  sed -E 's/^ *(suitename|name)="([^"]*)"$/\2/' |
    paste -d' ' - - |
    awk '{print $2 "." $1}'
}

list_tests() {
  local status="$1"
  local xml_file="$2"

  xmllint --xpath \
    "//testcase[@status=\"$status\"]/@*[name()=\"suitename\" or name()=\"name\"]" \
    "$xml_file" | xml_to_suite_test
}

main() {
  if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
    usage
    return 0
  fi
  if [[ $# -ne 2 ]]; then
    usage >&2
    return 2
  fi

  local status="$1"
  local xml_file="$2"

  if [[ ! -r "$xml_file" ]]; then
    echo "Cannot read result file: $xml_file" >&2
    return 1
  fi

  list_tests "$status" "$xml_file"
}

main "$@"
