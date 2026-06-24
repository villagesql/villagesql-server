#!/usr/bin/env bash
# Records a single extension test result as JSON, for the summary builder.
#
# Each test-extension matrix job calls this; matrix jobs can't share an output,
# so each writes a tiny artifact that villagesql_ext_compat_summary.sh collects.
#
# Usage:
#   villagesql_ext_record_result.sh <ext> <abi> <platform> \
#       <build_outcome> <mtr_outcome> <out_dir>
#
# Status is "success" only when both the build and MTR step outcomes are
# "success"; anything else (failure, skipped, cancelled) is "failure".
#
# Testable locally:
#   villagesql_ext_record_result.sh vsql-ai dev linux-x86_64 success success /tmp/r
#   cat /tmp/r/vsql-ai-linux-x86_64-dev.json

set -euo pipefail

if [ "$#" -ne 6 ]; then
  echo "usage: $0 <ext> <abi> <platform> <build_outcome> <mtr_outcome> <out_dir>" >&2
  exit 2
fi

ext="$1"
abi="$2"
platform="$3"
build_outcome="$4"
mtr_outcome="$5"
out_dir="$6"

status=success
if [ "$build_outcome" != "success" ] || [ "$mtr_outcome" != "success" ]; then
  status=failure
fi

mkdir -p "$out_dir"
jq -cn \
  --arg e "$ext" \
  --arg a "$abi" \
  --arg p "$platform" \
  --arg s "$status" \
  '{extension:$e, abi:$a, platform:$p, status:$s}' \
  > "$out_dir/${ext}-${platform}-${abi}.json"
