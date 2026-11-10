#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Finalize a draft release note and commit it.
#
# The draft note in the repo is required to have a "Draft release notes" line
# naming the last commit that its contents cover:
#
#   Draft release notes through commit `01521a7eb906`: Some commit subject (#1)
#
# This rewrites that line to name the current HEAD instead, and pushes it to the
# current branch as a new commit.
#
# The commit message is "Finalize <version> release notes", with the version
# taken from the VSQL_VERSION file with any pre-release suffix stripped, the
# form a release build uses.
#
# Usage:
#   villagesql/bld_tools/prepare_release_notes.sh <release_note>
#
#   <release_note> — the draft release note to finalize, as a path relative to
#                    the top of the source tree. Required; there is no default.
#
# Example:
#   villagesql/bld_tools/prepare_release_notes.sh \
#       Docs/release_notes/release_note_0_0_6.md

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$SOURCE_DIR/villagesql/bld_tools/build_info.sh"

RELEASE_NOTE="${1:?Usage: $0 <release_note>}"
NOTE_PATH="$SOURCE_DIR/$RELEASE_NOTE"

# The line this script rewrites is identified by this prefix.
DRAFT_PREFIX="Draft release notes through commit"

[[ -f "$NOTE_PATH" ]] || die "Release note not found: $NOTE_PATH"

DRAFT_LINES=$(grep -c "^$DRAFT_PREFIX" "$NOTE_PATH" || true)
if [[ "$DRAFT_LINES" -ne 1 ]]; then
    die "$RELEASE_NOTE has $DRAFT_LINES lines starting \"$DRAFT_PREFIX\", need 1"
fi

HEAD_SHA=$(git -C "$SOURCE_DIR" rev-parse --short HEAD) \
    || die "Cannot read HEAD of $SOURCE_DIR"
HEAD_SUBJECT=$(git -C "$SOURCE_DIR" log -1 --format=%s) \
    || die "Cannot read the commit message of HEAD"

DRAFT_LINE="$DRAFT_PREFIX \`$HEAD_SHA\`: $HEAD_SUBJECT"

TEMP_NOTE=$(mktemp)
trap 'rm -f "$TEMP_NOTE"' EXIT

# The prefix and the replacement pass through the environment rather than -v,
# so that a backslash in either arrives at awk intact.
DRAFT_PREFIX="$DRAFT_PREFIX" DRAFT_LINE="$DRAFT_LINE" awk '
    index($0, ENVIRON["DRAFT_PREFIX"]) == 1 { print ENVIRON["DRAFT_LINE"]; next }
    { print }' "$NOTE_PATH" >"$TEMP_NOTE"

# Copy rather than move, to leave the note with the permissions it had.
cp "$TEMP_NOTE" "$NOTE_PATH"
log_info "$RELEASE_NOTE: $DRAFT_LINE"

# The empty pre-release argument strips the suffix the version file carries.
VSQL_VERSION=$(vsql_json_version "$SOURCE_DIR" "")

COMMIT_MESSAGE="Finalize $VSQL_VERSION release notes"
git -C "$SOURCE_DIR" commit --quiet -m "$COMMIT_MESSAGE" -- "$RELEASE_NOTE"
log_info "Committed: $COMMIT_MESSAGE"
