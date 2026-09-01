#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Create the draft GitHub release for a tag.
#
# Everything comes from the tag, not from the working tree, so the release
# describes the commit it names no matter what is checked out. The tag is
# assumed to be present locally and current.
#
# The release is created as a draft and carries no label: not "Latest", not
# "Pre-release". Publishing it is a separate, manual step.
#
# The title is "VillageSQL <version>", with the version read from the
# VSQL_VERSION file at the tag and its pre-release suffix stripped, the form a
# release build uses.
#
# The body is the draft release note as committed at the tag, with one line
# rewritten. The note carries a line naming the last commit its contents cover:
#
#   Draft release notes through commit `01521a7eb906`: Some commit subject (#1)
#
# That line becomes, naming the tagged commit instead:
#
#   Release notes through commit `a3bba4f7a825`: Some commit subject (#1)
#
# Nothing else in the note is touched, and the note in the working tree is left
# alone; only the release body differs from what is committed.
#
# Usage:
#   villagesql/bld_tools/prepare_github_release.sh <tag> <release_note>
#
#   <tag>          — the git tag to release. Required; there is no default.
#   <release_note> — the draft release note, as a path relative to the top of
#                    the source tree, read at <tag>. Required; no default.
#
# Example:
#   villagesql/bld_tools/prepare_github_release.sh release/0.0.6 \
#       Docs/release_notes/release_note_0_0_6.md

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$SOURCE_DIR/villagesql/bld_tools/build_info.sh"

TAG="${1:?Usage: $0 <tag> <release_note>}"
RELEASE_NOTE="${2:?Usage: $0 <tag> <release_note>}"

# The line this script rewrites is identified by this prefix, which the
# replacement drops.
DRAFT_PREFIX="Draft release notes through commit"

command -v gh >/dev/null || die "The GitHub CLI (gh) is not on PATH"

TAG_SHA=$(git -C "$SOURCE_DIR" rev-parse --short "$TAG^{commit}") \
    || die "Cannot resolve tag $TAG; fetch it with: git fetch --tags"
TAG_SUBJECT=$(git -C "$SOURCE_DIR" log -1 --format=%s "$TAG^{commit}") \
    || die "Cannot read the commit message of $TAG"

TEMP_NOTE=$(mktemp)
TEMP_BODY=$(mktemp)
TEMP_VERSION=$(mktemp)
trap 'rm -f "$TEMP_NOTE" "$TEMP_BODY" "$TEMP_VERSION"' EXIT

git -C "$SOURCE_DIR" show "$TAG:$RELEASE_NOTE" >"$TEMP_NOTE" \
    || die "Release note not found at $TAG: $RELEASE_NOTE"

DRAFT_LINES=$(grep -c "^$DRAFT_PREFIX" "$TEMP_NOTE" || true)
if [[ "$DRAFT_LINES" -ne 1 ]]; then
    die "$RELEASE_NOTE has $DRAFT_LINES lines starting \"$DRAFT_PREFIX\", need 1"
fi

RELEASE_LINE="Release notes through commit \`$TAG_SHA\`: $TAG_SUBJECT"

# The prefix and the replacement pass through the environment rather than -v,
# so that a backslash in either arrives at awk intact.
DRAFT_PREFIX="$DRAFT_PREFIX" RELEASE_LINE="$RELEASE_LINE" awk '
    index($0, ENVIRON["DRAFT_PREFIX"]) == 1 { print ENVIRON["RELEASE_LINE"]; next }
    { print }' "$TEMP_NOTE" >"$TEMP_BODY"

log_info "$RELEASE_NOTE: $RELEASE_LINE"

# json_version.sh reads whichever VSQL_VERSION file this names, so the version
# is the tagged one rather than the working tree's.
git -C "$SOURCE_DIR" show "$TAG:VSQL_VERSION" >"$TEMP_VERSION" \
    || die "VSQL_VERSION file not found at $TAG"

# The empty pre-release argument strips the suffix the version file carries.
export VSQL_VERSION_FILE="$TEMP_VERSION"
VSQL_VERSION=$(vsql_json_version "$SOURCE_DIR" "")
unset VSQL_VERSION_FILE

RELEASE_TITLE="VillageSQL $VSQL_VERSION"

# --latest=false leaves the release unlabeled; without it GitHub picks a label.
gh release create "$TAG" \
    --draft \
    --latest=false \
    --verify-tag \
    --title "$RELEASE_TITLE" \
    --notes-file "$TEMP_BODY"

log_info "Drafted: $RELEASE_TITLE ($TAG)"
