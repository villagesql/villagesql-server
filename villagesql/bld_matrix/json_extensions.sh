#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Print bundled_extensions.txt as a compact JSON array, one object per entry.
#
# This is the parsed form of the manifest; it applies no filtering. Callers
# select the entries they want from the array.
#
# Each object:
#   url        — entry URL with any trailing "/" removed
#   branch     — branch or tag, which every entry must name
#   build      — build tool, "cmake" when the entry omits it
#   path       — subdirectory holding the extension, "" for the repo root
#   extension  — last segment of path, or of url when path is ""
#   abis       — ["stable","dev"] unless the entry pins one with abi=
#   bundle     — the narrowest build channel that ships the extension:
#                "release", "dev", or "none". A manifest entry says how widely
#                it ships (bundle=all, the default; bundle=dev; bundle=none),
#                and this names the channel that follows from it, so a "dev"
#                extension reaches pre-release artifacts only — the -dev
#                tarball and the dev Docker image — and is held back from
#                release builds. true/yes and false/no are read as all and
#                none. Any other bundle= value is an error, so a typo cannot
#                silently ship or withhold an extension.
#
# There is no default branch. An entry that names none — whether it stops at
# the url or goes straight to a key=value option — is an error. Unrecognized
# keys are ignored.
#
# Inputs (environment variables, all optional):
#   EXTENSIONS_FILE — path to bundled_extensions.txt
#                     (default: the copy in this script's own source tree)
#
# Testable locally:
#   villagesql/bld_matrix/json_extensions.sh | jq .
#   EXTENSIONS_FILE=/tmp/exts.txt villagesql/bld_matrix/json_extensions.sh

set -euo pipefail

MATRIX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$MATRIX_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

EXTENSIONS_FILE="${EXTENSIONS_FILE:-$SOURCE_DIR/villagesql/dev_server/bundled_extensions.txt}"

if [[ ! -f "$EXTENSIONS_FILE" ]]; then
    die "Extensions list not found: $EXTENSIONS_FILE"
fi

# -n with inputs, rather than a grep prefilter, so that a manifest holding
# nothing but comments yields [] instead of failing the pipeline on grep's
# empty-match exit status.
jq -Rcn '
    [
      inputs
      | gsub("^\\s+|\\s+$"; "")
      | select(length > 0 and (startswith("#") | not))
      | [splits("\\s+")] as $f
      | if (($f[1] // "") | test("^$|=")) then
          error("entry names no branch: \(.)")
        else . end
      | $f[2:] as $opts
      | ($opts | map(select(startswith("build=")) | ltrimstr("build=")) | .[0] //
        "cmake") as $build
      | ($opts | map(select(startswith("path=")) | ltrimstr("path=")) | .[0] // "")
          as $path
      | ($opts | map(select(startswith("abi=")) | ltrimstr("abi=")) | .[0]) as $abi
      | ($opts | map(select(startswith("bundle=")) | ltrimstr("bundle=")) | .[0] //
        "all" | ascii_downcase) as $bundle
      | {
          url:       ($f[0] | rtrimstr("/")),
          branch:    $f[1],
          build:     $build,
          path:      $path,
          extension: (if $path != "" then ($path | split("/") | last)
                      else ($f[0] | rtrimstr("/") | split("/") | last) end),
          abis:      (if $abi then [$abi] else ["stable","dev"] end),
          bundle:    (if   $bundle | IN("all", "true", "yes")  then "release"
                      elif $bundle == "dev"                    then "dev"
                      elif $bundle | IN("none", "false", "no") then "none"
                      else error("unknown bundle=\($bundle) in entry: \(.)")
                      end)
        }
    ]' "$EXTENSIONS_FILE"
