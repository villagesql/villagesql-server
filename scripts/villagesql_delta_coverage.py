#!/usr/bin/env python3
# Copyright (c) 2026 VillageSQL Contributors
"""VillageSQL differential ("delta") coverage filter.

Reduces an lcov coverage report to only the lines added or changed since
VillageSQL forked from upstream MySQL, so ordinary genhtml renders a familiar
navigable report scoped to VillageSQL-authored code rather than the inherited
MySQL tree.

The delta is `git diff <base_ref>..HEAD`, a snapshot comparison: it captures
every VillageSQL change present in HEAD regardless of when it was authored, and
excludes upstream code that is byte-identical in both trees. Use the upstream
release tag HEAD is built on (e.g. mysql-8.4.10) as <base_ref> -- diffing
against the fork point instead would misattribute later upstream patch releases
that VillageSQL has since merged in.

lcov tracefiles label each source file with an SF: (Source File) line and each
executable line with a DA: (line Data -- "DA:<line>,<hits>") entry. Only lines
with a DA: entry are considered; comments, blank lines, declarations, and
uninstantiated templates never produce one (the compiler emits no code, hence
no coverage data). SF: paths are kept absolute so genhtml can read the real
source files.

Usage:
  villagesql_delta_coverage.py <report.info> <base_ref> <out.info> [--repo DIR]

--repo defaults to the top level of the git repository containing this script.
Feed <out.info> to genhtml to produce the scoped report.
"""

import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict

# Source extensions worth intersecting with coverage; everything else in the
# diff (docs, CMake, .test/.result) has no DA: data and would be dropped anyway.
SOURCE_GLOBS = ["*.cc", "*.cpp", "*.c", "*.h", "*.hpp", "*.ic"]

_HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def repo_toplevel():
    # Resolve from the script's own directory, this script lives in the repo.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return subprocess.run(
        ["git", "-C", script_dir, "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True).stdout.strip()


def changed_lines(base, repo):
    """Map repo-relative path -> set of line numbers added/changed vs base."""
    out = subprocess.run(
        ["git", "-C", repo, "diff", f"{base}..HEAD", "--unified=0",
         "--no-color", "--", *SOURCE_GLOBS],
        capture_output=True, text=True, check=True).stdout
    changed, cur = defaultdict(set), None
    for line in out.splitlines():
        if line.startswith("+++ b/"):
            cur = line[6:]
        elif line.startswith("+++ /dev/null"):
            cur = None
        elif cur and line.startswith("@@"):
            m = _HUNK.match(line)
            if m:
                start = int(m.group(1))
                count = int(m.group(2)) if m.group(2) is not None else 1
                changed[cur].update(range(start, start + count))
    return changed


def to_relpath(sf_path, repo):
    """Repo-relative path for an lcov SF: path, or None if outside the repo."""
    prefix = repo.rstrip("/") + "/"
    return sf_path[len(prefix):] if sf_path.startswith(prefix) else None


# Dev-SDK headers compiled into (instrumented) extensions record coverage under
# the packaged include-dev path, which is a byte-identical copy of the source
# tree's villagesql/sdk/include. Remap those SF: paths back onto the source so
# they attribute to villagesql/sdk and merge with the unit tests' native
# coverage of the same headers, rather than being dropped as non-repo paths.
_SDK_REMAP = re.compile(r"^.*/villagesql-extension-sdk-[^/]*/include-dev/")


def remap_sf(sf_abs, repo):
    return _SDK_REMAP.sub(repo.rstrip("/") + "/villagesql/sdk/include/", sf_abs)


def iter_records(info_path):
    """Yield (sf_line, [(line, hits), ...]) for each record in an lcov file."""
    sf, da = None, []
    with open(info_path) as f:
        for line in f:
            if line.startswith("SF:"):
                sf, da = line.rstrip("\n"), []
            elif line.startswith("DA:"):
                ln, hits = line[3:].split(",")[:2]
                da.append((int(ln), int(hits)))
            elif line.startswith("end_of_record"):
                if sf is not None:
                    yield sf, da
                sf, da = None, []


def filter_to_delta(report, base_ref, out_path, repo):
    changed = changed_lines(base_ref, repo)
    # Accumulate hits per delta line, keyed by repo-relative path, SUMMING
    # across records. Summing merges coverage of the same source produced by
    # different builds -- notably the villagesql/sdk headers exercised both by
    # the instrumented tests and by the instrumented extensions (remapped from
    # include-dev).
    merged = {}  # relpath -> {line: summed_hits}
    for sf, da in iter_records(report):
        rel = to_relpath(remap_sf(sf[3:], repo), repo)
        if rel not in changed:
            continue
        acc = merged.setdefault(rel, {})
        for ln, hits in da:
            if ln in changed[rel]:
                acc[ln] = acc.get(ln, 0) + hits

    kept = 0
    with open(out_path, "w") as out:
        for rel in sorted(merged):
            acc = merged[rel]
            if not acc:
                continue
            out.write("TN:\n")
            # SF: absolute in the source tree so genhtml can read the source.
            out.write(f"SF:{repo.rstrip('/')}/{rel}\n")
            for ln in sorted(acc):
                out.write(f"DA:{ln},{acc[ln]}\n")
            out.write(f"LF:{len(acc)}\n")
            out.write(f"LH:{sum(1 for h in acc.values() if h > 0)}\n")
            out.write("end_of_record\n")
            kept += 1
    sys.stderr.write(f"kept {kept} files with delta line coverage\n")


def main():
    parser = argparse.ArgumentParser(
        description="Filter an lcov report down to the VillageSQL delta.")
    parser.add_argument("report", help="input lcov .info (e.g. report.info)")
    parser.add_argument("base_ref", help="upstream base git ref, e.g. a tag")
    parser.add_argument("out", help="output lcov .info scoped to the delta")
    parser.add_argument("--repo", default=None,
                        help="repo top level (default: git toplevel)")
    args = parser.parse_args()
    repo = args.repo or repo_toplevel()
    filter_to_delta(args.report, args.base_ref, args.out, repo)
    return 0


if __name__ == "__main__":
    sys.exit(main())
