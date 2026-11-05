# Generating gcov Code Coverage

VillageSQL builds with `gcov` instrumentation and produces two HTML reports:

- **Delta report** (`coverage-delta/`) — coverage of **VillageSQL-authored code
  only**: everything changed since the upstream MySQL release the tree is built
  on. This is the report you usually want.
- **Full report** (`code_coverage/`) — coverage of the entire server, the
  standard upstream fastcov output.

Coverage is collected with GCC's gcov, aggregated by
[`fastcov`](https://github.com/RPGillespie6/fastcov), and rendered by `genhtml`
(from `lcov`). The wiring lives in [`cmake/fastcov.cmake`](../cmake/fastcov.cmake).

## Prerequisites

- **GCC / gcov >= 10** (the build fails configure otherwise).
- **fastcov**, **lcov** (`genhtml`), **python3**, **git**, **cmake**, **perl**.
- Resources: a gcov build is large and memory-hungry. Budget roughly **16 GB
  RAM** and **80 GB+ free disk**; some Boost.Geometry translation units need
  several GB each, so a high `-j` can OOM.
- A **non-root** user to run the tests (see [Running as non-root](#running-as-non-root)).

## 1. Configure with gcov

Coverage requires a debug build with `ENABLE_GCOV`:

```bash
cd "$BUILD_DIR"
rm -f CMakeCache.txt
cmake "$SOURCE_DIR" \
    -DWITH_DEBUG=1 \
    -DENABLE_GCOV=1 \
    -DWITH_SYSTEM_LIBS=1 \
    -DWITH_NDBCLUSTER_STORAGE_ENGINE=0
```

- `-DWITH_DEBUG=1` gives `-O0 -g`, needed for accurate line coverage.
- `-DWITH_NDBCLUSTER_STORAGE_ENGINE=0` — NDB has no coverage and just bloats the
  build; disabling it is recommended.
- `-DWITH_SYSTEM_LIBS=1` is optional but reduces third-party rebuilds.

## 2. Build

```bash
make -jN            # pick N conservatively; gcov TUs are memory-heavy
```

This instruments the whole server **and** the in-tree test extensions (the
latter is what makes the dev SDK, `villagesql/sdk`, show up in the delta report).

## 3. Run tests and generate the delta report

The simplest path is the orchestrator script, run as a non-root user:

```bash
scripts/villagesql_coverage.sh "$BUILD_DIR"
```

It performs, against the gcov build:

1. `make fastcov-clean` — zero all counters.
2. In-tree `villagesql` mtr suite + villagesql unit tests (`ctest -L villagesql`).
   Installing the in-tree test extensions here is what produces the dev-SDK
   coverage.
3. `make fastcov-report` + `make fastcov-diff` — build `report.info` and render
   the delta report.

Output: **`$BUILD_DIR/coverage-delta/index.html`**.

By default it does **not** run the upstream MySQL regression (it adds little to
the delta while costing hours). To include it — mainly if you also want a
full report — pass mtr args, e.g.:

```bash
scripts/villagesql_coverage.sh "$BUILD_DIR" --suite=all
```

## 4. Or run the fastcov targets manually

If you want to control which tests run, drive the `make` targets yourself.
Coverage **accumulates** into the `.gcda` files, so anything that runs between
`fastcov-clean` and `fastcov-report` is included:

```bash
cd "$BUILD_DIR"
make fastcov-clean

# run whatever you want measured, e.g.:
# (--big-test includes longer tests, e.g. villagesql/startup upgrade scenarios,
#  that are skipped by default -- the orchestrator passes it too.)
( cd "$SOURCE_DIR/mysql-test" \
    && MTR_BINDIR="$BUILD_DIR" perl mysql-test-run.pl \
       --do-suite=villagesql --nounit-tests --parallel=auto --big-test )
ctest --test-dir "$BUILD_DIR" -L villagesql

make fastcov-report      # -> report.info
make fastcov-diff        # -> coverage-delta/   (VillageSQL delta report)
make fastcov-html        # -> code_coverage/    (full report, optional)
```

Targets:

| Target | Purpose |
|---|---|
| `fastcov-clean` | Zero all `.gcda` counters (start of a run). |
| `fastcov-report` | Aggregate `.gcda`/`.gcno` into `report.info`. |
| `fastcov-diff` | Filter `report.info` to the VillageSQL delta and render `coverage-delta/`. |
| `fastcov-html` | Render the full `code_coverage/` report (upstream target). |

## What the delta report measures

- **Base:** the upstream `mysql-<MAJOR>.<MINOR>.<PATCH>` tag the tree is built
  on (auto-detected from the version). The delta is `git diff <tag>..HEAD`, so
  only VillageSQL's own additions and changed lines are scored; upstream code
  (byte-identical in both trees) drops out. Override with
  `-DVILLAGESQL_COVERAGE_BASE=<ref>` if needed.
- **Excluded** from coverage (not product code): third-party libraries
  (`extra/libarchive`, boost, rapidjson, …), test code
  (`unittest/gunit/villagesql`, `villagesql/test-extensions`), and the frozen
  `villagesql/stable_sdk`. The dev SDK (`villagesql/sdk`) is kept and its
  coverage is folded in from the instrumented test extensions.
- Paths are shown relative to the source root.

## Notes and troubleshooting

- **`.gcno` and `.gcda` must both exist together.** `.gcno` is written at
  compile time (in the build tree), `.gcda` at run time; keep the build tree in
  place through report generation.
- **Parallel runs.** `mtr --parallel` and killed servers can occasionally leave
  a corrupt `.gcda`. If a report looks anomalous, `fastcov-clean` and re-run.
- **Don't `fastcov-clean` between runs you want combined** — coverage is
  cumulative until the next clean.
