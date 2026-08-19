# villagesql/percona_merge

Regression tests for VillageSQL behaviour that a Percona Server merge made
reachable. These test VillageSQL's own code; they live together because the
merge is their common origin, which makes the set reviewable as a unit,
greppable during the next Percona merge, and separable if a Percona feature is
ever dropped.

Runs with the rest of the VillageSQL tests under `--do-suite=village`, and alone
with `--suite=villagesql/percona_merge`.

## When to add one

Add a test when a merge-caused defect is fixed and nothing currently pins the
behaviour. The first entry here is the example: the `upgrade_needed` assert was
caught only because two upstream tests happen to assert on a process exit code,
while nothing stated that a server whose data dictionary cannot be read must
exit cleanly rather than abort.

Do not add one for:

- a `.result` re-record — the upstream test already covers the behaviour
- a fix to something that fails identically without the merge — that belongs
  wherever the original test lives
- a disposition that needs no test, such as a suite requiring external services

## Conventions

Beyond the repo rules in `CLAUDE.md`, each test's header comment must say which
merge it relates to, which defect it guards, and which upstream test (if any)
covers the same path elsewhere. Name the test after the behaviour it pins, not
after the test that exposed it — a reader should not have to look up an upstream
test name to learn what broke.
