# knn_review: status (2026-11-11)

Review suite for the custom-index work in PRs #1640 to #1656 (/27 to /43;
/11 to /26 are merged as #1471 to #1486).

- Branch: `tomas/knn-review-stack-40`. It holds #1640 to #1656, the test extension
  (`villagesql/test-extensions/vsql-knn-index-test`), this suite, temporary
  table support and helper_fn_name.
- Issues: `ISSUES.md` (KNN-n, NYI-n). It also explains how a test is skipped
  for a known issue and how to run it anyway (`KNN_REVIEW_ALL=1`).
- Generator: `knn_review_gen.py`. It writes every .test and its .result, with
  the expected results computed from a model of the table. Edit the generator,
  not the generated files, and rerun it:

      python3 mysql-test/suite/knn_review/knn_review_gen.py

  Its KNOWN_ISSUES dict decides which tests skip for which issue.
- Coverage: `COVERAGE.md`, one row per test (what it checks, index, debug,
  restart, known issue). The generator writes it with the tests.
- The tests named `hnsw_*` need vsql-vector deb/hnsw_index/35 (cc56eee): build
  it and put `vsql_vector.veb` in the server's veb directory. All the others
  need only the in-tree test extension.
- Run all suites (debug build):

      ./mysql-test/mysql-test-run.pl --force --parallel=auto \
        --suite=knn_review/ddl,knn_review/delete,knn_review/locking,knn_review/rollback,knn_review/select_correctness,knn_review/select_plan,knn_review/update

## Where it stands

- Open and tracked by an issue:
  - KNN-24 → #1398
  - KNN-11 / NYI-4 → #1391
  - KNN-21 → #1392
  - KNN-23 → #1382
  - KNN-30 → fix in #1675
- Open, not filed:
  - KNN-4, 5, 7, 8, 10, 20, 22, 31, 32, 33
  - KNN-29: after a restart, purge of HNSW entries deleted before it makes
    other rows unreachable; every KNN read then returns too few rows. Still
    on /43 with hnsw /37. Without the restart (delete/hnsw_purge_locking) or
    with purge before it (delete/hnsw_purge_then_restart) the reads are full.
- Known behaviour, tuning planned: KNN-27 (with HNSW, after purge a KNN query
  can return wrong rows or too few rows; also with 8 rows, about one round in
  20 to 60 of delete/hnsw_purge_locking_stress). delete/hnsw_purge_locking
  and delete/hnsw_purge_then_restart, the KNN-29 controls, skip with it.
- Fixed by #1654 and #1655: KNN-25 (crash recovery asserted on a PK mapping B-tree redo
  record). rollback/hnsw_crash_recovery passes.
- Fixed by #1656: KNN-28 (rollback at recovery of a prepared transaction that
  changed an HNSW index). Its three tests pass.
- Fixed by vsql-vector #101: KNN-26 (too few rows with most rows deleted and
  not purged). delete/hnsw_many_deleted passes.
- KNN-12 is fixed on main by #1544 (merged); #1546 follows it.

## Not covered yet

Cases suggested but not written yet:

Purge timing:
- Purge during an open KNN scan. Stop a scan midway with `debug_sync`, purge
  the entry it is on or about to reach, then continue. Most interesting with
  HNSW, where purge also changes graph neighbours.
- Purge with `innodb_purge_threads > 1` over many deletes at once, so several
  `purge` calls run on the same index.
- HNSW versions of snapshot_purge and purge_same_vector. The test extension's
  scan is brute force; HNSW purge also relinks graph nodes.

Restart and recovery (these need vsql-vector; the test extension keeps its
entries in memory, so a restart empties its index):
- Restart without the extension (later). Remove vsql_vector.veb, kill with an
  open transaction that changed an HNSW index, restart. The rollback has to
  fail cleanly or wait, not assert or skip the undo.
- Crash during DDL on an HNSW index (CREATE INDEX, DROP INDEX, RENAME TABLE),
  using the ddl_log_crash_* debug points. Table, index and victionary rows
  have to agree afterwards.

Detecting and dropping a bad index (with vsql-vector, across restarts). There
is no index check in the extension ABI, and CHECK TABLE reports a healthy KNN
index corrupt (KNN-10), so the question is whether a bad index can be dropped:
- Server that cannot start (KNN-28). Kill at crash_after_flush_engine_log,
  start with --innodb-force-recovery=3 (no rollback), DROP INDEX idx_v (or
  DROP TABLE), restart normally. The server has to come up with the table at
  its committed state. Open: whether the recovered transaction's table lock
  blocks the DROP.
- Index marked corrupted. CHECK TABLE marks it (KNN-10), then restart: is it
  still marked, do KNN queries fail (ER 1712) or fall back to a scan, and does
  DROP INDEX work and remove the victionary rows and the extension's storage.
  Recreating it needs NYI-4.
- Index with unreachable entries (KNN-29). Purge after a
  restart, then DROP INDEX and DROP TABLE and restart: no orphaned storage, no
  errors at startup.

Locking and isolation:
- READ UNCOMMITTED. A KNN read sees another session's uncommitted vector update
  at its new place, not the old one.
- A row changed more than once in one transaction, then rolled back. For
  example insert, vector update and delete, then ROLLBACK; or two vector
  updates and ROLLBACK TO a savepoint between them.

DDL alongside DML:
- Online DDL with concurrent DML. An inplace ALTER that doesn't rebuild (for
  example adding a B-tree index on grp), paused with `debug_sync` while another
  session inserts, deletes and updates vectors. The row log replayed afterwards
  has to keep the custom index correct.
