# knn_review: status (2026-11-11)

Review suite for the custom-index work on deb/custom-index/42 (b9315d6).

- Branch: `tomas/knn-review-stack-40`. It holds /42, the test extension
  (`villagesql/test-extensions/vsql-knn-index-test`), this suite, temporary
  table support and helper_fn_name.
- Issues: `ISSUES.md` (KNN-n, NYI-n). It also explains how a test is skipped
  for a known issue and how to run it anyway (`KNN_REVIEW_ALL=1`).
- Generator: `knn_review_gen.py`. It writes every .test and its .result, with
  the expected results computed from a model of the table. Edit the generator,
  not the generated files, and rerun it:

      python3 mysql-test/suite/knn_review/knn_review_gen.py

  Its KNOWN_ISSUES dict decides which tests skip for which issue.
- The tests named `hnsw_*` need vsql-vector deb/hnsw_index/33 (ce25b42): build
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
- Open, not filed:
  - KNN-4, 5, 7, 8, 10, 20, 22
  - KNN-26: with HNSW and most rows deleted (entries not purged yet), a plain
    KNN query returns too few rows, mostly 1
  - KNN-27: with HNSW, after purge of scattered deletes, a KNN query on rows
    on a line returns wrong rows
  - KNN-28: rollback at recovery of a prepared transaction (binary log on)
    that updated an HNSW vector asserts in row_undo_mod_upd_exist_sec
- Fixed in /42: KNN-25 (crash recovery asserted on a PK mapping B-tree redo
  record). rollback/hnsw_crash_recovery passes.
- KNN-12 also happens on main: #1541. Fixed by #1544, which is stacked on
  #1543; #1546 is stacked on #1544.

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
- Purge after a restart. Commit deletes, restart before purge runs, then purge.
  The delete-marked entries have to survive the restart and be removed
  afterwards.
- An XA transaction prepared before a restart, then XA RECOVER and XA COMMIT or
  XA ROLLBACK. rollback/xa covers this without a restart.

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
