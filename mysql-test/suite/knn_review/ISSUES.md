# knn_review: known issues

Each issue has an id. A test that fails because of an issue starts with

    --let $knn_issue = KNN-n <short title>
    --source suite/knn_review/include/known_issue.inc

so a run reports it as skipped with the id. To check whether an issue is
fixed, run its tests with `KNN_REVIEW_ALL=1` in the environment; when they
pass, remove the two lines (and the entry in the generator's KNOWN_ISSUES) and
mark the issue fixed here.

| Id | Title | Status | Tests |
|---|---|---|---|
| KNN-1 | Locking KNN read at REPEATABLE READ fails with ER 3219 (range lock) | fixed in /40 | locking/insert_select, locking/lock_wait_rr, locking/repeatable_read, select_plan/locking_repeatable_read |
| KNN-2 | UPDATE of the primary key crashes: old index entry built with a new column reference | fixed in /40 | update/pk_update |
| KNN-3 | A KNN scan that stops early never ends its extension cursor | fixed in /40 | select_correctness/cursor_leak |
| KNN-4 | A KNN scan skips NULL vectors the plain query fails on (result depends on the plan) | open | select_correctness/null_vectors_knn_scan |
| KNN-5 | A KNN scan ranks by the profile's first ORDER function, not the query's | open | select_correctness/order_function |
| KNN-6 | A RANGE_SCAN custom index rejects an equality read with ER 3219 (end range) | fixed in /40 | select_correctness/range_read |
| KNN-7 | A window function under a KNN ORDER BY ... LIMIT sees only LIMIT rows (wrong result) | open | select_correctness/window_under_knn_limit |
| KNN-8 | A HAVING clause stops the KNN scan (plan only, rows correct) | open | select_plan/having |
| KNN-9 | Unlocking a row rejected by WHERE in a READ COMMITTED locking KNN read crashes | fixed in /40 | locking/unlock_row |
| KNN-10 | CHECK TABLE reports the KNN index corrupt (0 entries) and marks it corrupted | open | ddl/check_table |
| KNN-11 | Building a KNN index from existing rows crashes (custom_column.cc old_index != nullptr) | moved to NYI-4 | ddl/create_index_populated, ddl/recreate_index, ddl/table_rebuild, ddl/primary_key_change |
| KNN-12 | After RENAME TABLE a KNN query fails (index_init() called for a KNN scan) | open | ddl/rename_table |
| KNN-13 | Entries invisible to the reader use up the KNN LIMIT (too few rows) | fixed in /40 | rollback/other_session_insert, rollback/snapshot_insert |
| KNN-14 | An MVCC_VISIBILITY index shows a row committed after the reader's snapshot (phantom) | obsolete | (tests removed) |
| KNN-15 | A READ COMMITTED locking KNN read drops rows whose entry is current | fixed in /40 | locking/current_entry_pk, locking/current_entry_btree, locking/read_committed |
| KNN-16 | VEF_INDEX_CAP_APPROXIMATE has the same bit as VEF_INDEX_CAP_KNN | fixed in /40 | unit test index_capability_bits-t |
| KNN-17 | UPDATE of a column-store column in an intrinsic temporary table crashes (write_trx_id of trx 0) | open | villagesql/extension/vsql-storage-test/storage_intrinsic_table (not skipped) |
| KNN-18 | INSERT of a vsql-vector SVECTOR into an intrinsic temporary table crashes (write_trx_id of trx 0) | open | villagesql/extension/vsql-storage-test/storage_intrinsic_table (not skipped) |
| KNN-19 | A column store created in the temporary tablespace gets a root page with stale bytes (vsql-vector asserts) | open | villagesql/extension/vsql-storage-test/storage_intrinsic_table, storage_temp_table (not skipped) |

## Not implemented yet

On deb/custom-index/26 only INSERT and UPDATE of non-indexed columns worked on
a table with a KNN index, and tests of the rest were skipped with an NYI id.
deb/custom-index/40 implements DML, so these tests run again; a failure in
them is now a bug and gets a KNN id. On 25df4f5 all of them pass. NYI-4 is
still not implemented on /40.

| Id | Not implemented on /26 | Tests |
|---|---|---|
| NYI-1 | DELETE, including the index-entry removal when an INSERT is rolled back | delete/* (5), rollback/rollback_insert, rollback_delete, other_session_delete, snapshot_delete |
| NYI-2 | UPDATE of the indexed vector | update/vector_update, rollback/rollback_vector_update, other_session_vector_update |
| NYI-3 | UPDATE of the primary key (see KNN-2 for the crash it gave) | update/pk_update, rollback/rollback_pk_update |
| NYI-4 | Building a KNN index over existing rows (CREATE INDEX on a populated table, table rebuild). Still not implemented on /40: Deb's own knn_create_index_populated is disabled (4c3da82). See KNN-11 for the crash | ddl/create_index_populated, ddl/recreate_index, ddl/table_rebuild, ddl/primary_key_change |

## KNN-1: Locking KNN read at REPEATABLE READ fails with ER 3219 (range lock)

- Fixed in deb/custom-index/40 (25df4f5): its tests pass.
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad.
- Earlier name: D6′.
- Symptom: `ERROR 3219 (HY000): InnoDB: Custom type operation failed`; server
  log: `Custom index doesn't support Secondary index range lock.`
- Cause: try_knn_order plans the KNN scan for a locking read at REPEATABLE
  READ. row_search_mvcc passes `set_also_gap_locks` (true for every locking
  read above READ COMMITTED) as `range_lock` to `Custom_index::row_search`, and
  `check_row_search_conditions` rejects it before the scan begins.
- Reached by: `SELECT ... FOR SHARE / FOR UPDATE`, and `INSERT ... SELECT`
  (its SELECT reads with shared locks at REPEATABLE READ).
- Repro: `INSERT INTO t_out SELECT id FROM t ORDER BY kvec_l2_distance(v,
  '[1,0,0,0]') LIMIT 2;` at the default isolation level.
- Expected: the nearest rows, locked with record locks (no gap locks).
- EXPLAIN shows the KNN scan (plan_uses_knn 1) while the query fails before
  any scan runs (knn_scan_ran 0, error 3219).

## KNN-2: UPDATE of the primary key crashes: old index entry built with a new column reference

- Moved to NYI-3: UPDATE of the primary key is not implemented yet. Kept as
  a finding for when it is.

- Seen on: deb/custom-index/26 (0094de4), with 528c6ad. Debug build.
- Symptom: assertion at `row0upd.cc:2337` in `row_upd_sec_index_entry_low`;
  server log: `Record in index idx_v of table test.t was not found on update`.
- Repro: `UPDATE t SET id = 100 WHERE id = 1;` on the standard 8-row table.
- What the log shows: the entry searched is `{col_ref 0x10, pk 1}`; row 1's
  entry is `{col_ref 0x08, pk 1}` (the 8 rows hold 0x08 to 0x0f). 0x10 is the
  next unused column reference, so the old entry appears to be built from the
  new copy of the vector rather than the old row.
- Related: the DELETE crash (earlier D1) asserts at the same place, but there
  the rebuilt entry equals the stored one and the search compares only one
  field. This one would fail even with that fixed. Not yet confirmed in code.

## KNN-3: A KNN scan that stops early never ends its extension cursor

- Fixed in deb/custom-index/40 (25df4f5): the test passes (no cursor left open).
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad.
- Symptom: after three `... LIMIT 1` KNN queries on a 3-row table,
  `kvec_open_cursors()` is 3 higher than before (expected 0). A scan that
  reads to the end (LIMIT 10) leaves none.
- Cause: `Custom_index::close_cursor()` (the extension's scan_end) runs only
  when row_search reaches the end of the index or fails. A LIMIT smaller than
  the table stops early, and nothing in index_end, reset, close or
  row_prebuilt_free ends the cursor; the next open_cursor overwrites
  `pcur->custom_cursor`.
- Effect: every early-stopped KNN query leaks the extension's cursor state.
- Expected: every cursor a scan opens is ended when the scan ends, however it
  ends.

## KNN-4: A KNN scan skips NULL vectors the plain query fails on (result depends on the plan)

- Still seen on deb/custom-index/40 (25df4f5): same result.
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad.
- Repro: table with rows (1, '[1,0,0,0]'), (2, NULL), (3, '[2,0,0,0]') and a
  kvec_l2 index on v.
  - `SELECT id FROM nv IGNORE INDEX (idx_v) ORDER BY kvec_l2_distance(v,
    '[1,0,0,0]') LIMIT 3` fails: `VDF error in function 'kvec_l2_distance':
    kvec_l2_distance: operand must not be NULL`.
  - The same query through the index returns 1 3.
- Cause: the KNN scan never returns the NULL row, so the distance function is
  never called on it. The plain plan calls it for every row.
- Expected: the same result whichever plan runs. Either the KNN scan is not
  used when the column can hold NULL and the function rejects NULL, or NULL
  handling is defined once (for example, the function returns NULL and NULL
  rows are excluded or ordered consistently).
- Note: pgvector's index scan also leaves out NULL rows, but its distance
  operator returns NULL rather than an error, so a sequential scan does not
  fail there.
- Not a crash or wrong rows among non-NULL vectors; a semantics decision.

## KNN-5: A KNN scan ranks by the profile's first ORDER function, not the query's

- Still seen on deb/custom-index/40 (25df4f5): same result.
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad.
- Repro: kvec_multi index (profile kvec_profile_multi binds kvec_l2_distance
  as fn_id 1 and kvec_cos_distance as fn_id 2) on rows
  (1, '[10,1,0,0]'), (2, '[0.5,0.5,0,0]'), (3, '[0,1,0,0]'), (4, '[2,0,0,0]').
  `SELECT id FROM t ORDER BY kvec_cos_distance(v, '[1,0,0,0]') LIMIT 2`
  returns 2 4 (the L2 order) instead of 4 1 (the cosine order, as with the
  index ignored).
- Cause: the optimizer matches the query's function to its binding
  (find_profile_function), but `Custom_index::fill_profile()` sends the fn_id
  of the profile's first ORDER binding whatever the query used. No sort
  follows the KNN scan, so the wrong order is returned.
- Expected: the scan ranks by the function the query orders by.
- Wrong results, silently.

## KNN-6: A RANGE_SCAN custom index rejects an equality read with ER 3219 (end range)

- Fixed in deb/custom-index/40 (25df4f5): the test passes.
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad, and the test
  extension's kvec_range index type (uncommitted in the deb26 worktree).
- Repro: kvec_range index (capability RANGE_SCAN) on v;
  `SELECT id FROM t FORCE INDEX (idx_v) WHERE v = '[0,1,0,0]'` fails with
  `ERROR 3219 (HY000): InnoDB: Custom type operation failed`; server log:
  `Custom index doesn't support End range condition.`
- Cause: the range optimizer plans a range read on the index, which sets the
  handler's `end_range`; `check_row_search_conditions` in
  storage/innobase/villagesql/custom_index.cc rejects any read with an
  end_range (`TODO(villagesql-indexing): Support end range condition check`).
  An equality is a range with an end key, so every keyed read is refused.
- Expected: an index that advertises RANGE_SCAN serves equality and range
  reads (here: row 2), or the optimizer does not plan a range read the index
  cannot run.
- Note: this is "Custom Index: Range scan capability" (0094de4, the top of
  /26), so range reads look unfinished rather than broken.

## KNN-7: A window function under a KNN ORDER BY ... LIMIT sees only LIMIT rows (wrong result)

- Still seen on deb/custom-index/40 (25df4f5): same result.
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad.
- Repro: standard 8-row table.
  `SELECT id, COUNT(*) OVER () AS total FROM t ORDER BY kvec_l2_distance(v,
  '[1,0,0,0]') LIMIT 2` returns `1 2` / `4 2`; expected `1 8` / `4 8`, which
  is what the same query returns with the index ignored.
- Cause (likely): the KNN scan gets the query's LIMIT, so it stops after 2
  rows, but window functions are computed over all rows before ORDER BY and
  LIMIT apply. A KNN scan may take the LIMIT only when nothing between the
  scan and the LIMIT needs more rows (window functions, and likely also
  aggregation, DISTINCT, GROUP BY, HAVING; those return correct results in
  this suite).
- Main returns 8 (PR #1370's test: 5 of 5 rows).
- Wrong results, silently.

## KNN-8: A HAVING clause stops the KNN scan (plan only, rows correct)

- Still seen on deb/custom-index/40 (25df4f5): same plan.
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad.
- Repro: `SELECT id, kvec_l2_distance(v, '[1,0,0,0]') AS d FROM t
  HAVING d <= 1.3 ORDER BY d LIMIT 10` is planned as a sort; main runs it as a
  KNN scan with the HAVING as a filter. The rows are correct either way
  (select_correctness/shapes).
- Narrowed down: the same query without HAVING (`... AS d FROM t ORDER BY d
  LIMIT 3`) does get the KNN scan (select_plan/knn_used), so the HAVING clause
  is what stops it, not the ORDER BY alias.
- Cause: JOIN::optimize clears the join's limit when there is a HAVING clause
  (sql/sql_optimizer.cc: `if (having_cond || calc_found_rows) m_select_limit =
  HA_POS_ERROR;`), since HAVING may reject rows after the LIMIT-th. That limit
  is what test_if_skip_sort_order passes to try_knn_order, which then gives up
  (villagesql/sql/optimizer.cc: `if (select_limit == HA_POS_ERROR) return
  false;  // KNN requires a limit`). SQL_CALC_FOUND_ROWS has the same effect.
- Possible fix: decide on the query's own LIMIT (the query expression's
  limit), and run the KNN scan without a row cap when HAVING follows it; the
  scan then continues until LIMIT rows pass HAVING. Compare KNN-7, where the
  cap is applied although a window function needs every row.
- Expected: a KNN scan, with HAVING applied to the rows it returns. Under the
  "no KNN when another index serves the WHERE" rule nothing excludes it.
- Performance only.

## KNN-9: Unlocking a row rejected by WHERE in a READ COMMITTED locking KNN read crashes

- Fixed in deb/custom-index/40 (25df4f5): the test passes.
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad. Debug build.
- Repro: READ COMMITTED, standard 8-row table,
  `SELECT id FROM t WHERE pay <> 'a' ORDER BY kvec_l2_distance(v, '[1,0,0,0]')
  LIMIT 2 FOR UPDATE` (the nearest row, 1, fails the WHERE).
- Symptom: assertion in `btr_pcur_t::restore_position`, from
  `row_prebuilt_t::try_unlock` <- `ha_innobase::unlock_row` <-
  `FilterIterator::Read`.
- Cause: at READ COMMITTED the server releases the lock on a row the WHERE
  rejects; try_unlock restores `prebuilt->clust_pcur` to find the record.
  Upstream `row_sel_get_clust_rec_for_mysql()` stores that position for every
  locking read; `Custom_index::fetch_cluster_row()` never calls
  `clust_pcur->store_position()`, so the restore runs on a position that was
  never stored. In a release build it may unlock the wrong record instead.
- Note: locking/read_committed runs the same query after other locking reads
  in the same transaction and passes, presumably because a position stored by
  an earlier read is restored instead (possibly the wrong record). The query as
  the first locking read of the session (locking/unlock_row) crashes.
- Expected: the rejected row's lock is released, the returned rows stay
  locked.

## KNN-10: CHECK TABLE reports the KNN index corrupt (0 entries) and marks it corrupted

- Still seen on deb/custom-index/40 (25df4f5): a KNN query after CHECK
  TABLE fails with ER 1712 "Index idx_v is corrupted".
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad.
- Repro: `CHECK TABLE t` on the standard 8-row table returns
  `Warning InnoDB: Index 'idx_v' contains 0 entries, should be 8.` and
  `error Corrupt`. KNN queries on the same table find all 8 rows, so the
  index itself is fine.
- Cause: `ha_innobase::check` scans every index with
  `row_scan_index_for_mysql` and compares its row count with the clustered
  index. The scan of the custom index counts 0, so the counts differ.
- Worse: on a mismatch `ha_innobase::check` calls `dict_set_corrupted(index)`,
  so a routine CHECK TABLE marks the KNN index corrupted in the dictionary,
  persistently. Whether later KNN queries then fail is checked by the test
  (a KNN query after each CHECK TABLE).
- Expected: CHECK TABLE either checks the custom index the right way (count
  its entries through the custom index or the extension) or skips it, and
  never marks a healthy index corrupted.

## KNN-11: Building a KNN index from existing rows crashes (custom_column.cc old_index != nullptr)

- Moved to NYI-4: building an index over existing rows is not implemented
  yet on /40 either (Deb's knn_create_index_populated is disabled).
- Still seen on deb/custom-index/40 (25df4f5): the same assertion, now
  custom_column.cc:1233, in all four tests.
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad. Debug build.
- Earlier name: R1.
- Symptom: `Assertion failure: custom_column.cc:1109: old_index != nullptr` in
  `villagesql::innodb::Custom_column::fetch_for_bulk_ddl`, from
  `ddl::Builder::copy_row` <- `ddl::Parallel_cursor::scan`.
- Repro: `CREATE INDEX idx_v ON p (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)`
  on a table that already has rows. The earlier /26 probe run hit the same
  assertion with `ALTER TABLE t FORCE` (rebuild) and with `ALTER TABLE ... DROP
  PRIMARY KEY, ADD PRIMARY KEY (k)` (main refuses the latter with an error).
- Cause (likely): the bulk DDL path reads each row's custom column value
  through the old index (`old_index`), which only exists when the table is
  rebuilt; an in-place CREATE INDEX has none, and the rebuild cases do not
  pass it either.
- Expected: the index is built from the existing rows (main does this
  row by row, PR #1077), or the DDL is refused with an error.
- Confirmed in knn_review: ddl/create_index_populated. table_rebuild,
  primary_key_change and recreate_index are skipped on the strength of the
  earlier probes; run them with KNN_REVIEW_ALL=1 to confirm.

## KNN-12: After RENAME TABLE a KNN query fails (index_init() called for a KNN scan)

- On deb/custom-index/40 (25df4f5) the debug build no longer asserts: the
  query fails with ER 3219 "InnoDB: Incorrect handler method index_init()
  called for KNN scan."
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad. Debug build.
- Repro: `RENAME TABLE t TO t2;` then
  `SELECT id FROM t2 ORDER BY kvec_l2_distance(v, '[1,0,0,0]') LIMIT 3`.
- Symptom: `Assertion failure: ha_innodb.cc:10390:!Custom_index::is_knn(
  m_prebuilt->index)` in `ha_innobase::index_init`, from
  `IndexScanIterator::Init` under `filesort` (`SortingIterator`).
- What it shows: after the rename the optimizer no longer plans a KNN scan; it
  plans a full scan of idx_v as an ordinary index, then a sort. InnoDB still
  knows idx_v is a KNN index and asserts that one is never opened that way.
- Likely cause, not confirmed in code: the server-side custom index metadata
  (what marks idx_v as KNN-capable to the optimizer) is keyed by the table name
  and not moved by RENAME TABLE, while InnoDB's is.
- Expected: the index keeps working under the new name.
- Also worth checking: ALTER TABLE ... RENAME, and RENAME across databases.

## KNN-13: Entries invisible to the reader use up the KNN LIMIT (too few rows)

- Fixed in deb/custom-index/40 (25df4f5): both tests pass. The extension
  now filters entries by the scan's read view, so invisible entries never
  reach the LIMIT.
- Seen on: deb/custom-index/26 (0094de4), with 528c6ad.
- Repro: standard 8-row table. Session 2 runs `START TRANSACTION; INSERT INTO
  t VALUES (9, 1, 'i', '[1,0,0.1,0]');` and does not commit (row 9 is the
  nearest after row 1). Session 1, at READ COMMITTED and at REPEATABLE READ:
  `SELECT id FROM t ORDER BY kvec_l2_distance(v, '[1,0,0,0]') LIMIT 3` returns
  `1 4`; expected `1 4 8`. After session 2 commits, it returns `1 9 4`
  (correct).
- Also with a committed row: a REPEATABLE READ snapshot started before
  session 2 inserted and committed row 9 returns `1 4` instead of `1 4 8`.
- Cause (likely): the KNN scan is capped at LIMIT index entries (1, 9, 4), and
  the server then drops row 9 as invisible to the reader, so fewer than LIMIT
  rows remain and the scan has already stopped.
- Same family as the earlier D4 (too few rows when a filter rejects entries;
  the scan ignores that its LIMIT is only exact if every entry is returned).
  With a WHERE clause the right rows come back (select_correctness/filters),
  so the cap is presumably not applied when there is a condition; MVCC
  visibility is not a condition the optimizer sees, so here it is.
- Expected: invisible entries do not count toward the LIMIT; the scan keeps
  going until LIMIT visible rows are returned.
- Wrong results, silently.

## KNN-14: An MVCC_VISIBILITY index shows a row committed after the reader's snapshot (phantom)

- Obsolete: "Custom Index: Drop MVCC visibility capability" (e39911b, in the
  rebased deb/custom-index/26) removed MVCC_VISIBILITY. The extension now
  filters entries by the scan's read view itself (scan_desc.read_view()), so
  the kvec_l2_mvcc index type and its tests were removed.

- Seen on: deb/custom-index/26 (0094de4), with 528c6ad, and the test
  extension's kvec_l2_mvcc index type (uncommitted in the deb26 worktree).
- Repro: kvec_l2_mvcc index on rows (1, '[1,0,0,0]'), (4, '[1,1,0,0]'),
  (8, '[1,0,1.2,0]'). Session 1: `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
  Session 2: `INSERT INTO t VALUES (9, '[1,0,0.1,0]')` (autocommit). Session 1:
  `SELECT id FROM t ORDER BY kvec_l2_distance(v, '[1,0,0,0]') LIMIT 3` returns
  `1 9 4`; expected `1 4 8`, since row 9 was committed after the snapshot.
- For an index that advertises MVCC_VISIBILITY the server relies on the
  extension to filter entries by the reader's view. Not yet known whether the
  extension's check (kvec_l2_mvcc) is wrong or the server passes it the wrong
  view. The Nov 3 test knn_mvcc_visibility (one row before the snapshot,
  LIMIT 10) passed in that session, so it may depend on the data or on that
  build.
- Contrast: with kvec_l2 (no MVCC_VISIBILITY, the server checks visibility)
  the same snapshot does not see row 9, but returns too few rows (KNN-13).
- Wrong results (a phantom row in a consistent read), silently.

## KNN-15: A READ COMMITTED locking KNN read drops rows whose entry is current

- Seen on: deb/custom-index/26 (0094de4). Earlier name: D2.
- Fixed in deb/custom-index/40: check_and_lock locks the clustered record
  first, then skips it if it is delete-marked or the extension's scan_validate
  rejects the entry (created by trx_ref and not delete-marked). This replaces
  our earlier fix (entry_matches_rec, compare the column reference).
- Repro: READ COMMITTED, `SELECT ... ORDER BY kvec_l2_distance(v, ...) LIMIT 3
  FOR UPDATE`. With an extension that reports trx_ref 0 it returns no rows;
  with one that reports the inserting transaction (vsql_knn_index_test) it
  drops a row after an UPDATE of a non-indexed column, and skips a row that
  another session has locked with an uncommitted change instead of waiting.
- Cause: `check_and_lock()` in custom_index.cc keeps a hit only if the
  clustered record's DB_TRX_ID equals the entry's trx_ref, and compares before
  locking. A payload UPDATE changes DB_TRX_ID but not the entry.
- Expected: lock the clustered record first, then keep it if its column
  reference equals the entry's (the entry is current), as
  row_sel_sec_rec_is_for_clust_rec does for ordinary secondary indexes.

## KNN-16: VEF_INDEX_CAP_APPROXIMATE has the same bit as VEF_INDEX_CAP_KNN

- Seen on: deb/custom-index/26 (0094de4); introduced in dd50d91 "Custom
  Index: Update SDK interface". Earlier name: D7.
- abi/preview/index.h defines both `VEF_INDEX_CAP_KNN` and
  `VEF_INDEX_CAP_APPROXIMATE` as `(vef_index_cap_t)1u << 4`.
- Effect: an index declaring APPROXIMATE (an HNSW index) is also KNN, and an
  exact KNN index cannot be told from an approximate one. Nothing in the
  server tests APPROXIMATE on its own yet, so nothing visibly breaks; it is
  the bit an exact-LIMIT decision needs (see KNN-13).
- Test: unittest/gunit/villagesql/index_capability_bits-t.cc.
- Fixed in the rebased deb/custom-index/26 (e39911b): MVCC_VISIBILITY was
  removed and APPROXIMATE is now `1u << 5`.

## KNN-17: UPDATE of a column-store column in an intrinsic temporary table crashes (write_trx_id of trx 0)

- Seen on: tomas/knn-review-stack-40 (deb/custom-index/40 25df4f5 + 6aff305
  "Support column-store columns in temporary tables"). Passed on /26.
- Not a knn_review test: villagesql/extension/vsql-storage-test
  storage_intrinsic_table, at `SELECT grp, MIN(n), MAX(n), COUNT(*) FROM t1
  GROUP BY grp ORDER BY grp` (an aggregate of a column-store column, kept in
  an intrinsic temporary table).
- Symptom: debug assertion `trx_id != EMPTY_TRX_ID` in Page::write_trx_id
  (storage_api.h), from vsql_storage_test's mark_delete. Stack:
  TemptableAggregateIterator::Init > ha_innobase::update_row >
  row_update_inplace_for_intrinsic > Custom_column::update > mark_impl >
  mark_in_column_store > StorageInterface::mark_delete.
- Cause: row_update_inplace_for_intrinsic (row0mysql.cc, added by 6aff305)
  calls Custom_column::update with trx_id 0, as an intrinsic table has no
  transaction, so the old value is delete-marked by trx 0. /40 made the
  delete-marking transaction a stored 6-byte id (2e63ea5 "SDK delete trx
  ref"), and Page::write_trx_id asserts it is not EMPTY_TRX_ID. In a release
  build write_trx_id returns without writing, so the record is marked but
  its delete trx id stays empty.
- Ours (the temp-tables commit, PR #1431), not Deb's.
- Expected: nothing reads an intrinsic table's old value (no undo, no other
  readers), so the in-place update should remove it (purge) instead of
  delete-marking it with a transaction that does not exist.

## KNN-18: INSERT of a vsql-vector SVECTOR into an intrinsic temporary table crashes (write_trx_id of trx 0)

- Seen on: tomas/knn-review-stack-40 with vsql-vector deb/hnsw_index/33
  (ce25b42), debug build.
- Repro: `CREATE TABLE t (id INT PRIMARY KEY, grp INT, v SVECTOR(2))`, three
  rows, `SET SESSION big_tables = ON`, then
  `SELECT /*+ NO_MERGE(d) */ d.id, d.v FROM (SELECT id, v FROM t) AS d`.
- Symptom: assertion `trx_id != EMPTY_TRX_ID` in Page::write_trx_id
  (storage_api.h), from vsql-vector's DataPage insert (data_page.cc, writes
  the inserting transaction with write_trx_id).
- Cause: the temp-tables commit stores extended columns on the intrinsic
  insert path with `trx ? trx->id : 0`, which is 0 for an intrinsic table (it
  has no transaction). /40's write_trx_id asserts the id is not empty. The
  test extensions in this tree write the inserting transaction with
  write_integer_8, so no test here reached it; KNN-17 was the update side of
  the same mismatch. Since "test extensions: store the last-writer trx_ref
  with write_trx_id" they write it like vsql-vector, and
  vsql-storage-test.storage_intrinsic_table crashes the same way in this
  tree.
- Not decided: either the server passes a transaction id that is never empty
  for an intrinsic table, or the SDK documents that trx_ref is EMPTY_TRX_ID
  for a table without transactions and a column store then clears the field
  (clear_trx_id) instead of writing it. The second changes vsql-vector.

## KNN-19: A column store created in the temporary tablespace gets a root page with stale bytes (vsql-vector asserts)

- Seen on: tomas/knn-review-stack-40 (with the KNN-17/18 fixes) and vsql-vector
  deb/hnsw_index/33 (ce25b42), debug build.
- Repro: SVECTOR table, `SET SESSION big_tables = ON`, two queries that
  materialize v in an intrinsic table (a NO_MERGE derived table, a CTE), then
  `SELECT id, v, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) FROM t`.
- Symptom: assertion `0 == root_page.read_integer_4(total_data_pages_off())`
  in vsql-vector RootPage::format (root_page.cc), from
  MultiColumnStore::create < Custom_column::create < ha_innobase::create <
  create_tmp_table_with_fallback < instantiate_tmp_table.
- Cause: InnoDB does not zero a page it allocates in the system temporary
  tablespace (fsp_init_file_page_low skips the memset for it), so a page
  freed by an earlier intrinsic table comes back with its old bytes. In every
  other tablespace a new page is zero filled, and vsql-vector relies on that
  ("Page data is zero filled by default"). The temp-tables commit is what
  puts column stores in the temporary tablespace, so this is ours.
- Expected: vef_storage_segment_create and vef_storage_page_alloc give the
  extension zero-filled pages in the temporary tablespace too.
- Tests: vsql_storage_test's create and insert now return an error ("new
  root page is not zeroed", "new page is not zeroed") for such a page.
  storage_intrinsic_table runs the window query after the other temporary
  tables were dropped; storage_temp_table drops a temporary table and creates
  it again.
