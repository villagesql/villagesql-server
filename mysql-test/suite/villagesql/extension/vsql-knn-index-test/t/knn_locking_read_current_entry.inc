# Body of knn_locking_read_current_entry*.test. The caller installs the
# extension and sets:
#   $kvec_key   - the key part of CREATE INDEX, e.g. "v" or "v <profile>"
#   $kvec_index - the vsql_knn_index_test index type
CREATE TABLE t (id INT PRIMARY KEY, c INT, v KVECTOR);
--eval CREATE INDEX idx_v ON t ($kvec_key) USING EXTENDED(vsql_knn_index_test.$kvec_index)
# Distance order from [1,0,0,0]: 1 2 3
INSERT INTO t VALUES (1, 0, '[1,0,0,0]'), (2, 0, '[0,1,0,0]'), (3, 0, '[0,0,2,0]');

SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;

--echo # 1. Fresh rows: expect 1 2 3
--disable_result_log
SELECT vsql_knn_index_test.kvec_reset_counters();
--enable_result_log
START TRANSACTION;
SELECT id FROM t
ORDER BY vsql_knn_index_test.kvec_l2_distance(v, '[1,0,0,0]') LIMIT 3 FOR UPDATE;
COMMIT;
--echo # The locking read used the KNN scan: expect 1
SELECT vsql_knn_index_test.kvec_scan_begin_count() > 0 AS knn_scan_ran;

--echo # 2. After an UPDATE of the non-indexed column c: expect 1 2 3
UPDATE t SET c = 1 WHERE id = 1;
START TRANSACTION;
SELECT id FROM t
ORDER BY vsql_knn_index_test.kvec_l2_distance(v, '[1,0,0,0]') LIMIT 3 FOR UPDATE;
COMMIT;
--echo # The same read without locking: expect 1 2 3
SELECT id FROM t
ORDER BY vsql_knn_index_test.kvec_l2_distance(v, '[1,0,0,0]') LIMIT 3;

--echo # 3. Row 2 is locked by another session's uncommitted UPDATE: expect
--echo #    ER_LOCK_NOWAIT, not a result without row 2
--connect (con1, localhost, root,,)
START TRANSACTION;
UPDATE t SET c = 2 WHERE id = 2;
--connection default
START TRANSACTION;
--error ER_LOCK_NOWAIT
SELECT id FROM t
ORDER BY vsql_knn_index_test.kvec_l2_distance(v, '[1,0,0,0]') LIMIT 3 FOR UPDATE NOWAIT;
COMMIT;
--connection con1
COMMIT;
--disconnect con1
--connection default

SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
DROP TABLE t;
