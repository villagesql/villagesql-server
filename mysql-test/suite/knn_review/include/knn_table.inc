# Creates table t with a kvec_l2 KNN index on v and 8 rows, and collects
# statistics. Distance order from [1,0,0,0], with distances:
#   1 (0)  4 (1)  8 (1.2)  2 (1.41)  5 (1.5)  3 (2.24)  6 (3)  7 (3.16)
# grp has a secondary index: grp 1 = 1 2 7, grp 2 = 3 4 8, grp 3 = 5 6.
# The extension must already be installed.

--disable_query_log
CREATE TABLE t (id INT PRIMARY KEY, grp INT, pay VARCHAR(10),
                v KVECTOR, KEY k_grp (grp));
CREATE INDEX idx_v ON t (v) USING EXTENDED(vsql_knn_index_test.kvec_l2);
INSERT INTO t VALUES
  (1, 1, 'a', '[1,0,0,0]'), (2, 1, 'b', '[0,1,0,0]'),
  (3, 2, 'c', '[0,0,2,0]'), (4, 2, 'd', '[1,1,0,0]'),
  (5, 3, 'e', '[2.5,0,0,0]'), (6, 3, 'f', '[4,0,0,0]'),
  (7, 1, 'g', '[0,0,0,3]'), (8, 2, 'h', '[1,0,1.2,0]');
--disable_result_log
ANALYZE TABLE t;
--enable_result_log
--enable_query_log
