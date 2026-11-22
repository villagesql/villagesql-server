# Checks the links of the HNSW graph on table t: exports the table, copies
# its .ibd file and runs hnsw_graph_check.py on the copy. Prints the number
# of each kind of problem; all are 0 for a correct graph.
# $svdump: path of vsql-vector's svector_page_dump tool.

--disable_query_log
FLUSH TABLES t FOR EXPORT;
--let $_hgc_datadir = `SELECT @@datadir`
--remove_files_wildcard $MYSQLTEST_VARDIR/tmp hnsw_graph_check.ibd
--copy_file $_hgc_datadir/test/t.ibd $MYSQLTEST_VARDIR/tmp/hnsw_graph_check.ibd
UNLOCK TABLES;
--enable_query_log
--exec python3 $MYSQL_TEST_DIR/suite/knn_review/include/hnsw_graph_check.py $svdump $MYSQLTEST_VARDIR/tmp/hnsw_graph_check.ibd
--remove_file $MYSQLTEST_VARDIR/tmp/hnsw_graph_check.ibd
