#!/usr/bin/env python3
# Generates the knn_review suites: each test and its expected (correct) result.
# Usage: knn_review_gen.py [suite_dir]; suite_dir defaults to the directory of
# this script (mysql-test/suite/knn_review).
import os
import sys

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))
INC = "suite/knn_review/include"
D = "vsql_knn_index_test.kvec_l2_distance"
ORDER = f"ORDER BY {D}(v, '[1,0,0,0]')"
ALL8 = ["1", "4", "8", "2", "5", "3", "6", "7"]
# Tests skipped because of an issue filed in ISSUES.md. Run them anyway with
# KNN_REVIEW_ALL=1 in the environment.
KNOWN_ISSUES = {
    "ddl/check_table": "KNN-10 CHECK TABLE reports the KNN index corrupt and marks it so",
    "ddl/rename_table": "KNN-12 After RENAME TABLE a KNN query fails (index_init called for a KNN scan)",
    "ddl/create_index_populated": "NYI-4 Building a KNN index over existing rows is not implemented yet",
    "ddl/recreate_index": "NYI-4 Building a KNN index over existing rows is not implemented yet",
    "ddl/table_rebuild": "NYI-4 Building a KNN index over existing rows is not implemented yet",
    "ddl/primary_key_change": "NYI-4 Building a KNN index over existing rows is not implemented yet",
    "select_correctness/null_vectors_knn_scan": "KNN-4 A KNN scan skips NULL vectors the plain query fails on",
    "select_correctness/order_function": "KNN-5 A KNN scan ranks by the first ORDER function of the profile",
    "select_correctness/window_under_knn_limit": "KNN-7 A window under a KNN ORDER BY LIMIT sees only LIMIT rows",
    "select_plan/having": "KNN-8 A HAVING clause stops the KNN scan",
    "ddl/drop_table_entries": "KNN-20 DROP TABLE does not call the custom index drop callback",
    "ddl/truncate_entries": "KNN-20 DROP TABLE does not call the custom index drop callback",
    "locking/phantom_for_update": "KNN-22 A locking KNN read takes no gap locks, so another session can insert a phantom",
    "locking/phantom_for_share": "KNN-22 A locking KNN read takes no gap locks, so another session can insert a phantom",
    "select_plan/dml_explain": "KNN-23 EXPLAIN of a KNN-ordered DELETE or UPDATE shows a plain index scan",
    "ddl/rename_drop_uninstall": "KNN-12 After RENAME TABLE a KNN query fails (index_init called for a KNN scan)",
    "ddl/alter_rename_table": "KNN-12 After RENAME TABLE a KNN query fails (index_init called for a KNN scan)",
    "locking/hnsw_current_entry_after_change": "KNN-24 With HNSW, entries a locking KNN read rejects use up the LIMIT",
    "locking/hnsw_lock_wait_then_delete": "KNN-24 With HNSW, entries a locking KNN read rejects use up the LIMIT",
    "locking/hnsw_lock_wait_then_vector_update": "KNN-24 With HNSW, entries a locking KNN read rejects use up the LIMIT",
    "delete/hnsw_many_deleted": "KNN-26 With HNSW and most rows deleted, a KNN query returns too few rows",
}
LOCK_NOWAIT = ("ER_LOCK_NOWAIT", "ERROR HY000: Statement aborted because "
               "lock(s) could not be acquired immediately and NOWAIT is set.")


class T:
    def __init__(self, suite, name, header):
        self.suite, self.name = suite, name
        self.t = [f"# {l}" if l else "#" for l in header.strip("\n").split("\n")]
        self.t.append("")
        self.r = []

    def blank(self):
        self.t.append("")

    def echo(self, text):
        self.t.append(f"--echo # {text}")
        self.r.append(f"# {text}")

    def comment(self, text):
        self.t += [f"# {l}" for l in text.split("\n")]

    def sql(self, stmt, rows=None, header="id", error=None, sorted_=False,
            warnings=None, echo=True):
        if error:
            self.t.append(f"--error {error[0]}")
        if sorted_:
            self.t.append("--sorted_result")
        self.t.append(stmt + ";")
        if echo:
            self.r.append(stmt + ";")
        if error:
            self.r.append(error[1])
            return
        if warnings:
            self.r.append("Warnings:")
            self.r += warnings
        if rows is not None:
            self.r.append(header)
            self.r += sorted(rows) if sorted_ else rows

    def raw(self, test_lines, result_lines=()):
        self.t += test_lines
        self.r += list(result_lines)

    def install(self, table=True):
        self.sql("INSTALL EXTENSION vsql_knn_index_test")
        if table:
            self.raw([f"--source {INC}/knn_table.inc"])

    def uninstall(self, drop="t"):
        if drop:
            self.sql(f"DROP TABLE {drop}")
        self.sql("UNINSTALL EXTENSION vsql_knn_index_test")

    def need_type(self, index_type, profile):
        self.raw([f"--let $knn_index_type = {index_type}",
                  f"--let $knn_profile = {profile}",
                  f"--source {INC}/require_index_type.inc"])

    def need_fn(self, fn):
        self.raw([f"--let $knn_function = {fn}",
                  f"--source {INC}/require_function.inc"])

    def plan(self, query, knn, ran, errno=0):
        self.t += [f"--let $knn_query = {query}",
                   f"--source {INC}/plan_check.inc"]
        self.r += [f"# {query}", "plan_uses_knn\tknn_scan_ran\tquery_errno",
                   f"{knn}\t{ran}\t{errno}"]

    def write(self):
        issue = KNOWN_ISSUES.get(f"{self.suite}/{self.name}")
        if issue:
            at = self.t.index("")
            self.t[at:at] = ["", f"--let $knn_issue = {issue}",
                             f"--source {INC}/known_issue.inc"]
        d = os.path.join(ROOT, self.suite)
        with open(os.path.join(d, "t", self.name + ".test"), "w") as f:
            f.write("\n".join(self.t).rstrip("\n") + "\n")
        with open(os.path.join(d, "r", self.name + ".result"), "w") as f:
            f.write("\n".join(self.r) + "\n")


def knn(t, k=3, where="", q="[1,0,0,0]", rows=None, extra="", hint="",
        table="t", col="id", **kw):
    w = f" WHERE {where}" if where else ""
    h = f" {hint}" if hint else ""
    t.sql(f"SELECT {col} FROM {table}{h}{w} ORDER BY {D}(v, '{q}') "
          f"LIMIT {k}{extra}", rows=rows, **{"header": col, **kw})


tests = []

# ---------------------------------------------------------------- select_correctness
t = T("select_correctness", "limits", """
A KNN query returns the nearest rows in distance order for any LIMIT and
OFFSET, including LIMIT 0, a LIMIT above the row count and an OFFSET past the
end. Ties are returned in either order.""")
t.install()
knn(t, 1, rows=["1"])
knn(t, 3, rows=["1", "4", "8"])
knn(t, 8, rows=ALL8)
knn(t, 20, rows=ALL8)
knn(t, 0, rows=[])
knn(t, 2, extra=" OFFSET 3", rows=["2", "5"])
knn(t, 3, extra=" OFFSET 6", rows=["6", "7"])
knn(t, 3, extra=" OFFSET 10", rows=[])
t.echo("From [0,0,0,0], rows 1 and 2 are both at distance 1")
knn(t, 2, q="[0,0,0,0]", rows=["1", "2"], sorted_=True)
knn(t, 3, q="[0,0,0,0]", rows=["1", "2", "4"], sorted_=True)
t.uninstall()
tests.append(t)

t = T("select_correctness", "columns", """
A KNN query returns the right values for the selected columns, including the
vector and the distance.""")
t.install()
t.sql(f"SELECT id, v FROM t {ORDER} LIMIT 3", header="id\tv",
      rows=["1\t[1,0,0,0]", "4\t[1,1,0,0]", "8\t[1,0,1.2,0]"])
t.sql(f"SELECT id, pay FROM t {ORDER} LIMIT 3", header="id\tpay",
      rows=["1\ta", "4\td", "8\th"])
t.sql(f"SELECT id, {D}(v, '[1,0,0,0]') AS d FROM t ORDER BY d LIMIT 3",
      header="id\td", rows=["1\t0", "4\t1", "8\t1.2000000476837158"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "filters", """
A KNN query with a WHERE clause returns the nearest rows that satisfy it,
whatever access the WHERE could use: none, the primary key, or the secondary
index on grp.""")
t.install()
t.echo("No index on the filter")
knn(t, where="pay IN ('b','c','h')", rows=["8", "2", "3"])
knn(t, where="id + 0 IN (2,3,8)", rows=["8", "2", "3"])
knn(t, where="pay = 'g'", rows=["7"])
knn(t, where="pay = 'zz'", rows=[])
t.echo("Primary key")
knn(t, where="id > 2", rows=["4", "8", "5"])
knn(t, 2, where="id BETWEEN 2 AND 5", rows=["4", "2"])
knn(t, where="id = 4", rows=["4"])
knn(t, hint="IGNORE INDEX (PRIMARY)", where="id > 2", rows=["4", "8", "5"])
t.echo("Secondary index on grp")
knn(t, 2, where="grp = 2", rows=["4", "8"])
knn(t, where="grp >= 2", rows=["4", "8", "5"])
knn(t, 2, where="grp = 2 AND pay <> 'd'", rows=["8", "3"])
knn(t, 2, hint="IGNORE INDEX (k_grp)", where="grp = 2", rows=["4", "8"])
knn(t, 2, hint="FORCE INDEX (idx_v)", where="grp = 2", rows=["4", "8"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "query_points", """
The query point can be a literal, a type constructor, a scalar subquery, a
column of a const table, or a prepared-statement parameter.""")
t.install()
knn(t, rows=["1", "4", "8"])
knn(t, q="[4,0,0,0]", rows=["6", "5", "1"])
t.sql(f"SELECT id FROM t ORDER BY {D}(v, vsql_knn_index_test.KVECTOR::from_string('[1,0,0,0]')) LIMIT 3",
      rows=["1", "4", "8"])
t.sql(f"SELECT id FROM t ORDER BY {D}(v, (SELECT v FROM t WHERE id = 1)) LIMIT 3",
      rows=["1", "4", "8"])
t.sql("CREATE TABLE c (k INT PRIMARY KEY, q KVECTOR)")
t.sql("INSERT INTO c VALUES (1, '[1,0,0,0]')")
t.sql(f"SELECT t.id FROM c, t WHERE c.k = 1 ORDER BY {D}(t.v, c.q) LIMIT 3",
      rows=["1", "4", "8"])
t.sql("DROP TABLE c")
t.echo("A user variable is not a constant the function accepts (a general VDF")
t.echo("restriction, not specific to KNN)")
t.sql("SET @q = '[1,0,0,0]'")
t.sql(f"SELECT id FROM t ORDER BY {D}(v, @q) LIMIT 3",
      error=("ER_VILLAGESQL_GENERIC_ERROR",
             "ERROR HY000: Cannot initialize function 'kvec_l2_distance': "
             "argument 2 must be a custom type or string constant"))
t.sql(f"PREPARE ps FROM 'SELECT id FROM t ORDER BY {D}(v, ?) LIMIT ?'")
t.sql("SET @q = '[1,0,0,0]', @k = 2")
t.sql("EXECUTE ps USING @q, @k", rows=["1", "4"])
t.sql("SET @k = 4")
t.sql("EXECUTE ps USING @q, @k", rows=["1", "4", "8", "2"])
t.sql("SET @q = '[4,0,0,0]', @k = 2")
t.sql("EXECUTE ps USING @q, @k", rows=["6", "5"])
t.sql("DEALLOCATE PREPARE ps")
t.uninstall()
tests.append(t)

t = T("select_correctness", "shapes", """
KNN ordering inside other query shapes: joins, more ORDER BY keys, DESC, no
LIMIT, subqueries, derived tables, HAVING and views.""")
t.install()
t.sql("CREATE TABLE g (grp INT PRIMARY KEY, name VARCHAR(10))")
t.sql("INSERT INTO g VALUES (1,'one'),(2,'two'),(3,'three')")
t.sql(f"SELECT t.id, g.name FROM t JOIN g ON t.grp = g.grp ORDER BY {D}(t.v, '[1,0,0,0]') LIMIT 3",
      header="id\tname", rows=["1\tone", "4\ttwo", "8\ttwo"])
t.sql(f"SELECT id FROM t {ORDER}, id LIMIT 3", rows=["1", "4", "8"])
t.sql(f"SELECT id FROM t {ORDER} DESC LIMIT 2", rows=["7", "6"])
t.sql(f"SELECT id FROM t {ORDER}", rows=ALL8)
t.sql(f"SELECT name FROM g WHERE grp IN (SELECT grp FROM (SELECT grp FROM t {ORDER} LIMIT 1) s)",
      header="name", rows=["one"])
t.sql(f"SELECT id FROM (SELECT id, {D}(v, '[1,0,0,0]') AS d FROM t ORDER BY d LIMIT 10) s WHERE d <= 1.3",
      rows=["1", "4", "8"])
t.sql(f"SELECT id, {D}(v, '[1,0,0,0]') AS d FROM t HAVING d <= 1.3 ORDER BY d LIMIT 10",
      header="id\td", rows=["1\t0", "4\t1", "8\t1.2000000476837158"])
t.sql("CREATE VIEW vw AS SELECT id, v FROM t")
knn(t, table="vw", rows=["1", "4", "8"])
t.sql("DROP VIEW vw")
t.sql("DROP TABLE g")
t.uninstall()
tests.append(t)

t = T("select_correctness", "hypergraph", """
KNN queries under the hypergraph optimizer return the same rows.""")
t.raw(["--source include/have_hypergraph.inc", ""])
t.install()
knn(t, rows=["1", "4", "8"])
knn(t, 2, where="grp = 2", rows=["4", "8"])
t.raw(["--disable_query_log", "SET optimizer_switch = 'hypergraph_optimizer=default';",
       "--enable_query_log"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "empty_table", """
A KNN query on a table with no rows returns nothing, and finds rows inserted
later.""")
t.install(table=False)
t.sql("CREATE TABLE e (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON e (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
knn(t, table="e", rows=[])
t.sql("INSERT INTO e VALUES (1, '[1,0,0,0]'), (2, '[0,1,0,0]')")
knn(t, table="e", rows=["1", "2"])
t.uninstall("e")
tests.append(t)

t = T("select_correctness", "no_primary_key", """
A KNN query on a table without a primary key.""")
t.install(table=False)
t.sql("CREATE TABLE n (a INT, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON n (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("INSERT INTO n VALUES (10, '[1,0,0,0]'), (20, '[0,1,0,0]'), (30, '[2,0,0,0]')")
knn(t, 2, table="n", col="a", rows=["10", "30"])
t.uninstall("n")
tests.append(t)

t = T("select_correctness", "null_vectors", """
NULL vectors in the indexed column. Inserting one succeeds, and a KNN query
that filters the NULL row out works. kvec_l2_distance rejects a NULL operand,
so with the index ignored, ordering every row by distance fails with the
function's error.""")
NULL_ERR = ("ER_UDF_ERROR", "ERROR HY000: VDF error in function 'kvec_l2_distance': kvec_l2_distance: operand must not be NULL")
t.install(table=False)
t.sql("CREATE TABLE nv (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON nv (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("INSERT INTO nv VALUES (1, '[1,0,0,0]'), (2, NULL), (3, '[2,0,0,0]')")
t.sql("SELECT id, v IS NULL AS is_null FROM nv ORDER BY id", header="id\tis_null",
      rows=["1\t0", "2\t1", "3\t0"])
knn(t, table="nv", hint="IGNORE INDEX (idx_v)", where="v IS NOT NULL", rows=["1", "3"])
knn(t, table="nv", where="v IS NOT NULL", rows=["1", "3"])
knn(t, table="nv", hint="IGNORE INDEX (idx_v)", error=NULL_ERR)
t.uninstall("nv")
tests.append(t)

t = T("select_correctness", "null_vectors_knn_scan", """
The same KNN query over a NULL vector gives the same result through the index
as with it ignored: kvec_l2_distance rejects the NULL operand, so both fail
with the function's error.""")
t.install(table=False)
t.sql("CREATE TABLE nv (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON nv (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("INSERT INTO nv VALUES (1, '[1,0,0,0]'), (2, NULL), (3, '[2,0,0,0]')")
knn(t, table="nv", hint="IGNORE INDEX (idx_v)", error=NULL_ERR)
knn(t, table="nv", error=NULL_ERR)
t.uninstall("nv")
tests.append(t)

t = T("select_correctness", "non_knn_access", """
Queries on the vector column that are not KNN: equality, IS NULL, a range,
FORCE INDEX on the KNN index, and ORDER BY the column itself. A KNN-only index
cannot serve them, so each is answered without it.""")
t.install()
t.sql("SELECT id FROM t WHERE v = '[0,1,0,0]'", rows=["2"])
t.sql("SELECT id FROM t WHERE v IN ('[0,1,0,0]', '[4,0,0,0]') ORDER BY id", rows=["2", "6"])
t.sql("SELECT id FROM t WHERE v IS NULL", rows=[])
t.sql("SELECT id FROM t WHERE v > '[0,1,0,0]' ORDER BY id", rows=["1", "4", "5", "6", "8"])
t.sql("SELECT id FROM t FORCE INDEX (idx_v) WHERE v = '[0,1,0,0]'", rows=["2"])
t.sql("SELECT COUNT(*) FROM t FORCE INDEX (idx_v)", header="COUNT(*)", rows=["8"])
t.sql("SELECT id FROM t ORDER BY v LIMIT 2", rows=["7", "3"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "order_function", """
A KNN scan ranks by the ORDER BY function the query uses. kvec_profile_multi
binds two: kvec_l2_distance and kvec_cos_distance.
From [1,0,0,0]: L2 order is 2 4 3 1, cosine order is 4 1 2 3.""")
t.install(table=False)
t.need_type("kvec_multi", "kvec_profile_multi")
t.sql("CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON t (v kvec_profile_multi) USING EXTENDED(vsql_knn_index_test.kvec_multi)")
t.sql("INSERT INTO t VALUES (1, '[10,1,0,0]'), (2, '[0.5,0.5,0,0]'), (3, '[0,1,0,0]'), (4, '[2,0,0,0]')")
COS = "vsql_knn_index_test.kvec_cos_distance"
t.sql(f"SELECT id FROM t IGNORE INDEX (idx_v) ORDER BY {COS}(v, '[1,0,0,0]') LIMIT 2", rows=["4", "1"])
t.sql(f"SELECT id FROM t ORDER BY {COS}(v, '[1,0,0,0]') LIMIT 2", rows=["4", "1"])
knn(t, 2, rows=["2", "4"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "cursor_leak", """
Every extension cursor a KNN scan opens is ended, also when the scan stops
before the end of the index (LIMIT smaller than the table).
kvec_open_cursors() counts cursors begun and not yet ended.""")
t.install(table=False)
t.need_fn("kvec_open_cursors")
t.sql("CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON t (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("INSERT INTO t VALUES (1, '[1,0,0,0]'), (2, '[0,1,0,0]'), (3, '[0,0,2,0]')")
t.raw(["--let $before = `SELECT vsql_knn_index_test.kvec_open_cursors()`"])
knn(t, 10, rows=["1", "2", "3"])
knn(t, 1, rows=["1"])
knn(t, 1, rows=["1"])
knn(t, 1, rows=["1"])
t.echo("Cursors left open: expect 0")
t.raw(["--disable_query_log",
       "--eval SELECT vsql_knn_index_test.kvec_open_cursors() - $before AS cursors_left_open",
       "--enable_query_log"], ["cursors_left_open", "0"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "range_read", """
A custom index advertising RANGE_SCAN serves an ordinary keyed read, not only
KNN ORDER BY.""")
t.install(table=False)
t.need_type("kvec_range", "kvec_profile_range")
t.sql("CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON t (v kvec_profile_range) USING EXTENDED(vsql_knn_index_test.kvec_range)")
t.sql("INSERT INTO t VALUES (1, '[1,0,0,0]'), (2, '[0,1,0,0]'), (3, '[0,0,2,0]')")
t.sql("SELECT id FROM t FORCE INDEX (idx_v) WHERE v = '[0,1,0,0]'", rows=["2"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "window", """
KNN ordering combined with window functions: a distance as the window's sort
key, a window over the rows of a KNN derived table, and the nearest row per
group.""")
t.install()
t.sql(f"SELECT id, ROW_NUMBER() OVER (ORDER BY {D}(v, '[1,0,0,0]')) AS rn FROM t ORDER BY id",
      header="id\trn", rows=["1\t1", "2\t4", "3\t6", "4\t2", "5\t5", "6\t7", "7\t8", "8\t3"])
t.sql(f"SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM (SELECT id FROM t {ORDER} LIMIT 4) AS nearest ORDER BY id",
      header="id\trn", rows=["1\t1", "2\t2", "4\t3", "8\t4"])
t.sql(f"SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY {D}(v, '[1,0,0,0]')) AS rn FROM t) s WHERE rn = 1 ORDER BY id",
      rows=["1", "4", "5"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "window_under_knn_limit", """
A window under an outer KNN ORDER BY ... LIMIT: window functions are computed
over every row before the ORDER BY and LIMIT apply, so COUNT(*) OVER () counts
all 8 rows.""")
t.install()
t.sql(f"SELECT id, COUNT(*) OVER () AS total FROM t {ORDER} LIMIT 2",
      header="id\ttotal", rows=["1\t8", "4\t8"])
t.sql(f"SELECT id, COUNT(*) OVER () AS total FROM t IGNORE INDEX (idx_v) {ORDER} LIMIT 2",
      header="id\ttotal", rows=["1\t8", "4\t8"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "distinct_group_by", """
KNN ordering with DISTINCT and GROUP BY. Ordering by a distance that is not
selected, or not grouped, is rejected as for any expression.""")
t.install()
t.sql(f"SELECT DISTINCT id FROM t {ORDER} LIMIT 3",
      error=("ER_FIELD_IN_ORDER_NOT_SELECT",
             "ERROR HY000: Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.t.v' which is not in SELECT list; this is incompatible with DISTINCT"))
t.sql(f"SELECT DISTINCT id, {D}(v, '[1,0,0,0]') AS d FROM t ORDER BY d LIMIT 3",
      header="id\td", rows=["1\t0", "4\t1", "8\t1.2000000476837158"])
t.sql(f"SELECT DISTINCT grp FROM (SELECT grp FROM t {ORDER} LIMIT 4) s ORDER BY grp",
      header="grp", rows=["1", "2"])
t.sql(f"SELECT grp, MIN({D}(v, '[1,0,0,0]')) AS md FROM t GROUP BY grp ORDER BY md, grp LIMIT 3",
      header="grp\tmd", rows=["1\t0", "2\t1", "3\t1.5"])
t.sql(f"SELECT grp FROM t GROUP BY grp {ORDER} LIMIT 3",
      error=("ER_WRONG_FIELD_WITH_GROUP",
             "ERROR 42000: Expression #1 of ORDER BY clause is not in GROUP BY clause and contains nonaggregated column 'test.t.v' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by"))
t.uninstall()
tests.append(t)

t = T("select_correctness", "two_indexes", """
Two KNN indexes on one column, L2 and cosine. Each metric's query returns its
own order, and dropping one index leaves the other working. A cosine query on
a table with only the L2 index is still answered in cosine order.
From [1,0,0,0]: L2 order is 1 3 4 2; cosine order is 1 and 2 (both 0), 3, 4.""")
COS = "vsql_knn_index_test.kvec_cos_distance"
t.install(table=False)
t.sql("CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_l2 ON t (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("CREATE INDEX idx_cos ON t (v kvec_profile_cos) USING EXTENDED(vsql_knn_index_test.kvec_cos)",
      warnings=["Warning\t1831\tDuplicate index 'idx_cos' defined on the table 'test.t'. This is deprecated and will be disallowed in a future release."])
t.sql("INSERT INTO t VALUES (1, '[1,0,0,0]'), (2, '[10,0,0,0]'), (3, '[1,1,0,0]'), (4, '[0,1,0,0]')")
knn(t, 4, rows=["1", "3", "4", "2"])
t.sql(f"SELECT id FROM t ORDER BY {COS}(v, '[1,0,0,0]') LIMIT 2", rows=["1", "2"], sorted_=True)
t.sql(f"SELECT id FROM t ORDER BY {COS}(v, '[1,0,0,0]'), id LIMIT 4", rows=["1", "2", "3", "4"])
t.sql("DROP INDEX idx_cos ON t")
knn(t, 4, rows=["1", "3", "4", "2"])
t.sql(f"SELECT id FROM t ORDER BY {COS}(v, '[1,0,0,0]'), id LIMIT 4", rows=["1", "2", "3", "4"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "composite_primary_key", """
KNN queries on a table whose primary key has seven parts of different types
and charsets. The rows found through the index are the right rows.""")
t.install(table=False)
t.sql("CREATE TABLE docs (k_vc VARCHAR(75) CHARACTER SET utf8mb4 NOT NULL, k_ti TINYINT NOT NULL, k_cu CHAR(8) CHARACTER SET utf8mb4 NOT NULL, k_bi BIGINT NOT NULL, k_vb VARBINARY(70) NOT NULL, k_cl CHAR(10) CHARACTER SET latin1 NOT NULL, k_bin BINARY(16) NOT NULL, v KVECTOR NOT NULL, tag VARCHAR(16), PRIMARY KEY (k_vc, k_ti, k_cu, k_bi, k_vb, k_cl, k_bin))")
t.sql("CREATE INDEX idx_v ON docs (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("INSERT INTO docs VALUES (REPEAT('e', 75), -128, REPEAT(_utf8mb4 'x', 8), -9223372036854775808, 0x0000000441424344, 'alpha', 0x00112233445566778899AABBCCDDEEFF, '[1,2,3,4]', 'one')")
t.sql("INSERT INTO docs VALUES (REPEAT(_utf8mb4 '€', 25), 127, 'short', 9223372036854775807, 0x41004200430044, 'beta ', 0x00000000000000000000000000000000, '[5,6,7,8]', 'two')")
t.sql("INSERT INTO docs VALUES ('', 0, REPEAT(_utf8mb4 'y', 8), 0, 0x00, 'gamma', 0x0102030405060708090A0B0C0D0E0F10, '[9,1,2,3]', 'three')")
t.sql("INSERT INTO docs VALUES (REPEAT(_utf8mb4 '€', 25), 127, 'other', 9223372036854775807, 0xFF, 'delta', 0x0F0E0D0C0B0A09080706050403020100, '[4,3,2,1]', 'four')")
knn(t, 1, table="docs", col="tag", q="[5,6,7,8]", rows=["two"])
knn(t, 1, table="docs", col="tag", q="[1,2,3,4]", rows=["one"])
knn(t, 4, table="docs", col="tag", q="[1,2,3,4]", rows=["one", "four", "two", "three"])
t.sql(f"SELECT k_ti, k_bi, k_cl, tag FROM docs ORDER BY {D}(v, '[1,2,3,4]') LIMIT 4",
      header="k_ti\tk_bi\tk_cl\ttag",
      rows=["-128\t-9223372036854775808\talpha\tone", "127\t9223372036854775807\tdelta\tfour",
            "127\t9223372036854775807\tbeta\ttwo", "0\t0\tgamma\tthree"])
t.uninstall("docs")
tests.append(t)

t = T("select_correctness", "query_point_errors", """
Query points the distance function rejects: NULL and an empty string. The
error is the function's, whether or not a KNN scan could be planned, and a
valid query point works afterwards.""")
t.install()
t.sql(f"SELECT id FROM t ORDER BY {D}(v, NULL) LIMIT 2",
      error=("ER_UDF_ERROR", "ERROR HY000: VDF error in function 'kvec_l2_distance': kvec_l2_distance: operand must not be NULL"))
t.sql(f"SELECT id FROM t ORDER BY {D}(v, NULL)",
      error=("ER_UDF_ERROR", "ERROR HY000: VDF error in function 'kvec_l2_distance': kvec_l2_distance: operand must not be NULL"))
t.sql(f"SELECT id FROM t ORDER BY {D}(v, '') LIMIT 2",
      error=("ER_WRONG_VALUE", "ERROR HY000: Incorrect kvector value: ''"))
knn(t, 2, q="[2,0,0,0]", rows=["5", "1"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "index_hints", """
Index hints on a KNN query do not change its rows: FORCE INDEX and IGNORE
INDEX on the KNN index, and FORCE INDEX on the primary key.""")
t.install()
knn(t, hint="FORCE INDEX (idx_v)", rows=["1", "4", "8"])
knn(t, hint="IGNORE INDEX (idx_v)", rows=["1", "4", "8"])
knn(t, hint="FORCE INDEX (PRIMARY)", rows=["1", "4", "8"])
knn(t, hint="USE INDEX (idx_v)", rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

# ---------------------------------------------------------------- select_plan
PLAN_HDR = """
Each line shows whether EXPLAIN plans a KNN scan, whether executing the query
ran one, and the query's error number."""

t = T("select_plan", "knn_used", """
Queries that should run as a KNN scan: a distance ORDER BY with LIMIT on a
table whose WHERE clause, if any, no other index can serve.""" + PLAN_HDR)
t.install()
t.plan(f"SELECT id FROM t {ORDER} LIMIT 3", 1, 1)
t.plan(f"SELECT id FROM t {ORDER} LIMIT 2 OFFSET 3", 1, 1)
t.plan(f"SELECT id, v FROM t {ORDER} LIMIT 3", 1, 1)
t.plan(f"SELECT id, {D}(v, '[1,0,0,0]') AS d FROM t ORDER BY d LIMIT 3", 1, 1)
t.plan(f"SELECT id FROM t WHERE pay IN ('b','c','h') {ORDER} LIMIT 3", 1, 1)
t.plan(f"SELECT id FROM t WHERE id + 0 IN (2,3,8) {ORDER} LIMIT 3", 1, 1)
t.plan(f"SELECT id FROM t ORDER BY {D}(v, (SELECT v FROM t WHERE id = 1)) LIMIT 3", 1, 1)
t.sql("CREATE VIEW vw AS SELECT id, v FROM t")
t.plan(f"SELECT id FROM vw {ORDER} LIMIT 3", 1, 1)
t.sql("DROP VIEW vw")
t.sql("CREATE TABLE c (k INT PRIMARY KEY, q KVECTOR)")
t.sql("INSERT INTO c VALUES (1, '[1,0,0,0]')")
t.plan(f"SELECT t.id FROM c, t WHERE c.k = 1 ORDER BY {D}(t.v, c.q) LIMIT 3", 1, 1)
t.sql("DROP TABLE c")
t.sql("CREATE TABLE e (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON e (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.plan(f"SELECT id FROM e {ORDER} LIMIT 3", 1, 1)
t.sql("DROP TABLE e")
t.uninstall()
tests.append(t)

t = T("select_plan", "having", """
A HAVING clause on the selected distance does not stop the KNN scan: HAVING is
applied to the rows the scan returns.""" + PLAN_HDR)
t.install()
t.plan(f"SELECT id, {D}(v, '[1,0,0,0]') AS d FROM t HAVING d <= 1.3 ORDER BY d LIMIT 10", 1, 1)
t.uninstall()
tests.append(t)

t = T("select_plan", "other_index", """
A WHERE clause another index can serve. The index plan wins and the rows are
sorted (no KNN scan), unless the query names the KNN index itself or ignores
the other index.""" + PLAN_HDR)
t.install()
t.echo("Primary key")
t.plan(f"SELECT id FROM t WHERE id = 4 {ORDER} LIMIT 3", 0, 0)
t.plan(f"SELECT id FROM t WHERE id > 2 {ORDER} LIMIT 3", 0, 0)
t.plan(f"SELECT id FROM t WHERE id BETWEEN 2 AND 5 {ORDER} LIMIT 2", 0, 0)
t.echo("Secondary index")
t.plan(f"SELECT id FROM t WHERE grp = 2 {ORDER} LIMIT 2", 0, 0)
t.plan(f"SELECT id FROM t WHERE grp >= 2 {ORDER} LIMIT 3", 0, 0)
t.echo("The other index ignored, or the KNN index forced")
t.plan(f"SELECT id FROM t IGNORE INDEX (PRIMARY) WHERE id > 2 {ORDER} LIMIT 3", 1, 1)
t.plan(f"SELECT id FROM t IGNORE INDEX (k_grp) WHERE grp = 2 {ORDER} LIMIT 2", 1, 1)
t.plan(f"SELECT id FROM t FORCE INDEX (idx_v) WHERE grp = 2 {ORDER} LIMIT 2", 1, 1)
t.uninstall()
tests.append(t)

t = T("select_plan", "knn_not_used", """
Queries a KNN scan cannot answer on its own: descending order, a second
ORDER BY key, and a join that does not start from the indexed table.""" + PLAN_HDR)
t.install()
t.plan(f"SELECT id FROM t {ORDER} DESC LIMIT 2", 0, 0)
t.plan(f"SELECT id FROM t {ORDER}, id LIMIT 3", 0, 0)
t.sql("CREATE TABLE g (grp INT PRIMARY KEY, name VARCHAR(10))")
t.sql("INSERT INTO g VALUES (1,'one'),(2,'two'),(3,'three')")
t.plan(f"SELECT t.id, g.name FROM t JOIN g ON t.grp = g.grp ORDER BY {D}(t.v, '[1,0,0,0]') LIMIT 3", 0, 0)
t.sql("DROP TABLE g")
t.uninstall()
tests.append(t)

t = T("select_plan", "locking", """
Locking reads. A READ COMMITTED locking read runs as a KNN scan, and EXPLAIN
shows the plan that runs. A SERIALIZABLE read needs predicate locks a KNN scan
cannot take, so it is sorted instead.""" + PLAN_HDR)
t.install()
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
t.sql("START TRANSACTION")
t.plan(f"SELECT id FROM t {ORDER} LIMIT 3 FOR SHARE", 1, 1)
t.plan(f"SELECT id FROM t {ORDER} LIMIT 3 FOR UPDATE", 1, 1)
t.sql("COMMIT")
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
t.plan(f"SELECT id FROM t {ORDER} LIMIT 3", 0, 0)
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
t.uninstall()
tests.append(t)

t = T("select_plan", "locking_repeatable_read", """
REPEATABLE READ locking reads (the default isolation level) run as a KNN scan
with record locks only, and EXPLAIN shows the plan that runs.""" + PLAN_HDR)
t.install()
t.sql("START TRANSACTION")
t.plan(f"SELECT id FROM t {ORDER} LIMIT 3 FOR SHARE", 1, 1)
t.plan(f"SELECT id FROM t {ORDER} LIMIT 3 FOR UPDATE", 1, 1)
t.sql("COMMIT")
t.uninstall()
tests.append(t)

t = T("select_plan", "order_function", """
A KNN scan is used for each ORDER BY function the profile binds.""" + PLAN_HDR)
t.install(table=False)
t.need_type("kvec_multi", "kvec_profile_multi")
t.sql("CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON t (v kvec_profile_multi) USING EXTENDED(vsql_knn_index_test.kvec_multi)")
t.sql("INSERT INTO t VALUES (1, '[10,1,0,0]'), (2, '[0.5,0.5,0,0]'), (3, '[0,1,0,0]'), (4, '[2,0,0,0]')")
t.plan(f"SELECT id FROM t {ORDER} LIMIT 2", 1, 1)
t.plan(f"SELECT id FROM t ORDER BY {COS}(v, '[1,0,0,0]') LIMIT 2", 1, 1)
t.uninstall()
tests.append(t)

t = T("select_plan", "window", """
Window functions with KNN ordering. A KNN ORDER BY ... LIMIT over a window
and a KNN derived table under a window run as a KNN scan.""" + PLAN_HDR)
t.install()
t.plan(f"SELECT id, COUNT(*) OVER () AS total FROM t {ORDER} LIMIT 2", 1, 1)
t.plan(f"SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM (SELECT id FROM t {ORDER} LIMIT 4) AS nearest ORDER BY id", 1, 1)
t.uninstall()
tests.append(t)

t = T("select_plan", "index_hints", """
Index hints: forcing the KNN index, or naming it in USE INDEX, keeps the KNN
scan; ignoring it, or forcing the primary key, does not use it.""" + PLAN_HDR)
t.install()
t.plan(f"SELECT id FROM t FORCE INDEX (idx_v) {ORDER} LIMIT 3", 1, 1)
t.plan(f"SELECT id FROM t USE INDEX (idx_v) {ORDER} LIMIT 3", 1, 1)
t.plan(f"SELECT id FROM t IGNORE INDEX (idx_v) {ORDER} LIMIT 3", 0, 0)
t.plan(f"SELECT id FROM t FORCE INDEX (PRIMARY) {ORDER} LIMIT 3", 0, 0)
t.uninstall()
tests.append(t)

t = T("select_plan", "two_indexes", """
Each metric's query uses the index whose profile binds its function; a
function no index binds is sorted, never answered by another index.""" + PLAN_HDR)
t.install(table=False)
t.sql("CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_l2 ON t (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("CREATE INDEX idx_cos ON t (v kvec_profile_cos) USING EXTENDED(vsql_knn_index_test.kvec_cos)",
      warnings=["Warning\t1831\tDuplicate index 'idx_cos' defined on the table 'test.t'. This is deprecated and will be disallowed in a future release."])
t.sql("INSERT INTO t VALUES (1, '[1,0,0,0]'), (2, '[10,0,0,0]'), (3, '[1,1,0,0]'), (4, '[0,1,0,0]')")
t.plan(f"SELECT id FROM t {ORDER} LIMIT 4", 1, 1)
t.plan(f"SELECT id FROM t ORDER BY {COS}(v, '[1,0,0,0]') LIMIT 4", 1, 1)
t.sql("DROP INDEX idx_cos ON t")
t.plan(f"SELECT id FROM t {ORDER} LIMIT 4", 1, 1)
t.plan(f"SELECT id FROM t ORDER BY {COS}(v, '[1,0,0,0]') LIMIT 4", 0, 0)
t.uninstall()
tests.append(t)

# ---------------------------------------------------------------- locking
t = T("locking", "read_committed", """
READ COMMITTED locking KNN reads (FOR SHARE, FOR UPDATE) return the nearest
rows, the same as a plain read and as a locking read with the index
ignored.""")
t.install()
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
knn(t, rows=["1", "4", "8"])
t.sql("START TRANSACTION")
knn(t, extra=" FOR SHARE", rows=["1", "4", "8"])
knn(t, extra=" FOR UPDATE", rows=["1", "4", "8"])
knn(t, 2, where="pay <> 'a'", extra=" FOR UPDATE", rows=["4", "8"])
knn(t, hint="IGNORE INDEX (idx_v)", extra=" FOR UPDATE", rows=["1", "4", "8"])
t.sql("COMMIT")
t.echo("After an UPDATE of a non-indexed column of row 1")
t.sql("UPDATE t SET pay = 'zz' WHERE id = 1")
t.sql("START TRANSACTION")
knn(t, extra=" FOR UPDATE", rows=["1", "4", "8"])
t.sql("COMMIT")
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
t.uninstall()
tests.append(t)

t = T("locking", "repeatable_read", """
REPEATABLE READ locking KNN reads (FOR SHARE, FOR UPDATE) return the nearest
rows. REPEATABLE READ is the default, so this is any locking KNN read run with
default settings.""")
t.install()
t.sql("START TRANSACTION")
knn(t, extra=" FOR SHARE", rows=["1", "4", "8"])
knn(t, extra=" FOR UPDATE", rows=["1", "4", "8"])
knn(t, 2, where="pay <> 'a'", extra=" FOR UPDATE", rows=["4", "8"])
knn(t, hint="IGNORE INDEX (idx_v)", extra=" FOR UPDATE", rows=["1", "4", "8"])
t.sql("COMMIT")
t.uninstall()
tests.append(t)

t = T("locking", "serializable", """
SERIALIZABLE reads, plain and locking, return the nearest rows.""")
t.install()
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
t.sql("START TRANSACTION")
knn(t, rows=["1", "4", "8"])
knn(t, extra=" FOR UPDATE", rows=["1", "4", "8"])
t.sql("COMMIT")
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
t.uninstall()
tests.append(t)

for iso in ("READ COMMITTED", "REPEATABLE READ"):
    name = "lock_wait_rc" if iso.startswith("READ") else "lock_wait_rr"
    t = T("locking", name, f"""
{iso}: a row locked by another session makes a locking KNN read wait
(reported here by NOWAIT), not skip it; SKIP LOCKED skips it; a plain read
does not wait.""")
    t.raw(["--source include/count_sessions.inc", ""])
    t.install()
    t.raw(["--connect (con1, localhost, root,,)"])
    t.sql("START TRANSACTION")
    t.sql("SELECT id FROM t WHERE id = 4 FOR UPDATE", rows=["4"])
    t.raw(["--connection default"])
    t.sql(f"SET SESSION TRANSACTION ISOLATION LEVEL {iso}")
    t.sql("START TRANSACTION")
    knn(t, extra=" FOR UPDATE NOWAIT", error=LOCK_NOWAIT)
    knn(t, extra=" FOR UPDATE SKIP LOCKED", rows=["1", "8", "2"])
    knn(t, rows=["1", "4", "8"])
    t.sql("COMMIT")
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    t.raw(["--connection con1"])
    t.sql("COMMIT")
    t.raw(["--disconnect con1", "--connection default"])
    t.uninstall()
    t.raw(["--source include/wait_until_count_sessions.inc"])
    tests.append(t)

t = T("locking", "unlock_row", """
A READ COMMITTED locking KNN read releases the lock on a row the WHERE clause
rejects, and keeps the locks on the rows it returns.""")
t.raw(["--source include/count_sessions.inc", ""])
t.install()
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
t.sql("START TRANSACTION")
t.echo("The nearest row (1) fails the WHERE clause")
knn(t, 2, where="pay <> 'a'", extra=" FOR UPDATE", rows=["4", "8"])
t.raw(["--connect (con1, localhost, root,,)"])
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
t.sql("START TRANSACTION")
t.echo("Row 1 was rejected, so its lock was released")
t.sql("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", rows=["1"])
t.echo("Row 4 was returned, so it is still locked")
t.sql("SELECT id FROM t WHERE id = 4 FOR UPDATE NOWAIT", error=LOCK_NOWAIT)
t.sql("ROLLBACK")
t.raw(["--disconnect con1", "--connection default"])
t.sql("COMMIT")
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
t.uninstall()
t.raw(["--source include/wait_until_count_sessions.inc"])
tests.append(t)

for variant in ("pk", "btree"):
    t = T("locking", f"current_entry_{variant}", f"""
A READ COMMITTED locking KNN read returns the rows whose index entry is
current, whatever else happened to the row: after an UPDATE of a non-indexed
column the row is still returned, and a row locked by another session's
uncommitted change is waited for (reported by NOWAIT), not skipped.
Index type: {"kvec_l2, which stores the primary key" if variant == "pk" else
              "kvec_l2_btree, which stores no primary key (the server maps the column reference)"}.""")
    t.raw(["--source include/count_sessions.inc", ""])
    t.install(table=False)
    if variant == "btree":
        t.need_type("kvec_l2_btree", "kvec_profile_l2_btree")
        key, itype = "v kvec_profile_l2_btree", "kvec_l2_btree"
    else:
        key, itype = "v", "kvec_l2"
    t.sql("CREATE TABLE t (id INT PRIMARY KEY, c INT, v KVECTOR)")
    t.sql(f"CREATE INDEX idx_v ON t ({key}) USING EXTENDED(vsql_knn_index_test.{itype})")
    t.sql("INSERT INTO t VALUES (1, 0, '[1,0,0,0]'), (2, 0, '[0,1,0,0]'), (3, 0, '[0,0,2,0]')")
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    t.sql("START TRANSACTION")
    knn(t, extra=" FOR UPDATE", rows=["1", "2", "3"])
    t.sql("COMMIT")
    t.sql("UPDATE t SET c = 1 WHERE id = 1")
    t.sql("START TRANSACTION")
    knn(t, extra=" FOR UPDATE", rows=["1", "2", "3"])
    t.sql("COMMIT")
    t.raw(["--connect (con1, localhost, root,,)"])
    t.sql("START TRANSACTION")
    t.sql("UPDATE t SET c = 2 WHERE id = 2")
    t.raw(["--connection default"])
    t.sql("START TRANSACTION")
    knn(t, extra=" FOR UPDATE NOWAIT", error=LOCK_NOWAIT)
    t.sql("COMMIT")
    t.raw(["--connection con1"])
    t.sql("ROLLBACK")
    t.raw(["--disconnect con1", "--connection default"])
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    t.uninstall()
    t.raw(["--source include/wait_until_count_sessions.inc"])
    tests.append(t)

t = T("locking", "insert_select", """
INSERT ... SELECT with KNN ordering. At REPEATABLE READ (the default) the
SELECT reads its rows with shared locks, so it is a locking KNN read.""")
t.install()
t.sql("CREATE TABLE t_out (id INT PRIMARY KEY)")
t.sql(f"INSERT INTO t_out SELECT id FROM t {ORDER} LIMIT 2")
t.sql("SELECT id FROM t_out ORDER BY id", rows=["1", "4"])
t.sql("DROP TABLE t_out")
t.uninstall()
tests.append(t)

# ---------------------------------------------------------------- delete
t = T("delete", "delete_row", """
A deleted row is gone from KNN results.""")
t.install()
t.sql("DELETE FROM t WHERE id = 1")
knn(t, rows=["4", "8", "2"])
knn(t, 8, rows=["4", "8", "2", "5", "3", "6", "7"])
t.sql("DELETE FROM t WHERE id = 8")
knn(t, rows=["4", "2", "5"])
t.uninstall()
tests.append(t)

t = T("delete", "delete_where", """
Rows deleted by a WHERE clause, by the primary key and by a secondary index,
are gone from KNN results.""")
t.install()
t.sql("DELETE FROM t WHERE grp = 2")
knn(t, 8, rows=["1", "2", "5", "6", "7"])
t.sql("DELETE FROM t WHERE id > 5")
knn(t, 8, rows=["1", "2", "5"])
t.uninstall()
tests.append(t)

t = T("delete", "delete_order_by", """
DELETE ... ORDER BY distance LIMIT n deletes the nearest rows.""")
t.install()
t.sql(f"DELETE FROM t ORDER BY {D}(v, '[0,0,0,3]') LIMIT 1")
t.sql("SELECT id FROM t ORDER BY id", rows=["1", "2", "3", "4", "5", "6", "8"])
t.sql(f"DELETE FROM t {ORDER} LIMIT 2")
t.sql("SELECT id FROM t ORDER BY id", rows=["2", "3", "5", "6", "8"])
knn(t, 8, rows=["8", "2", "5", "3", "6"])
t.uninstall()
tests.append(t)

t = T("delete", "delete_reinsert", """
A row deleted and inserted again with the same key appears once, with its new
vector.""")
t.install()
t.sql("DELETE FROM t WHERE id = 1")
t.sql("INSERT INTO t VALUES (1, 1, 'a', '[1,0,0,0]')")
knn(t, rows=["1", "4", "8"])
t.sql("DELETE FROM t WHERE id = 4")
t.sql("INSERT INTO t VALUES (4, 2, 'd', '[9,9,9,9]')")
knn(t, 8, rows=["1", "8", "2", "5", "3", "6", "7", "4"])
t.uninstall()
tests.append(t)

t = T("delete", "delete_all", """
After deleting every row, a KNN query returns nothing, and finds rows inserted
later.""")
t.install()
t.sql("DELETE FROM t")
knn(t, rows=[])
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0,0]')")
knn(t, rows=["9"])
t.uninstall()
tests.append(t)

# ---------------------------------------------------------------- update
t = T("update", "payload_update", """
An UPDATE that does not change the vector leaves KNN results unchanged, also
UPDATE ... ORDER BY distance LIMIT n.""")
t.install()
t.sql("UPDATE t SET pay = 'zz' WHERE id = 1")
t.sql("UPDATE t SET grp = 3 WHERE id = 4")
knn(t, rows=["1", "4", "8"])
t.sql("SELECT id, grp, pay FROM t WHERE id IN (1, 4) ORDER BY id", header="id\tgrp\tpay",
      rows=["1\t1\tzz", "4\t3\td"])
t.sql(f"UPDATE t SET pay = 'yy' {ORDER} LIMIT 2")
t.sql("SELECT id FROM t WHERE pay = 'yy' ORDER BY id", rows=["1", "4"])
knn(t, rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

t = T("update", "vector_update", """
An UPDATE of the indexed vector moves the row in KNN results.""")
t.install()
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
knn(t, 8, rows=["4", "8", "2", "5", "3", "6", "7", "1"])
t.sql("UPDATE t SET v = '[1,0.1,0,0]' WHERE id = 6")
knn(t, 8, rows=["6", "4", "8", "2", "5", "3", "7", "1"])
t.sql("UPDATE t SET v = NULL WHERE id = 6")
knn(t, 8, where="v IS NOT NULL", rows=["4", "8", "2", "5", "3", "7", "1"])
t.uninstall()
tests.append(t)

t = T("update", "pk_update", """
An UPDATE of the primary key keeps the row in KNN results under its new key.""")
t.install()
t.sql("UPDATE t SET id = 100 WHERE id = 1")
knn(t, rows=["100", "4", "8"])
t.sql("UPDATE t SET id = id + 10")
knn(t, rows=["110", "14", "18"])
t.uninstall()
tests.append(t)

# ---------------------------------------------------------------- rollback
t = T("rollback", "rollback_payload_update", """
A transaction sees its own uncommitted payload update, KNN results are
unchanged by it, and a rollback restores the old value.""")
t.install()
t.sql("START TRANSACTION")
t.sql("UPDATE t SET pay = 'zz' WHERE id = 1")
t.sql("SELECT pay FROM t WHERE id = 1", header="pay", rows=["zz"])
knn(t, col="id, pay", header="id\tpay", rows=["1\tzz", "4\td", "8\th"])
t.sql("ROLLBACK")
knn(t, col="id, pay", header="id\tpay", rows=["1\ta", "4\td", "8\th"])
t.uninstall()
tests.append(t)

t = T("rollback", "rollback_insert", """
A transaction sees its own uncommitted insert in KNN results, and a rollback
removes it (removing the index entry).""")
t.install()
t.sql("START TRANSACTION")
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]')")
knn(t, rows=["1", "9", "4"])
t.sql("ROLLBACK")
knn(t, rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

t = T("rollback", "rollback_delete", """
A transaction does not see its own uncommitted delete in KNN results, and a
rollback brings the row back.""")
t.install()
t.sql("START TRANSACTION")
t.sql("DELETE FROM t WHERE id = 4")
knn(t, rows=["1", "8", "2"])
t.sql("ROLLBACK")
knn(t, rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

t = T("rollback", "rollback_vector_update", """
A transaction sees its own uncommitted vector update in KNN results, and a
rollback restores the old vector.""")
t.install()
t.sql("START TRANSACTION")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
knn(t, rows=["4", "8", "2"])
t.sql("ROLLBACK")
knn(t, rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

t = T("rollback", "rollback_pk_update", """
A transaction sees its own uncommitted primary key update in KNN results, and
a rollback restores the old key.""")
t.install()
t.sql("START TRANSACTION")
t.sql("UPDATE t SET id = 100 WHERE id = 1")
knn(t, rows=["100", "4", "8"])
t.sql("ROLLBACK")
knn(t, rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

def other_session(name, header, changes, after_commit):
    t = T("rollback", name, header)
    t.raw(["--source include/count_sessions.inc", ""])
    t.install()
    t.raw(["--connect (con1, localhost, root,,)"])
    t.sql("START TRANSACTION")
    for c in changes:
        t.sql(c)
    t.raw(["--connection default"])
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    knn(t, rows=["1", "4", "8"])
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    knn(t, rows=["1", "4", "8"])
    t.raw(["--connection con1"])
    t.sql("COMMIT")
    t.raw(["--disconnect con1", "--connection default"])
    knn(t, rows=after_commit)
    t.uninstall()
    t.raw(["--source include/wait_until_count_sessions.inc"])
    tests.append(t)

other_session("other_session_insert", """
Another session's uncommitted insert and payload update are invisible to KNN
reads at READ COMMITTED and REPEATABLE READ, and visible once committed.""",
              ["INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]')",
               "UPDATE t SET pay = 'zz' WHERE id = 1"], ["1", "9", "4"])
other_session("other_session_delete", """
Another session's uncommitted delete is invisible to KNN reads at READ
COMMITTED and REPEATABLE READ, and visible once committed.""",
              ["DELETE FROM t WHERE id = 4"], ["1", "8", "2"])
other_session("other_session_vector_update", """
Another session's uncommitted vector update is invisible to KNN reads at READ
COMMITTED and REPEATABLE READ, and visible once committed.""",
              ["UPDATE t SET v = '[9,9,9,9]' WHERE id = 4"], ["1", "8", "2"])

def snapshot(name, header, change, before, after, mvcc=False):
    t = T("rollback", name, header)
    t.raw(["--source include/count_sessions.inc", ""])
    if mvcc:
        t.install(table=False)
        t.need_type("kvec_l2_mvcc", "kvec_profile_l2_mvcc")
        t.sql("CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR)")
        t.sql("CREATE INDEX idx_v ON t (v kvec_profile_l2_mvcc) USING EXTENDED(vsql_knn_index_test.kvec_l2_mvcc)")
        t.sql("INSERT INTO t VALUES (1, '[1,0,0,0]'), (4, '[1,1,0,0]'), (8, '[1,0,1.2,0]')")
    else:
        t.install()
    t.sql("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    t.raw(["--connect (con1, localhost, root,,)"])
    t.sql(change)
    t.raw(["--connection default"])
    knn(t, rows=before)
    t.sql("COMMIT")
    knn(t, rows=after)
    t.raw(["--disconnect con1"])
    t.uninstall()
    t.raw(["--source include/wait_until_count_sessions.inc"])
    tests.append(t)

SNAP = """
A REPEATABLE READ snapshot does not see a row another session {what} after the
snapshot starts; a new snapshot does."""
MVCC = """
The same as {base}, with an index type that advertises MVCC_VISIBILITY (it
filters entries by the reader's view itself)."""
INS9 = "INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]')"
snapshot("snapshot_insert", SNAP.format(what="inserts and commits"), INS9,
         ["1", "4", "8"], ["1", "9", "4"])
snapshot("snapshot_delete", SNAP.format(what="deletes and commits"),
         "DELETE FROM t WHERE id = 4", ["1", "4", "8"], ["1", "8", "2"])

# ---------------------------------------------------------------- ddl
t = T("ddl", "create_index_populated", """
CREATE INDEX on a table that already has rows indexes them all.""")
t.install(table=False)
t.sql("CREATE TABLE p (id INT PRIMARY KEY, v KVECTOR)")
t.sql("INSERT INTO p VALUES (1, '[1,0,0,0]'), (2, '[0,1,0,0]'), (3, '[2,0,0,0]')")
t.sql("CREATE INDEX idx_v ON p (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
knn(t, table="p", rows=["1", "3", "2"])
t.sql("INSERT INTO p VALUES (4, '[1,0.5,0,0]')")
knn(t, table="p", rows=["1", "4", "3"])
t.uninstall("p")
tests.append(t)

t = T("ddl", "table_rebuild", """
Table rebuilds keep the KNN index complete and usable.""")
t.install()
t.sql("ALTER TABLE t FORCE")
knn(t, rows=["1", "4", "8"])
t.sql("ALTER TABLE t ADD COLUMN extra INT")
knn(t, rows=["1", "4", "8"])
t.sql("ALTER TABLE t DROP COLUMN extra, ALGORITHM=COPY")
knn(t, rows=["1", "4", "8"])
t.sql("OPTIMIZE TABLE t", header="Table\tOp\tMsg_type\tMsg_text",
      rows=["test.t\toptimize\tnote\tTable does not support optimize, doing recreate + analyze instead",
            "test.t\toptimize\tstatus\tOK"])
knn(t, rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

t = T("ddl", "primary_key_change", """
Changing the primary key of a table with a custom index is refused, and the
index keeps working.""")
t.install(table=False)
t.sql("CREATE TABLE pk (id INT PRIMARY KEY, k INT NOT NULL, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON pk (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("INSERT INTO pk VALUES (1, 30, '[1,0,0,0]'), (2, 20, '[0,1,0,0]'), (3, 10, '[2,0,0,0]')")
t.sql("ALTER TABLE pk DROP PRIMARY KEY, ADD PRIMARY KEY (k)",
      error=("ER_VILLAGESQL_GENERIC_ERROR",
             "ERROR HY000: VillageSQL: Changing the primary key of a table with a USING EXTENDED index is not supported"))
knn(t, table="pk", rows=["1", "3", "2"])
t.uninstall("pk")
tests.append(t)

t = T("ddl", "check_table", """
CHECK TABLE on a table with a custom index reports it OK, and the index keeps
working afterwards.""")
t.install()
t.sql("CHECK TABLE t", header="Table\tOp\tMsg_type\tMsg_text",
      rows=["test.t\tcheck\tstatus\tOK"])
knn(t, rows=["1", "4", "8"])
t.sql("CHECK TABLE t EXTENDED", header="Table\tOp\tMsg_type\tMsg_text",
      rows=["test.t\tcheck\tstatus\tOK"])
knn(t, rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

t = T("ddl", "drop_index", """
After DROP INDEX the same KNN query still answers, sorted.""")
t.install()
t.sql("DROP INDEX idx_v ON t")
knn(t, rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

t = T("ddl", "recreate_index", """
An index dropped and created again on the populated table is complete and
usable.""")
t.install()
t.sql("DROP INDEX idx_v ON t")
t.sql("CREATE INDEX idx_v ON t (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
knn(t, rows=["1", "4", "8"])
knn(t, 8, rows=ALL8)
t.uninstall()
tests.append(t)

t = T("ddl", "rename_table", """
After RENAME TABLE the KNN index is complete and usable under the new name, and
again after renaming back.""")
t.install()
t.sql("RENAME TABLE t TO t2")
knn(t, table="t2", rows=["1", "4", "8"])
t.sql("RENAME TABLE t2 TO t")
knn(t, rows=["1", "4", "8"])
t.uninstall()
tests.append(t)

t = T("ddl", "truncate_table", """
After TRUNCATE TABLE a KNN query returns nothing, and finds rows inserted
later.""")
t.install()
t.sql("TRUNCATE TABLE t")
knn(t, rows=[])
t.sql("INSERT INTO t VALUES (2, 1, 'b', '[0,1,0,0]'), (6, 3, 'f', '[4,0,0,0]')")
knn(t, rows=["2", "6"])
t.uninstall()
tests.append(t)


# ================================================================ more cases
# Expected results below are computed from a model of the table, so a change
# to the data recomputes them. Rows: id -> (grp, pay, vector).
import math

BASE = {1: (1, "a", (1, 0, 0, 0)), 2: (1, "b", (0, 1, 0, 0)),
        3: (2, "c", (0, 0, 2, 0)), 4: (2, "d", (1, 1, 0, 0)),
        5: (3, "e", (2.5, 0, 0, 0)), 6: (3, "f", (4, 0, 0, 0)),
        7: (1, "g", (0, 0, 0, 3)), 8: (2, "h", (1, 0, 1.2, 0))}
Q = (1, 0, 0, 0)


def vec(text):
    return tuple(float(x) for x in text.strip("[]").split(","))


def near(rows, k=3, q=Q, pred=lambda i, r: True):
    """Ids of the k nearest rows with a non-NULL vector, asserting no tie
    decides the order or the cut."""
    hits = sorted((math.dist(r[2], q), i) for i, r in rows.items()
                  if r[2] is not None and pred(i, r))
    top = hits[:k + 1]
    for a, b in zip(top, top[1:]):
        assert abs(a[0] - b[0]) > 1e-9 or a[1] == b[1], ("tie", a, b)
    return [str(i) for _, i in hits[:k]]


def model(**changes):
    rows = dict(BASE)
    for i, v in changes.items():
        i = int(i[1:])
        if v is None:
            rows.pop(i)
        else:
            rows[i] = v
    return rows


def with_vec(rows, i, v):
    rows = dict(rows)
    g, pay, _ = rows[i]
    rows[i] = (g, pay, None if v is None else vec(v))
    return rows


def btree_table(t):
    """The standard table, indexed with the REF_LOOKUP index type."""
    t.need_type("kvec_l2_btree", "kvec_profile_l2_btree")
    t.sql("CREATE TABLE t (id INT PRIMARY KEY, grp INT, pay VARCHAR(10), "
          "v KVECTOR, KEY k_grp (grp))")
    t.sql("CREATE INDEX idx_v ON t (v kvec_profile_l2_btree) "
          "USING EXTENDED(vsql_knn_index_test.kvec_l2_btree)")
    t.sql("INSERT INTO t VALUES (1, 1, 'a', '[1,0,0,0]'), (2, 1, 'b', '[0,1,0,0]'), "
          "(3, 2, 'c', '[0,0,2,0]'), (4, 2, 'd', '[1,1,0,0]'), "
          "(5, 3, 'e', '[2.5,0,0,0]'), (6, 3, 'f', '[4,0,0,0]'), "
          "(7, 1, 'g', '[0,0,0,3]'), (8, 2, 'h', '[1,0,1.2,0]')")


def g_table(t):
    t.sql("CREATE TABLE g (grp INT PRIMARY KEY, name VARCHAR(10))")
    t.sql("INSERT INTO g VALUES (1,'one'),(2,'two'),(3,'three')")


# ---------------------------------------------------------------- update
t = T("update", "vector_update_btree", """
An UPDATE of the indexed vector moves the row in KNN results, with the index
type that stores no primary key (REF_LOOKUP: the server passes the entry
reference to mark_delete and purge).""")
t.install(table=False)
btree_table(t)
m = with_vec(BASE, 1, "[9,9,9,9]")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
knn(t, 8, rows=near(m, 8))
m = with_vec(m, 6, "[1,0.1,0,0]")
t.sql("UPDATE t SET v = '[1,0.1,0,0]' WHERE id = 6")
knn(t, 8, rows=near(m, 8))
m = with_vec(m, 6, None)
t.sql("UPDATE t SET v = NULL WHERE id = 6")
knn(t, 8, where="v IS NOT NULL", rows=near(m, 8))
t.uninstall()
tests.append(t)

t = T("update", "vector_update_same_value", """
An UPDATE that sets the vector to its current value, or to an equal one,
leaves the row once in KNN results at the same place.""")
t.install()
t.sql("UPDATE t SET v = v WHERE id = 1")
knn(t, 8, rows=ALL8)
t.sql("UPDATE t SET v = '[1,0,0,0]' WHERE id = 1")
knn(t, 8, rows=ALL8)
t.sql("UPDATE t SET v = v")
knn(t, 8, rows=ALL8)
t.sql("SELECT COUNT(*) FROM t", header="COUNT(*)", rows=["8"])
t.uninstall()
tests.append(t)

t = T("update", "vector_update_repeated", """
The same row's vector updated several times in one transaction is in KNN
results once, at its latest vector, inside the transaction and after commit.""")
t.install()
t.sql("START TRANSACTION")
m = BASE
for v in ["[9,9,9,9]", "[1,0,0,0.5]", "[3,0,0,0]"]:
    m = with_vec(m, 1, v)
    t.sql(f"UPDATE t SET v = '{v}' WHERE id = 1")
    knn(t, 8, rows=near(m, 8))
t.sql("COMMIT")
knn(t, 8, rows=near(m, 8))
t.uninstall()
tests.append(t)

t = T("update", "vector_update_null_roundtrip", """
A vector set to NULL leaves KNN results, and comes back when set to a value
again.""")
t.install()
m = with_vec(BASE, 2, None)
t.sql("UPDATE t SET v = NULL WHERE id = 2")
knn(t, 8, where="v IS NOT NULL", rows=near(m, 8))
m = with_vec(m, 2, "[1,0,0,0.5]")
t.sql("UPDATE t SET v = '[1,0,0,0.5]' WHERE id = 2")
knn(t, 8, rows=near(m, 8))
t.uninstall()
tests.append(t)

t = T("update", "update_order_by_moves_rows", """
UPDATE ... ORDER BY distance LIMIT n that changes the vector updates the n rows
that were nearest when it started: a row it moves is not found again, whether
it moves away from the query point or toward it.""")
t.install()
far = near(BASE, 2)
t.sql(f"UPDATE t SET v = '[9,9,9,9]', pay = 'far' {ORDER} LIMIT 2")
t.sql("SELECT id FROM t WHERE pay = 'far' ORDER BY id", rows=sorted(far))
m = BASE
for i in far:
    m = with_vec(m, int(i), "[9,9,9,9]")
moved = near(m, 3)
t.sql(f"UPDATE t SET v = '[1,0,0,0.01]', pay = 'near' {ORDER} LIMIT 3")
t.sql("SELECT id FROM t WHERE pay = 'near' ORDER BY id", rows=sorted(moved, key=int))
knn(t, 3, rows=sorted(moved, key=int), sorted_=True)
t.sql("SELECT COUNT(*) FROM t", header="COUNT(*)", rows=["8"])
t.uninstall()
tests.append(t)

t = T("update", "upsert", """
INSERT ... ON DUPLICATE KEY UPDATE and REPLACE that change the vector move the
row in KNN results; on a new key they insert it.""")
t.install()
m = with_vec(BASE, 1, "[9,9,9,9]")
t.sql("INSERT INTO t VALUES (1, 1, 'a', '[9,9,9,9]') AS new ON DUPLICATE KEY UPDATE v = new.v")
knn(t, 8, rows=near(m, 8))
m = with_vec(m, 4, "[1,0,0,0.2]")
t.sql("REPLACE INTO t VALUES (4, 2, 'd', '[1,0,0,0.2]')")
knn(t, 8, rows=near(m, 8))
m = dict(m)
m[9] = (1, "i", vec("[1,0,0.1,0]"))
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]') AS new ON DUPLICATE KEY UPDATE v = new.v")
knn(t, 9, rows=near(m, 9))
t.sql("SELECT COUNT(*) FROM t", header="COUNT(*)", rows=["9"])
t.uninstall()
tests.append(t)

t = T("update", "multi_table_update", """
A multi-table UPDATE that changes the vector moves the rows it matched.""")
t.install()
g_table(t)
m = BASE
for i, r in BASE.items():
    if r[0] == 2:
        m = with_vec(m, i, "[9,9,9,9]")
t.sql("UPDATE t JOIN g ON t.grp = g.grp SET t.v = '[9,9,9,9]' WHERE g.name = 'two'")
knn(t, 5, rows=near(m, 5))
knn(t, 8, col="grp", header="grp", rows=sorted(str(m[int(i)][0]) for i in near(BASE, 8)), sorted_=True)
t.sql("DROP TABLE g")
t.uninstall()
tests.append(t)

# ---------------------------------------------------------------- delete
t = T("delete", "delete_btree", """
Deleted rows are gone from KNN results, with the index type that stores no
primary key (REF_LOOKUP).""")
t.install(table=False)
btree_table(t)
m = model(r1=None)
t.sql("DELETE FROM t WHERE id = 1")
knn(t, 8, rows=near(m, 8))
m = {i: r for i, r in m.items() if r[0] != 2}
t.sql("DELETE FROM t WHERE grp = 2")
knn(t, 8, rows=near(m, 8))
t.uninstall()
tests.append(t)

t = T("delete", "multi_table_delete", """
A multi-table DELETE removes the rows it matched from KNN results.""")
t.install()
g_table(t)
m = {i: r for i, r in BASE.items() if r[0] != 1}
t.sql("DELETE t FROM t JOIN g ON t.grp = g.grp WHERE g.name = 'one'")
knn(t, 8, rows=near(m, 8))
t.sql("DROP TABLE g")
t.uninstall()
tests.append(t)

t = T("delete", "delete_insert_same_trx", """
A row deleted and inserted again twice in one transaction, with a different
vector each time, is in KNN results once, at its last vector.""")
t.install()
t.sql("START TRANSACTION")
t.sql("DELETE FROM t WHERE id = 1")
t.sql("INSERT INTO t VALUES (1, 1, 'a', '[3,0,0,0]')")
t.sql("DELETE FROM t WHERE id = 1")
t.sql("INSERT INTO t VALUES (1, 1, 'a', '[1.5,0,0,0]')")
m = with_vec(BASE, 1, "[1.5,0,0,0]")
knn(t, 8, rows=near(m, 8))
t.sql("COMMIT")
knn(t, 8, rows=near(m, 8))
t.sql("SELECT COUNT(*) FROM t", header="COUNT(*)", rows=["8"])
t.uninstall()
tests.append(t)

t = T("delete", "purge_entries", """
After a committed DELETE or vector UPDATE is purged, the index holds one entry
per row again: purge removes the entries of deleted rows and the old entries
of updated vectors.""")
t.install()
t.need_fn("kvec_entry_count")
t.raw(["--let $base = `SELECT vsql_knn_index_test.kvec_entry_count()`"])
t.sql("DELETE FROM t WHERE id = 7")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 6")
t.comment("wait_innodb_all_purged.inc can return before purge has removed the\n"
          "index entries, so wait for the entry count itself.")
t.raw(["--let $wait_timeout = 60",
       "--let $wait_condition = SELECT vsql_knn_index_test.kvec_entry_count() - $base = -1",
       "--source include/wait_condition.inc"])
t.raw(["--disable_query_log",
       "--eval SELECT vsql_knn_index_test.kvec_entry_count() - $base AS entries_change",
       "--enable_query_log"],
      ["entries_change", "-1"])
m = with_vec(model(r7=None), 6, "[9,9,9,9]")
knn(t, 8, rows=near(m, 8))
t.uninstall()
tests.append(t)

t = T("ddl", "drop_table_entries", """
DROP TABLE releases the entries of the table's KNN index, including entries
of deleted and updated rows not purged yet: the extension's entry count is
the same before CREATE TABLE and after DROP TABLE.""")
t.install(table=False)
t.need_fn("kvec_entry_count")
t.raw(["--let $base = `SELECT vsql_knn_index_test.kvec_entry_count()`"])
t.raw([f"--source {INC}/knn_table.inc"])
t.raw(["--disable_query_log",
       "--eval SELECT vsql_knn_index_test.kvec_entry_count() - $base AS entries_change",
       "--enable_query_log"], ["entries_change", "8"])
t.sql("DROP TABLE t")
t.raw(["--disable_query_log",
       "--eval SELECT vsql_knn_index_test.kvec_entry_count() - $base AS entries_change",
       "--enable_query_log"], ["entries_change", "0"])
t.echo("With a delete and a vector update committed but not purged")
t.raw([f"--source {INC}/knn_table.inc"])
t.sql("DELETE FROM t WHERE id = 7")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 6")
t.sql("DROP TABLE t")
t.raw(["--disable_query_log",
       "--eval SELECT vsql_knn_index_test.kvec_entry_count() - $base AS entries_change",
       "--enable_query_log"], ["entries_change", "0"])
t.uninstall(drop=None)
tests.append(t)

def entries_delta(t, expected):
    t.raw(["--disable_query_log",
           "--eval SELECT vsql_knn_index_test.kvec_entry_count() - $base AS entries_change",
           "--enable_query_log"], ["entries_change", str(expected)])


def wait_entries(t, expected):
    """Waits until purge has run and the entry count has come down to
    expected (or below), then prints the change."""
    t.raw(["--source include/wait_innodb_all_purged.inc",
           "--let $wait_timeout = 60",
           f"--let $wait_condition = SELECT vsql_knn_index_test.kvec_entry_count() - $base <= {expected}",
           "--source include/wait_condition.inc"])
    entries_delta(t, expected)


t = T("rollback", "snapshot_purge", """
Purge keeps the index entries an open REPEATABLE READ snapshot still needs.
Another session deletes a row and moves another one's vector and commits after
the snapshot started; while the snapshot is open the old entries stay, and its
KNN reads return the table as it was. Once it ends, purge removes them.""")
t.raw(["--source include/count_sessions.inc", ""])
t.install()
t.need_fn("kvec_entry_count")
t.raw(["--let $base = `SELECT vsql_knn_index_test.kvec_entry_count()`"])
t.sql("START TRANSACTION WITH CONSISTENT SNAPSHOT")
knn(t, 8, rows=ALL8)
t.raw(["--connect (con1, localhost, root,,)"])
t.sql("DELETE FROM t WHERE id = 1")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 4")
t.sql("SET GLOBAL innodb_purge_run_now = ON")
t.raw(["--connection default"])
knn(t, 8, rows=ALL8)
entries_delta(t, 1)
t.sql("COMMIT")
m = with_vec(model(r1=None), 4, "[9,9,9,9]")
wait_entries(t, -1)
knn(t, 8, rows=near(m, 7))
t.raw(["--disconnect con1"])
t.uninstall()
t.raw(["--source include/wait_until_count_sessions.inc"])
tests.append(t)

t = T("delete", "purge_same_vector", """
Purge removes only the entries it should when a row is back at a vector it
had before: a vector moved away and back, and a row deleted and inserted again
with the same key and vector, each in separate transactions. After purge the
index holds one entry per row and both rows are found at their place.""")
t.install()
t.need_fn("kvec_entry_count")
t.raw(["--let $base = `SELECT vsql_knn_index_test.kvec_entry_count()`"])
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
t.sql("UPDATE t SET v = '[1,0,0,0]' WHERE id = 1")
t.sql("DELETE FROM t WHERE id = 2")
t.sql("INSERT INTO t VALUES (2, 1, 'b', '[0,1,0,0]')")
wait_entries(t, 0)
knn(t, 8, rows=ALL8)
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
t.sql("START TRANSACTION")
knn(t, 5, extra=" FOR UPDATE", rows=near(BASE, 5))
t.sql("COMMIT")
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
t.uninstall()
tests.append(t)

t = T("ddl", "drop_index_entries", """
DROP INDEX releases the entries of the KNN index: the extension's entry count
returns to what it was before the index was built.""")
t.install(table=False)
t.need_fn("kvec_entry_count")
t.raw(["--let $base = `SELECT vsql_knn_index_test.kvec_entry_count()`"])
t.raw([f"--source {INC}/knn_table.inc"])
entries_delta(t, 8)
t.sql("DROP INDEX idx_v ON t")
entries_delta(t, 0)
t.uninstall()
tests.append(t)

t = T("ddl", "truncate_entries", """
TRUNCATE TABLE releases the entries of the KNN index: the extension's entry
count afterwards counts only rows inserted after it.""")
t.install(table=False)
t.need_fn("kvec_entry_count")
t.raw(["--let $base = `SELECT vsql_knn_index_test.kvec_entry_count()`"])
t.sql("CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON t (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("INSERT INTO t VALUES (1, '[1,0,0,0]'), (2, '[0,1,0,0]')")
entries_delta(t, 2)
t.sql("TRUNCATE TABLE t")
entries_delta(t, 0)
t.sql("INSERT INTO t VALUES (1, '[1,0,0,0]')")
entries_delta(t, 1)
knn(t, rows=["1"])
t.uninstall()
tests.append(t)

t = T("ddl", "temporary_table", """
A KNN index on a temporary table either works like on a base table or is
refused when it is created; it never returns wrong rows.""")
t.install(table=False)
t.sql("CREATE TEMPORARY TABLE t (id INT PRIMARY KEY, grp INT, pay VARCHAR(10), v KVECTOR)")
t.sql("INSERT INTO t VALUES (1, 1, 'a', '[1,0,0,0]'), (2, 1, 'b', '[0,1,0,0]'), "
      "(3, 2, 'c', '[0,0,2,0]'), (4, 2, 'd', '[1,1,0,0]'), "
      "(5, 3, 'e', '[2.5,0,0,0]'), (6, 3, 'f', '[4,0,0,0]'), "
      "(7, 1, 'g', '[0,0,0,3]'), (8, 2, 'h', '[1,0,1.2,0]')")
knn(t, rows=near(BASE))
t.sql("CREATE INDEX idx_v ON t (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)",
      error=("ER_VILLAGESQL_GENERIC_ERROR", "ERROR HY000: InnoDB: Indexing for types with "
             "column storage is supported only with custom index."))
knn(t, rows=near(BASE))
t.sql("DROP TEMPORARY TABLE t")
t.uninstall(drop=None)
tests.append(t)

# ---------------------------------------------------------------- rollback
t = T("rollback", "rollback_btree", """
A transaction sees its own insert, vector update and delete in KNN results, and
a rollback undoes all three, with the index type that stores no primary key
(REF_LOOKUP).""")
t.install(table=False)
btree_table(t)
t.sql("START TRANSACTION")
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]')")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 4")
t.sql("DELETE FROM t WHERE id = 8")
m = with_vec(model(r8=None), 4, "[9,9,9,9]")
m[9] = (1, "i", vec("[1,0,0.1,0]"))
knn(t, 8, rows=near(m, 8))
t.sql("ROLLBACK")
knn(t, 8, rows=ALL8)
t.uninstall()
tests.append(t)

t = T("rollback", "rollback_to_savepoint", """
ROLLBACK TO SAVEPOINT undoes the changes after the savepoint and keeps those
before it; a full rollback then undoes the rest.""")
t.install()
t.sql("START TRANSACTION")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
t.sql("SAVEPOINT s")
t.sql("DELETE FROM t WHERE id = 4")
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]')")
t.sql("UPDATE t SET v = '[0,0,0,0.5]' WHERE id = 7")
t.sql("ROLLBACK TO SAVEPOINT s")
knn(t, 8, rows=near(with_vec(BASE, 1, "[9,9,9,9]"), 8))
t.sql("ROLLBACK")
knn(t, 8, rows=ALL8)
t.uninstall()
tests.append(t)

t = T("rollback", "rollback_knn_dml", """
DELETE and UPDATE ordered by distance, rolled back, leave the table as it was.""")
t.install()
t.sql("START TRANSACTION")
t.sql(f"DELETE FROM t {ORDER} LIMIT 2")
t.sql(f"UPDATE t SET v = '[9,9,9,9]' {ORDER} LIMIT 1")
t.sql("ROLLBACK")
knn(t, 8, rows=ALL8)
t.uninstall()
tests.append(t)


def snapshot_seq(name, header, changes, after, btree=False):
    t = T("rollback", name, header)
    t.raw(["--source include/count_sessions.inc", ""])
    if btree:
        t.install(table=False)
        btree_table(t)
    else:
        t.install()
    t.sql("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    t.raw(["--connect (con1, localhost, root,,)"])
    for c in changes:
        t.sql(c)
    t.raw(["--connection default"])
    knn(t, 8, rows=ALL8)
    t.sql("COMMIT")
    knn(t, 8, rows=near(after, 8))
    t.raw(["--disconnect con1"])
    t.uninstall()
    t.raw(["--source include/wait_until_count_sessions.inc"])
    tests.append(t)


SNAPV = """
A REPEATABLE READ snapshot keeps seeing the table as it was when another session
{what} and commits after the snapshot started: the old index entries stay
usable and the new ones are not seen. A new snapshot sees the change."""
snapshot_seq("snapshot_vector_update", SNAPV.format(what="moves a row away from the query point"),
             ["UPDATE t SET v = '[9,9,9,9]' WHERE id = 4"], with_vec(BASE, 4, "[9,9,9,9]"))
snapshot_seq("snapshot_vector_move_near", SNAPV.format(what="moves a far row next to the query point"),
             ["UPDATE t SET v = '[1,0,0,0.1]' WHERE id = 7"], with_vec(BASE, 7, "[1,0,0,0.1]"))
snapshot_seq("snapshot_delete_reinsert", SNAPV.format(what="deletes a row and inserts it again with another vector"),
             ["DELETE FROM t WHERE id = 4", "INSERT INTO t VALUES (4, 2, 'd', '[9,9,9,9]')"],
             with_vec(BASE, 4, "[9,9,9,9]"))
BT = "\nThe index type stores no primary key (REF_LOOKUP)."
snapshot_seq("snapshot_vector_update_btree", SNAPV.format(what="moves a row away from the query point") + BT,
             ["UPDATE t SET v = '[9,9,9,9]' WHERE id = 4"], with_vec(BASE, 4, "[9,9,9,9]"), btree=True)
snapshot_seq("snapshot_vector_move_near_btree", SNAPV.format(what="moves a far row next to the query point") + BT,
             ["UPDATE t SET v = '[1,0,0,0.1]' WHERE id = 7"], with_vec(BASE, 7, "[1,0,0,0.1]"), btree=True)
snapshot_seq("snapshot_delete_reinsert_btree", SNAPV.format(what="deletes a row and inserts it again with another vector") + BT,
             ["DELETE FROM t WHERE id = 4", "INSERT INTO t VALUES (4, 2, 'd', '[9,9,9,9]')"],
             with_vec(BASE, 4, "[9,9,9,9]"), btree=True)

# ---------------------------------------------------------------- locking
for kind in ("pk", "btree"):
    t = T("locking", f"current_entry_after_vector_update_{kind}", f"""
READ COMMITTED locking KNN reads after a committed vector UPDATE skip the old
index entry of the moved row and find it at its new place, in this session's
transaction and after it ({"the index stores the primary key" if kind == "pk" else
"REF_LOOKUP index type"}).""")
    if kind == "pk":
        t.install()
    else:
        t.install(table=False)
        btree_table(t)
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
    m = with_vec(BASE, 1, "[9,9,9,9]")
    t.sql("START TRANSACTION")
    knn(t, extra=" FOR UPDATE", rows=near(m))
    t.sql("UPDATE t SET v = '[1,0,0,0.1]' WHERE id = 6")
    m = with_vec(m, 6, "[1,0,0,0.1]")
    knn(t, extra=" FOR UPDATE", rows=near(m))
    knn(t, 8, extra=" FOR SHARE", rows=near(m, 8))
    t.sql("COMMIT")
    knn(t, 8, extra=" FOR UPDATE", rows=near(m, 8))
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    t.uninstall()
    tests.append(t)

t = T("locking", "skip_locked_nowait", """
A locking KNN read with SKIP LOCKED returns the nearest rows that are not
locked by another transaction; with NOWAIT it fails at once on a locked one.""")
t.raw(["--source include/count_sessions.inc", ""])
t.install()
t.raw(["--connect (con1, localhost, root,,)"])
t.sql("START TRANSACTION")
t.sql("SELECT id FROM t WHERE id = 1 FOR UPDATE", rows=["1"])
t.raw(["--connection default"])
for iso in ("READ COMMITTED", "REPEATABLE READ"):
    t.sql(f"SET SESSION TRANSACTION ISOLATION LEVEL {iso}")
    t.sql("START TRANSACTION")
    knn(t, extra=" FOR UPDATE SKIP LOCKED", rows=near(model(r1=None)))
    knn(t, extra=" FOR SHARE SKIP LOCKED", rows=near(model(r1=None)))
    knn(t, extra=" FOR UPDATE NOWAIT",
        error=("ER_LOCK_NOWAIT", "ERROR HY000: Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set."))
    t.sql("COMMIT")
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
t.raw(["--connection con1"])
t.sql("COMMIT")
t.raw(["--disconnect con1", "--connection default"])
t.uninstall()
t.raw(["--source include/wait_until_count_sessions.inc"])
tests.append(t)


def lock_wait_then(name, header, change, after, end="COMMIT"):
    t = T("locking", name, header)
    t.raw(["--source include/count_sessions.inc", ""])
    t.install()
    t.raw(["--connect (con1, localhost, root,,)"])
    t.sql("START TRANSACTION")
    t.sql(change)
    t.raw(["--connection default"])
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    t.sql("START TRANSACTION")
    q = f"SELECT id FROM t {ORDER} LIMIT 3 FOR UPDATE"
    t.raw([f"--send {q}"], [f"{q};"])
    t.raw(["--connection con1",
           "--let $wait_condition = SELECT COUNT(*) = 1 FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'",
           "--source include/wait_condition.inc"])
    t.sql(end)
    t.raw(["--connection default", "--reap"], ["id"] + near(after))
    t.sql("COMMIT")
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    t.raw(["--disconnect con1"])
    t.uninstall()
    t.raw(["--source include/wait_until_count_sessions.inc"])
    tests.append(t)


LW = """
A READ COMMITTED locking KNN read waits for the lock on a row another session
{what}; once that session commits, the read returns the nearest rows as they
are now."""
lock_wait_then("lock_wait_then_delete", LW.format(what="has deleted"),
               "DELETE FROM t WHERE id = 1", model(r1=None))
lock_wait_then("lock_wait_then_vector_update", LW.format(what="has moved away from the query point"),
               "UPDATE t SET v = '[9,9,9,9]' WHERE id = 1", with_vec(BASE, 1, "[9,9,9,9]"))
lock_wait_then("lock_wait_then_payload_update", LW.format(what="has updated a non-indexed column of"),
               "UPDATE t SET pay = 'zz' WHERE id = 1", BASE)

LWR = """
A READ COMMITTED locking KNN read waits for the lock on a row another session
{what}; when that session rolls back, the read returns the nearest rows as
they were, with the row at its original place."""
lock_wait_then("lock_wait_then_rollback_delete", LWR.format(what="has deleted"),
               "DELETE FROM t WHERE id = 1", BASE, end="ROLLBACK")
lock_wait_then("lock_wait_then_rollback_vector_update",
               LWR.format(what="has moved away from the query point"),
               "UPDATE t SET v = '[9,9,9,9]' WHERE id = 1", BASE, end="ROLLBACK")

t = T("update", "update_where_knn_subquery", """
An UPDATE or DELETE whose WHERE takes the nearest rows from a KNN subquery on
the same table changes exactly those rows.""")
t.install()
two = near(BASE, 2)
t.sql(f"UPDATE t SET pay = 'x' WHERE id IN (SELECT id FROM (SELECT id FROM t {ORDER} LIMIT 2) AS s)")
t.sql("SELECT id FROM t WHERE pay = 'x' ORDER BY id", rows=sorted(two, key=int))
t.sql(f"UPDATE t SET v = '[9,9,9,9]' WHERE id IN (SELECT id FROM (SELECT id FROM t {ORDER} LIMIT 2) AS s)")
m = BASE
for i in two:
    m = with_vec(m, int(i), "[9,9,9,9]")
knn(t, 6, rows=near(m, 6))
t.sql(f"DELETE FROM t WHERE id IN (SELECT id FROM (SELECT id FROM t {ORDER} LIMIT 1) AS s)")
m = dict(m); m.pop(int(near(m, 1)[0]))
knn(t, 5, rows=near(m, 5))
t.uninstall()
tests.append(t)

# ---------------------------------------------------------------- select_correctness
t = T("select_correctness", "lateral", """
A KNN ordering inside a LATERAL derived table runs once per outer row and
returns that group's nearest row.""")
t.install()
g_table(t)
per_grp = [f"{g}\t{near(BASE, 1, pred=lambda i, r: r[0] == g)[0]}" for g in (1, 2, 3)]
t.sql(f"SELECT g.grp, s.id FROM g, LATERAL (SELECT id FROM t WHERE t.grp = g.grp {ORDER} LIMIT 1) AS s ORDER BY g.grp",
      header="grp\tid", rows=per_grp)
t.sql(f"SELECT g.grp, s.id FROM g, LATERAL (SELECT id FROM t IGNORE INDEX (k_grp) WHERE t.grp = g.grp {ORDER} LIMIT 1) AS s ORDER BY g.grp",
      header="grp\tid", rows=per_grp)
t.sql("DROP TABLE g")
t.uninstall()
tests.append(t)

t = T("select_correctness", "union", """
Two KNN queries with different query points combined with UNION ALL and UNION
each return their own nearest rows.""")
t.install()
q7 = (0, 0, 0, 3)
both = near(BASE, 2) + near(BASE, 1, q=q7)
U = (f"(SELECT id FROM t {ORDER} LIMIT 2) {{op}} "
     f"(SELECT id FROM t ORDER BY {D}(v, '[0,0,0,3]') LIMIT 1)")
t.sql(U.format(op="UNION ALL"), rows=both, sorted_=True)
t.sql(U.format(op="UNION"), rows=both, sorted_=True)
t.sql(f"(SELECT id FROM t {ORDER} LIMIT 2) UNION (SELECT id FROM t {ORDER} LIMIT 3)",
      rows=near(BASE, 3), sorted_=True)
t.uninstall()
tests.append(t)

t = T("select_correctness", "prepared_after_dml", """
A prepared KNN query executed again after INSERT, UPDATE and DELETE returns the
nearest rows of the table as it is now.""")
t.install()
t.sql(f'PREPARE ps FROM "SELECT id FROM t {ORDER} LIMIT 2"')
t.sql("EXECUTE ps", rows=near(BASE, 2))
m = with_vec(BASE, 1, "[9,9,9,9]")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
t.sql("EXECUTE ps", rows=near(m, 2))
m = dict(m); m.pop(4)
t.sql("DELETE FROM t WHERE id = 4")
t.sql("EXECUTE ps", rows=near(m, 2))
m[9] = (1, "i", vec("[1,0,0.1,0]"))
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]')")
t.sql("EXECUTE ps", rows=near(m, 2))
t.sql("DEALLOCATE PREPARE ps")
t.uninstall()
tests.append(t)

t = T("select_correctness", "procedure_cursor", """
A stored procedure that fetches a KNN query through a cursor gets the nearest
rows in order, on each call and after the table changes.""")
t.install()
t.raw(["--disable_query_log", "--delimiter |",
       "CREATE PROCEDURE knn_ids() BEGIN",
       "  DECLARE done INT DEFAULT 0;",
       "  DECLARE x INT;",
       "  DECLARE s VARCHAR(100) DEFAULT NULL;",
       f"  DECLARE c CURSOR FOR SELECT id FROM t {ORDER} LIMIT 3;",
       "  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;",
       "  OPEN c;",
       "  l: LOOP",
       "    FETCH c INTO x;",
       "    IF done THEN LEAVE l; END IF;",
       "    SET s = CONCAT_WS(',', s, x);",
       "  END LOOP;",
       "  CLOSE c;",
       "  SELECT s AS ids;",
       "END|", "--delimiter ;", "--enable_query_log"])
t.sql("CALL knn_ids()", header="ids", rows=[",".join(near(BASE))])
t.sql("CALL knn_ids()", header="ids", rows=[",".join(near(BASE))])
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
t.sql("CALL knn_ids()", header="ids", rows=[",".join(near(with_vec(BASE, 1, "[9,9,9,9]")))])
t.sql("DROP PROCEDURE knn_ids")
t.uninstall()
tests.append(t)


t = T("select_correctness", "self_insert_select", """
INSERT ... SELECT that copies the nearest rows of a table into the same table
copies exactly those rows; the copies are found by KNN queries afterwards.""")
t.install()
two = near(BASE, 2)
t.sql(f"INSERT INTO t SELECT id + 100, grp, pay, v FROM t {ORDER} LIMIT 2")
t.sql("SELECT id FROM t WHERE id > 100 ORDER BY id", rows=[str(int(i) + 100) for i in sorted(two, key=int)])
knn(t, 4, rows=sorted(two + [str(int(i) + 100) for i in two]), sorted_=True)
t.sql("SELECT COUNT(*) FROM t", header="COUNT(*)", rows=["10"])
t.uninstall()
tests.append(t)

t = T("select_correctness", "order_expressions", """
ORDER BY forms that are not a plain distance call (an expression of it, the
operands swapped, a select-list position, the distance to another column)
return the right rows, whether or not they use the KNN scan.""")
t.install()
t.sql(f"SELECT id FROM t ORDER BY {D}(v, '[1,0,0,0]') + 0 LIMIT 3", rows=near(BASE))
t.sql(f"SELECT id FROM t ORDER BY {D}(v, '[1,0,0,0]') * 2 LIMIT 3", rows=near(BASE))
t.sql(f"SELECT id FROM t ORDER BY -{D}(v, '[1,0,0,0]') DESC LIMIT 3", rows=near(BASE))
t.sql(f"SELECT id FROM t ORDER BY {D}('[1,0,0,0]', v) LIMIT 3", rows=near(BASE))
t.sql(f"SELECT id, {D}(v, '[1,0,0,0]') FROM t ORDER BY 2 LIMIT 3",
      header=f"id\t{D}(v, '[1,0,0,0]')",
      rows=["1\t0", "4\t1", "8\t1.2000000476837158"])
t.sql(f"SELECT a.id FROM t AS a JOIN t AS b ON b.id = 1 ORDER BY {D}(a.v, b.v) LIMIT 3",
      rows=near(BASE))
t.uninstall()
tests.append(t)

def big_rows(n, q):
    rows = {i: (0, "", (i % 17, (i * 7) % 13, (i * 3) % 11, 0)) for i in range(1, n + 1)}
    return rows


def sq_dists(rows, q, k):
    d = sorted(round(math.dist(r[2], q) ** 2) for r in rows.values())
    return ",".join(str(x) for x in d[:k])


t = T("select_correctness", "larger_table", """
On a 300-row table, before and after deleting and updating many rows, a KNN
query returns rows at the same distances as the query with the index ignored
(squared distances, which are integers here, so ties compare equal).""")
t.install(table=False)
t.sql("CREATE TABLE b (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON b (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
GEN = ("INSERT INTO b WITH RECURSIVE s (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 300) "
       "SELECT i, {v} FROM s")
VTXT = "CONCAT('[', i % 17, ',', (i * 7) % 13, ',', (i * 3) % 11, ',0]')"
t.echo("A computed string is not implicitly cast to the custom type")
t.sql(GEN.format(v=VTXT),
      error=("ER_VILLAGESQL_GENERIC_ERROR",
             "ERROR HY000: Incorrect kvector value: cannot implicitly cast string "
             "expression. Use explicit conversion for column 'v' at row 1"))
t.sql(GEN.format(v=f"vsql_knn_index_test.KVECTOR::from_string({VTXT})"))
qb = (5, 5, 5, 0)
B = f"{D}(v, '[5,5,5,0]')"
def big_check(t, rows, k):
    exp = sq_dists(rows, qb, k)
    for hint in ("", " IGNORE INDEX (idx_v)"):
        t.sql(f"SELECT GROUP_CONCAT(ROUND(d * d) ORDER BY d) AS sq FROM "
              f"(SELECT {B} AS d FROM b{hint} ORDER BY d LIMIT {k}) s",
              header="sq", rows=[exp])
rows = big_rows(300, qb)
big_check(t, rows, 10)
t.sql("DELETE FROM b WHERE id % 3 = 0")
rows = {i: r for i, r in rows.items() if i % 3}
t.sql("UPDATE b SET v = '[5,5,5,1]' WHERE id % 50 = 1")
rows = {i: (r if i % 50 != 1 else (0, "", (5, 5, 5, 1))) for i, r in rows.items()}
big_check(t, rows, 10)
t.sql("SELECT COUNT(*) FROM b", header="COUNT(*)", rows=[str(len(rows))])
t.uninstall("b")
tests.append(t)


# ================================================================ round 3
INS_ROWS = ("INSERT INTO {tbl} VALUES (1, 1, 'a', '[1,0,0,0]'), (2, 1, 'b', '[0,1,0,0]'), "
            "(3, 2, 'c', '[0,0,2,0]'), (4, 2, 'd', '[1,1,0,0]'), "
            "(5, 3, 'e', '[2.5,0,0,0]'), (6, 3, 'f', '[4,0,0,0]'), "
            "(7, 1, 'g', '[0,0,0,3]'), (8, 2, 'h', '[1,0,1.2,0]')")

t = T("rollback", "statement_rollback", """
A statement that fails part way is rolled back on its own: the index entries
of the rows it had already inserted or updated are undone, and the
transaction's earlier statements are kept.""")
t.install()
t.sql("START TRANSACTION")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 6")
m = with_vec(BASE, 6, "[9,9,9,9]")
t.echo("A multi-row INSERT that fails on its last row")
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]'), (10, 1, 'j', '[1,0,0,0.2]'), (1, 1, 'a', '[1,0,0,0]')",
      error=("ER_DUP_ENTRY", "ERROR 23000: Duplicate entry '1' for key 't.PRIMARY'"))
knn(t, 8, rows=near(m, 8))
t.echo("A multi-row UPDATE of the vector that fails on its fifth row")
t.sql("UPDATE t SET v = '[9,9,9,8]', pay = IF(id = 5, 'xxxxxxxxxxxxxxxx', pay) ORDER BY id",
      error=("ER_DATA_TOO_LONG", "ERROR 22001: Data too long for column 'pay' at row 5"))
knn(t, 8, rows=near(m, 8))
t.sql("COMMIT")
knn(t, 8, rows=near(m, 8))
t.sql("SELECT COUNT(*) FROM t", header="COUNT(*)", rows=["8"])
t.uninstall()
tests.append(t)

t = T("update", "insert_ignore", """
INSERT IGNORE skips the rows with a duplicate key and inserts the others, which
KNN queries find.""")
t.install()
t.sql("INSERT IGNORE INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]'), (1, 1, 'a', '[9,9,9,9]'), (10, 1, 'j', '[1,0,0,0.2]')",
      warnings=["Warning\t1062\tDuplicate entry '1' for key 't.PRIMARY'"])
m = dict(BASE)
m[9] = (1, "i", vec("[1,0,0.1,0]"))
m[10] = (1, "j", vec("[1,0,0,0.2]"))
knn(t, 10, rows=near(m, 10))
t.uninstall()
tests.append(t)

t = T("rollback", "large_rollback", """
A transaction that inserts 200 rows sees them in KNN results, and its rollback
removes all their index entries.""")
t.install()
t.need_fn("kvec_entry_count")
t.raw(["--let $base = `SELECT vsql_knn_index_test.kvec_entry_count()`"])
t.sql("START TRANSACTION")
t.sql("INSERT INTO t WITH RECURSIVE s (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 200) "
      "SELECT 1000 + i, 1, 'z', vsql_knn_index_test.KVECTOR::from_string(CONCAT('[1,0,0,', i / 1000, ']')) FROM s")
knn(t, 3, rows=["1", "1001", "1002"])
entries_delta(t, 200)
t.sql("ROLLBACK")
knn(t, 8, rows=ALL8)
entries_delta(t, 0)
t.uninstall()
tests.append(t)

t = T("rollback", "xa", """
Changes of a prepared XA transaction are invisible to KNN reads of other
sessions; XA ROLLBACK undoes them and XA COMMIT makes them visible.""")
t.raw(["--source include/count_sessions.inc", ""])
t.install()
t.raw(["--connect (con1, localhost, root,,)"])
for xid, end, after in (("'x1'", "ROLLBACK", BASE),
                        ("'x2'", "COMMIT", None)):
    t.raw(["--connection con1"])
    t.sql(f"XA START {xid}")
    t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]')")
    t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 4")
    t.sql(f"XA END {xid}")
    t.sql(f"XA PREPARE {xid}")
    t.raw(["--connection default"])
    knn(t, 8, rows=ALL8)
    t.raw(["--connection con1"])
    t.sql(f"XA {end} {xid}")
    t.raw(["--connection default"])
    if after is None:
        after = with_vec(BASE, 4, "[9,9,9,9]")
        after[9] = (1, "i", vec("[1,0,0.1,0]"))
    knn(t, 9, rows=near(after, 9))
t.raw(["--disconnect con1", "--connection default"])
t.uninstall()
t.raw(["--source include/wait_until_count_sessions.inc"])
tests.append(t)

t = T("rollback", "deadlock_rollback", """
The transaction rolled back to break a deadlock has its vector update undone;
the other transaction's changes commit.""")
t.raw(["--source include/count_sessions.inc", ""])
t.install()
t.raw(["--connect (con1, localhost, root,,)"])
t.sql("START TRANSACTION")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
t.raw(["--connection default"])
t.sql("START TRANSACTION")
t.comment("More undo than con1, so con1 is the one rolled back.")
t.sql("INSERT INTO t VALUES (20, 1, 'p', '[7,7,7,7]'), (21, 1, 'q', '[7,7,7,8]'), (22, 1, 'r', '[7,7,8,7]')")
t.sql("UPDATE t SET v = '[8,8,8,8]' WHERE id = 4")
t.raw(["--connection con1"])
q = "UPDATE t SET pay = 'x' WHERE id = 4"
t.raw([f"--send {q}"], [f"{q};"])
t.raw(["--connection default",
       "--let $wait_condition = SELECT COUNT(*) = 1 FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'",
       "--source include/wait_condition.inc"])
t.sql("UPDATE t SET pay = 'y' WHERE id = 1")
t.raw(["--connection con1", "--error ER_LOCK_DEADLOCK", "--reap"],
      ["ERROR 40001: Deadlock found when trying to get lock; try restarting transaction"])
t.raw(["--connection default"])
t.sql("COMMIT")
m = with_vec(BASE, 4, "[8,8,8,8]")
for i, v in ((20, "[7,7,7,7]"), (21, "[7,7,7,8]"), (22, "[7,7,8,7]")):
    m[i] = (1, "x", vec(v))
knn(t, 7, rows=near(m, 7))
t.sql("SELECT id, pay FROM t WHERE id IN (1, 4) ORDER BY id", header="id\tpay", rows=["1\ty", "4\td"])
t.raw(["--disconnect con1"])
t.uninstall()
t.raw(["--source include/wait_until_count_sessions.inc"])
tests.append(t)

t = T("update", "triggers", """
A vector set by a BEFORE INSERT trigger is the one indexed, and a vector copied
by an AFTER DELETE trigger into another table with a KNN index is found
there.""")
t.install()
t.sql("CREATE TABLE arch (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON arch (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET NEW.v = '[1,0,0,0.3]'")
t.sql("CREATE TRIGGER ad AFTER DELETE ON t FOR EACH ROW INSERT INTO arch VALUES (OLD.id, OLD.v)")
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[9,9,9,9]')")
m = dict(BASE)
m[9] = (1, "i", vec("[1,0,0,0.3]"))
knn(t, 3, rows=near(m, 3))
t.sql("DELETE FROM t WHERE id IN (4, 6)")
knn(t, 3, table="arch", rows=near({4: BASE[4], 6: BASE[6]}, 3))
t.sql("DROP TRIGGER bi")
t.sql("DROP TRIGGER ad")
t.sql("DROP TABLE arch")
t.uninstall()
tests.append(t)

t = T("delete", "foreign_key_cascade", """
Rows of a child table deleted or updated by a foreign key cascade (InnoDB's own
row operations, not the SQL layer's) leave KNN results right, and a rolled
back cascade restores them.""")
t.install(table=False)
t.sql("CREATE TABLE p (grp INT PRIMARY KEY)")
t.sql("INSERT INTO p VALUES (1), (2), (3)")
t.sql("CREATE TABLE t (id INT PRIMARY KEY, grp INT, pay VARCHAR(10), v KVECTOR, "
      "FOREIGN KEY (grp) REFERENCES p (grp) ON DELETE CASCADE ON UPDATE CASCADE)")
t.sql("CREATE INDEX idx_v ON t (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql(INS_ROWS.format(tbl="t"))
t.sql("START TRANSACTION")
t.sql("DELETE FROM p WHERE grp = 3")
knn(t, 8, rows=near({i: r for i, r in BASE.items() if r[0] != 3}, 8))
t.sql("ROLLBACK")
knn(t, 8, rows=ALL8)
m = {i: r for i, r in BASE.items() if r[0] != 2}
t.sql("DELETE FROM p WHERE grp = 2")
knn(t, 8, rows=near(m, 8))
t.sql("UPDATE p SET grp = 5 WHERE grp = 1")
knn(t, 8, rows=near(m, 8))
knn(t, 8, where="grp = 5", rows=near(m, 8, pred=lambda i, r: r[0] == 1))
t.sql("DROP TABLE t")
t.uninstall("p")
tests.append(t)

t = T("update", "two_vector_columns", """
A table with a KNN index on each of two vector columns: changing one column
moves the row only in that column's KNN results.""")
t.install(table=False)
t.sql("CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR, w KVECTOR)")
t.sql("CREATE INDEX idx_v ON t (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
t.sql("CREATE INDEX idx_w ON t (w) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
W = {i: BASE[9 - i][2] for i in BASE}
vals = ", ".join(f"({i}, '[{','.join(f'{x:g}' for x in BASE[i][2])}]', "
                 f"'[{','.join(f'{x:g}' for x in W[i])}]')" for i in sorted(BASE))
t.sql(f"INSERT INTO t VALUES {vals}")
def two(t, mv, mw, k=8):
    knn(t, k, rows=near(mv, k))
    t.sql(f"SELECT id FROM t ORDER BY {D}(w, '[1,0,0,0]') LIMIT {k}",
          rows=near(mw, k))
mv = dict(BASE)
mw = {i: (0, "", W[i]) for i in W}
two(t, mv, mw)
t.sql("UPDATE t SET w = '[9,9,9,9]' WHERE id = 8")
mw = with_vec(mw, 8, "[9,9,9,9]")
two(t, mv, mw)
t.sql("START TRANSACTION")
t.sql("UPDATE t SET v = '[9,9,9,9]', w = '[1,0,0,0.1]' WHERE id = 1")
two(t, with_vec(mv, 1, "[9,9,9,9]"), with_vec(mw, 1, "[1,0,0,0.1]"))
t.sql("ROLLBACK")
two(t, mv, mw)
t.sql("DELETE FROM t WHERE id = 4")
mv.pop(4); mw.pop(4)
two(t, mv, mw, 7)
t.uninstall()
tests.append(t)

t = T("ddl", "instant_ddl", """
ALTER TABLE operations that do not rebuild the table (instant ADD and DROP
COLUMN, RENAME COLUMN, an in-place secondary index) leave the KNN index
working, also for rows written after them.""")
t.install()
t.sql("ALTER TABLE t ADD COLUMN extra INT, ALGORITHM=INSTANT")
knn(t, rows=near(BASE))
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0.1,0]', 5)")
m = dict(BASE)
m[9] = (1, "i", vec("[1,0,0.1,0]"))
knn(t, rows=near(m))
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
m = with_vec(m, 1, "[9,9,9,9]")
knn(t, rows=near(m))
t.sql("ALTER TABLE t RENAME COLUMN pay TO pay2, ALGORITHM=INSTANT")
knn(t, col="id, pay2", header="id\tpay2", rows=[f"{i}\t{m[int(i)][1]}" for i in near(m)])
t.sql("ALTER TABLE t ADD INDEX k_pay (pay2), ALGORITHM=INPLACE")
knn(t, rows=near(m))
t.sql("ALTER TABLE t DROP COLUMN extra, ALGORITHM=INSTANT")
knn(t, rows=near(m))
t.sql("DELETE FROM t WHERE id = 9")
m.pop(9)
knn(t, 8, rows=near(m, 8))
t.uninstall()
tests.append(t)

t = T("ddl", "rename_drop_uninstall", """
A table renamed and then dropped leaves nothing behind that depends on the
extension, so the extension can be uninstalled.""")
t.install()
t.sql("RENAME TABLE t TO t2")
t.sql("DROP TABLE t2")
t.uninstall(drop=None)
tests.append(t)

t = T("ddl", "alter_rename_table", """
After ALTER TABLE ... RENAME TO, also to another database, the KNN index keeps
working under the new name, and dropping the table frees the extension.""")
t.install()
t.sql("ALTER TABLE t RENAME TO t2")
knn(t, table="t2", rows=near(BASE))
t.sql("CREATE DATABASE d2")
t.sql("ALTER TABLE t2 RENAME TO d2.t3")
knn(t, table="d2.t3", rows=near(BASE))
t.sql("DROP DATABASE d2")
t.uninstall(drop=None)
tests.append(t)

def phantom(name, header, setup, read):
    t = T("locking", name, header)
    t.raw(["--source include/count_sessions.inc", ""])
    t.install()
    for st in setup:
        t.sql(st)
    t.sql("START TRANSACTION")
    knn(t, extra=read, rows=near(BASE))
    t.raw(["--connect (con1, localhost, root,,)"])
    t.sql("SET SESSION innodb_lock_wait_timeout = 1")
    t.comment("The insert must wait for the reader (it would change the reader's\n"
              "nearest rows), so it times out.")
    t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0,0.1]')",
          error=("ER_LOCK_WAIT_TIMEOUT", "ERROR HY000: Lock wait timeout exceeded; try restarting transaction"))
    t.raw(["--disconnect con1", "--connection default"])
    knn(t, extra=read, rows=near(BASE))
    t.sql("COMMIT")
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    t.uninstall()
    t.raw(["--source include/wait_until_count_sessions.inc"])
    tests.append(t)


phantom("phantom_serializable", """
A SERIALIZABLE transaction's KNN read is repeatable: another session cannot
insert a row that would become one of the nearest rows while the transaction
is open, as next-key locks prevent for an ordinary index.""",
        ["SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"], "")
phantom("phantom_for_update", """
A REPEATABLE READ locking KNN read (FOR UPDATE) is repeatable within the
transaction: another session cannot insert a row that would become one of the
nearest rows, as next-key locks prevent for an ordinary index.""",
        [], " FOR UPDATE")
phantom("phantom_for_share", """
A REPEATABLE READ locking KNN read (FOR SHARE) is repeatable within the
transaction: another session cannot insert a row that would become one of the
nearest rows.""",
        [], " FOR SHARE")
phantom("phantom_serializable_for_update", """
A SERIALIZABLE transaction's locking KNN read (FOR UPDATE) is repeatable:
another session cannot insert a row that would become one of the nearest
rows while the transaction is open.""",
        ["SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"], " FOR UPDATE")


t = T("select_correctness", "churn", """
On a 300-row table, after rounds of vector updates, deletes, reinserts and a
rolled back batch, KNN queries from three query points return rows at the same
distances as the queries with the index ignored (squared distances, integers
here).""")
t.install(table=False)
t.sql("CREATE TABLE b (id INT PRIMARY KEY, v KVECTOR)")
t.sql("CREATE INDEX idx_v ON b (v) USING EXTENDED(vsql_knn_index_test.kvec_l2)")
FS = "vsql_knn_index_test.KVECTOR::from_string"
t.sql("INSERT INTO b WITH RECURSIVE s (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 300) "
      f"SELECT i, {FS}(CONCAT('[', i % 17, ',', (i * 7) % 13, ',', (i * 3) % 11, ',0]')) FROM s")
rows = {i: (0, "", (i % 17, (i * 7) % 13, (i * 3) % 11, 0)) for i in range(1, 301)}
QS = [(5, 5, 5, 0), (0, 0, 0, 0), (16, 12, 10, 0)]
def churn_check(t, rows):
    for q in QS:
        qt = "[" + ",".join(str(x) for x in q) + "]"
        exp = sq_dists(rows, q, 12)
        for hint in ("", " IGNORE INDEX (idx_v)"):
            t.sql(f"SELECT GROUP_CONCAT(ROUND(d * d) ORDER BY d) AS sq FROM "
                  f"(SELECT {D}(v, '{qt}') AS d FROM b{hint} ORDER BY d LIMIT 12) s",
                  header="sq", rows=[exp])
for rnd in range(3):
    a, b_, c = 7 + rnd, 11 + rnd, 3 + rnd
    t.echo(f"Round {rnd + 1}")
    t.sql(f"UPDATE b SET v = {FS}(CONCAT('[', (id * {c}) % 17, ',', id % 13, ',', (id + {rnd}) % 11, ',1]')) "
          f"WHERE id % {a} = {rnd}")
    rows = {i: (r if i % a != rnd else (0, "", ((i * c) % 17, i % 13, (i + rnd) % 11, 1)))
            for i, r in rows.items()}
    t.sql(f"DELETE FROM b WHERE id % {b_} = 0")
    rows = {i: r for i, r in rows.items() if i % b_ != 0}
    t.sql(f"INSERT INTO b WITH RECURSIVE s (i) AS (SELECT {b_} UNION ALL SELECT i + {b_ * 2} FROM s WHERE i < 280) "
          f"SELECT i, {FS}(CONCAT('[', i % 5, ',', i % 7, ',', i % 3, ',2]')) FROM s")
    # The recursive CTE also emits the first value at or past 280.
    k = b_
    rows[k] = (0, "", (k % 5, k % 7, k % 3, 2))
    while k < 280:
        k += b_ * 2
        rows[k] = (0, "", (k % 5, k % 7, k % 3, 2))
    t.sql("START TRANSACTION")
    t.sql(f"UPDATE b SET v = '[5,5,5,0]' WHERE id % 13 = {rnd}")
    t.sql("DELETE FROM b WHERE id % 17 = 1")
    t.sql("ROLLBACK")
    churn_check(t, rows)
t.sql("SELECT COUNT(*) FROM b", header="COUNT(*)", rows=[str(len(rows))])
t.uninstall("b")
tests.append(t)

def ran(t, query, expected):
    """Runs query and reports whether the KNN scan ran (EXPLAIN aside)."""
    t.raw(["--disable_query_log", "--disable_result_log",
           "SELECT vsql_knn_index_test.kvec_reset_counters();",
           "--enable_result_log", "--enable_query_log"])
    t.sql(query)
    t.sql("SELECT vsql_knn_index_test.kvec_scan_begin_count() > 0 AS knn_scan_ran",
          header="knn_scan_ran", rows=[str(expected)])


t = T("select_plan", "dml", """
DELETE and UPDATE ordered by distance with a LIMIT run the KNN scan, like a
SELECT: not with DESC, without a LIMIT, or when another index serves the
WHERE. The rows left are checked after each statement.""")
t.install()
ran(t, f"DELETE FROM t {ORDER} LIMIT 1", 1)
m = model(r1=None)
ran(t, f"UPDATE t SET pay = 'x' {ORDER} LIMIT 2", 1)
t.sql("SELECT id FROM t WHERE pay = 'x' ORDER BY id", rows=sorted(near(m, 2), key=int))
ran(t, f"UPDATE t SET pay = 'y' {ORDER} DESC LIMIT 1", 0)
t.sql("SELECT id FROM t WHERE pay = 'y'", rows=[near(m, 7)[-1]])
ran(t, f"DELETE FROM t WHERE grp = 2 {ORDER} LIMIT 1", 0)
m.pop(int(near(m, 1, pred=lambda i, r: r[0] == 2)[0]))
ran(t, f"UPDATE t SET pay = 'z' WHERE pay <> 'x' {ORDER}", 0)
knn(t, 8, rows=near(m, 8))
t.uninstall()
tests.append(t)

t = T("select_plan", "dml_explain", """
EXPLAIN of a DELETE or UPDATE ordered by distance with a LIMIT shows the KNN
scan it runs, as for a SELECT.""")
t.install()
t.plan(f"DELETE FROM t {ORDER} LIMIT 1", 1, 1)
t.plan(f"UPDATE t SET pay = 'x' {ORDER} LIMIT 2", 1, 1)
t.uninstall()
tests.append(t)

# ---------------------------------------------------------------- locking, HNSW
# The same locking reads with vsql-vector's HNSW index, so the extension's
# validate decides whether an entry is current. vsql_vector.veb must be in the
# server's veb directory.
HD = "vsql_vector.l2_distance"


def hnsw_install(t):
    t.sql("INSTALL EXTENSION vsql_vector")
    t.sql("CREATE TABLE t (id INT PRIMARY KEY, grp INT, pay VARCHAR(10), "
          "v SVECTOR(4), KEY k_grp (grp))")
    t.sql("CREATE INDEX idx_v ON t (v) USING EXTENDED(vsql_vector.hnsw)")
    t.sql("INSERT INTO t VALUES (1, 1, 'a', '[1,0,0,0]'), (2, 1, 'b', '[0,1,0,0]'), "
          "(3, 2, 'c', '[0,0,2,0]'), (4, 2, 'd', '[1,1,0,0]'), "
          "(5, 3, 'e', '[2.5,0,0,0]'), (6, 3, 'f', '[4,0,0,0]'), "
          "(7, 1, 'g', '[0,0,0,3]'), (8, 2, 'h', '[1,0,1.2,0]')")


def hnsw_uninstall(t):
    t.sql("DROP TABLE t")
    t.sql("UNINSTALL EXTENSION vsql_vector")


def hknn(t, k=3, extra="", rows=None):
    t.sql(f"SELECT id FROM t ORDER BY {HD}(v, '[1,0,0,0]') LIMIT {k}{extra}",
          rows=rows)


t = T("locking", "hnsw_current_entry_after_change", """
READ COMMITTED locking KNN reads with an HNSW index (vsql-vector) skip the old
entry of a row whose vector was updated and the entry of a deleted row, and
find the updated row at its new place, in this session's transaction and
after it.""")
hnsw_install(t)
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
m = with_vec(BASE, 1, "[9,9,9,9]")
hknn(t, extra=" FOR UPDATE", rows=near(m))
t.sql("DELETE FROM t WHERE id = 4")
m = dict(m); m.pop(4)
hknn(t, extra=" FOR UPDATE", rows=near(m))
t.sql("START TRANSACTION")
t.sql("UPDATE t SET v = '[1,0,0,0.1]' WHERE id = 6")
m = with_vec(m, 6, "[1,0,0,0.1]")
hknn(t, extra=" FOR UPDATE", rows=near(m))
t.sql("DELETE FROM t WHERE id = 8")
m = dict(m); m.pop(8)
hknn(t, extra=" FOR UPDATE", rows=near(m))
hknn(t, 8, extra=" FOR SHARE", rows=near(m, 8))
t.sql("COMMIT")
hknn(t, 8, extra=" FOR UPDATE", rows=near(m, 8))
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
hnsw_uninstall(t)
tests.append(t)


def hnsw_lock_wait_then(name, header, change, after):
    t = T("locking", name, header)
    t.raw(["--source include/count_sessions.inc", ""])
    hnsw_install(t)
    t.raw(["--connect (con1, localhost, root,,)"])
    t.sql("START TRANSACTION")
    t.sql(change)
    t.raw(["--connection default"])
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    t.sql("START TRANSACTION")
    q = f"SELECT id FROM t ORDER BY {HD}(v, '[1,0,0,0]') LIMIT 3 FOR UPDATE"
    t.raw([f"--send {q}"], [f"{q};"])
    t.raw(["--connection con1",
           "--let $wait_condition = SELECT COUNT(*) = 1 FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'",
           "--source include/wait_condition.inc"])
    t.sql("COMMIT")
    t.raw(["--connection default", "--reap"], ["id"] + near(after))
    t.sql("COMMIT")
    t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    t.raw(["--disconnect con1"])
    hnsw_uninstall(t)
    t.raw(["--source include/wait_until_count_sessions.inc"])
    tests.append(t)


t = T("rollback", "hnsw_crash_recovery", """
Changes of a transaction that is still open when the server is killed are
rolled back at restart, in the HNSW index (vsql-vector) too: an insert, a
delete and a vector update. KNN reads afterwards return the table as it was
before the transaction.""")
hnsw_install(t)
t.raw(["--connect (con1, localhost, root,,)"])
t.sql("START TRANSACTION")
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0,0.1]')")
t.sql("DELETE FROM t WHERE id = 4")
t.sql("UPDATE t SET v = '[9,9,9,9]' WHERE id = 1")
t.raw(["--connection default", "--source include/kill_and_restart_mysqld.inc",
       "--disconnect con1",
       "--let $wait_timeout = 60",
       "--let $wait_condition = SELECT COUNT(*) = 0 FROM information_schema.innodb_trx",
       "--source include/wait_condition.inc"],
      ["# Kill and restart"])
t.sql("SELECT COUNT(*) AS n FROM t", rows=["8"], header="n")
hknn(t, 8, rows=ALL8)
hknn(t, rows=near(BASE))
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
t.sql("START TRANSACTION")
hknn(t, extra=" FOR UPDATE", rows=near(BASE))
t.sql("COMMIT")
t.sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
t.echo("The index takes new changes after recovery")
t.sql("INSERT INTO t VALUES (9, 1, 'i', '[1,0,0,0.1]')")
t.sql("DELETE FROM t WHERE id = 4")
hknn(t, rows=near(with_vec(model(r4=None), 1, "[1,0,0,0]") | {9: (1, "i", (1, 0, 0, 0.1))}))
hnsw_uninstall(t)
tests.append(t)


HLW = """
A READ COMMITTED locking KNN read with an HNSW index (vsql-vector) waits for
the lock on a row another session {what}; once that session commits, the read
returns the nearest rows as they are now."""
hnsw_lock_wait_then("hnsw_lock_wait_then_delete", HLW.format(what="has deleted"),
                    "DELETE FROM t WHERE id = 1", model(r1=None))
hnsw_lock_wait_then("hnsw_lock_wait_then_vector_update",
                    HLW.format(what="has moved away from the query point"),
                    "UPDATE t SET v = '[9,9,9,9]' WHERE id = 1",
                    with_vec(BASE, 1, "[9,9,9,9]"))


t = T("delete", "hnsw_many_deleted", """
With an HNSW index (vsql-vector), a KNN query with LIMIT 10 returns the 10
nearest rows also when most rows are deleted and their entries are still in
the index. Rows lie on a line; the rows between the nearest one and the rest
are deleted. Purge is stopped (debug build) so it can't remove the deleted
entries first; once it runs, the query is checked again.""")
t.raw(["--source include/have_debug.inc", ""])
t.sql("INSTALL EXTENSION vsql_vector")
t.sql("CREATE TABLE t (id INT PRIMARY KEY, v SVECTOR(4))")
t.sql("CREATE INDEX idx_v ON t (v) USING EXTENDED(vsql_vector.hnsw)")
t.sql("INSERT INTO t VALUES " + ",".join(f"({i},'[{i},0,0,0]')" for i in range(1, 101)))
t.sql("SET GLOBAL innodb_purge_stop_now = ON")
t.sql("DELETE FROM t WHERE id BETWEEN 2 AND 90")
NEAR10 = ["1"] + [str(i) for i in range(91, 100)]
for hint in ("", " IGNORE INDEX (idx_v)"):
    t.sql(f"SELECT id FROM t{hint} ORDER BY {HD}(v, '[0,0,0,0]') LIMIT 10",
          rows=NEAR10)
t.sql("SET GLOBAL innodb_purge_run_now = ON")
t.raw(["--source include/wait_innodb_all_purged.inc"])
t.echo("After purge")
t.sql(f"SELECT id FROM t ORDER BY {HD}(v, '[0,0,0,0]') LIMIT 10", rows=NEAR10)
t.sql("DROP TABLE t")
t.sql("UNINSTALL EXTENSION vsql_vector")
tests.append(t)

for t in tests:
    t.write()
print(len(tests), "tests")
