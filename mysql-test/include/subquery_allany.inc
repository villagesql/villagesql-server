# TODO:
# Add support for quantified comparison predicate in join condition
# Add support for quantified comparison predicate in HAVING clause
# Add support for quantified comparison predicate in QUALIFY clause (?)
# Add support for quantified comparison predicate with aggregated subquery

--echo
--echo # quantified comparison predicate in SELECT list
--echo # non-nullable columns
--echo # no dependent subquery predicate
--echo

let $query=
SELECT v <>ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >=ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <=ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >=ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <=ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ALL (SELECT v FROM t0row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ALL (SELECT v FROM t1row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ALL (SELECT v FROM t2row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ALL (SELECT v FROM t4row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <>ANY (SELECT v FROM t0row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <>ANY (SELECT v FROM t1row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <>ANY (SELECT v FROM t2row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <>ANY (SELECT v FROM t4row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo
--echo # quantified comparison predicate in SELECT list
--echo # nullable outer column
--echo # no dependent subquery predicate
--echo

let $query=
SELECT vn <>ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn >ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn >=ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <=ALL (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn >ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn >=ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <=ANY (SELECT val FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ALL (SELECT v FROM t0row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ALL (SELECT v FROM t1row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ALL (SELECT v FROM t2row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ALL (SELECT v FROM t4row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <>ANY (SELECT v FROM t0row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <>ANY (SELECT v FROM t1row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <>ANY (SELECT v FROM t2row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <>ANY (SELECT v FROM t4row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo
--echo # quantified comparison predicate in SELECT list
--echo # nullable column in subquery
--echo # no dependent subquery predicate
--echo

let $query=
SELECT v <>ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >=ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <=ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >=ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <=ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ALL (SELECT n FROM t1row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ALL (SELECT vn FROM t2row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ALL (SELECT vn FROM t3row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v =ALL (SELECT vn FROM t4row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <>ANY (SELECT n FROM t1row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <>ANY (SELECT vn FROM t2row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <>ANY (SELECT vn FROM t3row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v <>ANY (SELECT vn FROM t4row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo
--echo # quantified comparison predicate in SELECT list
--echo # nullable columns
--echo # no dependent subquery predicate
--echo

let $query=
SELECT vn <>ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn >ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn >=ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <=ALL (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn >ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn >=ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <=ANY (SELECT valn FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ALL (SELECT n FROM t1row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ALL (SELECT vn FROM t2row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ALL (SELECT vn FROM t3row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn =ALL (SELECT vn FROM t4row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <>ANY (SELECT n FROM t1row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <>ANY (SELECT vn FROM t2row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <>ANY (SELECT vn FROM t3row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn <>ANY (SELECT vn FROM t4row) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo
--echo # quantified comparison predicate in WHERE clause
--echo # non-nullable columns
--echo # no dependent subquery predicate
--echo

let $query=
SELECT *
FROM ot
WHERE v <>ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >=ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <=ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >=ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <=ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ALL (SELECT v FROM t0row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ALL (SELECT v FROM t1row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ALL (SELECT v FROM t2row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ALL (SELECT v FROM t4row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <>ANY (SELECT v FROM t0row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <>ANY (SELECT v FROM t1row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <>ANY (SELECT v FROM t2row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <>ANY (SELECT v FROM t4row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo
--echo # quantified comparison predicate in WHERE clause
--echo # nullable outer column
--echo # no dependent subquery predicate
--echo

let $query=
SELECT *
FROM ot
WHERE vn <>ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn >ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn >=ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <=ALL (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn >ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn >=ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <=ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ALL (SELECT v FROM t0row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ALL (SELECT v FROM t1row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ALL (SELECT v FROM t2row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ALL (SELECT v FROM t4row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <>ANY (SELECT v FROM t0row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <>ANY (SELECT v FROM t1row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <>ANY (SELECT v FROM t2row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <>ANY (SELECT v FROM t4row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo
--echo # quantified comparison predicate in WHERE clause
--echo # nullable column in subquery
--echo # no dependent subquery predicate
--echo

let $query=
SELECT *
FROM ot
WHERE v <>ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >=ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <=ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >=ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <=ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ALL (SELECT n FROM t1row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ALL (SELECT vn FROM t2row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ALL (SELECT vn FROM t3row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v =ALL (SELECT vn FROM t4row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <>ANY (SELECT n FROM t1row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <>ANY (SELECT vn FROM t2row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <>ANY (SELECT vn FROM t3row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v <>ANY (SELECT vn FROM t4row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo
--echo # quantified comparison predicate in WHERE clause
--echo # nullable columns
--echo # no dependent subquery predicate
--echo

let $query=
SELECT *
FROM ot
WHERE vn <>ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn >ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn >=ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <=ALL (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn >ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn >=ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <=ANY (SELECT valn FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ALL (SELECT n FROM t1row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ALL (SELECT vn FROM t2row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ALL (SELECT vn FROM t3row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn =ALL (SELECT vn FROM t4row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <>ANY (SELECT n FROM t1row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <>ANY (SELECT vn FROM t2row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <>ANY (SELECT vn FROM t3row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE vn <>ANY (SELECT vn FROM t4row);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo #
--echo # Some negative testing
--echo #

--echo # No tables in outer query block

let $query=
SELECT 1, 1 >ANY (SELECT val FROM it) AS q1, 2 >ANY (SELECT val FROM it) AS q2;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT 1 WHERE 2 >ANY (SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo # No tables in inner query block

let $query=
SELECT 1, 1 >ANY (SELECT 0) AS q1 FROM ot;

--sorted_result
eval $query;     
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT 1 FROM ot WHERE 1 >ANY (SELECT 0);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo # Set operation in subquery

let $query=
SELECT v >ALL (SELECT val FROM it UNION SELECT val FROM it) AS b
FROM ot; 

--sorted_result
eval $query;     
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT val FROM it UNION SELECT val FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo # Implicitly grouped subquery

let $query=
SELECT v >ALL (SELECT MAX(val) FROM it) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT MAX(val) FROM it);

--sorted_result    
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo # Explicitly grouped subquery

let $query=
SELECT v >ALL (SELECT val FROM it GROUP BY val) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT val FROM it GROUP BY val);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >ALL (SELECT MAX(valn) FROM it GROUP BY val) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT MAX(valn) FROM it GROUP BY val);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo # Subquery with WINDOW function

let $query=
SELECT v >ALL (SELECT MAX(val) OVER () FROM it) AS b
FROM ot;

--sorted_result
--error ER_NOT_SUPPORTED_YET
eval $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT MAX(val) OVER () FROM it);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo # Subquery with LIMIT (unsupported) and ORDER BY (supported)

let $query=
SELECT v >ALL (SELECT val FROM it LIMIT 3) AS b
FROM ot;

--sorted_result
--error ER_NOT_SUPPORTED_YET
eval $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT val FROM it LIMIT 3);

--sorted_result
--error ER_NOT_SUPPORTED_YET
eval $query;

let $query=
SELECT v >ALL (SELECT val FROM it ORDER BY val DESC) AS b
FROM ot;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT val FROM it ORDER BY val DESC);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT v >ALL (SELECT val FROM it ORDER BY val DESC LIMIT 3) AS b
FROM ot;

--sorted_result
--error ER_NOT_SUPPORTED_YET
eval $query;

let $query=
SELECT *
FROM ot
WHERE v >ALL (SELECT val FROM it ORDER BY val DESC LIMIT 3);

--sorted_result
--error ER_NOT_SUPPORTED_YET
eval $query;

--echo # Subquery with outer reference in SELECT list

let $query=
SELECT v >ALL (SELECT val FROM itr WHERE otr.rf = itr.rf) AS b
FROM otr
WHERE rf = 1;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT vn >ALL (SELECT valn FROM itr WHERE otr.rf = itr.rf) AS b
FROM otr
WHERE rf = 1;

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

--echo # Subquery with outer reference in WHERE clause

let $query=
SELECT *
FROM otr
WHERE v >ALL (SELECT val FROM itr WHERE otr.rf = itr.rf);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;

let $query=
SELECT *
FROM otr
WHERE vn >ALL (SELECT valn FROM itr WHERE otr.rf = itr.rf);

--sorted_result
eval $query;
--replace_regex $elide_costs
eval explain format=tree $query;
