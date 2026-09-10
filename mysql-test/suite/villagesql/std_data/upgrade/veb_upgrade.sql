-- Datadir contents for upgrade with extension.

USE test;

INSTALL EXTENSION update_test VERSION '1.0.0';

CREATE TABLE t (id INT PRIMARY KEY, val update_test.COUNTER);
INSERT INTO t VALUES (1, '10'), (2, '20'), (3, '5');

# Before the update, counter_double resolves to v1 (returns N*2).
SELECT id, update_test.counter_double(val) AS doubled FROM t ORDER BY id;

ALTER EXTENSION update_test VERSION '2.0.0' AT RESTART;
