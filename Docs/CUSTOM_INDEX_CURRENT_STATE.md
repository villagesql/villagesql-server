# Custom Index Current State

This document describes the current custom-index experiment. The implementation
is preview-level and intentionally biased toward proving the extension boundary,
DML maintenance, and one optimizer access path before hardening persistence,
transactions, and costing.

## Architecture Overview

Custom indexes are registered by extensions through the preview index SDK. An
extension defines an index type with lifecycle callbacks, DML callbacks, scan
callbacks, capabilities, storage properties, and optional index parameters. The
server stores index metadata in VillageSQL system tables and resolves the
registered index type when SQL creates or uses an extended index.

The SQL surface is:

```sql
CREATE INDEX idx_name ON table_name (columns...)
  USING EXTENDED(extension_name.index_type_name [WITH ...]);
```

The metadata path stores custom indexes and custom index columns in the
VillageSQL Victionary. Index type descriptors are populated when an extension is
installed and removed when it is uninstalled. Uninstall checks also verify that
registered index types are not still referenced.

The runtime path is implemented in `villagesql/sql/custom_index_runtime.cc`.
It maintains an in-process map of loaded custom indexes, keyed by
`db.table.index`. When table DML runs, server hooks call into this runtime,
which resolves the table's custom indexes from Victionary, loads the extension
index implementation if needed, extracts key columns and primary-key columns
from the row, and calls extension DML callbacks.

The current DML hooks are:

- insert: `insert`
- delete: `mark_delete(delete_mark=true)`
- update: delete old row, then insert new row

DDL metadata removal also informs the runtime. When `DROP INDEX` or
`DROP TABLE` removes custom index metadata, the metadata modifier schedules the
matching loaded runtime index for drop. On transaction commit, the runtime calls
the extension `drop` callback and erases the loaded `db.table.index` entry. On
rollback, the scheduled runtime drop is discarded.

Transaction handling is currently conservative. The runtime tracks touched
indexes per statement and transaction. On rollback it marks touched in-memory
indexes dirty rather than attempting undo. A dirty index is rejected by the KNN
runtime path until it is rebuilt. This is not yet a complete transaction model.

Custom index storage was originally memory-only (the MVECTOR and original BLOOM
extensions still work that way). A second storage backend — table-storage-backed,
described below — is now in place and is the one the new bloom_hidden test
extension uses. The CustomIndexBackend abstraction (declared in
`villagesql/sql/custom_index_backend.h`) lets a backend opt into per-statement
write preparation, per-row hooks, and DDL lifecycle hooks
(`pre_create_storage`, `cleanup_failed_create`, `on_drop`). The memory-only
backends inherit the defaults; the table_storage backend owns lifecycle through
these hooks.

### Table-storage-backed indexes

A new backend stores the index data in a server-owned table under
`villagesql.__hidden_<logical_name>`. The server, not the extension, owns the
hidden table's lifecycle — DDL on the base table creates, drops, and rebuilds
the hidden storage. The backing capability is
`vsql::preview::table_storage` (formerly `vsql::preview::hidden_table` —
renamed for parity with `vsql::preview::innodb_page_storage`; the
physical table itself is still named `__hidden_*` to indicate it's
hidden from user view). Implementation lives in
`villagesql/services/preview/custom_index_table_storage_backend.cc` and
`villagesql/services/preview/table_storage.cc`.

Lifecycle:

- `CREATE TABLE ... INDEX ... USING EXTENDED(...)`:
  `Metadata_modifier::process_create` calls
  `custom_index_pre_create_storage` at the top, which dispatches to the
  backend's `pre_create_storage`. The table_storage backend calls
  `materialize_physical_table_storage` (a public entry point distinct from
  the descriptor-only ABI `table_storage_create` — see below) which builds
  an `Alter_info` from the `vef_table_storage_def_t` and invokes
  `mysql_create_table` with `IF NOT EXISTS`. The nested DDL runs before
  the outer CREATE TABLE starts staging victionary entries to avoid
  conflicts.
- `CREATE INDEX` / `ALTER TABLE ADD INDEX`:
  `Metadata_modifier::process_alter` runs the same `pre_create_storage`
  hook at the top, before staging victionary entries, so the hidden
  storage exists by the time the ALTER copy fires.
- `DROP TABLE`: `sql_parse.cc` snapshots the set of backing `vef_type_index_intf_t`
  for the doomed tables via `custom_index_snapshot_for_drop` BEFORE
  `mysql_rm_table` runs, then calls `custom_index_drop_snapshotted_storage`
  AFTER the base drop succeeds. The snapshot is needed because victionary
  entries are removed during `mysql_rm_table`, so post-drop lookup wouldn't
  find anything.

Transactional consistency: the hidden table is registered with the same
transaction the base table writes against (via the engine's normal
`external_lock` enrollment). COMMIT, ROLLBACK, and SAVEPOINT all work without
any custom code — the hidden-table rows live and die with the user's
transaction. Cross-session isolation is the standard InnoDB MVCC behavior.

DDL backfill: `CREATE INDEX` / `ALTER ADD INDEX` is force-copied (inplace
disabled in `sql/sql_table.cc` when
`villagesql::alter_info_adds_custom_index(alter_info)` returns true). The
engine's row-by-row copy fires `ha_write_row` on the rebuild table
(`#sql-...`), which triggers the runtime's per-row hooks. No separate
backfill scan is needed.

ALTER rebuild bridging: rebuild writes hit a share with `tmp_table` set
and `table_name = "#sql-..."`, so victionary lookups under the rebuild's
name find nothing. `mysql_alter_table` stashes the user-visible db/table
on THD (`villagesql_alter_target_db`/`_table`), cleared by `AlterGuard`,
and the runtime redirects lookups for rebuild tables to that target. The
new victionary entry for the in-flight index is still uncommitted at
copy time, so the runtime uses an uncommitted-aware
`SystemTableMap::get_prefix(thd, ...)` rather than the default
committed-only view.

ABI deadlock note: the ABI vtable `vef_preview_table_storage_t::create`
(extension-facing) only allocates the in-memory descriptor; it does not
materialize the physical table. The DDL paths use a separate
`materialize_physical_table_storage` entry point. This split exists because
extensions call `create` from `on_load`, which runs inside the runtime's
`g_runtime_mu`; `mysql_create_table` fires `custom_index_commit`, which
re-enters that lock and would deadlock.

#### Graph-traversal ABI

Three additions to `vef_preview_table_storage_t` enable graph-shaped
indexes (linked lists, trees, HNSW-style graphs) where one row of the
hidden table needs to reference another:

- **Secondary indexes** on the hidden table. `vef_table_storage_def_t`
  gained `secondary_indexes` and `secondary_index_count` fields. Each
  entry is `vef_table_storage_index_def_t{ name, column_indices,
  column_count, unique }`. The server's
  `materialize_physical_table_storage_impl` emits a `Key_spec` per
  secondary index alongside the PK. Scans against secondary indexes use
  the new `VEF_TABLE_STORAGE_SCAN_SECONDARY_INDEX` scan type, plus a
  `secondary_index_name` and a `vef_table_storage_scan_direction_t`
  (ASC/DESC) on the scan descriptor; an empty `key_values` seeks to the
  first/last row in the chosen direction. Used by extensions to look up
  the entry point ("scan `by_seq` DESC LIMIT 1, take that row") without
  full-scanning the hidden table.

- **`scan_position`**: returns opaque ref bytes for the row a cursor is
  currently positioned on. These bytes are the hidden table's own row
  reference (`handler::ref` under the hood — clustered-index key for
  InnoDB). MariaDB calls these "grefs"; extensions store them inside
  payload columns to record links between hidden-table rows. Pointer
  lifetime is tied to the cursor; the extension copies if it needs the
  bytes to survive `scan_end`.

- **`scan_seek`**: opens a single-row cursor at the row identified by a
  previously-captured ref. Equivalent of `ha_rnd_pos`. Combined with
  `scan_position` this lets an extension walk arbitrary linked
  structures through the hidden table in O(1) per hop, instead of doing
  a secondary-index lookup at every step.

- **`update_row`**: mutate an existing row in place. Look up by PK,
  overwrite all non-PK column values, identity (and therefore the
  row's gref) is preserved. Useful for extensions that need to flag
  rows (e.g. tombstones) or maintain mutable side-state (e.g. HNSW
  neighbor lists) without changing identity. The extension must supply
  all columns; partial updates aren't supported today.

- **`ref_length`**: query the engine's ref width for a hidden-table
  handle. Extensions storing grefs in payload columns can use this to
  size those columns to the underlying engine's actual ref length
  rather than guessing.

Tombstone pattern: deletion of a row that other rows still point at via
gref would dangle those links. Extensions implement logical deletion by
adding a `deleted` column (or making an existing column nullable, as
MariaDB does with `tref`) and using `update_row` to flip the flag
rather than calling `delete_row`. The walker checks the flag and emits
only live rows, but follows links through tombstoned ones so the
structure stays navigable.

Per-row read symmetry: `copy_field_value` emits integer column values
as decimal ASCII (matching how extensions supply them through
`vef_table_storage_value_t`), not as raw little-endian bytes. This is so
the round trip `scan_fetch` → `update_row` works without the extension
having to re-encode. Extensions reading integer columns must parse the
bytes as ASCII decimal; defensive "try binary by length first" code
will misparse small values (a 2-byte ASCII "10" looks like uint16 binary
12592).

The runtime deliberately skips DML maintenance for the internal
`villagesql.*` schema. This avoids re-entering Victionary/custom-index logic
while INSTALL/UNINSTALL EXTENSION and other metadata writes are being persisted.
The table_storage backend's physical storage also lives in this schema
(`villagesql.__hidden_*`), so the same filter keeps the runtime from trying
to maintain custom indexes on the hidden tables that themselves back custom
indexes. That skip is a temporary server-side escape hatch and should
eventually become an explicit internal-operation context flag. Tests that
need to read hidden tables directly set the standard
`+d,skip_dd_table_access_check` debug flag.

Custom type columns require special key extraction. For custom-typed key
columns, the runtime uses `Field::val_str()` and stores the actual encoded
custom-type bytes in temporary owned buffers before invoking the extension. For
ordinary columns, the current path still uses the field data pointer and packed
length.

## Optimizer Work

Only the hypergraph optimizer is currently wired to use custom indexes. The
implemented optimizer path is for KNN-style `ORDER BY ... LIMIT` queries over a
custom index with the `KNN` capability.

The optimizer-specific VillageSQL code lives in:

- `villagesql/sql/custom_index_hypergraph_optimizer.h`
- `villagesql/sql/custom_index_hypergraph_optimizer.cc`

That module owns the custom discovery and execution details:

- detects supported distance expressions
- resolves a matching custom index on the referenced field
- checks index capabilities
- captures the encoded query vector
- registers a scan spec behind an opaque custom scan id
- estimates current cost
- provides the display name for planning/explain
- creates the custom row iterator

The optimizer integration is intentionally surgical. Custom distance scans are
represented as `INDEX_DISTANCE_SCAN`, with an explicit `is_custom_index` flag in
the optimizer structs. When that flag is false, `key_idx` is a native MySQL
`TABLE::key_info[]` index. When it is true, `key_idx` carries an opaque
VillageSQL custom scan id. The VillageSQL helper maps that id back to its scan
specification.

Current optimizer touch points are:

- `build_interesting_orders.cc`: asks VillageSQL to add any custom KNN
  interesting orderings for each base table.
- `join_optimizer.cc`: recognizes custom distance scans through
  `is_custom_index` for range construction, costing, update/delete safety
  checks, and path naming.
- `access_path.cc`: creates the VillageSQL custom distance iterator instead of
  the built-in `IndexDistanceScanIterator` when the scan id is custom.
- `explain_access_path.cc`: emits custom index distance scan text for EXPLAIN.

The custom iterator performs a KNN scan through `custom_index_runtime.cc`, then
fetches each base-table row by primary key. This means the current query path is
ANN/KNN order from the extension followed by primary-key row lookups.

Current limitations:

- only hypergraph optimizer support
- distance-function recognition is a hardcoded allowlist
  (`MVECTOR.mvector_l2_distance`,
  `VSQL_VEC_CHAIN_TEST.vec_chain_l2_distance`) in
  `GetMVectorDistanceFunction`. Each index type that wants ORDER BY
  routing must be added to the list. The proper fix is to declare the
  distance VDF as a property of the index type at registration so the
  optimizer can resolve it via the victionary.
- only one-column KNN indexes are supported
- a primary key is required
- cost estimation is a placeholder based on primary-key read cost
- scan specs are kept in a process-wide preview registry and need explicit
  statement-lifetime cleanup before hardening
- no optimizer path is wired for bloom/filter indexes yet
- the runtime currently only opens a hidden-table handle on the write
  path (via `prepare_table_writes` / `before_callback`). The scan path
  has no equivalent, so table-storage-backed indexes open their own
  read handle inside `scan_begin`. Correct but per-scan; a future
  `prepare_table_reads` hook would amortize handle setup across a
  statement.

## MVECTOR Index Work

The MVECTOR extension lives in:

- `villagesql/test-extensions/mvector/src/extension.cc`
- `mysql-test/suite/villagesql/extension/mvector/`

It defines the custom type `MVECTOR` and a memory-only nearest-neighbor index
type named `mvector_nn`.

The extension exposes vector functions for construction, conversion, and
distance calculations. The KNN optimizer path currently recognizes:

```sql
ORDER BY MVECTOR.mvector_l2_distance(v, MVECTOR::from_string('[1,0,0]'))
LIMIT k
```

when `v` is a column with a matching custom `mvector_nn` index.

The memory index stores:

- encoded vector bytes
- copied primary-key bytes
- an internal row reference
- delete markers

The extension implements lifecycle and DML callbacks:

- `create`
- `load`
- `drop`
- `insert`
- `mark_delete`
- `purge`

The `drop` callback clears both the extension storage entries and the MVECTOR
test mirror map used by manual VDFs and scan counters. This matters because MTR
can run several tests against the same server process; without runtime cleanup,
a later test that recreates `test.t.idx_v` would reuse stale in-memory entries.

It also implements scan callbacks for KNN:

- `scan_begin`
- `scan_position`
- `scan_fetch`
- `scan_save`
- `scan_restore`
- `scan_end`

`scan_begin` validates a one-column KNN query, computes L2 distance against all
non-deleted entries, sorts by distance and insertion position, applies the
requested limit, and returns a cursor. `scan_fetch` returns the selected entry's
stored key and primary-key bytes. The server iterator then fetches the base row
by primary key.

MVECTOR also has VDFs that manually exercise the in-memory index, including:

- clear/upsert/delete/count helpers
- `mvector_search_nn`
- scan counters used by tests to verify optimizer usage

The optimizer test verifies that an `ORDER BY mvector_l2_distance(...) LIMIT`
query starts the index scan and fetches the expected number of rows. This is the
current proof that the hypergraph optimizer path is using the custom index.

Current limitations:

- memory-only index state
- no durable index storage
- no automatic rebuild on restart
- rollback marks indexes dirty instead of undoing changes
- KNN scan is a simple in-memory full scan and sort
- function recognition is hard-coded to the MVECTOR L2 VDF name

## BLOOM Index Work

The BLOOM extension lives in:

- `villagesql/test-extensions/bloom/src/extension.cc`
- `mysql-test/suite/villagesql/extension/bloom/`

It defines a memory-only Bloom signature index type named `bloom`. It is meant
to exercise the same custom-index registration, DDL, and DML maintenance hooks
with a non-vector index shape.

The index computes a Bloom signature over one or more key columns. Supported
index parameters include:

- `length`: signature bit length, currently constrained to `64..8192`
- `hashes`: default number of hashes, currently constrained to `1..32`
- `colN`: per-column hash count override

The extension implements lifecycle and DML callbacks:

- `create`
- `load`
- `drop`
- `insert`
- `mark_delete`
- `purge`

The `drop` callback clears both the extension storage entries and the Bloom test
mirror map used by manual VDFs. This keeps test state scoped to the lifetime of
the SQL index, even when the server process is reused across tests.

It also implements point-lookup scan callbacks:

- `scan_begin`
- `scan_position`
- `scan_fetch`
- `scan_save`
- `scan_restore`
- `scan_end`

The registered capability is `POINT_LOOKUP`, not `KNN`. `scan_begin` currently
supports full-key point lookup scan descriptors and filters entries by Bloom
signature containment. Because Bloom filters can produce false positives, this
is a candidate-filtering index shape rather than a definitive equality index.

The extension also exposes manual VDFs for testing and experimentation:

- `bloom_index_clear`
- `bloom_index_insert`
- `bloom_index_update`
- `bloom_index_upsert`
- `bloom_index_delete`
- `bloom_index_count`
- `bloom_might_contain`
- `bloom_search`

The server maintains Bloom indexes through the same generic DML hooks as
MVECTOR. There is no optimizer integration for Bloom yet. The likely next
optimizer step would be a filter/predicate access path that asks the Bloom
index for candidate primary keys, then verifies the original SQL predicate
against the fetched base rows.

Current limitations:

- memory-only index state
- no durable index storage
- no optimizer access path
- point scan support exists at the extension ABI level but is not yet consumed
  by SQL planning
- false positives require base-row predicate verification in any future
  optimizer path
- rollback handling is the same dirty-index model as MVECTOR

## BLOOM_HIDDEN Index Work

A second bloom extension lives in:

- `villagesql/test-extensions/vsql-bloom-hidden-test/src/extension.cc`
- `mysql-test/suite/villagesql/extension/table_storage/`

It exercises the table-storage-backed storage path described in the Architecture
section. Unlike the original BLOOM extension, this one owns no in-extension
storage — all data lives in the server-managed
`villagesql.__hidden_bloom_test` table. The extension provides inspector
VDFs (`bloom_create`, `bloom_load_cache`, `bloom_search`, `bloom_insert`,
`bloom_delete`, `bloom_might_contain`) that bind to the hidden table via the
preview hidden-table capability ABI and serve as the test surface for
verifying server-driven materialization, maintenance, and drop.

Test suite covers:

- `bloom_hidden_basic`: end-to-end attach/insert/load/search/delete via
  inspector VDFs.
- `bloom_hidden_create_table`: inline `CREATE TABLE ... INDEX ... USING
  EXTENDED(bloom)` materializes the hidden storage during DDL.
- `bloom_hidden_physical_create` / `bloom_hidden_physical_drop`: server
  ownership of the physical table across CREATE/DROP.
- `bloom_hidden_index_maintenance`: INSERT/UPDATE/DELETE on the base
  table propagate through to the hidden storage.
- `bloom_hidden_backfill`: `CREATE INDEX` and `ALTER ADD INDEX` on a
  populated table backfill via the ALTER copy.
- `bloom_hidden_transactional`: COMMIT, ROLLBACK (single- and
  multi-statement), SAVEPOINT, and cross-session isolation.

Current limitations:

- the hidden table's logical name is hardcoded `bloom_test` in the
  extension, so only one bloom_hidden index can exist system-wide at a
  time.
- the base table must have a user-declared primary key (see PK
  requirement TODO in `custom_index_runtime.cc`); the long-term plan is
  to switch to MariaDB-style row references (`handler::ref` +
  `ha_rnd_pos`) so any base table can be indexed.
- `cleanup_failed_create` rollback path is disabled — a table-cache
  "still in use" assertion fires when `mysql_rm_table` runs inline with
  the outer failing DDL. A leaked hidden table can result if the outer
  CREATE/ALTER fails after `pre_create_storage` succeeded.
- no startup recovery hook for orphaned hidden tables (e.g. crash
  between hidden CREATE and the base table's commit).
- no optimizer access path; reads go through inspector VDFs, not via
  SQL queries that use the index.

## UNIQ_LOWER Index Work

A second table-storage-backed extension lives in:

- `villagesql/test-extensions/vsql-uniq-lower-test/src/extension.cc`
- `mysql-test/suite/villagesql/extension/index_uniq_lower/`

It enforces case-insensitive uniqueness on a column by storing
`lower(value)` as the PRIMARY KEY of the hidden table. InnoDB's
native PK uniqueness check does the work — the extension's `insert`
just attempts the write, and a duplicate-key error from the hidden
table propagates back through `ha_write_row` to fail the user's
INSERT. This is the first extension that uses the architecture for
write-side veto (the bloom_hidden DML path never refuses).

Test suite covers:

- `uniq_lower_basic`: distinct values insert + delete cleanly.
- `uniq_lower_dup_insert`: case-different duplicate fails the INSERT
  with no row left behind on the base table.
- `uniq_lower_delete_reinsert`: deleting a row frees its lowered key
  for reinsertion; subsequent duplicates still fail.
- `uniq_lower_alter_fail`: `ALTER ADD INDEX` on a table that already
  contains case-different duplicates fails the ALTER, base table is
  unchanged.

Architectural validations added by this extension:

- Write veto path: extension returning failure from `intf->insert`
  propagates to `ha_write_row` and fails the SQL statement
  atomically. Pre-create + commit on every other code path also bails
  cleanly. The `pre_create_storage` and runtime error paths must
  surface an SQL-level error via `villagesql_error` to avoid the
  `send_statement_status` assertion when the DA is otherwise clean.
- Runtime "soft failure" log level was downgraded from ERROR to
  WARNING and `dirty=true` was dropped — duplicate-key rejection is
  expected behaviour for a unique-style index, not a corruption
  signal. The ABI still conflates soft and hard failures into a
  single `bool` return; a future ABI revision should distinguish
  them.
- The text base column requires `CHARACTER SET ascii COLLATE
  ascii_bin` for the extension's case-insensitive uniqueness to be
  the only uniqueness check that matters. The default `ascii`
  collation (`ascii_general_ci`) would have InnoDB's native UNIQUE
  match first.

## VEC_CHAIN Index Work

A graph-shaped table-storage-backed extension lives in:

- `villagesql/test-extensions/vsql-vec-chain-test/`
  (`extension.cc`, `type.h`/`type.cc`, `index.h`/`index.cc`)
- `mysql-test/suite/villagesql/extension/vec_chain/`

The extension defines:

- A `VSQL_VEC_CHAIN(N)` custom type — fixed-dimension float/double
  vector, schema parallel to MVECTOR.
- Two distance VDFs: `vec_chain_l2_distance` (used by the optimizer
  hook for ORDER BY routing) and `vec_chain_dot_product`.
- A `vec_chain` custom index type backed by the hidden-table
  capability with secondary indexes, `scan_position`, `scan_seek`,
  and `update_row`.
- Two inspector VDFs for tests: `vec_chain_inspect_knn(dim, query,
  limit)` walks the chain directly (bypassing the optimizer) and
  `vec_chain_scan_count()` returns the process-wide count of
  `intf->scan_begin` invocations so tests can assert the optimizer
  actually routed through the index.

The "chain" is a linked list of hidden-table rows. Each row holds
`pk_id` (base PK bytes), `vec`, `prev_gref` (the previous head's
gref), `seq` (monotonic insert counter), and `deleted` (tombstone
flag). PRIMARY KEY is `pk_id`; secondary KEY is `by_seq (seq)`. On
each insert the extension scans `by_seq DESC LIMIT 1` to find the
current head, captures its gref via `scan_position`, and stores it
as the new row's `prev_gref`. KNN scan walks newest-to-oldest via
`scan_seek(prev_gref)`, computes L2 distance per node, sorts hits
with `std::stable_sort` (ties preserve newest-first traversal
order), and truncates to the query's `LIMIT`.

Tombstone semantics: `mark_delete` calls `update_row` to flip the
row's `deleted` flag to 1 rather than physically removing it. The
walker skips emitting tombstoned rows but still follows their
`prev_gref` links, so deleting a middle-of-chain node doesn't
truncate the chain.

Optimizer integration: the function-name allowlist in
`GetMVectorDistanceFunction` accepts
`VSQL_VEC_CHAIN_TEST.vec_chain_l2_distance` alongside MVECTOR's, so
`ORDER BY vec_chain_l2_distance(v, VSQL_VEC_CHAIN::from_string('[...]'))
LIMIT k` queries route through `vec_chain_index_scan_begin`. Requires
`--source include/have_hypergraph.inc` in tests because the hook lives
in the hypergraph optimizer.

Test suite covers:

- `vec_chain_basic`: type-only smoke (column declaration, INSERT
  from string, distance VDF).
- `vec_chain_index_ddl`: registering the index materializes
  `villagesql.__hidden_vec_chain` with the declared schema including
  the `by_seq` secondary index.
- `vec_chain_index_maintenance`: INSERT/DELETE propagate, tombstone
  flips visible (total/live row counts + HEX of tombstoned PK).
- `vec_chain_index_chain`: chain linking visible via `prev_gref` /
  `LENGTH(prev_gref)`; all non-first rows share the same gref
  length.
- `vec_chain_index_knn`: KNN via the inspector VDF (independent of
  the optimizer), including middle-of-chain tombstone behaviour.
- `vec_chain_index_orderby`: KNN via the SQL surface (`ORDER BY
  ... LIMIT`), including tie-handling, mid-tie truncation,
  dynamically-inserted-then-queried rows, and tombstones-through-
  the-optimizer. The `scans_run` counter asserts every ORDER BY
  actually invoked the custom index.

ABI surface exercised that no prior extension had touched:

- secondary indexes on the hidden table descriptor;
- `VEF_TABLE_STORAGE_SCAN_SECONDARY_INDEX` with `DESC` direction;
- `scan_position` + `scan_seek` for graph-style traversal;
- `update_row` for in-place mutation (used for tombstoning);
- `ref_length` (queried implicitly via the captured gref length).

Current limitations:

- The hidden table's logical name is hardcoded `vec_chain` (same
  per-extension singleton constraint as bloom_hidden).
- The chain doesn't handle update-of-vector — base-table UPDATE
  would go through delete + insert with a new chain position, which
  works but reorders nodes by recency rather than preserving the
  original insertion point.
- The runtime opens a fresh hidden-table read handle inside every
  `scan_begin` because the runtime doesn't yet provide one on the
  read path. Correct but per-scan; a future `prepare_table_reads`
  backend hook would amortize handle setup.
- Sequence number is a process-local `std::atomic` seeded to 1; on
  restart, the next insert reuses seq=1. Doesn't break the chain
  (the chain follows gref, not seq) but it does break the "find the
  head by `MAX(seq)`" lookup unless re-seeded from the table on
  load. TODO is noted at the storage struct.
- KNN is O(N) — the chain is a linear list, not a navigable graph.
  The whole point of this extension is to exercise the gref ABI,
  not to be a fast vector index. A real HNSW would use the same
  ABI surface with a different data structure.
