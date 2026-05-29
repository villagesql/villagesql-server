# Custom Index — Merged Upstream PRs and Remaining Branch Work

Record of upstream PRs from Debarun (`@villagedeb`) that affect
`tomas/experimentation`, what was absorbed during rebase, and the
merge-strategy follow-ups still pending on the branch. Pair with
`Docs/CUSTOM_INDEX_HANDOFF.md` (open work) and
`Docs/CUSTOM_INDEX_CURRENT_STATE.md` (architecture overview).

Originally drafted 2026-06-12 as `CUSTOM_INDEX_UPCOMING_PRS.md`.
Renamed and updated 2026-06-23 after #650/#651 landed. Updated
again 2026-07-09 to survey the next wave (#720 merged, #721-#723
open) — all tagged "Part of #386" (tracking issue: "Server: Index
access interface", still open, assigned to `@villagedeb`).
Updated 2026-08-02 to survey the July wave that landed
(#721/#722/#723/#807/#808/#873/#897/#906/#907) — most importantly
#808 which starts filling the DML invocation gap I catalogued
in June (#1 in the gap list).
Updated 2026-08-03: #809 landed (profile-descriptor plumbing on
the InnoDB side). #926 and #927 opened as the pair that finally
retires the branch's profile_fn/helper_fn/key_len_fn stubs on
Debarun's `Custom_index`.

## PR #651 — "Custom Index: Test corrections"

**Merged 2026-06-16 as `a197b2ac089`.** Branch absorbed during rebase.

Pure test rewrite. Updated the existing syntax tests
(`create_table_index`, `alter_table_index`) from referencing fictional
identifiers (`hnsw`, `myext.hnsw`, `hnsw_l2_profile`) to real names
registered by the `vsql_index_test` extension
(`dummy_index_hnsw`, `dummy_type_vector`, `dummy_profile_hnsw_l2`),
and framed the tests with
`SET PERSIST vsql_allow_preview_extensions = ON; INSTALL EXTENSION
vsql_index_test;` / `UNINSTALL EXTENSION ...`.

**Branch impact:** none. The branch ships its own suites under
`vec_chain/`, `bloom/`, `mvector/`, `index_uniq_lower/`,
`table_storage/` — independent of `vsql-index-test`. Rebase produced
trivial result-file conflicts (the branch had local edits to the
same files from an earlier draft); HEAD's version was taken
wholesale.

## PR #650 — "Custom Index: DDL InnoDB" ⚠ structural overlap

**Merged 2026-06-22 as `07b16f2dda9`.** Branch absorbed mechanically;
the merge-strategy follow-ups below are still pending.

### What #650 added

1. **New `Custom_index` runtime** in
   `storage/innobase/villagesql/custom_index.{cc,h}`. Per-`dict_index_t`
   object holding a `shared_ptr<const IndexContext>`, a
   `vef_index_ctx_t`, and the extension's `storage_ctx`. Allocated on
   the index heap, destroyed in `dict_mem_index_free`.
2. **`KEY::custom_index_context`** field carries the resolved
   `IndexContext*` from the victionary through `KEY` ->
   `ddl::Index_defn::m_custom_index_context` ->
   `Custom_index::load(dict_index_t*, IndexContext*)`.
3. **`MaybeInjectCustomIndex(thd, share, keyinfo)`** populates
   `KEY::custom_index_context`. Called from
   `mysql_alter_table` (right after `fill_alter_inplace_info`, for
   every key in `key_info_buffer`) and from `dd_open_table_one` via
   `dd_find_index` for table-open paths.
4. **DDL hookup inside InnoDB:**
   - `ha_innobase::create_index` -> `Custom_index::load()` attaches
     runtime state to `dict_index_t`.
   - `dict_create_index_tree_in_mem` short-circuits for custom
     indexes — calls `Custom_index::create(index, trx_id)` instead of
     building a B-tree page. That invokes `intf.parse` (to validate
     and persist the WITH-clause options) then `intf.create`.
   - `row_drop_table_for_mysql` and `commit_try_norebuild` call
     `Custom_index::drop` instead of freeing a B-tree.
   - `Loader::validate_indexes` (DEBUG only) skips custom indexes.
   - `dict_index_add_to_cache_w_vcol` re-loads `Custom_index` onto
     the cache-internal `dict_index_t` (the prototype is freed at the
     end of that function).
5. **`m_index_contexts`** system table added to `VictionaryClient`,
   tracking client-managed IndexContexts with use-counts.
   `remove_extension_from_victionary` refuses UNINSTALL when
   `use_count > 1` for any of the extension's IndexContexts ("index
   type is currently in use").
6. **`AcquireIndexContextClientManaged(const IndexContext*)`** helper
   in `villagesql/types/util.cc`, parallel to the existing
   `AcquireTypeContextClientManaged`.

### Where this overlaps with the branch

The branch's `custom_index_runtime.cc` does something architecturally
parallel but at a different layer:

| What                              | Branch                                                                                | PR #650                                                          |
| --------------------------------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| Owns "loaded index" state         | `g_loaded_indexes` map keyed `db.table.index` -> `std::shared_ptr<...>`               | `dict_index_t::custom_index`                                     |
| Calls `intf.create`               | `process_create` -> `custom_index_pre_create_storage` (pre-InnoDB)                    | `dict_create_index_tree_in_mem` (deep inside InnoDB create)      |
| Calls `intf.drop`                 | `custom_index_schedule_drop` -> drains on commit                                      | `commit_try_norebuild` / `row_drop_table_for_mysql`              |
| Calls `intf.parse` on WITH params | `Metadata_modifier::validate_custom_index_params` (at SQL-parse, fail-fast UX)        | `parse_index_options` inside `Custom_index::create`              |
| Resolves IndexContext             | `MaybeInjectCustomIndex` (branch added this; #650 keeps it but uses it for DDL too)   | Reads `KEY::custom_index_context` previously populated by branch |
| Rebuild `#sql-…` bridging         | `villagesql_alter_target_db/table` THD stash + uncommitted-aware victionary lookups   | Not needed: routes through `KEY::custom_index_context` directly  |
| Per-row maintenance               | `ha_write_row` hook calls `intf.insert` / `intf.mark_delete`                          | Untouched — branch keeps owning this                             |
| Force-copy ALTER                  | `alter_info_adds_custom_index` -> `HA_ALTER_INPLACE_NOT_SUPPORTED`                    | Untouched — branch keeps owning this                             |
| Optimizer KNN hook                | `custom_index_hypergraph_optimizer.cc`                                                | Untouched — branch keeps owning this                             |

### Concrete conflicts now active on the branch

After the 2026-06-23 rebase, the branch builds but these double-call
hazards are live:

- **Double `intf.create`** for every custom index — once from the
  branch's `pre_create_storage`, once from
  `dict_create_index_tree_in_mem`. The extension sees two
  `create` calls for one index.
- **Double `intf.drop`** for every dropped custom index — branch's
  runtime-drain on commit + #650's `Custom_index::drop` in the InnoDB
  drop paths.
- **Double `intf.parse`** — branch validates at SQL-parse,
  #650 invokes at dict-create. Both want to call. Less harmful
  (parse is deterministic) but the branch caches the parsed result
  while #650 expects to allocate fresh on the index heap.

Expect test failures in `vec_chain/`, `bloom/`, and similar suites
until the merge strategy below is applied.

### Gap in #650: InnoDB DML iteration doesn't skip custom indexes

PR #650 hooks `intf.create` / `intf.drop` at the dict layer but
leaves the InnoDB DML paths unaware of custom indexes. Custom
indexes have `page == FIL_NULL` (no B-tree); InnoDB's row insert /
update / undo loops iterate `dict_table_t->indexes` treating each
non-`DICT_FTS` entry as a B-tree to operate on, and trip
`dict_index_check_search_tuple`'s `ut_ad(index->page != FIL_NULL)`
assert on the first INSERT.

**Branch-local patch (2026-06-23)** — eight sites get an
`!Custom_index::is_custom(index)` skip alongside the existing
`DICT_FTS` check, each marked
`TODO(villagesql-indexing): upstream to #650 follow-up`:

- `storage/innobase/row/row0ins.cc:3627` — `row_ins` insert loop.
- `storage/innobase/row/row0upd.cc:3245` — `row_upd` update loop.
- `storage/innobase/row/row0umod.cc:873, 985, 1127` —
  undo-modify (rollback) iteration.
- `storage/innobase/row/row0uins.cc:416` — `row_undo_ins` rollback
  of an inserted row (hit by uniq_lower's duplicate-key rejection
  → statement rollback path).
- `storage/innobase/row/row0purge.cc:686, 1304` — purge worker
  cleaning up delete-marked rows, walks indexes and asserts.

Same files got `#include "villagesql/custom_index.h"` alongside
the existing `villagesql/custom_column.h`.

The unified rule: anywhere InnoDB iterates `dict_table_t->indexes`
expecting each entry to have a B-tree, custom indexes must be
skipped. Found in two waves — the first INSERT crash exposed the
DML loops (3 files / 5 sites); the second
duplicate-reject-then-purge crash exposed the rollback (`row0uins`)
and purge (`row0purge`) loops (2 files / 3 sites). Worth a
follow-up sweep on `row0sel.cc` and the DDL/import/stats paths
before relying on FORCE INDEX / ALTER / ANALYZE on custom indexes.

### Merge strategy (still pending on the branch)

The follow-up work is mostly *subtractive*:

1. **Don't duplicate `intf.create` / `intf.drop`.** Pick one owner.
   Most natural is #650's — the dict layer is where every other index
   lifecycle lives. The branch's `pre_create_storage` shrinks to
   *only* the table_storage-backend-specific "materialize the hidden
   table before InnoDB touches the index" step. That step has to
   happen before the dict mutex is acquired (the hidden table
   creation re-enters DDL), so #650's call site is too late for it.
   The generic `intf.create` call disappears from the branch.
2. **Switch `custom_index_runtime.cc` from its `db.table.index` map
   to reading `dict_index_t::custom_index`.** Cuts roughly 100 lines.
   Removes the runtime's own commit/rollback dirty-marking — it
   becomes whatever dict-layer + InnoDB give us for free. Lets us
   drop the `villagesql_alter_target_db/table` THD stash (the
   rebuild's `#sql-…` `KEY` now carries `custom_index_context`
   directly, no name-translation needed).
3. **Drop the branch's `intf.drop` paths**
   (`custom_index_schedule_drop`, runtime-drain on commit). Let #650
   own them via `Custom_index::drop`.
4. **Keep on the branch:**
   - The optimizer hook (`custom_index_hypergraph_optimizer.cc`).
   - The table_storage backend itself
     (`custom_index_table_storage_backend.cc` plus
     `services/preview/table_storage.cc`).
   - The per-row `ha_write_row` / `ha_delete_row` maintenance.
   - The force-copy decision in `mysql_alter_table`.
   - `MaybeInjectCustomIndex` (#650 calls it; just stays where it is).
   - The `intf.parse` validation in `Metadata_modifier::add_indexes`
     (fail-fast UX win — diagnoses at SQL-parse, before any DDL
     work). Make sure #650's `parse_index_options` doesn't depend on
     the branch's call having or not having run.

### `IndexContext` use-count interaction

#650's "can't UNINSTALL while in use" check is a real correctness
improvement. The branch's runtime keeps `shared_ptr<TypeContext>` on
the loaded-index map but does not surface those as `IndexContext`
use-counts. After the merge work:

- Reading `dict_index_t::custom_index` (which owns
  `shared_ptr<const IndexContext>` via #650) already participates in
  #650's use-count check. So step #2 of the merge strategy (switch
  runtime to read from `dict_index_t::custom_index`) automatically
  fixes this — the runtime stops needing its own ref-keeping.
- If we still want a runtime-side handle for some operation, use
  `AcquireIndexContextClientManaged`.

### Estimated effort

~half a day. Most is deletion. The optimizer integration, the
per-row write hook, and the test extensions don't move.

### Note on #650's `parse_index_options` TODO

#650 leaves a TODO at `Custom_index::load` saying the load path
should also call `parse_index_options` (so options are populated when
re-opening an existing index from disk). The branch doesn't need
anything here, but worth knowing the limitation when investigating
"options not set" issues on restart.

## Rebase notes — 2026-06-23

Three things bit during this rebase that future-you should watch for.

### Silent comment drops

Git only flags `<<<<<` regions where both sides edited the same
lines. When `main` had added comments in a region the branch didn't
conflict-edit, the 3-way merge silently keeps the branch's older
snapshot — quietly dropping main's new comments. Hit this on:

- `villagesql/sdk/include/villagesql/abi/preview/index.h` — six
  struct-field doc comments dropped from `vef_index_profile_reg_t`
  and `vef_preview_index_profile_ext_desc_t` (originally added in
  #632).
- `villagesql/sdk/include/villagesql/preview/index_builder.h` — the
  full INDEX FUNCTION / INDEX PROFILE / EXTENSION REGISTRATION
  worked-example doc block (~95 lines, originally from #557) was
  silently replaced by the branch's earlier minimal example.

**After any rebase touching upstream-active files:** run
`diff <(git show <upstream>:<path>) <path>` for the headers and
restore any comment-only deletions. Conflict markers are not
sufficient to catch this.

### Branch-side comment additions that violate CLAUDE.md

The same diff sweep also surfaces *branch-side* additions that don't
match house style. Found:

- `#include "../types.h"  // vef_vdf_func_t, vef_type_t` in
  `index.h` — CLAUDE.md explicitly forbids IWYU-style comments on
  `#include` lines.
- A blank line splitting `<stdint.h>` from `"../types.h"` (would
  group them as separate include blocks; main keeps them together).
- Editorial suffix `— function-binding scaffold` on the
  `vsql::preview::index_profile capability` section heading —
  branch-side framing, not adopted upstream.

Rule: after restoring main's comment deletions, sweep `> //`
(branch-only) additions and check against CLAUDE.md before
keeping.

### BLOOM.veb naming bug

`villagesql/test-extensions/CMakeLists.txt` registered the bloom
test extension as `vsql_add_test_extension(bloom bloom)`, producing
`bloom.veb`. The bloom test suite does `INSTALL EXTENSION BLOOM`
(matching `manifest.json`'s `"name": "BLOOM"`), and VEB lookup is
case-sensitive on the filename. Fixed by changing the artifact name
to `BLOOM`, parallel to MVECTOR's `vsql_add_test_extension(mvector
MVECTOR)`.

Worth checking: any new branch-side test extension whose manifest
`name` uses different case than the `vsql_add_test_extension` second
argument will silently fail at `INSTALL EXTENSION` time. Match the
manifest name exactly when registering.



## The July wave — #720 landed, #721-#723 open (as of 2026-07-09)

All four tagged "Part of #386" (Debarun's tracking issue for the
overall index-access-interface work; still open, no visible
checklist). Individually small; collectively they fill three of the
gaps I catalogued in June.

### PR #720 — "Custom Index: C++ API" — **MERGED 2026-07-07 as `8bb18ca3494`**

SDK refactor. Not a new runtime capability; reshapes the C++-facing
API extensions target. This is the biggest touch to the branch's
extension source files.

Key deltas:

- **Nested-scope naming under a single `Index` class.**
  `IndexSupport::KNN` → `Index::Support::KNN`,
  `IndexStorage::HAS_ROW_REF` → `Index::Storage::HAS_ROW_REF`,
  `IndexOrdering::ASC` → `Index::Ordering::ASC`,
  `IndexStorageCtx<T>` → `Index::StorageCtx<T>`.
- **New `IndexScanKey` / `IndexScanDesc` classes** wrap the raw
  ABI structs at the extension boundary. Every extension callback
  changes signature: instead of `const vef_index_ctx_t*`,
  `vef_index_scan_desc_t*`, `vef_storage_col_data_t*`, callbacks
  now receive `const Index&`, `const IndexScanDesc&`,
  `IndexScanKey::KeyPartData*`, `IndexScanKey::KeyPartRef*`, etc.
- **New C-ABI callbacks on `vef_index_ctx_t`:**
  `col_ref_to_data_fn` and `col_data_to_ref_fn` — resolve
  extension-stable column refs to/from column data. Non-null when
  `VEF_INDEX_STORAGE_HAS_COLUMN_REF` is set in storage_props.
  Server-side implementations pending; #720 wires the field
  declarations only.
- **`profile_fn` / `helper_fn` signature change:** `void *args`
  → `const void *const *args`. Args is now an array of pointers.

**Branch impact:** applied 2026-07-09 (later) — see also
`Docs/CUSTOM_INDEX_HANDOFF.md` § "Update 2026-07-09 (later)".
All five branch extensions ported (MVECTOR, VSQL_VEC_CHAIN,
vsql-bloom-hidden-test, vsql-uniq-lower-test, plain `bloom`).
Callback signatures reshaped by the mapping documented in the
HANDOFF section. Not conceptually hard; broad mechanical diff.

**SDK holes filled locally.** Debarun's `Index` wrapper is
minimal — reference impl uses stubs that don't need most
accessors. Real extensions needed three:

- `Index::get_num_key_cols()` — mvector validates 1-column KNN.
- `Index::get_index_ref()` — used as runtime index name in
  mvector's mirror maps; hidden-table extensions need it too.
- `Index::get_table_storage_handle()` — the three hidden-table
  extensions have no other way to reach the handle through the
  new wrapper.

All three are one-line accessors added on `Index` in
`preview/index_builder.h`. Worth upstreaming as a small PR — the
reference impl doesn't need them, but any non-toy extension does.

**Branch runtime updates too.** `custom_index_runtime.cc` was
also touched:
- `dummy_profile_fn` third parameter: `void *` →
  `const void *const *` (match the new `vef_index_profile_fn`
  signature).
- Added `helper_fn` install — the ABI comment now says "Index
  profile helper call interface. Always non-NULL." Reusing the
  same dummy_profile_fn is fine since branch extensions don't
  call helpers via the server dispatcher.
- Left `col_ref_to_data_fn` / `col_data_to_ref_fn` NULL:
  contract requires them only when
  `VEF_INDEX_STORAGE_HAS_COLUMN_REF` is set, which no branch
  extension declares.

**`bloom_scan_begin` refactor note.** Old code passed
`scan_desc->keys[0].key_columns` (mutable `KeyPartData*`) to
`bloom_signature_for_storage`. `IndexScanKey::operator[]`
returns `const KeyPartData&`, so the subagent copied into a
local vector and passed its `.data()`. Slightly less efficient
than the original but avoids `const_cast`. If bloom perf ever
matters, either add a `data()`/`raw_columns()` accessor to
`IndexScanKey` or change the helper to take `const KeyPartData*`.

### PR #721 — "Custom Index: Load from InnoDB" — **OPEN**

Closes the `intf.load` gap I called out earlier. This is the
architectural piece that lets persisted custom-index storage
reconnect after server restart.

- **`Custom_index::load` split into two.** Old `load()` (a misnomer
  — it only allocated runtime state) is renamed **`attach()`**.
  New **`load()`** actually invokes the extension's `intf.load`
  to reconnect to persisted storage. Called from
  `dict_index_add_to_cache_w_vcol` (i.e. every time an index
  enters the dict cache, including all server-restart open-table
  paths).
- **Persistent storage-ref plumbing.** New
  `DD_INDEX_EXTENDED_STORAGE_REF` field on
  `dd::Index::se_private_data`. `Custom_index::save_ref` writes it
  at CREATE INDEX time; `attach` reads it back. The extension's
  opaque `storage_ref` (returned by `intf.create`) survives across
  restarts.
- **Construction now returns `dberr_t`.** Old `load()` was void;
  new `attach()` returns `dberr_t` and callers wire up error
  propagation. Multi-site callsite edits in
  `ha_innobase::create_index`, `dd_find_index`, and
  `ddl0ddl.cc::create_index`.
- **Various hardening:** `ut_a(is_custom(index))` at entry to
  `create`/`drop`; drop-path null-checks removed (the invariant
  now holds).

**Branch impact:** the branch's runtime doesn't call `intf.load`
either — this is a gap on both sides that #721 closes. Once merged,
the branch's persistent extensions (bloom_hidden, uniq_lower,
vec_chain) get storage-ref persistence "for free." Merge cleanup
becomes even more subtractive: any branch-side
"reload persisted options on restart" TODOs can be deleted.

### PR #722 — "Custom Index: Fix Multi-Segment Bug" — **OPEN**

Small `storage/innobase/villagesql/storage_abi.cc` fix inside
`vef_drop_one_segment`. `fseg_free_step_not_header` assumes the
segment header lives on one of the segment's own fragment pages —
true for segment 0 (which hosts the root/header), false for
segments 1..N whose headers live outside their fragment pages.
The old code called `_not_header` for every segment; the fix uses
`_not_header` only for segment 0 and `fseg_free_step` for the rest.
Also removes a redundant `mlog_write_ulint(FIL_PAGE_TYPE, ...)`
initialization (page type is set elsewhere).

**Branch impact:** none directly. Bites extensions using multi-
segment `preview_storage` (the page-level primitive path).
Branch's extensions all use `table_storage`. This is the bug that
would trip the "bitmap index over page_storage" work sketched in
Tier 3 #14 of `CUSTOM_INDEX_HANDOFF.md` — worth knowing when we
eventually revisit that.

### PR #723 — "Custom Index: Disable Partitioned Table" — **OPEN**

Explicit `"InnoDB: Custom index is not supported on partitioned
tables"` error at CREATE INDEX time, wired in
`ddl0ddl.cc::create_index` (via `dict_table_is_partition(table)`)
and `ha_innodb.cc::create_index` (via `dd_table_is_partitioned`).
Two new test cases in `index_errors.test`.

**Branch impact:** none — branch has never used partitioned tables.
Closes gap #10 from the June survey (via explicit rejection, not
support).

## Updated gap picture (2026-07-09)

Of the 13 gaps catalogued in June:

| # | Gap                                                     | Status                                              |
|---|---------------------------------------------------------|-----------------------------------------------------|
| 1 | DML `intf.insert`/`mark_delete`/`purge` invocation      | **Branch-only.** No upstream signal.                |
| 2 | Scan dispatch (`intf.scan_begin` etc.)                  | **Branch-only.** Only branch's KNN optimizer hook.  |
| 3 | `intf.load` never called                                | **Closing via #721** (open PR).                     |
| 4 | `profile_fn`/`helper_fn`/`key_len_fn` are stubs         | Not closed; #720 tweaked signature but stubs remain.|
| 5 | `MaybeInjectCustomIndex` not called from CREATE TABLE   | Branch's `dd_table_share.cc` call still needed.     |
| 6 | InnoDB row-layer iteration doesn't skip custom indexes  | **Branch-only.** 8 sites patched locally.           |
| 7 | ALTER-rebuild copy doesn't bridge to custom indexes     | **Branch-only.**                                    |
| 8 | No cost/cardinality/stats for custom indexes            | No signal.                                          |
| 9 | No FORCE INDEX / USE INDEX integration                  | No signal.                                          |
|10 | No partitioning support                                 | **Closed via #723** (open PR) — explicit rejection. |
|11 | No replication awareness                                | No signal.                                          |
|12 | No crash recovery / redo                                | No signal.                                          |
|13 | No transactional integration beyond create/drop         | No signal.                                          |

Debarun's cadence looks like ~1-2 small PRs per week, each closing
one concrete gap. At that rate the remaining branch-only gaps (#1,
#2, #6, #7 — the load-bearing runtime pieces) are still months of
upstream work away. **The branch's `custom_index_runtime.cc` and
related pieces will stay load-bearing for a while.** Treat the
merge-strategy section above as long-lived, not transitional.

## When to pick up the next rebase

Post-2026-07-09 (later) state:

- **#720 already absorbed.** All five branch extensions ported;
  three `Index::` accessors added to the SDK locally. Nothing
  more to do until Debarun accepts the accessor additions
  upstream (or restructures around them).
- **#721 next when it lands.** Unblocks server-restart correctness
  for branch's persistent extensions; lets us delete the "reload
  persisted options" TODOs. Small conflict surface (branch
  doesn't touch `Custom_index::load` internals).
- **#722 and #723** are branch-orthogonal but harmless to absorb.

Follow-up sweeps once the row-layer iteration skip lands upstream:
verify `row0sel.cc`, `row0log.cc`, `row0import.cc`, `dict0stats.cc`,
`dict0upgrade.cc` for the same pattern (FORCE INDEX / online ALTER /
ANALYZE / import-tablespace paths — not exercised by current tests
but potential landmines).

## Fixed-width ABI standardization: landed and reverted

PR **#755** ("Standardize ABI on fixed width types") landed
2026-07-07 and was **reverted the next day by #773** pending
"further investigation, perhaps when we tackle Windows." Main's
current state is *pre*-fixed-width.

Sanity checked our `abi/preview/table_storage.h` while we were
in the neighbourhood: **already uses `uint32_t`/`uint64_t`
throughout** (29 sites) and only uses `unsigned char *` for raw
byte-buffer pointers — matching the convention in `storage.h`.
Nothing to change today.

When Debarun re-attempts the cleanup (post-Windows work), our
headers may need a follow-up sweep — likely `unsigned char *` →
`uint8_t *` and any surviving stray `int`/`unsigned int` in
neighbouring branch headers. Small; worth doing in the same
window they finalize the ABI shape upstream.

## The late-July / August wave — 9 more PRs landed (surveyed 2026-08-02)

Since the 2026-07-09 rebase, ten commits touched indexing on
main. The July "open" trio (#721/#722/#723) landed within a day
plus a series of follow-ons. Most consequential is **#808 —
DML gap #1 is starting to close.**

### PR #808 — "Custom Index: Implement insert path" — **MERGED 2026-07-27**

This is the first upstream call to `intf.insert`. Closes the
first row of the gap table (previously "branch-only DML
invocation").

Debarun wires it in at `row_ins_sec_index_entry_low` (~line
3249, right before `offsets_heap` allocation — same site as the
branch's `!is_custom` skip at row0ins.cc:3627):

```cpp
using villagesql::innodb::Custom_index;
if (Custom_index::is_custom(index)) {
  return Custom_index::insert(index, thr_get_trx(thr)->id, entry,
                              dup_chk_only);
}
```

`Custom_index::insert` in `custom_index.cc`:

- Extracts `num_key_columns` key columns and `num_pk_columns` PK
  columns from the `dtuple_t*`, skipping duplicates the same way
  `row_build_row_ref` does for regular secondary indexes.
- Hands them to `intf.insert(index_ctx, storage_ctx, trx_id,
  key_columns.data(), pkey_columns.data(), &key_ref, error_msg,
  ...)`.
- Asserts `!dup_chk_only` — intrinsic tables are not supported.
- Has a TODO to persist `key_ref` "once `mark_delete()`/`purge()`
  are implemented" — those are the next PRs to expect.

**Branch impact — actually NO double-invocation.** Correction
2026-08-02: the branch's July skip at `row_ins.cc:3639`
(`!Custom_index::is_custom(node->index)` inside `row_ins()`)
sits **above** Debarun's #808 injection at
`row_ins_sec_index_entry_low:3253` in the call chain
(`row_ins` → `row_ins_index_entry_step` → `row_ins_index_entry`
→ `row_ins_sec_index_entry`/`_low`). The branch's skip fires
first and short-circuits before InnoDB descends into Debarun's
new call site.

So for standard `INSERT INTO t VALUES (...)`:
1. `handler::ha_write_row` → `write_row` → InnoDB → `row_ins`
   → **skipped at 3639** (Debarun's insert never fires).
2. `handler::ha_write_row` → `custom_index_after_write_row` →
   branch's `apply_to_custom_indexes` → branch's `intf.insert`
   → the extension sees exactly one insert.

**Alt-path caveats (not currently exercised by branch tests):**
- Online DDL row-log replay (`row0log.cc`) calls
  `row_ins_sec_index_entry_low` directly, bypassing the
  branch's skip. Debarun's #808 site would fire. But the
  branch already blocks inplace ALTER on tables with custom
  indexes (`alter_info_adds_custom_index` →
  `HA_ALTER_INPLACE_NOT_SUPPORTED` at `sql_table.cc:17728`),
  so online DDL isn't a real path.
- Intrinsic tables and FK check paths in `row0mysql.cc` call
  `row_ins_sec_index_entry` directly. Debarun's site fires
  but asserts `!dup_chk_only`; intrinsic tables set it, so
  those hit the assert. Not a real branch path either.

**mark_delete/purge** still branch-only until Debarun follows
up. Watch for those PRs — the branch's skip pattern likely
protects those too if he follows the same insertion-point
convention, but confirm on merge.

### PR #721 — "Custom Index: Load from InnoDB" — **MERGED 2026-07-10**

Closes gap #3. Behaves as documented in the "The July wave"
section above. Merge cleanup on the branch: delete any
"reload persisted options on restart" TODOs.

### PR #722 — "Custom Index: Fix Multi-Segment Bug" — **MERGED 2026-07-10**
### PR #723 — "Custom Index: Disable Partitioned Table" — **MERGED 2026-07-10**

Both landed as expected. #722 branch-orthogonal (matters when
Tier 3 bitmap-on-page_storage happens); #723 explicit rejection
of custom indexes on partitioned tables (no branch impact).

### PR #906 — "Add unload hook for storage cleanup" — **MERGED 2026-07-30**

New optional ABI method **`vef_type_index_unload_func_t unload`**
on `vef_type_index_intf_t` (alongside `create`/`drop`/`load`/
`insert`/`mark_delete`/`purge`). Called from `Custom_index::free_all`
on cache eviction, table close, or shutdown. Semantically:
`drop` removes storage, `unload` releases in-memory resources
for that storage handle.

Site is null-guarded (`if (intf.unload != nullptr)`) so
extensions that don't declare one are fine at the ABI level.

**Branch impact — actually nothing to do.** The SDK's
`GlobalBuilder::build()` **auto-installs** `intf.unload =
UnloadWrapper<Context>::invoke` at line 1081 of
`preview/index_builder.h`. The wrapper tears down the
extension arena. There is a matching TODO in the SDK
("Expose unload as a builder-configurable hook (e.g.
`.unload<F>()`)") noting no extension-facing hook exists yet.
All five branch extensions register via `make_index_type<Name,
Ctx>()...global().build()`, so they already have the correct
unload wrapper installed automatically. **No stubs needed.**

Runtime-side caveat: `custom_index_runtime.cc` uses its own
`RuntimeArena` (a `std::vector<std::unique_ptr<unsigned
char[]>>` owned by the `LoadedIndex`), which is destructed
naturally when the map entry is erased. The runtime deliberately
does NOT call `intf.unload` — if it did, the SDK's
`UnloadWrapper` would `reinterpret_cast` the runtime's fake
`vef_storage_arena_t*` to `Arena*` and call `~Arena()` on
garbage. Current arrangement is safe; keep it that way.

### PR #807 — "Custom Index: Fix restart issue" — **MERGED 2026-07-17**

Two invariants Debarun didn't get right on the first #721 pass:

1. `Custom_index::load()` should be **skipped during the index
   creation path** (it should fire only on reopen — create has
   its own `intf.create` call).
2. The extension's opaque `storage_ref` has to be **restored
   into the storage context** after load or it gets overwritten
   with 0 in the DD on the next write.

Both live entirely on Debarun's side. Branch shouldn't be
affected but worth confirming with the persistent extensions
(bloom_hidden, uniq_lower, vec_chain).

### PR #873 — "Custom Index: Fix leak in SDK C++ wrapper" — **MERGED 2026-07-24**

Arena leak fix in `preview/index_builder.h` +
`preview/storage_api.h` + `preview/storage_builder.h`. Moves
Arena allocation into the extension arena instead of process
heap. Small. Tagged "Part of #822" (sanitizer cleanliness),
not #386.

Branch extensions include these headers so we pick this up
automatically on rebase.

### PR #897 — "Fix compilation error in release mode" — **MERGED 2026-07-28**

Release-mode `-Werror=empty-body` fix in a `ut_ad()` else branch
inside `custom_index.cc`. Purely a build fix; no ABI impact.

### PR #907 — "Fix cursor leak in custom column insert" — **MERGED 2026-07-29**

Column-side, not index. `custom_column.cc` cursor leak fix.
No branch impact.

### PR #809 — "Custom Index: Load index profiles" — **MERGED 2026-08-03 as `6be6d8838`**

Loads profile metadata for key columns and threads it onto
`Custom_index::key_profiles_` (a `std::vector<shared_ptr<const
IndexProfileDescriptor>>` per index instance). Adds
`Custom_index::add_profile(index, profile, key_pos)` called from
`create_index` (both `ddl0ddl.cc` and `ha_innodb.cc` paths) and
from `dd_find_index`. Adds
`AcquireIndexProfileDescriptorClientManaged` (parallel to the
existing `AcquireTypeContextClientManaged`) so the descriptor
outlives table-share invalidations.

**Standalone effect:** none on runtime behavior. This is
plumbing — the profile descriptors sit on `Custom_index` waiting
for callers. #927 (below) is the first caller.

**Branch impact when it lands:** the "watch for" note from the
2026-08-02 draft is answered: #809 alone doesn't turn our stub
`dummy_profile_fn` into a real dispatcher. #927 does that.
So the branch's `.helper_fn = dummy_profile_fn` install stays
until we rebase past #927.

## The August profile-dispatch pair — #926 and #927 (both OPEN 2026-08-03)

These two together retire the branch's `profile_fn` / `helper_fn`
/ `key_len_fn` stubs on Debarun's `Custom_index` (not on the
branch's `LoadedIndex` — separate objects). They also settle the
ABI shape for how the server invokes extension-registered profile
functions.

### PR #926 — "Custom Index: SDK profile functions" — **OPEN**

Two ABI additions plus a static-typing tightening:

- **New `key_pos` parameter on `vef_index_profile_fn`.**
  Signature was `(index_ref, fn_id, args, nargs, result)`; now
  `(index_ref, key_pos, fn_id, args, nargs, result)`. The
  extension implementation gets to know which key column is
  driving the call, so profile bindings can be per-column (an
  index like HNSW-L2 on `col_a` and HNSW-cosine on `col_b`
  would use different profiles per column).
- **New `vef_protocol_t protocol` field on
  `vef_index_profile_fn_binding_t`.** Records which VDF ABI
  version the bound function expects (`vef_invalue_v1_t` vs
  `vef_invalue_t`). #927 uses it to select the right invocation
  path.
- **`Index::profile(...)` and `Index::helper(...)` static_assert
  that args are `vef_storage_col_data_t`.** Profile functions
  can now only operate on column data — no scalar arguments,
  no arbitrary types. Constrains what VDFs can bind as profile
  functions. Distance VDFs (custom-column input) fit; scalar
  helpers wouldn't.
- **Two new `Index::` accessors:** `get_num_key_cols()` and
  `get_max_col_len(key_pos)`. Both duplicate accessors the
  branch added locally. On rebase, the branch's local additions
  should be deleted (Debarun's now-canonical versions take
  their place).

Also adds a `protocol` field on `IndexFunctionDesc` for the C++
builder side.

**Branch impact:**
- **Signature break on our `dummy_profile_fn`.** Currently
  `void (vef_index_ref_t, uint32_t, const void*const*,
  uint32_t, void*)`. Rebase requires inserting `uint32_t key_pos`
  before `fn_id`.
- **Delete two of our three local `Index::` accessors.**
  `get_num_key_cols` collides with Debarun's; the
  `get_primary_max_col_len` variant we added is superseded by
  his `get_max_col_len`. Only `get_index_ref` and
  `get_table_storage_handle` remain as branch-local additions
  worth upstreaming.

### PR #927 — "Custom Index: Profile callbacks" — **OPEN**

Server-side dispatcher. Replaces `vef_index_profile_stub` /
`vef_index_max_key_len_stub` in `custom_index.cc` with real
implementations:

- `vef_index_profile_fn_impl` and `vef_index_helper_fn_impl`
  look up the profile via `Custom_index::profile_for_key(
  key_pos)` (added by #809), find the binding by `fn_id`, and
  invoke the VDF via `vef_vdf_args_t` / `vef_vdf_result_t`.
- Uses the indexed column's `TypeContext` (via
  `dict_col_t::custom_column->type_context()`) to populate
  `vef_invalue_t::type_params` — so bound VDFs see the column
  parameters (dimension, element type, etc.) at invocation
  time.
- `vef_index_max_key_len_impl` returns the actual max storage
  length for the requested column via
  `dict_index_t::get_field(key_pos)->col->len`, honoring
  prefix indexes.
- Restrictions: `protocol >= VEF_PROTOCOL_3` and return type
  `VEF_TYPE_REAL` are asserted. Distance VDFs fit both. Only 8
  args max (`MAX_PROFILE_FN_ARGS`).

**Branch impact:**
- **The stubs are no longer stubs on Debarun's side** —
  `Custom_index::index_ctx()` gets real function pointers
  installed by `init_index_ctx`. Extensions calling
  `Index::profile(key_pos, fn_id, ...)` from inside their own
  callbacks now actually invoke the bound VDF.
- **The branch's `LoadedIndex.ctx` still has stubs**, because
  the branch's runtime allocates its own `vef_index_ctx_t` and
  installs `.helper_fn = dummy_profile_fn`. Same
  double-lifecycle story as `create`/`drop`/`insert`. The
  runtime shrink (delete `LoadedIndex`, use
  `dict_index_t::custom_index` directly) is the same subtractive
  cleanup pending on the branch.

**Neither PR closes gap #4 fully.** Gap #4 was "profile_fn /
helper_fn / key_len_fn are stubs." #927 replaces the stubs
where the server owns them. The branch's runtime still has its
own copies until we do the shrink.

**Neither PR touches scan dispatch (gap #2).** These invoke
profile-bound VDFs from server-internal callsites (e.g. cost
estimation, ordering checks that Debarun hasn't wired yet).
The extension calling `intf.scan_begin` — the branch's
optimizer routes through this — is still branch-owned.

### Ordering fact worth remembering

Debarun's stack for profile functions is arriving as
**#809 → #926 → #927**:
- #809 stores profile descriptors on `Custom_index`.
- #926 adjusts the ABI so `key_pos` and `protocol` are
  available at dispatch time.
- #927 wires the dispatcher.

Each on its own is inert. Together they turn Debarun's
`Custom_index::index_ctx()` from "callable but returns garbage"
into a working profile-function dispatcher.

## Umbrella issue status

- **#386** ("[Server]: Index access interface") — still open,
  still assigned to `@villagedeb`. All the "Custom Index: …"
  PRs above are tagged Part of #386.
- **#822** ("[Server]: Sanitizer cleanliness") — new umbrella
  for sanitizer runs. #873/#906/#907 tagged here instead of
  #386 since they're memory-management fixes surfaced by
  sanitizer runs, not new capability.

## Updated gap picture (2026-08-03)

Of the 13 gaps catalogued in June:

| # | Gap                                                     | Status                                               |
|---|---------------------------------------------------------|------------------------------------------------------|
| 1 | DML `intf.insert` invocation                            | **Closing via #808** (merged). No double-fire in practice: branch's row_ins.cc:3639 skip pre-empts. |
| 2 | Scan dispatch (`intf.scan_begin` etc.)                  | Still branch-only.                                   |
| 3 | `intf.load` never called                                | **CLOSED via #721** (merged 2026-07-10).             |
| 4 | `profile_fn`/`helper_fn`/`key_len_fn` are stubs         | **Closing via #809 + #926 + #927** (809 merged 2026-08-03; 926/927 open). Real dispatcher lands on Debarun's `Custom_index`; branch's `LoadedIndex.ctx` still stubs its own until the runtime shrink. |
| 5 | `MaybeInjectCustomIndex` not called from CREATE TABLE   | No change.                                           |
| 6 | InnoDB row-layer iteration doesn't skip custom indexes  | Branch still owns 8-site skip; #808 routes around it |
|   |                                                         | at insert but doesn't add the general skip.          |
| 7 | ALTER-rebuild copy doesn't bridge to custom indexes     | No change; branch-only.                              |
| 8 | No cost/cardinality/stats for custom indexes            | No change. Branch's `handler::read_cost` PK-substitute is still the placeholder. |
| 9 | No FORCE INDEX / USE INDEX integration                  | No change.                                           |
|10 | No partitioning support                                 | **CLOSED via #723** (explicit rejection).            |
|11 | No replication awareness                                | No change.                                           |
|12 | No crash recovery / redo                                | Partially: #807 fixed restart-time storage_ref.      |
|13 | No transactional integration beyond create/drop         | Partially: #906's unload hook is a small piece.      |

Debarun's cadence stayed steady into August. In the ~1 week
since the 2026-07-27 wave, #809 landed and the profile-dispatch
pair (#926 + #927) opened. Gap #4 is the third gap to start
closing (after #1 insert, #3 load).

`mark_delete`/`purge` invocation is still the next obvious PR
to expect — Debarun's own TODO in `Custom_index::insert` calls
it out. Watch for those.

## What to do on the branch (immediate)

1. **~~Add an `intf.insert` skip~~** — false alarm; the branch's
   July skip at `row_ins.cc:3639` already sits above Debarun's
   #808 injection point in the call chain and prevents it from
   firing. Verified 2026-08-02. No double-invocation to fix.
   *Corollary:* the full runtime shrink is genuinely a bigger
   project than "flip a skip" because it requires unifying
   the branch's `LoadedIndex.storage` with Debarun's
   `Custom_index::storage_ctx` (currently two independent
   storages per index — see "Concrete conflicts now active"
   section above). Defer until we're doing all three DML
   paths together and can also solve storage unification.
2. **~~Add `.unload` stubs~~** — false alarm; the SDK's
   `GlobalBuilder::build()` auto-installs `UnloadWrapper` so
   every extension already has a correct `intf.unload`.
   Verified 2026-08-02. Nothing to do.
3. **Delete restart-related TODOs** on the branch (#721+#807
   closed the gap).
4. **Confirm persistent extensions** (bloom_hidden, uniq_lower,
   vec_chain) survive server restart — #721+#807 should make
   this work; verify.
5. **Pending #926 merge**: signature-update the branch's
   `dummy_profile_fn` from
   `(vef_index_ref_t, uint32_t, const void*const*, uint32_t, void*)`
   to `(vef_index_ref_t, uint32_t /*key_pos*/, uint32_t /*fn_id*/,
   const void*const*, uint32_t, void*)`. Trivial signature
   surgery; will be a compile-break otherwise.
6. **Pending #926 merge**: delete two of our three local
   `Index::` accessors — `get_num_key_cols()` and the
   `get_primary_max_col_len` variant collide with Debarun's
   canonical additions. `get_index_ref()` and
   `get_table_storage_handle()` remain branch-local, worth
   upstreaming.
7. **Pending #927 merge**: no immediate action, but note that
   the branch's `LoadedIndex.ctx` still installs
   `dummy_profile_fn` / `dummy_key_len_fn`. Debarun's real
   dispatchers live in `custom_index.cc` and install onto
   `Custom_index::index_ctx()`, not our runtime's ctx. The
   runtime shrink (retire `LoadedIndex`, use
   `dict_index_t::custom_index` directly) is when the branch
   picks these up too.
