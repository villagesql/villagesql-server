# Custom Index Work — Continuation Handoff

This document captures the working state of the table-storage-backed
custom-index architecture so the next person can pick it up. Pair with
`Docs/CUSTOM_INDEX_CURRENT_STATE.md` for the conceptual overview;
this file focuses on what's open and what to do next.

For absorbed upstream PRs and the still-pending follow-up work (most
importantly the merge strategy now that `@villagedeb`'s #650 wired
custom-index lifecycle into InnoDB's dict layer), see
`Docs/CUSTOM_INDEX_MERGED_PRS.md`.

## Where things stand

A graph-shaped, table-storage-backed custom index (`vec_chain`) is fully
working end-to-end:

- Custom type registration: `VSQL_VEC_CHAIN(N)`.
- Server-driven hidden-table materialization with secondary indexes.
- Per-row maintenance (insert, delete) with chain linking via
  captured grefs.
- Tombstones on delete so chain links stay valid.
- KNN scan via gref traversal.
- `ORDER BY vec_chain_l2_distance(v, q) LIMIT k` routed through the
  hypergraph optimizer to the custom index, validated by a
  scan-count assertion.

Two other extensions exercise the same backend:
`vsql-bloom-hidden-test` (point-lookup index) and
`vsql-uniq-lower-test` (write-veto for uniqueness). Together they
cover most of the architectural surface.

Branch: `tomas/experimentation`. Single dev commit on top of `main`.

## Update 2026-06-12 — rebased through Debarun's #558-632 stack

The handoff originally named #558-561 as in-flight. All have landed
plus three follow-ups: #630 (extension qualification), #631
(TABLE_SHARE metadata), #632 (helper profile functions). The branch
has been rebased on top of all of them.

Key absorbed changes:

- `extension_data` -> `capability_config` rename (#559).
- `vef_signature_t signature` replaces inlined `return_type`/
  `param_types[]`/`num_params` in profile bindings (#632).
- `ordering` bitmask (combinable `ASC | DESC`) replaces
  `ordering_asc` bool (#632).
- `helpers[]` + `helper_fn` callback added — profile functions split
  into user-visible (functions) and implementation-internal (helpers)
  (#632).
- `IndexTypeDescriptor` / `IndexProfileDescriptor` live in victionary
  (#558).
- `find_default_profile` resolves a default profile per column when
  no `WITH(profile=…)` is named, using
  `resolve_unique_descriptor<>` (#632).

Branch-side adaptations:

- MVECTOR and vec_chain now register an index profile binding their
  L2-distance VDF (Tier 2 #5 prerequisite is now met — see below).
- `Metadata_modifier::add_indexes` was rewritten on the branch
  during the rebase to adopt #632's structural shape; we also added
  the `validate_custom_index_params` helper (calls `intf.parse` at
  SQL-parse time for fail-fast UX). The `villagesql_custom_index_proceed`
  DBUG flag is *not* honored inside `add_indexes` — it bypasses the
  parser "not yet implemented" guard only, as Debarun's
  `vsql-index-test` testsuite expects. The branch's runtime files
  (`custom_index_runtime.cc::custom_index_pre_create_storage` and
  `alter_info_adds_custom_index`) still honor the flag at their own
  dispatch points.
- `find_default_profile` was softened (returns success + sets a new
  `out_has_custom_type=false` flag) when the indexed column has no
  custom type, so bloom-on-VARCHAR and uniq_lower-on-TEXT still work.
  Worth raising upstream as a small PR — anyone landing a third
  non-vector custom index would hit the same wall otherwise.
- Four `assert(!profile_extension_name.empty())` sites in
  `metadata_modifier.cc` were softened to `continue` / no-op, since
  profile is now optional for non-custom-type columns.

Tier 2 #5 status: the prerequisite is met. The function-name
allowlist in `GetMVectorDistanceFunction` can now be replaced by a
victionary lookup against `index_profile_descriptors`. Estimated
half-day of work; see CUSTOM_INDEX_HANDOFF.md Tier 2 #5 for the
recipe.

Profile-VDF auto-registration: the SDK comment in `vsql-index-test`
says profile functions get "registered automatically when the
profile is registered." That is aspirational —
`villagesql/veb/register.cc` stores bindings on
`IndexProfileDescriptor` but does not push them into
`victionary.funcs()`. MVECTOR and vec_chain currently double-register
their distance VDF (once as a `make_func<...>()` for SQL visibility,
once as a `make_index_function<...>()` bound into the profile). When
auto-registration lands upstream, drop the duplicate `make_func`.

## Update 2026-06-23 — rebased through #650 (DDL InnoDB) and #651

Both PRs from the previous "upcoming" doc landed and the branch was
rebased onto `ebb782cceb7` (the tip is `vef: Add Statement Events
Capability (#645)`).

PRs absorbed since the 2026-06-12 update:

- **#650** (`07b16f2dda9`) — Custom Index: DDL InnoDB. Wires the
  custom-index ABI into InnoDB's dict layer (`Custom_index` per
  `dict_index_t`, `KEY::custom_index_context`, `intf.create`/`drop`
  from inside InnoDB).
- **#651** (`a197b2ac089`) — test corrections; no branch impact.
- **#638** + **#664** + **#647** — variable-length custom types,
  v4 protocol gating, `veb_register_type_v4.cc`. Branch's MVECTOR
  and VSQL_VEC_CHAIN go through this path. No conflict, but worth
  re-running the install-time tests for both types.
- **#645** — Statement Events Capability. Orthogonal to custom
  indexes.

Rebase outcome: branch builds, but the structural overlap with #650
predicted in `Docs/CUSTOM_INDEX_MERGED_PRS.md` is now active.
`intf.create`, `intf.drop`, and `intf.parse` all fire twice for
every custom index — once from the branch's runtime, once from
#650's dict-layer hooks. Test failures expected until the merge
strategy in `MERGED_PRS.md` is applied. Estimated half-day of
mostly-subtractive work.

Also patched: #650 left a gap in InnoDB's index iteration —
`row_ins` / `row_upd` / `row_undo_mod*` / `row_undo_ins` / `row_purge`
walk `dict_table_t->indexes` and treat each non-FTS entry as a
B-tree, asserting on custom indexes' `page == FIL_NULL`. Branch
added an `!Custom_index::is_custom(index)` skip at eight sites
(`row0ins.cc`, `row0upd.cc`, `row0umod.cc`, `row0uins.cc`,
`row0purge.cc`) alongside the existing DICT_FTS check, each marked
`TODO(villagesql-indexing): upstream to #650 follow-up`. Found in
two waves — DML paths via the first INSERT crash, rollback/purge
paths via uniq_lower's duplicate-reject. Other iteration sites
(`row0sel`, `row0log`, import/stats/upgrade) may need the same
when FORCE INDEX / online ALTER / ANALYZE start exercising them.

See `Docs/CUSTOM_INDEX_MERGED_PRS.md` for the full merge plan plus
rebase-trap notes (silent comment drops, IWYU rule, BLOOM.veb
artifact-naming bug) — three things that bit during this rebase and
are worth watching on the next.

## Update 2026-07-09 — survey of #720–#723

Debarun opened the next wave. All four PRs are tagged "Part of
#386" (the umbrella index-access-interface tracking issue,
assigned to him, still open).

- **#720 (MERGED 2026-07-07)** — Custom Index: C++ API. SDK
  refactor: `IndexSupport::KNN` → `Index::Support::KNN` etc.
  (single `Index` class hosting all the enums + `StorageCtx<T>`),
  new `IndexScanKey`/`IndexScanDesc` classes wrapping the raw
  ABI structs at the extension boundary, callback signatures
  reshaped. Also adds two new server-provided callbacks on
  `vef_index_ctx_t` (`col_ref_to_data_fn`, `col_data_to_ref_fn`),
  and `profile_fn`/`helper_fn` args go from `void *` to
  `const void *const *`. Next rebase will touch every branch
  extension source file — mechanical but broad.
- **#721 (OPEN)** — Custom Index: Load from InnoDB. Closes the
  `intf.load` gap. Splits old `Custom_index::load` into `attach`
  (allocate runtime state) + new `load` (actually calls
  `intf.load` on the extension). Persists extension's opaque
  `storage_ref` via new `DD_INDEX_EXTENDED_STORAGE_REF` field in
  `dd::Index::se_private_data`, so custom-index storage
  reconnects after server restart. Branch's persistent
  extensions (bloom_hidden, uniq_lower, vec_chain) get this
  functionality for free once landed; delete any branch-side
  "reload persisted options on restart" TODOs.
- **#722 (OPEN)** — `storage_abi.cc::vef_drop_one_segment`
  multi-segment fix (`fseg_free_step_not_header` only applies to
  segment 0; use `fseg_free_step` for the rest). Branch-orthogonal
  today; relevant when the Tier 3 #14 bitmap-on-page_storage
  work happens.
- **#723 (OPEN)** — Explicit rejection of custom indexes on
  partitioned tables. No branch impact.

Updated gap picture (of the 13 gaps catalogued 2026-06-23):

- #3 (`intf.load` never called) — closing via #721.
- #10 (partitioning) — closing via #723 (explicit rejection).
- Others unchanged.

Branch-only load-bearing gaps that no upstream PR addresses:
#1 DML invocation, #2 scan dispatch, #6 row-layer iteration
skips (branch's 8-site patch), #7 ALTER-rebuild bridging.
Also-open with no signal: #4 profile_fn stubs, #5 CREATE TABLE
call site, #8 stats, #9 FORCE INDEX, #11 replication, #12
recovery, #13 transactional integration.

At Debarun's observed cadence (~1-2 small PRs/week, each closing
one concrete gap), the branch's `custom_index_runtime.cc` and the
`row0*.cc` iteration skips will remain load-bearing for months.
Treat the merge-strategy plan in `CUSTOM_INDEX_MERGED_PRS.md` as
long-lived, not transitional.

See `Docs/CUSTOM_INDEX_MERGED_PRS.md` § "The July wave" for the
line-level survey.

## Update 2026-07-09 (later) — rebased through #720 SDK refactor

Rebased onto `02abdce276a` (`Add release only tests (#783)`).
#720 (Custom Index: C++ API) is the one that bites — mechanically
rewrites every extension's callback signatures. All five branch
extensions ported: `mvector`, `bloom`, `vsql-uniq-lower-test`,
`vsql-bloom-hidden-test`, `vsql-vec-chain-test`.

### Mapping applied to every extension

Callback signatures were reshaped extension-side. The mapping:

- `IndexStorageCtx<T>` → `Index::StorageCtx<T>`
- `IndexSupport::KNN` → `Index::Support::KNN`
- `IndexStorage::HAS_ROW_REF` → `Index::Storage::HAS_ROW_REF`
- `IndexOrdering::ASC` → `Index::Ordering::ASC`
- `const vef_index_ctx_t*` → `const Index&` (all callbacks)
- `vef_storage_space_ref_t` → `Space::Ref`
- `vef_storage_trx_ref_t` → `Segment::TrxRef`
- `vef_storage_ref_t` → `Index::StorageRef`
- `vef_storage_col_data_t*` → `IndexScanKey::KeyPartData*`
- `vef_storage_col_ref_t*` → `IndexScanKey::KeyPartRef*`
- `vef_index_cursor_ref_t` → `Index::Cursor`
- `vef_index_cursor_op_t` → `Index::CursorOp` (with `::Next`,
  `::Prev`)
- `vef_storage_mtr_ref_t` → `MtrCtx::Ref`
- `const vef_index_scan_desc_t*` → `const IndexScanDesc&`
- `VEF_STORAGE_EMPTY_COLUMN_REF` → `IndexScanKey::EMPTY_REF`
- `scan_desc->scan_type != VEF_INDEX_SCAN_TYPE_KNN` →
  `!scan_desc.is_knn()` (similarly `is_range()`, `is_point()`)
- `scan_desc->keys[i]` → `scan_desc[i]` (returns `IndexScanKey`)
- `index_ctx->num_key_columns` → `index.get_num_key_cols()`
- `index_ctx->num_primary_key_columns` →
  `index.get_primary_num_key_cols()`
- `index_ctx->index_ref` → `index.get_index_ref()`
- `index_ctx->table_storage_handle` →
  `index.get_table_storage_handle()`

The `.table_storage<>()` callback (e.g.
`vec_chain_index_table_storage_def`) stays on its raw-ABI
signature per the SDK's `TableStorageDefWrapper` contract.

### SDK additions needed to complete the port

Debarun's `Index` wrapper is minimal (reference impl uses stubs
that don't need most accessors). Real extensions needed three
methods, added locally:

- `Index::get_num_key_cols()` — mvector's insert validates the
  1-column KNN invariant against `num_key_columns`.
- `Index::get_index_ref()` — mvector uses the opaque ref as the
  runtime index name for its mirror maps; hidden-table extensions
  need it too.
- `Index::get_table_storage_handle()` — the three hidden-table
  extensions have no other way to reach the handle through the
  new wrapper.

All three are single-line accessors on `Index` (`preview/
index_builder.h`). Worth upstreaming as a small PR — the reference
impl doesn't need them but any non-toy extension does.

### Branch runtime signature updates

`custom_index_runtime.cc` was also touched by #720's `profile_fn`
signature change:

- `dummy_profile_fn` third parameter: `void *` → `const void *const *`.
- Added `helper_fn` install: `loaded->ctx.helper_fn =
  dummy_profile_fn;` — the ABI now contractually requires
  `helper_fn` non-NULL (comment: "Index profile helper call
  interface. Always non-NULL."). Reusing the same dummy fn is
  fine since none of the branch extensions call helpers via the
  server dispatcher.
- `col_ref_to_data_fn` / `col_data_to_ref_fn` intentionally left
  NULL — ABI requires them only when
  `VEF_INDEX_STORAGE_HAS_COLUMN_REF` is set. Branch extensions
  all use `HAS_ROW_REF + REF_LOOKUP`, none declare
  `HAS_COLUMN_REF`, so NULL is contract-conformant.

### Fixed-width ABI standardization (#755) was reverted

PR #755 ("Standardize ABI on fixed width types") landed 2026-07-07
and was **reverted the next day** by #773 pending "further
investigation, perhaps when we tackle Windows." Main's current
state is *pre*-fixed-width, so nothing to align to today.

Sanity check while we were there: our
`abi/preview/table_storage.h` **already uses `uint32_t`/`uint64_t`
throughout** (29 sites) and only uses `unsigned char *` for raw
byte buffers — matching the convention in the neighbouring
`storage.h`. Nothing to change today. When Debarun re-attempts
the fixed-width cleanup (post-Windows work), our headers may need
a follow-up sweep (probably `unsigned char *` → `uint8_t *`).

## Update 2026-08-02 — rebased through the late-July wave

Nine more indexing PRs landed since 2026-07-09. Full survey in
`Docs/CUSTOM_INDEX_MERGED_PRS.md` § "The late-July / August
wave". Highlights that shape the branch's near-term work:

- **#808 (MERGED 2026-07-27) — insert path.** First upstream
  call to `intf.insert`, wired in at
  `row_ins_sec_index_entry_low:3253`. **Turns out the branch's
  July skip at `row_ins.cc:3639` sits above #808's site in the
  call chain and pre-empts it** (`row_ins` → `_step` → `entry`
  → `sec_entry_low`; our skip in `row_ins()` returns before
  descending). So for standard INSERT there's no
  double-invocation. Alt-paths (row-log replay, intrinsic
  tables, FK check) not exercised by branch tests — the branch
  blocks inplace ALTER on custom-index tables anyway. Verified
  2026-08-02.
- **#721/#722/#723** all merged as expected. Restart
  correctness now works upstream (#721+#807); partitioned
  tables explicitly rejected.
- **#807 (MERGED 2026-07-17) — restart-issue fix.** Two
  invariants Debarun corrected on the #721 pass: skip
  `Custom_index::load()` during index creation; restore
  `storage_ref` into storage context after load. Purely
  server-side; branch shouldn't be affected but worth
  confirming with the persistent extensions.
- **#906 (MERGED 2026-07-30) — new `intf.unload` ABI callback.**
  Optional; null-guarded at the call site. Turns out branch
  extensions need no changes: the SDK's `GlobalBuilder::build()`
  auto-installs `intf.unload = UnloadWrapper<Ctx>::invoke`
  (there's a TODO in the SDK to expose `.unload<F>()` as a
  builder hook, but the default wrapper does the right thing —
  tears down the extension arena). All five branch extensions
  register via `make_index_type<...>()...global().build()`, so
  they already have the correct unload wrapper. Runtime side
  (`custom_index_runtime.cc`) deliberately does NOT call
  `intf.unload` — the SDK wrapper would `reinterpret_cast` the
  runtime's fake `vef_storage_arena_t*` and crash on a garbage
  `~Arena()`. Current arrangement is safe; keep it that way.
- **#873** — SDK Arena leak fix in `preview/index_builder.h` /
  `preview/storage_api.h` / `preview/storage_builder.h`.
  Picked up automatically on rebase.
- **#897** — release-mode `-Werror=empty-body` fix in
  `custom_index.cc`. No ABI impact.
- **#907** — column-side cursor leak. No branch impact.

**Open (2026-08-02):** #809 "Custom Index: Load index
profiles." Loads profile metadata for key columns so InnoDB
can invoke profile callbacks (distance, compare). If it turns
our runtime's stub `dummy_profile_fn` into a real dispatcher,
our runtime's `.helper_fn = dummy_profile_fn` install may
become deletable. Watch the diff when it merges.

### Immediate branch work triggered by this wave

1. **~~Add `intf.insert` skip~~** — false alarm; branch's
   July skip at `row_ins.cc:3639` already pre-empts #808's
   injection point. Verified 2026-08-02. No double-invocation
   in practice for the branch's exercised paths.
2. **~~Add `.unload` stubs~~** — false alarm; SDK's
   `GlobalBuilder::build()` auto-installs `UnloadWrapper` (see
   #906 note above). Verified 2026-08-02. Nothing to do.
3. **Delete restart-related TODOs** on the branch (#721+#807
   closed the gap).
4. **Confirm persistent extensions** (bloom_hidden,
   uniq_lower, vec_chain) survive server restart — #721+#807
   should make this work; verify.

### Updated gap picture (2026-08-02)

Of the 13 gaps catalogued in June:

- **#1 DML insert** — closing via #808 (no double-fire; skip pre-empts).
- **#3 `intf.load`** — CLOSED via #721.
- **#10 partitioning** — CLOSED via #723 (rejection).
- **#12 recovery** — partial (storage_ref restore).
- **#13 transactional integration** — partial (unload hook).
- All others: no change. Branch still owns #2 (scan
  dispatch), #6 (row-layer iteration skips 8-site patch), #7
  (ALTER-rebuild bridging).

Debarun's cadence held: 5-6 indexing PRs in ~4 weeks
including one substantial capability (#808). Expect
`mark_delete`/`purge` invocation next — his own TODO in
`Custom_index::insert` calls it out.

## Update 2026-08-03 — #809 landed; #926/#927 opened

Debarun's next tranche is the profile-function dispatch pair.
#809 (plumbing) landed today; #926 (SDK ABI change) and #927
(InnoDB dispatcher) opened for review. Together they retire
the branch's `profile_fn`/`helper_fn`/`key_len_fn` stubs on
Debarun's `Custom_index` (not on the branch's `LoadedIndex` —
separate objects; runtime shrink still pending).

- **#809 (MERGED 2026-08-03 as `6be6d8838`)** — Custom Index:
  Load index profiles. Threads
  `Custom_index::key_profiles_[key_pos]` from the DDL layer.
  No runtime behavior change on its own; enables #927.
- **#926 (OPEN)** — Custom Index: SDK profile functions. ABI
  changes:
  - `vef_index_profile_fn` gains a `uint32_t key_pos`
    parameter (before `fn_id`). Our `dummy_profile_fn` signature
    will need updating at rebase time.
  - `vef_index_profile_fn_binding_t` gains a
    `vef_protocol_t protocol` field for the VDF invocation ABI
    selection.
  - `Index::profile()`/`helper()` builder args restricted via
    static_assert to `vef_storage_col_data_t`.
  - **Adds two `Index::` accessors that duplicate our local
    additions:** `get_num_key_cols()` and `get_max_col_len(key_pos)`.
    On rebase, delete our locally-added versions. Only
    `get_index_ref()` and `get_table_storage_handle()` remain
    branch-local (worth upstreaming as a small PR).
- **#927 (OPEN)** — Custom Index: Profile callbacks. Real
  server-side dispatchers replacing `vef_index_profile_stub`
  and `vef_index_max_key_len_stub`. Uses
  `Custom_index::profile_for_key(key_pos)` (from #809) →
  `IndexProfileDescriptor::functions()`/`helpers()` → finds
  `fn_id` binding → invokes VDF via
  `vef_vdf_args_t`/`vef_vdf_result_t`. Restricted to
  `protocol >= VEF_PROTOCOL_3` and `VEF_TYPE_REAL` return.

### Branch impact when the pair lands

- **#926 forces two edits:**
  1. `dummy_profile_fn` signature in
     `villagesql/sql/custom_index_runtime.cc` — insert
     `uint32_t key_pos` between the first two params.
  2. Delete `Index::get_num_key_cols()` and
     `Index::get_max_col_len(key_pos)` from our local
     `preview/index_builder.h` additions.
- **#927 doesn't force any edit** because Debarun's
  dispatchers install on `Custom_index::index_ctx()`, not on
  the branch's `LoadedIndex.ctx` (which still owns its own
  `vef_index_ctx_t`). The branch's dispatchers stay in
  place — same double-lifecycle story as `create`/`drop`/
  `insert`.

### Updated gap picture (2026-08-03)

Gap #4 (`profile_fn`/`helper_fn`/`key_len_fn` stubs) is
**partial-closing** — #927 replaces the stubs on Debarun's
`Custom_index`, but the branch's runtime still uses its own
copies. Full close requires the runtime shrink (retire
`LoadedIndex`, use `dict_index_t::custom_index` directly),
which is the same deferred cleanup covering the
`create`/`drop`/`insert` overlap.

Other gaps unchanged from 2026-08-02.

See `Docs/CUSTOM_INDEX_MERGED_PRS.md` § "The August
profile-dispatch pair" for line-level detail.

## ABI surface that landed this session

In `villagesql/sdk/include/villagesql/abi/preview/table_storage.h`:

- `vef_table_storage_index_def_t` plus `secondary_indexes` /
  `secondary_index_count` on `vef_table_storage_def_t`.
- `VEF_TABLE_STORAGE_SCAN_SECONDARY_INDEX` scan type,
  `vef_table_storage_scan_direction_t` enum, and
  `secondary_index_name` / `direction` fields on
  `vef_table_storage_scan_t`.
- `vef_table_storage_ref_length_fn ref_length`.
- `vef_table_storage_scan_position_fn scan_position`.
- `vef_table_storage_scan_seek_fn scan_seek`.
- `vef_table_storage_update_fn update_row`.

Server-side implementations in
`villagesql/services/preview/table_storage.cc`. All preview surface, no
ABI compat ceremony needed (per MEMORY.md rule).

## Tests added

Under `mysql-test/suite/villagesql/extension/`:

- `vec_chain/` — six tests: `_basic`, `_index_ddl`,
  `_index_maintenance`, `_index_chain`, `_index_knn`,
  `_index_orderby`.
- `index_uniq_lower/` — four tests: `_basic`, `_dup_insert`,
  `_delete_reinsert`, `_alter_fail`.
- `table_storage/` — bloom_hidden tests from earlier work, still
  passing.

Run with:
```
./mysql-test/mysql-test-run.pl --suite=villagesql/extension/vec_chain
./mysql-test/mysql-test-run.pl --suite=villagesql/extension/index_uniq_lower
./mysql-test/mysql-test-run.pl --suite=villagesql/extension/table_storage
```

## Open work, by priority

### Tier 1 — known correctness gaps

These are real holes that should be closed before declaring the
architecture done.

1. **`cleanup_failed_create` rollback path disabled** (TODO in
   `villagesql/services/preview/custom_index_table_storage_backend.cc`).
   Hits a table-cache "still in use" assertion when `mysql_rm_table`
   runs inline with the outer failing DDL. Today a leaked hidden
   table can result if the outer CREATE/ALTER fails after
   `pre_create_storage` succeeded. The `uniq_lower_alter_fail` test
   manually cleans up around this. Proper fix: close the THD's
   cached handle for the hidden table before the inner `mysql_rm_table`.

2. **PK requirement on the base table** (TODO at
   `villagesql/sql/custom_index_runtime.cc:collect_primary_key_columns`).
   Today `apply_to_custom_indexes` extracts PK column bytes as row
   identity. Tables without a user PK are rejected. The TODO walks
   through the design for a dual-mode REF/PK identity model. See
   "REF mode" in `alter_info_adds_custom_index`'s comment block for
   full design notes; the comment is the spec.

3. **`prepare_table_reads` backend hook missing.**
   `vec_chain_index_scan_begin` opens its own hidden-table handle
   inside every scan because the runtime doesn't supply one on the
   read path. Correct but per-scan. Add a `prepare_table_reads`
   parallel to `prepare_table_writes` in `CustomIndexBackend`, call
   from the runtime's scan path, and the read handle gets reused
   across multiple scans in the same statement.

4. **Per-index hidden-table naming.** Each table-storage-backed
   extension hardcodes one `logical_name`, so only one such index
   can exist system-wide. Should derive from `db.table.index`.

### Tier 2 — generalizations

Things that work today by hardcoding but should be data-driven.

5. **Optimizer's distance-VDF allowlist — wait for Debarun's
   index_profile stack, then remove the allowlist.**
   `GetMVectorDistanceFunction` in
   `villagesql/sql/custom_index_hypergraph_optimizer.cc` hardcodes
   two function names. Commit #557 ("Custom Index: sdk capability",
   already on main) introduced the SDK + parser + storage scaffold
   for `vsql::preview::index_profile`. A stack of four open PRs
   from Debarun (`@villagedeb`) is in flight to complete the end-
   to-end wiring:

   - **#558 — Custom Index: descriptors.** Adds
     `IndexTypeDescriptor` / `IndexProfileDescriptor` in
     `villagesql/schema/descriptor/` and wires both into
     `VictionaryClient`. **Already landed** on main as
     `8cf84694679`. We rebased through it — index_type was
     ours+main both-added (took main's), index_profile is
     purely additive (no consumer yet so nothing to integrate
     today; it sits in the victionary unused).
   - **#559 — validation and registration pipeline.** Hooks
     profile registration through `veb/register.cc` and
     validation through `veb/validate.cc`. Also renames
     `vef_required_capability_t.extension_data` →
     `capability_config` (small mechanical follow-up for us).
   - **#560 — capability services.** New
     `villagesql/services/preview/index_type.{cc,h}` and
     `index_profile.{cc,h}` — the server-side capability handlers
     that consume the `vsql::preview::index_type` and
     `vsql::preview::index_profile` capabilities at install time.
     Also introduces a new `vsql-index-test` extension and makes
     `IndexTypeCapability<N>` properly derive from
     `CapabilityBase`.
   - **#561 — reorganize index tests.** Moves the existing
     custom-index tests into a new `vsql-index-test` extension
     suite (sibling to our `vec_chain`, `index_uniq_lower`,
     etc.).

   **Shape of `IndexProfileDescriptor` (now visible on main):**
   each profile records a `(profile_name, extension_name,
   extension_version)` key, a type reference (which custom type
   the profile applies to), an index type reference (which
   custom index type), a list of
   `vef_index_profile_fn_binding_t` (each carries a `fn_id` +
   name + return/param types + deterministic flag), an
   ASC/DESC ordering flag, and a `default_for_type` flag (which
   profile to pick when CREATE INDEX doesn't name one).

   **The fn_id design is worth noting:** function bindings are
   call-by-id, not call-by-name. An index implementation can
   call `vef_index_profile_fn(fn_id=1, ...)` from inside its
   runtime to invoke the bound function without name
   resolution. Useful for graph indexes whose algorithm is
   generic across distance metrics (HNSW with L2 vs cosine vs
   dot product — same graph code, different bound function for
   fn_id=1 depending on which profile is selected at CREATE
   INDEX).

   **Once #559-561 land:**

   a) Rebase and absorb the mechanical changes — most importantly
      the `extension_data` → `capability_config` rename. Our
      extensions don't directly call that name (they use the SDK
      builders) so the SDK change inside `IndexTypeCapability` is
      the main thing.
   b) Add a `make_index_profile(...)` registration block to each
      of our extensions that wants ORDER-BY routing. For
      vec_chain that looks like:

      ```cpp
      make_index_profile("vec_chain_l2")
        .for_type(VSQL_VEC_CHAIN_TYPE)
        .using_index("vec_chain")
        .with_function(/*fn_id=*/1, &vec_chain_l2_distance,
                       "l2_distance")
        .ordering(IndexOrdering::ASC)
        .default_for_type(true)
        .build();
      ```

      Stored as an `IndexProfileDescriptor` keyed
      `vec_chain_l2.vsql_vec_chain_test.0.0.1`.
   c) Replace `GetMVectorDistanceFunction`'s hardcoded list
      with a victionary lookup against the
      `index_profile_descriptors` map. Given an `Item_udf_func`
      call by qualified name, find a profile binding that
      registers that function → get the bound profile → get
      the index type → check the column has an index of that
      type. No hardcoded extension/function names.
   d) Possibly also adopt the new `make_index_function` builder
      for our distance VDFs (currently a stub, will get wired
      in Debarun's stack) so they carry full metadata as index
      functions rather than plain VDFs. This is what lets the
      profile reference them by id.
   e) **Optional** — rewrite vec_chain's distance computation
      to go through `vef_index_profile_fn(fn_id=1, ...)`
      instead of calling `vec_chain_l2_distance_raw` directly.
      Today vec_chain is hardcoded to L2; with this change the
      same graph code would work for cosine/dot/L2 depending
      on which profile is registered against the index type.
      Useful when vec_chain is replaced by HNSW; not strictly
      needed for the optimizer migration itself.

   **Composition with table_storage:** the profile system is
   orthogonal to the storage capability — a profile describes
   the *SQL-visible function surface* of an index, not where
   the index stores its bytes. The two compose: an extension
   declares both `make_index_type<...>().table_storage<...>()`
   (storage shape) and `make_index_profile(...).using_index(...)
   .with_function(...)` (function surface). The optimizer uses
   profile to route a query; the runtime uses index_type to
   call insert/scan; the backend uses table_storage to
   materialize hidden tables. Three layers, three independent
   responsibilities.

   **Why this is tier 2:** the allowlist works for our test
   surface; nothing user-visible is broken. But anyone adding a
   third ORDER-BY-capable extension to the allowlist should do
   the index_profile migration first.

6. **Soft vs hard maintenance failures.** `intf->insert` returns a
   single `bool`. We use it for both "duplicate-key rejection"
   (expected, should be quiet) and "the storage is corrupted" (real
   error, should be loud). Today both log at WARNING with no
   distinction; tests `mtr.add_suppression` the line. Worth adding a
   soft-vs-hard signal in the result. Note where it'd land: the
   warning log + `dirty` flag in
   `villagesql/sql/custom_index_runtime.cc:apply_to_custom_indexes`.

7. **Soft failures + nicer error message.** When the
   table-storage-backed `intf->insert` returns failure because the
   hidden table's PK rejected a duplicate, the user sees
   `ERROR 1030 (HY000): Got error 122 - 'Internal (unspecified) error
   in handler'`. Should be `ER_DUP_ENTRY` with the column value. Tied
   to (6).

8. **Backend column-length quirk.** In
   `materialize_physical_table_storage_impl`, `max_length > 255` gets
   rounded to 4096 unconditionally, which can exceed InnoDB's
   3072-byte PK limit. We hit this with `prev_gref` and worked
   around it by sizing requests carefully. Real fix: pass the
   requested length through to the column type spec.

### Tier 3 — architecture extensions

Bigger pieces that would unlock new capability rather than fix gaps.

9. **REF mode for base-table identity.** See the long TODO comment
   on `alter_info_adds_custom_index` for the full design. Two
   modes (PK and REF) per custom index, choice driven by whether
   the base table has a user PK. PK mode keeps the current
   inplace-ALTER-friendly behaviour; REF mode requires MariaDB-style
   broader force-copy (also documented in the TODO). The contract
   choice ("server picks the mode; extension is mode-agnostic") is
   also in the comment.

10. **Hidden-table ABI generalizations.** A few obvious next moves:

    - **Partial UPDATE.** `update_row` requires all columns today;
      a partial form (specify only changed columns) would let
      tombstoning pass only the `deleted=1` value instead of
      reading and rewriting the whole row.
    - **AUTO_INCREMENT-style columns.** The vec_chain extension
      hand-maintains a `seq` counter because the ABI doesn't
      surface auto-increment. Trivial server-side, valuable
      extension-side.
    - **BOOL column type.** Currently shoehorned into UINT64 with
      ASCII "0"/"1" values.
    - **Startup recovery.** Backends that cache state across
      statements (e.g. vec_chain's `next_seq` counter) need a
      load-time hook to seed from the persisted hidden table. Add
      a callback on `CustomIndexBackend`/`vef_type_index_intf_t`
      and call it after the server has its hidden-table handle.

11. **Real graph index.** vec_chain proved the gref ABI works. The
    same ABI is what HNSW needs. Building a `vector_hnsw` extension
    against this ABI is the actual production-grade vector-index
    play. ~1000-1500 lines based on adapting MariaDB's mhnsw
    (`~/githome/mariadb-server/sql/vector_mhnsw.cc`). No new
    `table_storage` ABI surface required — but item 13 below is
    the prerequisite without which HNSW would be unusably slow.

12. **Crash recovery test.** We claim transactional consistency
    works "for free" via InnoDB; an actual `--restart` test that
    kills the server mid-insert and verifies hidden-table state
    matches base-table state would prove it.

13. **Extension-managed two-layer cache (shared + transactional)
    for graph indexes.** vec_chain re-fetches every node from the
    hidden table on every scan hop because there's no caching
    layer. Fine for a 5-row POC chain; catastrophic for an
    HNSW with millions of nodes. MariaDB's mhnsw solves this
    with two `MHNSW_Share` instances per index:

    - A **shared** cache (one per `TABLE_SHARE`), `Hash_set` of
      decoded `FVectorNode` keyed by gref. Holds committed state.
      Refcounted so concurrent searches don't get the graph torn
      out. Per-node `mysql_mutex_t` partitioned (8 buckets) so
      multiple sessions can load different nodes concurrently.
      Bounded by `mhnsw_max_cache_size`; oversized cache resets
      whole.
    - A **per-transaction overlay** (one per `THD × TABLE_SHARE`,
      hung off `thd->ha_data`). Catches in-progress writes so the
      writing session sees its own uncommitted graph. On rollback
      it's dropped. On commit they invalidate the touched nodes
      in the shared cache (rather than merging — explicit comment
      in mhnsw.cc says merging "doesn't make sense for
      ann_benchmarks") and the next search refetches from the
      hidden table.

    To support this pattern cleanly, the server's custom-index
    runtime needs to give extensions a place to hang per-share
    and per-thd state with commit/rollback hooks. Today
    extensions can only stash state in:

    - file-scope globals (vec_chain's `g_vec_chain_scan_count`)
      — process-wide, not transactional.
    - `IndexStorageCtx<T>::user()` — per-loaded-index, not
      per-THD.

    Neither covers the "(table_share, thd) overlay with commit
    callbacks" shape. The right addition is probably a pair of
    callbacks on `CustomIndexBackend` (or the
    `vef_type_index_intf_t` ABI itself) that the runtime invokes
    at trx_commit / trx_rollback, plus an extension-allocated
    THD slot the runtime threads through. Read MariaDB's
    `MHNSW_Trx` and its `transaction_participant` registration
    (`sql/vector_mhnsw.cc` lines 681-806) for the full pattern —
    they use MySQL's `transaction_participant` to hook the
    standard trx lifecycle, which we'd want to mirror.

    Until this lands, any graph index built against our gref
    ABI will pay O(disk read) per hop instead of O(memory
    lookup), making it useful only for tiny datasets. vec_chain
    survives because the test data is tiny; HNSW would not.

14. **Bitmap index on the `storage` capability — second backend.**
    All three of our existing extensions (bloom_hidden, uniq_lower,
    vec_chain) sit on `table_storage`. A second backend over the
    `storage` capability (page-level InnoDB primitives) would
    validate the `CustomIndexBackend` abstraction is genuinely
    backend-agnostic, and would lower-bound a row's worth of
    bookkeeping from one row in a hidden table to a few bits in a
    page.

    Candidate: a classic per-value bitmap index for low-cardinality
    columns. One packed-bitmap chain per distinct value of the
    indexed column. INSERT sets a bit in one page; DELETE clears
    one; equality scan walks the bitmap and yields rows whose
    bits are set. Useful as an architectural showcase even if the
    feature itself is less exciting than HNSW (Oracle has them;
    Postgres builds them only transiently per query; modern OLAP
    has mostly moved to roaring bitmaps and columnar storage).

    Scope notes from the design discussion:

    - **Architecture:** add a thin
      `villagesql/services/preview/custom_index_storage_backend.{cc,h}`
      that mirrors the table_storage backend but threads
      `vef_index_ctx_t::storage` instead of
      `hidden_table_handle`. Extension uses the page-level
      primitives directly (`vef_storage_mtr_start`,
      `vef_storage_page_load`, `vef_storage_page_write_int`,
      `vef_storage_mtr_commit`) — the higher-level
      `vef_type_storage_intf_t` create/insert/select hooks
      are for custom-type column storage, not arbitrary
      page management, so bitmap bypasses them.
    - **Row identity:** do NOT use base-PK value as bit
      position. Maintain a dense `pk_id → row_no` map in a
      side-storage page chain; bit index = row_no. PK-as-bit
      shortcut would lock the index to dense INT PKs and force
      a rewrite to generalize. ~150 lines extra for the side
      map; worth it.
    - **Value discovery:** lazy, not declared-up-front.
      Directory grows: each new value triggers allocation of a
      new bitmap chain head. Declared-at-CREATE-INDEX values
      would lock the directory page format and is fundamentally
      not what a bitmap index is. ~75 lines extra for directory
      growth.
    - **Indexed column type:** INT-only for the POC. Extending
      to VARCHAR/etc. later just means changing the key
      extraction in maintenance callbacks; on-disk layout is
      unaffected.
    - **Transactional semantics:** come for free via mtrs +
      InnoDB redo/undo. Every page write goes through an mtr
      that references the user trx_ref; rollback unwinds the
      bits InnoDB-side, no extension-side work.
    - **Page layout:** root page = directory (value → first
      bitmap page in chain). Per-value chain pages hold a
      packed bit array. Page header + trailer leave ≈ 16,344
      bytes ≈ 130,752 bits per page, so each chain head can
      cover ~130K rows before needing a second page.
    - **Scan:** `scan_begin` looks up the chain for the
      queried value, sets the cursor's "current page" pointer
      to chain head. `scan_next` finds the next set bit
      (crossing pages when exhausted). `scan_fetch` returns
      the row's PK columns (via the row_no → pk_id reverse
      map).

    Total estimate ~1000-1100 lines. Largest unknown is the
    page_storage primitive API itself — we haven't written
    against it before. Worth a small spike just to write+read a
    single bit before tackling the full index.

## Notable cross-cutting points

### `CustomIndexBackend` is ours, not part of the extension surface

`CustomIndexBackend` (in `villagesql/sql/custom_index_backend.{cc,h}`)
is **a server-internal abstraction we introduced** to wire the
custom-index ABI into the runtime. It is NOT part of the upstream
extension SDK and extensions do not implement it. The split matters
because anyone reading the code may otherwise mistake it for an
extension-facing concept.

**What main gives you for custom indexes:**

- `vef_type_index_intf_t` — the index type's callbacks (insert,
  mark_delete, scan_*, etc.). C-ABI function pointers.
- `vef_index_ctx_t` — per-loaded-index context the runtime hands to
  each callback. Carries the storage reference,
  `hidden_table_handle` (table_storage), etc.
- `vef_storage_*` page-level primitives — for extensions that want
  to manage InnoDB-managed pages directly.
- `IndexTypeDescriptor`, `IndexProfileDescriptor`, the custom_indexes
  / custom_index_columns system tables — metadata layer in
  victionary.
- Optimizer hook (`custom_index_hypergraph_optimizer.cc`) for
  `ORDER BY distance LIMIT` recognition.

**What main does NOT give you:**

- No DML maintenance runtime — nothing hooks `ha_write_row` /
  `ha_delete_row` to call the index's `intf->insert` /
  `mark_delete`.
- No DDL lifecycle integration — nothing calls `intf->create` at
  CREATE INDEX time or `intf->drop` at DROP INDEX time.
- No transaction integration — nothing wires the index into
  `trx_commit` / `trx_rollback`.

So on plain main the custom-index system is **metadata-only**: the
ABI defines the callbacks but nothing on the server invokes them.
Our `villagesql/sql/custom_index_runtime.{cc,h}` is the connecting
tissue — the runtime that hooks `ha_write_row`, looks up custom
indexes for the table, and invokes `intf->insert`/etc. with the
right context.

`CustomIndexBackend` is the strategy interface inside that runtime
for **storage backends**:

- One backend per storage strategy.
- Currently one implementation: `TableStorageCustomIndexBackend`
  (`villagesql/services/preview/custom_index_table_storage_backend.{cc,h}`).
- A second is planned for page-level storage (the "bitmap on
  page_storage" item in tier 3).

A backend's responsibility is everything generic across "storage
strategy X" that doesn't fit naturally in the per-index extension
callbacks:

- DDL: `pre_create_storage`, `cleanup_failed_create` — materializing
  / tearing down whatever backing storage the strategy needs at
  CREATE INDEX / DROP INDEX time.
- Per-statement open/close of a storage handle (table_storage opens
  an MDL-locked handle once per statement; page_storage doesn't
  need this).
- Context population: `before_callback`/`after_callback` populate
  the relevant pointer in `vef_index_ctx_t` (e.g.
  `hidden_table_handle`) before each extension callback fires.
- Per-statement teardown: `on_statement_end`.

**Implication for adding a new storage-backed index type:**

Don't invent a parallel runtime. Add a new `CustomIndexBackend`
subclass alongside the existing one. The extension only writes its
own `vef_type_index_intf_t` callbacks; it doesn't know or care that
`CustomIndexBackend` exists. The backend is the server-side
plumbing that makes those callbacks fire at the right time with the
right context.

For the bitmap-on-page-storage work in tier 3, that means:

- One new file: `villagesql/services/preview/custom_index_page_storage_backend.{cc,h}` (or similar).
- Subclass `CustomIndexBackend`, override the lifecycle hooks to
  use `vef_storage_segment_create` / `vef_storage_segment_drop`
  instead of `materialize_physical_table_storage` /
  `mysql_rm_table`.
- Likely thinner than the table_storage backend because page_storage
  doesn't need a long-lived per-statement handle.
- Register it in `capability_registry.cc` next to the table_storage
  backend registration.

### THD-stashed ALTER target

`mysql_alter_table` sets `thd->villagesql_alter_target_db/table` to
the user-visible name of the table being altered. The custom-index
runtime reads those when `ha_write_row` fires on a `#sql-...`
rebuild table so victionary lookups happen under the user-visible
name. Cleared by `AlterGuard`. Used because the rebuild's
`TABLE_SHARE` has the tmp name, not the user-visible one.

Pattern is generic — any code in the rebuild's per-row path that
needs to know "what's the target table" can use these fields.

### Uncommitted-aware victionary lookups

`SystemTableMap::get_prefix(thd, ...)` merges committed entries
with the THD's uncommitted PendingOperations. Used by the custom
index runtime during ALTER copy because the new index entry is
still uncommitted when the row-by-row copy is running. Lives in
`villagesql/schema/victionary_client.h`. Other code that needs to
see the in-flight DDL state on the same THD can use this too.

### Integer column encoding symmetry in table_storage

`copy_field_value` for UINT64/INT64 columns now emits ASCII
decimal (not raw 8-byte little-endian) so that `scan_fetch →
update_row` round-trips through extensions without re-encoding.
This was a pre-existing inconsistency we hit when writing back
tombstone-flipped rows.

**Implication for extension authors:** any extension that reads
integer column values via `scan_fetch` should parse the bytes as
ASCII decimal (matching how `insert` / `update_row` accept them).
Older bloom_hidden code that tried fixed-width binary decode
first was caught by this — `parse_uint64` now goes
ASCII-decimal-first with a binary fallback for back-compat.
Worth being aware of when adding new column types.

### Hidden table location

`kTableStorageSchema` is `"villagesql"`. Hidden tables live as
`villagesql.__hidden_<logical_name>`. Tests that need to read them
directly use `SET SESSION debug = '+d,skip_dd_table_access_check'`.

### Extension layout

The new `vsql-vec-chain-test` extension is split into
`extension.cc` (entry point only), `type.{h,cc}`, and
`index.{h,cc}`. This is the first multi-file test extension in the
tree. The `villagesql/test-extensions/shared/CMakeLists.txt`
template was changed to glob `src/*.cc` (with
`CONFIGURE_DEPENDS`), so other extensions can follow the same
pattern by just adding files.

## How to start the next session

1. Read `Docs/CUSTOM_INDEX_CURRENT_STATE.md` for the conceptual
   overview, including the per-extension summaries.
2. Read this file for the open issues list.
3. Look at the TODO blocks (search `TODO(villagesql-indexing)` in
   `villagesql/sql/custom_index_runtime.h` and
   `villagesql/sql/custom_index_runtime.cc`) for the dual-mode
   design and force-copy policy notes — these are the deepest
   architectural questions and the comments are the spec.
4. Run the existing test suites end-to-end to confirm the baseline:
   ```
   ./mysql-test/mysql-test-run.pl --do-suite=village --parallel=auto
   ```
5. Pick one of Tier 1 items (cleanup_failed_create is the closest
   to "real correctness regression", REF mode is the most
   architecturally interesting).
