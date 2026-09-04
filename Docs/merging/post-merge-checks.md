# Post-merge checks

Things that do **not** conflict. The merge resolves clean, the build succeeds, and then
a test fails hours later — or worse, doesn't, and ships wrong.

`recurring-conflicts.md` is for what git stopped and asked you about. This file is for
what nothing will stop you about. Both are lookups; this is the one you have to
remember to open.

---

## Case sensitivity (`lctn`)

macOS is case-insensitive. `lctn=1` results record locally, but `lctn=0` and
`information_schema_cs` need a case-sensitive filesystem and can only be produced on
Linux.

This is not merely a re-record inconvenience — it is a **silent-staleness generator**.
Anything with a per-`lctn` variant can be updated on the platform you develop on and
left stale on the one you don't, and local runs will never tell you. The I_S checksums
below stayed broken for six weeks exactly this way.

## I_S schema checksums

Applies to `i_s_schema_definition_debug` and `dd_schema_definition_debug`.

**There are two published checksums per version, and a schema change moves both.**

```
INSERT INTO I_S_published_schema
  VALUES ('80400', '80400', 0, '<checksum for lower_case_table_names=0>');
INSERT INTO I_S_published_schema
  VALUES ('80400', '80400', 1, '<checksum for lower_case_table_names=1>');
```

**You cannot compute the lctn=0 value on macOS.** It requires a case-sensitive
filesystem, so it has to come from a Linux run — in practice, from the CI failure
message, which prints the checksum it computed. This is the trap: someone changes the
I_S schema, records the lctn=1 value their laptop can produce, and ships a stale lctn=0
row that fails on Linux only. It happened, and went unnoticed for six weeks. If you
change the schema you owe **both** rows, not just the one you can generate.

**Diagnosing: if only one of the two rows fails, the schema did not change.** A genuine
schema change moves both checksums. One failing alone means that row was already stale.
This is the cheapest discriminator available for "did my merge break this, or was it
already broken", and it costs one local test run.

**Updating `IS_DD_VERSION` — bump only when upstream bumps.**

`IS_DD_VERSION` is a MySQL version number (`80400` = 8.4.0), but it stamps *the release
at which the I_S view definitions last changed*, not the current server version. Oracle
leaves it alone across releases that do not touch I_S — 8.4.0 through 8.4.11 all sit at
`80400`, and `metadata.h` says so explicitly ("80024..80028: Not published. There are no
changes from version 80023.").

**The rule: bump it when upstream bumps it. Do not bump it for VillageSQL-specific
additions to I_S.** The constant lives in Oracle's numbering space; inventing our own
value there would collide with whatever they publish later.

Know what the constant controls, because the rule has a consequence.
`update_server_I_S_metadata()` regenerates the I_S system views only when the version
stored in the DD differs from the compiled one:

```cpp
if (d->get_target_I_S_version() == actual_version &&
    !dd::bootstrap::DD_bootstrap_ctx::instance().dd_upgrade_done())
  return false;                       // equal -> skip regeneration entirely
```

So a VillageSQL-only change to the I_S schema does **not** trigger regeneration on an
in-place upgrade between VillageSQL builds: the stored and compiled versions both read
`80400`, and the server keeps the older view definitions. Fresh installs are unaffected,
since views are generated from the compiled definitions. This is an accepted trade-off
of keeping out of Oracle's numbering space, not an oversight — but if a VillageSQL I_S
change ever needs to reach upgraded servers, that is the mechanism it has to solve, and
it is a product decision rather than a re-record.

**So the merge-time action is: override the published checksum in place.** Our schema
legitimately differs from Oracle's at the same I_S version because of VEF, which is why
our rows already diverge from upstream's. Confirm the diff is benign before recording —
a checksum change with no corresponding upstream I_S change deserves an explanation, not
a paste.

## The `sys` view metadata repair

`refresh_sys_view_metadata()` in `villagesql/sql/initialize.cc` replays the
`CREATE OR REPLACE VIEW` statement of a handful of `sys` views on every startup that
needs it, to undo the column metadata that installing our `INFORMATION_SCHEMA` overrides
rewrites. It finds those statements by scanning the generated `mysql_sys_schema[]` array
for `"VIEW <name>"`, using the view names in `kAffectedSysViews`.

That couples us to upstream's sys schema in two ways a merge can break.

**View names.** If upstream renames, splits or drops one of the listed views, the scan
finds no statement for it. This is caught, not silent: the replay requires exactly one
match per name and fails startup otherwise, so the `villagesql/information_schema`
suite's `sys_view_metadata` test will fail loudly. Fix it by updating
`kAffectedSysViews` to match `scripts/sys_schema/`.

**Statement order.** The replay follows `mysql_sys_schema[]` order, which is the file
order in `scripts/sys_schema/CMakeLists.txt`. That order is topological today, and
upstream cannot easily change that — a child view listed before its parent would fail
every fresh `--initialize` with `ER_NO_SUCH_TABLE`. The repair depends on it, because
replaying a parent re-rewrites its children's metadata, so children must be replayed
after their parents. If a merge ever does reorder that list, `sys_view_metadata` is the
test that notices.

**New sys views.** If upstream adds a `sys` view that reads `INFORMATION_SCHEMA.COLUMNS`
or `INFORMATION_SCHEMA.STATISTICS`, it needs adding to `kAffectedSysViews`, and so does
anything that reads *it*. Nothing detects that automatically — the symptom is a
`sysschema` result diff where nullability flips `YES` to `NO`, and `sysschema` only runs
in the weekly full suite. `grep -ril "information_schema.\(columns\|statistics\)"
scripts/sys_schema/` lists the candidates.
