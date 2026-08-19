# Recurring Conflicts

Files that conflict on **every** Oracle upstream merge, with the resolution we apply
and the reasoning behind it. The point of this file is that these should be lookups,
not judgment calls, on the next merge.

Add a file here the second time it conflicts. Record *why*, not just *what* — a
disposition without a reason is not reusable, and the next person cannot tell whether
circumstances have changed.

**Scope: any merge target.** Everything here arises from VillageSQL's divergence from
Oracle, so it fires regardless of which Oracle branch you are merging. Material that
applies to only one target lives in `recurring-conflicts-<target>.md` — see
[`recurring-conflicts-9.7.md`](recurring-conflicts-9.7.md) for the 9.7 port. Keep it
that way: someone resolving a conflict should not have to read past entries that cannot
affect them to find the one that can. When an entry in a target file turns out to fire
elsewhere too, promote it here rather than copying it into a second target file.

---

## The rule: take-ours is not the end of the resolution

For fork-identity and version files we almost always reject upstream's text. That is
the easy half. The half that gets missed:

> **Their change to a file we take-ours on often implies a corresponding change we
> *do* want — somewhere else.**

Taking ours resolves the conflict. It does not answer the question their change asked.
So for every file below, the procedure is two steps, not one:

1. Take ours.
2. **Read their diff anyway** and ask: does this imply an edit on our side, in this
   file or another one?

```bash
git diff <base> <theirs> -- <path>
```

This is not hypothetical. It fired on the first merge after this file was written —
see `LICENSE`, where step 2 surfaced a live licensing gap that step 1 would have
buried.

---

## `MYSQL_VERSION`

**Conflicts because:** every release bumps `MYSQL_VERSION_PATCH`.

**Resolution: take ours, then bump the patch level by hand.** Keep our shape; adopt
only the new version number.

Our shape deliberately **omits `MYSQL_VERSION_EXTRA`**, with this comment:

```
# MYSQL_VERSION_EXTRA is intentionally omitted. VillageSQL sets EXTRA_VERSION
# from VSQL_VERSION in cmake/mysql_version.cmake: it includes the VillageSQL
# version before reading this file, and MYSQL_GET_CONFIG_VALUE skips variables
# that are already set, so a MYSQL_VERSION_EXTRA here would be ignored anyway.
```

**Why the omission is deliberate — verify it yourself rather than trusting this note.**
`cmake/mysql_version.cmake` includes the VillageSQL version *before* reading
`MYSQL_VERSION`, and the reader macro is:

```cmake
MACRO(MYSQL_GET_CONFIG_VALUE keyword var)
 IF(NOT ${var})
   ...
```

`IF(NOT ${var})` means an already-set variable is never overwritten. `EXTRA_VERSION` is
already set from `VSQL_VERSION`, so a `MYSQL_VERSION_EXTRA` line in this file is inert.

**Do not "restore" the line to match upstream.** It is inert, so restoring it breaks
nothing — which is exactly what makes the mistake easy. It is still wrong: it
reintroduces a line VillageSQL deliberately dropped, discards the comment explaining
why. This was gotten wrong during
the 8.4.11 merge, on the reasoning that the omission looked like incidental drift from
the 8.4.10 merge. The intent was documented in `cmake/mysql_version.cmake` the whole
time.

## `LICENSE`

**Conflicts because:** VillageSQL prepends its own license preamble, and Oracle
refreshes the "License Book" every release.

**Resolution: take ours, whole file — then diff theirs and check for new third-party
license text.**

Our `LICENSE` is a short VillageSQL preamble plus GPLv2, and it explicitly points at
`Docs/LICENSE.historical` for third-party licensing. That pointer is what makes step 2
mandatory: **if upstream adds a license notice for a dependency we ship, taking ours
drops the notice, and nothing else picks it up.**

How to tell the two cases apart quickly:

| Upstream diff looks like | Meaning | Action |
|---|---|---|
| ~5 insertions / ~5 deletions, only version strings and the "Last updated" month | cosmetic release churn | take ours, done |
| substantially more insertions than deletions | new third-party license text | take ours, **and** port the new section into `Docs/LICENSE.historical` |

**8.4.10 → 8.4.11:** 5 insertions / 5 deletions, purely `8.4.10`→`8.4.11` and
"February"→"June 2026". Cosmetic. Nothing to port.

**A release that adds real text is not hypothetical.** Oracle has shipped a release
whose `LICENSE` change was 167 insertions / 5 deletions, adding an **OpenTelemetry C++**
notice (`opentelemetry-cpp`, Apache-2.0) plus a "4th Party Dependencies" section — for a
dependency already bundled and pinned in cmake for two releases. They were correcting an
omission in their own license book. Take-ours drops that text and nothing else picks it
up.

### Known gap: `Docs/LICENSE.historical` is stale

Recorded here because it is the thing `LICENSE` defers to, and it is not currently
being maintained across merges.

- Frozen at `MySQL 8.4.5 Community`, `Last updated: March 2025`.
- Last touched by `955afe4768f0` ("VillageSQL 0.0.1 release", 2026-03-09) — snapshotted
  once, never refreshed through 8.4.6-8.4.11.
- Whenever we bundle a dependency whose notice it does not carry, that is a compliance
  gap rather than untidiness: Apache-2.0 and similar licences require the notice
  accompany distribution.

Refreshing it is its own task, not merge work. But every merge should check whether it
just made the gap wider.

## `CONTRIBUTING.md`

**Conflicts because:** VillageSQL replaced Oracle's contribution process, and upstream
periodically rewrites theirs.

**Resolution: take ours, whole file.**

We deliberately removed the Oracle Contributor Agreement instructions and the
`bugs.mysql.com` workflow, in favour of our own guide and `contributing-guide/`. Upstream
edits to that text are never wanted directly.

**Step 2 for this file:** upstream sometimes adds *process* ideas worth having in our
own words — an AI-assistance disclosure section, for instance. Adopting an idea is a
deliberate choice, not a merge resolution; note it and move on.

## `sql/sql_yacc.yy`

**Conflicts because:** VillageSQL appends its own parser tokens directly after
upstream's last token, so any upstream change to the final tokens collides with our
block.

**Resolution: take-both, semantically.** Apply upstream's change to their token, and
keep the VillageSQL tokens that follow.

VillageSQL's block, as of 8.4.11:

```
// TODO(villagesql-rebase): Check if token number needs updating during MySQL rebase
%token<lexer.keyword> EXTENSION_SYM              1215  /* VILLAGESQL */
%token                DOUBLE_COLON               1216  /* VILLAGESQL OPERATOR */
%token<lexer.keyword> VERSION_SYM                1217  /* VILLAGESQL */
```

**8.4.11 instance:** `Bug#36584265` ("Five mistakes in
`information_schema.keywords_reserved`") changed `TABLESAMPLE_SYM` (1214) from
`%token<lexer.keyword>` to `%token` — demoting it from a non-reserved keyword usable as
an identifier to a **reserved** word. We took upstream's line verbatim, including its
spacing, and kept our three tokens after it.

**Watch for:** a reserved/non-reserved change upstream has downstream test
consequences. `mysql-test/r/information_schema_keywords.result` is in the overlap
whenever this happens. Confirm that result file reflects the change rather than
assuming a clean auto-merge was correct.

**Token numbering:** our tokens are numbered immediately after upstream's highest. If
upstream ever adds a token at our numbers, they must be renumbered — that is what the
`TODO(villagesql-rebase)` comment is there to catch.

---

# Known false positives

The conflict-marker grep never comes back empty. These hits are committed upstream and
present in **all** parents. Do not "fix" them.

Verify provenance before adding anything here — and before touching any marker you find:

```bash
git grep -c '<pattern>' <base>   -- <path>
git grep -c '<pattern>' <ours>   -- <path>
git grep -c '<pattern>' <theirs> -- <path>
```

Equal counts across all parents means inherited, not residue.

## `storage/ndb/src/kernel/blocks/backup/Backup.cpp`

Contains a literal `<<<<<<< HEAD` line. Present in vanilla MySQL, in Percona, and on
VillageSQL `main`. Not merge residue.

## `mysql-test/suite/test_service_sql_api/r/test_sql_stmt.result`

Contains 116 lines of the form `<<<<<<<<<<<< Current context >>>>>>>>>>>>>>>`. This is
test output formatting, not a conflict marker. Count verified identical (116) in base,
ours, and theirs during the 8.4.11 merge.

## `mysql-test/suite/test_service_sql_api/r/test_sql_reset_connection.result`

Contains two lines of long `<<<<<<<`/`>>>>>>>` runs used as test-output section
separators around the `reset_connection()` output. Count verified identical (2) on
8.4 `main` and on both sides of a merge.

## Suggested grep

```bash
git grep -nE '^(<<<<<<<|>>>>>>>)' -- . \
  ':!storage/ndb/src/kernel/blocks/backup/Backup.cpp' \
  ':!mysql-test/suite/test_service_sql_api/r/'
```
