# `--initialize --default_table_encryption=ON` aborts on MySQL 8.4.11

**Status:** fix proposed in this commit. **Reproduced on the Percona 8.4.10-10 merge branch,
not yet reproduced on `main` itself** — see *Verification* below, which is the weakest part of
this and the part to finish before relying on it.

**Believed to be an upstream MySQL regression, not a VillageSQL or Percona defect.** Worth an
upstream bug report.

---

## Symptom

```
mysqld --initialize --default_table_encryption=ON   # with a keyring configured
```

```
[ERROR] [MY-000061] [Server] 3825  Request to create 'unencrypted' table while using
                                   an 'encrypted' tablespace..
[ERROR] [MY-013455] [Server] The newly created data directory ... is unusable.
[ERROR] [MY-010119] [Server] Aborting
```

The datadir is left unusable. There is no workaround other than initializing without
`--default_table_encryption=ON` and turning it on afterwards.

## Mechanism

`scripts/mysql_system_tables.sql` creates roughly 34 system tables in the shared `mysql`
tablespace, each spelling out the encryption attribute explicitly:

```sql
SET @str = CONCAT(@cmd, ' ENGINE= INNODB ROW_FORMAT=DYNAMIC TABLESPACE=mysql '
                        'ENCRYPTION=\'', @is_mysql_encrypted, '\'');
```

It has to be explicit, because InnoDB requires a table's encryption attribute to match the
shared tablespace it lives in, and the default would otherwise come from the schema. So
`@is_mysql_encrypted` must equal the real state of the `mysql` tablespace.

Before 8.4.11 the script probed that state at runtime:

```sql
SET @is_mysql_encrypted = (SELECT ENCRYPTION FROM
    information_schema.INNODB_TABLESPACES WHERE NAME='mysql');
```

Upstream commit `729fe05ddd6`, for **Bug#39114059** *"Determining if mysql table space is
encrypted is slow with many table spaces"*, removed that probe — `I_S.INNODB_TABLESPACES`
does not scale with tablespace count — and on the initialize path replaced it with a constant
in `sql/bootstrap.cc`:

```cpp
std::ignore = dd::execute_query(thd, "SET @is_mysql_encrypted = 'N'");
```

The commit message states the premise plainly: *"During initialize no check is performed as
the mysql tablespace is known not to be encrypted."*

**That premise is false under `--default_table_encryption=ON`.** There the `mysql` tablespace
is itself created encrypted, the 34 system tables are then created `ENCRYPTION='N'` inside
it, InnoDB rejects the mismatch, and initialize aborts.

The same commit **does** compute the value correctly for the *upgrade* path,
`sql/dd/impl/upgrade/server.cc`. Only initialize got the constant. That asymmetry is the
strongest evidence this is an oversight rather than a deliberate narrowing.

## The fix

Read the real state instead of assuming it, using **the query upstream already wrote in that
same commit** for the upgrade path:

```sql
SET @is_mysql_encrypted =
  (SELECT (IF((GET_DD_TABLESPACE_PRIVATE_DATA(se_private_data, 'flags') & 8192) <> 0,
              'Y', 'N'))
   FROM mysql.tablespaces WHERE name = 'mysql')
```

This preserves the point of Bug#39114059: it reads `mysql.tablespaces`, which has an index on
`name`, rather than `I_S.INNODB_TABLESPACES`. `8192 == FSP_FLAGS_MASK_ENCRYPTION`
(`FSP_FLAGS_POS_ENCRYPTION` is bit 13 in `storage/innobase/include/fsp0types.h`); the header
cannot be included in `bootstrap.cc`, which is why upstream's own copy of this query carries
the same magic number and the same comment explaining it.

Reading a DD table needs `thd->parsing_system_view`, set and restored with a scope guard, as
on the upgrade path.

## Verification

**Verified** — on the Percona 8.4.10-10 merge branch, macOS, debug build:

| test | result |
|---|---|
| `percona_innodb.sys_tablespace_encrypt` | pass (control) |
| `percona_innodb.encrypt_mysql_ibd` | pass |
| `percona_innodb.session_temp_tablespaces_encrypt_bootstrap` | pass |

Both failing tests passed on the pre-8.4.11 Percona merge (`mtr-wide-debug.log`, 2026-08-11),
which dates the regression to 8.4.11 rather than to the merge.

**Not verified, and this is the gap:**

- **No reproduction on `main` itself.** The bug is invisible here because **upstream ships no
  test that bootstraps with `--default_table_encryption=ON`** — the only two that do are
  Percona's, and they do not exist on this branch. That is precisely why 8.4.11 shipped with
  it and why main's CI is green.
- An attempt to write a standalone `main` test failed for an unrelated reason worth recording,
  so the next person does not repeat it: driving `mysqld --initialize` from MTR needs the
  keyring visible to the *new* instance, not just to MTR's server. Using
  `suite/component_keyring_file/inc/setup_component.inc` is not enough — initialize dies
  earlier with `[InnoDB] Check keyring fail, please check the keyring is loaded.` and never
  reaches the encrypted-tablespace path. Percona's test uses
  `setup_component_customized.inc`; model on that. Also redact the datadir path
  (`--replace_result`), or the recorded result is machine-specific.
- The **upgrade path** is untouched by this change but was not re-run.
- **No non-debug build, no Linux, no full-suite run.**

## What would make this solid

1. Finish the standalone `main` test per the note above, so the regression is pinned where it
   actually lives instead of only where Percona happens to look.
2. Confirm on Linux and in a non-debug build.
3. Report upstream. The report is easy to write: their own commit contains the correct query
   for one path and the constant for the other.

## Residual risk in the fix

If `mysql.tablespaces` has no row named `mysql` at this point in initialize, the `SELECT`
yields NULL, `@is_mysql_encrypted` becomes NULL, and every `CONCAT(...)` in
`mysql_system_tables.sql` becomes NULL — an obscure failure. Upstream's upgrade-path use has
the same exposure, so it is inherited rather than introduced, but initialize runs much earlier
in DD construction than upgrade does. Implicit evidence that it is fine: MTR bootstraps its
own datadir with plain `--initialize`, so every passing test exercises the non-encrypted path
through this code. Worth making explicit rather than leaving inferred.
