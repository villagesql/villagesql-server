# VillageSQL 0.0.7

Draft release notes through commit `3aa4ebbbeab`: index: bulk build fix (#1281)

The GitHub release assets are available at https://github.com/villagesql/villagesql-server/releases.
The Docker Hub release artifacts are available at https://hub.docker.com/r/villagesql/server.
The Cargo release artifacts are available at https://crates.io/crates/villagesql.

## What's New

### Extension Framework

- **Per-session system variables (preview)** — Extensions can declare `INT` and `STR` system variables with a per-connection value through the `vsql::session_var` capability. (`699a0bbf286`, #1059; `494c58d1770`, #1057; `9fd09db4f34`, #1115; `17df24684f5`, #1209)
- **`on_init` and `on_deinit` hooks** — A protocol 4 extension can declare a pair of lifecycle hooks. They run after the extension's capabilities are set up and before they are torn down, so `on_init` can read the extension's own system variables. (`c857bdf32ce`, #1216)
- **VDFs can resolve their own type parameters** — A VDF can register a `bind_and_check_types` hook to decide its return-type parameters from its arguments, for functions the built-in rules cannot resolve. (`999445f7302`, #1200; `cf058c6f9f8`, #1201; `4c77e153575`, #1204; `909a4245c3e`, #1239; #403)
- **Authentication methods choose how roles are reconciled (preview)** — The `vsql::preview::auth` capability replaces `auto_grant` with `roles_mode`, which activates, grants, or fully syncs the roles an identity provider supplies. An extension using `auto_grant` needs updating. (`6f668d151dd`, #1188)
- **Enum system variables** — The `vsql::sys_var` capability gains `make_enum()` for a variable whose value is one of a fixed list of names. (`26e94477f9f`, #1179)
- **Extension registrations are validated at load** — The server checks an extension's registration for missing or conflicting type functions and aggregate callbacks, on release builds as well as debug builds. (`9f8c371bcc8`, #1172)
- **Client information in statement events (preview)** — Statement events report the client's process id, connector library, and application name. (`89e0e43f38e`, #1178)
- **Persisted extension variables apply at `INSTALL`** — A persisted value for an extension variable with an `on_change` callback is applied at `INSTALL EXTENSION`, and a `SET` racing an `UNINSTALL EXTENSION` no longer crashes. (`361196a45d1`, #1129)
- **`ALTER EXTENSION ... AT RESTART` applies the whole update** — An update applied at restart no longer leaves the extension or a custom column at the old version, a state in which `SHOW CREATE TABLE` crashed the server. (`b36bdbe142b`, #1267, #1261)
- **`STRING` results carry their declared character set** — A VDF `STRING` result is `utf8mb4_bin` at runtime, so JSON functions treat it as text without a `CONVERT`. (`e6c298d8925`, #1054)
- **VEB installs are synced to disk** — `INSTALL EXTENSION` `fsync()`s what it writes. (`ea958becc70`, #1145)
- **SDK version comes from the server build** — The SDK version no longer carries the codebase name. (`323a6ec9265`, #1096; `f892efbb034`, #1064)
- **SDK documentation** — The comments for the protocol 1 SDK are gone, and the header comments on intrinsic defaults match what the server enforces: one default, a string or a VDF, and the VDF takes no arguments. (`3d0d1484935`, #901; `a5d49e67f99`, #898; `8682dfe1528`, #1170)

### Custom Types

- **Type parameters are validated** — Duplicate, nameless, and valueless parameters are rejected before the extension sees them. (`c17123af77d`, #915)
- **Custom type DDL is refused under `LOCK TABLES`** — The statement fails up front and leaves the table unchanged. (`902649b7d34`, #1192)
- **Temporary tables** — A temporary table no longer inherits a custom type from a base table of the same name. (`76d4c704720`, #1191)
- **`ALTER TABLE ... RENAME TO` with a custom column change** — The column metadata follows the new table name, including into another database. (`2389cb12444`, #1240, #1194)
- **`DROP DATABASE`** — Metadata for stored programs with custom-type parameters is deleted with the database. (`738b26db28d`, #1121)
- **Encoded string literals** — A string literal that the server encodes into a custom type is treated as binary data throughout, including bytes outside ASCII. (`820cdbc4a72`, #1266)

### Clone

- **Clone validates extensions before transferring data** — The recipient checks that every extension installed on the donor is present locally with a matching version and checksum, and aborts with an error naming any extension that is missing or differs. (`8587d977342`, #1122)

### Information Schema

- **`sys` view column nullability matches upstream** — `schema_object_overview`, `x$schema_flattened_keys`, `schema_auto_increment_columns`, and `schema_redundant_indexes` match upstream MySQL. (`f595f2e2251`, #1110)

### Custom Indexes (in development)

Custom index DDL is refused by default, with `Extended Index feature not yet implemented`; setting `vsql_allow_preview_extensions=ON` accepts it. Everything in this section is behind that flag, with the one exception noted below.

- **Metadata and DDL surfaces** — `SHOW CREATE TABLE` renders a custom index as valid DDL. `INFORMATION_SCHEMA.STATISTICS.INDEX_TYPE` and `SHOW INDEX` report the extension's own index type name for a custom index. To hold those names, the `INDEX_TYPE` column widens to `varchar(64) utf8mb4_bin` and `sys.schema_object_overview.object_type` to `varchar(72)`; the wider columns are visible on every server, flag or not. (`d9eefc101ce`, #1067; `a7929efbb62`, #1076; `93813b3e96e`, #1035; `bd46c3a559e`, #1015; `6f416c97ce8`, #1111)
- **Nearest-neighbor scan** — A KNN-capable custom index that stores the owning row's primary key serves `ORDER BY <distance> ... LIMIT` queries nearest first with no filesort, in both optimizers, skipping rows the transaction cannot see. The classic optimizer honors `IGNORE INDEX` and `FORCE INDEX (PRIMARY)` against it. (`131989f41e6`, #1214; `a436a42ee7f`, #1210; `d58d8395752`, #1212; `553f55f989f`, #1213; `01a642ac9c1`, #1262; `2cc9e962560`, #1269; `24b34bd992e`, #1245; `209f285b271`, #1249)
- **Index builds, `ALTER TABLE`, and DML** — A custom index builds over existing rows and survives a table rebuild. Two statements are rejected: a `DELETE` against an index that needs the server to store a per-entry reference, and an `UPDATE` that changes an indexed column or the primary key. Other `DELETE` and `UPDATE` statements run. Changing the primary key with `ALTER TABLE` is refused. (`0cbff6a6848`, #1223; `f2a864ac6c8`, #1077; `1f8833c56b6`, #1232; `5b9f9ace1f3`, #1268; `f3c62f066cc`, #1270; `3aa4ebbbeab`, #1281)
- **Helper function lookup at index open** — An extension can resolve the helper bound at a key part when the index opens, through `helper_fn_name_fn`. (`92264416ce0`, #1215)
- **Extension index errors reach the client** — A failing create or drop hook returns its message as the statement's error. (`3df9b2737ea`, #1220)
- **KNN test extension** — `vsql_knn_index_test` backs the MTR coverage of the distance scan. (`8b0927e752f`, #1241; `badf9e3eeb3`, #1247; `6280ffdb213`, #1248; `d9b40292eb6`, #1276; `f95f6faefc7`, #1243; `0971bbb7456`, #1274; `69ca9a7ae84`, #1273; `46b437cfec5`, #1272; `3a4924f2818`, #1275)

### Stability

- **Server starts on big-endian platforms** — Verified on `linux/s390x`. (`33efda8d2f9`, #1049)
- **Valgrind memcheck and ThreadSanitizer** — Nightly workflows run the VillageSQL tests and the bundled extensions under Valgrind memcheck and under ThreadSanitizer; the ThreadSanitizer run suppresses upstream MySQL findings and nothing of VillageSQL's. One fix came out of them: `register_variable()` no longer holds `LOCK_plugin` across `set_persisted_options()`. (`cbe719062ed`, #1157; `3cc9f680ca7`, #1160; `c6c8f3faacd`, #1165; `b14c1e68215`, #1169; `76e9c89557e`, #1173; `95ed8cc3bf8`, #1181; `c122288a82e`, #1205; `a0bb37b159f`, #1246; `96ca4d8fb1a`, #1185; `bf1302d7793`, #1226; `90a540c25e6`, #1219; `1e1188bf18f`, #1217; `3688a86c84b`, #1195; `606d247f0bb`, #1193; `ad7583bb24b`, #1234; `da5c2094842`, #1242)
- **Test coverage** — New MTR and unit tests cover the storage ABI, extension metadata locking, custom-type stored-procedure parameters, `FULLTEXT` with custom columns, extended-storage rollback, deferred version updates, and `ALTER EXTENSION` to an earlier version. The `connection_not_alive` test is stable under parallel runs. (`d665614d687`, #984; `54ce39b7620`, #957; `e6f17e8c41f`, #1153; `bb95002a73a`, #1137; `10a099c37bd`, #1123; `c54c1640202`, #1171)
- **View over a dropped custom-type column** — `SHOW CREATE VIEW` reports a broken view with warning 1356 instead of crashing the server. (`0701227ff6f`, #1228, #1199)

### Build & Compatibility

- **Native RPM and DEB packaging** — Packaging sources and build scripts land in `packaging/villagesql-rpm/` and `packaging/villagesql-deb/`. The packages take over an existing MySQL install in place. No workflow publishes them yet, so the release artifacts are unchanged. (`ea623810efb`, #931)
- **Ubuntu 26.04 builds** — CI builds on Ubuntu 26.04, and two bugs its compiler surfaced are fixed. (`cf3499be13c`, #1097; `9057fe31137`, #1107; `9d85e0a5119`, #1108)
- **macOS builds pin OpenSSL 3** — The build scripts point at `openssl@3`, since Homebrew's unversioned `openssl` is now OpenSSL 4. (`cbf6aa2e8dd`, #1254)
- **Client banners name VillageSQL** — Every binary that prints the shared copyright notice, `mysqld` and `mysql` among them, prints a VillageSQL copyright line above the Oracle line. (`f14e0a5d6eb`, #1198)
- **Docker** — The server image streams the bootstrap server's log, the dev container runs as the host user, and each codebase has its own multi-platform `latest` tag (`mysql-8.4_latest`, `mysql-9.7_latest`, `percona-8.4_latest`) and release tag, for example `mysql-8.4_0.0.7`. (`06ae8da633c`, #1152; `85f12e9eb14`, #1126; `6f1c4988e37`, #1065; `4dbfcca7c5a`, #1174)
- **Release automation** — Releases are cut, tested, and published through dispatched workflows. (`2b985c04f13`, #1083; `2b99129bcfc`, #1125; `5e8ab844da8`, #1082; `b4344ce7dda`, #1086; `3b47edc9de8`, #1091; `91b250d46eb`, #1093; `350b0e0cdf7`, #1095; `38495b40f4f`, #1175)
- **CI** — The nightly dispatch drives the sanitizer and extension-compat runs, the Full Test Suite runs nightly on macOS, and pull request labels, review, and merge run through one prow-based workflow. (`39d5d8b8955`, #1071; `a7790b71ef2`, #1072; `6442c74867e`, #1038; `6d147a1b021`, #1114; `2deed4a37f8`, #1146; `2c7bb6fa3a6`, #1130; `2a1a1344475`, #1134; `4d7152888a8`, #1163; `7f8af9db1c8`, #1164; `2eb58eefbb6`, #1167; `41181a73177`, #1166; `b7133dd53f6`, #1235, #1218; `d259762bbc1`, #1277; `9998b5e1dc4`, #1207; `f436c0f953b`, #1080; `aba58cfba3f`, #1176)
- **README and policy** — The README and `AGENTS.md` give the current build and test commands and how to install the `clang-format` version the linter needs, and the contributing guide's AI policy covers issues and bars AI code review assistants. (`030bccd2aae`, #1141; `1d56259388d`, #1154; `99ed4310e91`, #1147; `6dc6ca1c6c9`, #1061; `aa68a9c2a55`, #1056; `ebb1c10167e`, #1177; `20d5fed75ec`, #1257)

## Community

Thanks to @EvgeniyPatlan for the native RPM and DEB packaging. (`ea623810efb`, #931)
