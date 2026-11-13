# VillageSQL 0.0.8

Draft release notes through commit `a0bda74c`: Stage custom column renames as delete then insert (#1558)

The GitHub release assets are available at https://github.com/villagesql/villagesql-server/releases.
The Docker Hub release artifacts are available at https://hub.docker.com/r/villagesql/server.
The Cargo release artifacts are available at https://crates.io/crates/villagesql.

## What's New

### Extension Framework

- **`BINARY` VDF arguments and results (protocol 5)** — A VDF can take and return raw bytes, and a protocol 5 extension receives every `STRING` argument as `utf8mb4`. (`00a43f63a04`, #1411)
- **Constant argument values in `prerun` (protocol 5)** — A `prerun` sees the value of each constant argument through `const_args`, so it can accept a `NULL` literal without accepting every `STRING`. (`d17f5613a93`, #1503)
- **Binary form of a custom type (protocol 5)** — A custom type can declare `from_binary` and `to_binary`, available in SQL as `TYPE::from_binary` and `TYPE::to_binary`, to build a value from bytes and to return a value as bytes. (`99e0c558df2`, #1493; `16cd1f86440`, #1554)
- **Variadic VDFs can declare `bind_and_check_types`** — A variadic function can name the type of each bare literal among its arguments. (`9c26e4500ad`, #1287)
- **Custom type arguments in prepared statements** — A VDF with a custom type argument works in a prepared statement, and a prepared statement accepts only `NULL` or string values for a custom type parameter. (`ea3757cba9f`, #1296; `8502bfbace1`, #1314; `821bfbf7ccf`, #1421)
- **Custom type parameters from a `?`** — In a prepared statement, the string value bound to a `?` sets a custom type's parameters, as in `TVECTOR::from_string(?)` or `tvector_dim(?)` from the `vsql_tvector` example extension. Checks that need those parameters run at `EXECUTE`. (`5a111f69686`, #1489; `5442a85e37b`, #1490; `e355f12dea5`, #1491)
- **Fewer reprepares** — The server prepares a statement that calls a VDF again at `EXECUTE` only when the VDF needs it, such as for `prerun`, a `bind_and_check_types` hook, parameters inferred from a `?`, or an aggregate. (`6d96d69352d`, #1530)
- **Prepared statement fixes** — `PREPARE` no longer crashes when a `?` is inside an expression passed to a VDF. A custom type `OUT` parameter of a prepared `CALL` gives the user variable the custom type value, as a direct `CALL` does. After a failed reprepare, a VDF's `postrun` runs, and the next `EXECUTE` uses its newly bound values. (`509b09e464e`, #1519; `8b1029496c1`, #1507; `3bfdffa1266`, #1535; `985e971ca69`, #1537)
- **Custom type parameters are standardized** — Parameter text is converted to `utf8mb4` before it is normalized, and malformed UTF-8 or an embedded NUL is rejected. (`441dbf67c4d`, #1283)
- **VDF result types** — A `REAL` VDF keeps its fraction over `INT` arguments or no arguments. A custom type result is sized from its resolved type, so sorting, grouping, and `CREATE TABLE ... SELECT` work. `IS NULL` on an aggregate VDF that returns `NULL` gives the correct answer. (`d8be0006f08`, #1327; `cd1d5ff6b34`, #1297; `636c360e839`, #1341)
- **VDF cleanup** — `postrun` runs when setup fails after `prerun`, so state that `prerun` allocated is released. (`c4a0a6689ce`, #979)
- **Intrinsic default VDFs** — A type's intrinsic default VDF can no longer be called from SQL. (`83654c5c3f9`, #1304)
- **Ambiguous custom type names** — The error for a custom type name that more than one extension provides lists those extensions and says to use the qualified name `extension.type`. (`583f619a818`, #1293)
- **Type-method names** — Method prefixes follow the same identifier rules as type names, so a type named `CAFÉ` accepts `café::from_string`. This also applies to protocol 3 extensions. (#1529)
- **Extension install and update** — A `.veb` expansion is staged and renamed into place, so a crash during install does not leave an unusable `.so`. A truncated `.veb` is rejected. (`64b3a3c8853`, #1250; `62375da16ff`, #1308)
- **Extension upgrade** — A failed extension update at restart rolls back only that extension, and the upgrade checks length changes for variable-length types. (`9c4b731c250`, #1343; `42871275adc`, #1256)
- **Extension exceptions (protocol 5)** — A VDF that throws an exception returns an error to the statement. An extension built with this SDK also returns an error when its custom type's `from_string` function throws. (`28d9f609278`, #1231; `b01b6b72ee8`, #1355)
- **Global status variable `Villagesql_veb_cache_repaired`** — Counts the times the server rebuilt a `.veb` expansion whose cached `.so` was missing or empty. (`967bde76c2a`, #1361)
- **System variable fixes (preview)** — String and integer variables are handled separately, which fixes null-termination bugs in string values. (`e6435ab0a88`, #1334)
- **Grants from an external authentication method are not binlogged (preview)** — They do not replicate. (`029df5265d5`, #1335)
- **Statement events (preview)** — A failed `INSTALL EXTENSION` no longer silences other extensions' statement hooks. (`2bae1ccd679`, #1405)
- **Session variables (preview)** — After `SET GLOBAL` on a string session variable, a session that has not set its own value reads the new global value. (`654226b1c64`, #1340)

### Custom Types

- **Input is converted to `utf8mb4`** — A string that the server encodes into a custom type is converted from its charset first, on insert, update, `LOAD DATA`, comparison, and prepared-statement parameters. (`c046b601673`, #1345)
- **Values the type cannot parse or encodes as `NULL`** — A value the encode function cannot parse stores the type's intrinsic default, and strict mode still rejects the statement. An encode that returns `NULL` stores `NULL`, and one that raises an error aborts with the extension's message. (`98c2d1f8e73`, #1353; `f70fab8f8bc`, #1351)
- **`LOAD XML` and `LOAD DATA`** — `LOAD XML` reports a `NULL` from an encode into a `NOT NULL` column, and both statements report a bad value in strict mode once. (`a2e239e16a9`, #1409)
- **Unreadable values print a placeholder** — A value that cannot be decoded for printing shows `<error decoding value>`. (`4cc6321d2bb`, #1230)
- **Temporary tables** — A custom type column is refused on a non-InnoDB temporary table. A failed `CREATE TEMPORARY TABLE` no longer leaves the type in use, which blocked `UNINSTALL EXTENSION`. (`51112e889ad`, #1422; `8920b323416`, #1430)
- **Renames keep custom columns** — A `RENAME TABLE` statement whose pairs swap or chain table names keeps the custom column types. An `ALTER TABLE` that swaps or chains custom column names no longer crashes the server. (`a3ff14f42bf`, #1543; `a0bda74c8c2`, #1558)
- **`DROP DATABASE`** — A database with a stored routine that has a custom type parameter and a table with a custom type column no longer crashes the server. `DROP DATABASE` and `ALTER DATABASE` on the `villagesql` schema are refused. (`fb39aca0a57`, #1316; `4b3f751d0f1`, #1357)

### Custom Indexes (in development)

Custom index DDL is refused by default, with `Extended Index feature not yet implemented`; setting `vsql_allow_preview_extensions=ON` accepts it.

- **Default index profiles** — The server rejects an extension that registers two default index profiles for the same data type and index type. (`35c0c11e1c7`, #1292)
- **`CREATE TABLE ... LIKE`** — The copy keeps the index profile of a custom index. (`940268df3ae`, #1455)
- **Renames keep custom indexes** — `RENAME TABLE`, `ALTER TABLE ... RENAME`, and `ALTER TABLE ... RENAME INDEX` keep a custom index. (`adbddb63104`, #1544; `e2f394d88ca`, #1546)
- **Prepared statements** — A nearest-neighbor query picks the custom index when its parameter is bound at execute. (`d37e60d52bd`, #1298)

### Build & Compatibility

- **macOS** — Configure finds Homebrew's `openssl@3` when no OpenSSL is named, and stops with `brew install openssl@3` when it is missing. The server builds with Xcode 27. (`5efd730e986`, #1416; `57dd227fcd8`, #1400)
- **Server log** — `LogVSQL` messages no longer carry extra single quotes. (`a0d05d781ad`, #1282)

## Community

Thanks to @kash2104 for the placeholder on a value that cannot be decoded and for the `string_view` cleanup of the `from_raw` callers. (`4cc6321d2bb`, #1230; `7eb5f3f2dba`, #1410)

Thanks to @jobala for the `is_null()` check on `PrerunArgs` (#1073), which #1503 builds on.
