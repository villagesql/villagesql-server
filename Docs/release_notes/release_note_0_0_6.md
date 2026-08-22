# VillageSQL 0.0.6

Draft release notes through commit `c05a40cb377`: Schema update - Rerecord mysql tests (#1031).

The GitHub release assets are available at https://github.com/villagesql/villagesql-server/releases.
The Docker Hub release artifacts are available at https://hub.docker.com/r/villagesql/server.
The Cargo release artifacts are available at https://crates.io/crates/villagesql.

## What's New

### New Codebases

- **`mysql-9.7` and `percona-8.4` builds** — 0.0.6 ships three server lines: `mysql-8.4` tracking MySQL 8.4.11, `mysql-9.7` tracking MySQL 9.7.2, and `percona-8.4` with Percona Server 8.4.10-10 merged in. Release tarballs are named per codebase, and the server reports its codebase as a prefix on `villagesql_server_version` (for example `mysql-9.7_0.0.6`). A data directory belongs to one codebase; the server refuses to start against a data directory initialized by a different one. (built from the per-codebase branches, not a single main PR)

### Authentication Extensions (preview)

- **Authentication capability** — Extensions can now provide a login method through the `vsql::preview::auth` capability, invoked on the pre-authentication handshake. Ships with the SDK surface for writing one. (`d7e0800ea87`, #762; `8533deb86a3`, #867; `7dfd0025c33`, #875)
- **Token-driven role activation** — An authentication extension can name the roles to activate for the session, so a JWT's claims can decide what the login starts with. Only roles already granted to the user are honored, so a token cannot escalate privileges. (`ddac93e3578`, #814)
- **Automatic user creation** — An authentication extension can create the account on first successful login. (`8500503f23d`, #903)
- **Automatic role granting** — An authentication extension can opt in, per login, to having the server `GRANT` the roles it stages to an account that already exists, so a token claiming a role the account was never granted takes effect instead of being skipped. Left unset, the activate-only default stands and the DBA keeps ownership of grants. (`bebf89cb588`, #910)
- **Any client plugin that offers an unscrambled secret is accepted** — Authentication no longer requires an exact match with the pinned client plugin, which cost extra round trips and tied the client to one specific plugin. (`bbc2dd12848`, #912)
- **`IDENTIFIED WITH <method> BY '<password>'` is now rejected** — The `BY` clause is not meaningful for the current authentication extensions and was silently accepted before. (`18d36c72122`, #874)
- **Persisted authentication method fix** — Fixes a bug in how an extension-provided authentication method was persisted. (`4bbd3a832af`, #881)

### Extensions

- **MySQL services capability** — Extensions can consume MySQL component services through a new capability. This release exposes the consumer side only; providing services is not yet supported. (`4c56f3f00e1`, #954)
- **`vsql_oauth2` ships with the server** — The vsql-oauth2 authentication extension joins the bundled extensions list, so its `.veb` is included in the server release artifacts. (`5f98c2ba3f0`, #1018)
- **Extension init and deinit hooks** — `on_init()` and `on_deinit()` on the extension builder register a function to run extension-side at load and unload, for local setup such as selecting CPU-specific implementations or allocating extension-owned state. (`95d5f59a9b3`, #947)
- **Richer statement telemetry** — The statement-event capability now reports the performance-schema statement digest hash and the per-statement handler row-access counters (`read_key`, `read_next`, `read_rnd_next`, and the rest), which quantify the access method that the existing flags only flagged. (`de3d664cbae`, #921)
- **`EXTENSION_ADMIN` privilege** — Extension management is now gated by its own dynamic privilege rather than `SUPER`. In-place upgrades grant it to existing `SUPER` holders. (`9854529cb56`, #752; `9e0577bd0b6`, #941)
- **Rust extensions in nightly testing** — The nightly extension suite builds and tests Rust extensions alongside the C++ ones. (`c323f70848c`, #932; `f2e6b27ad9e`, #949)
- **SDK `SysVarDescriptor` example corrected** — The brace-init example in the comment above `SysVarDescriptor` showed four descriptor lines, three of which did not compile. (`25a7e3f32ff`, #1021)
- **`vsql_allow_preview_extensions` can no longer be changed with `SET GLOBAL`** — Both directions are now rejected. Previously turning it off was accepted and then silently reverted on restart. (`079184a2216`, #960)

### Custom Types

- **Duplicate names within one extension are rejected at install** — An extension registering two custom types, two VDFs, two index types, or two index profiles under the same name is now refused at `INSTALL EXTENSION` instead of being accepted. (`2074660c4f2`, #834; `3c6c6f70270`, #854; `c50dbe40ef5`, #918; `36860278e24`, #919)
- **Over-large types are rejected at install** — A type declaring a storage footprint wider than a column can hold used to fail later from MySQL's field machinery, either with a `ER_TOO_BIG_FIELDLENGTH` message that quoted a limit the user never chose, or by silently rewriting the column to a `BLOB` outside strict mode. The declared footprint is now checked at install time, capped at 65532 bytes. (`950945e1bb2`, #836)
- **`resolve_params` sizes are validated** — A resolved `persisted_length` may not exceed the declared `max_persisted_length`, and a resolved `max_decode_buffer_length` must be greater than zero. (`b69ca1a5aad`, #835)
- **Type builder invariants are checked at compile time** — Misconfigured custom types now fail to compile rather than failing at `INSTALL EXTENSION` or DDL time: `params<P>()` and `int_to_params()` require `resolve_params()`, variable-length types must not declare `persisted_length()`, `max_persisted_length()` is only valid for variable-length or parameterized types, and the two intrinsic-default forms are mutually exclusive. (`db340305af0`, #833)

### Example Extensions

- **`vsql_tvector` lost float precision on read** — The text conversion capped at 6 significant digits where a float needs about 7.2, so a stored `1.2345679` came back as `1.23457`. Any read-modify-write — a dump and restore, `INSERT ... SELECT` through a client — silently changed the data, and float is the default element type, so the default configuration was the lossy one. Doubles no longer render binary artefacts such as `1.0778787000000001` either. (`97cf46cbe67`, #999; #1007)
- **`vsql_complex` lost precision on read** — The same defect on the complex type's double components: `999999999.1234568` rendered as `1e+09`. Adds a round-trip fidelity test that fails on every row against the old code. (`8b088c2d848`, #996; #998)
- **`vsql_complex` failed to build against an older deployment target** — libc++ marks the floating-point overloads of `std::from_chars` availability-gated, so building the extension reported `error: 'from_chars' is unavailable: introduced in macOS 26.0`. The components are parsed with `strtod` instead. (`072fe7b4065`, #1027)

### Schema & Upgrades

- **VillageSQL system tables moved to `utf8mb4_bin`** — All six VillageSQL system tables change from `utf8mb4_0900_ai_ci` to `utf8mb4_bin`, so identifiers that differ only in case or accents stay distinct. An in-place upgrade converts the tables and bumps the stored schema version to 0.0.6, startup validation catches a table left on the wrong collation, the `INFORMATION_SCHEMA.COLUMNS` view join casefolds explicitly, and the victionary checks for duplicate rows when it reloads at startup. A follow-up aligns the affected `INFORMATION_SCHEMA` view collations with upstream: `DATA_TYPE` and `COLUMN_TYPE` in `INFORMATION_SCHEMA.COLUMNS` compare case-sensitively again (`utf8mb4_bin`, as upstream), while the new `EXTENSION_NAME` and `EXTENSION_VERSION` columns in `INFORMATION_SCHEMA.EXTENSIONS` stay case-insensitive, matching how `INSTALL EXTENSION` treats names. (`41d6f86b943`, #987; `c05a40cb377`, #1031)
- **Upgrades now run under `--upgrade=MINIMAL`** — The VillageSQL upgrade was skipped whenever the server started with `--upgrade=MINIMAL`, which left the server running against an older schema than it was built for and could block startup. It now runs in that mode too. (`a2a952155b4`, #1004)
- **Cross-codebase upgrades are blocked** — Each VillageSQL version carries a codebase identifier (for example, `mysql-8.4`), and version comparisons are only meaningful within one codebase. Starting the server against a data directory initialized by a build with a different codebase now fails with an error at startup instead of attempting an upgrade. (`baf7446683d`, #1036)

### Stability

The server and the bundled extensions now run under ASAN, UBSAN and LSAN every night. Several of the fixes below came out of those runs. (`c14c14040f1`, #956; `95044fa29b1`, #880; `81dd34a866d`, #959)

- **Server deadlock on concurrent extension queries** — Concurrent queries calling an extension function over a custom-type table could deadlock the whole server, through a nested read lock on the victionary's writer-preference lock. (`4e599fe2a9c`, #913)
- **Dotted identifiers in the victionary** — Renaming a custom-type column failed, and crashed debug builds, when the database, table, or column name contained a dot. Victionary update lookups split the dot-joined key instead of using the key components. Distinct names could also still collide in the victionary's dot-joined map key — `my.db` + `tab.one` joins to the same key as `my.db.tab` + `one` — so one entry silently overwrote the other; dots inside each component are now escaped when the key is built. (`4967b3ffba4`, #981; `c07b9ae8295`, #1024)
- **Startup deadlock on pending extension updates** — Applying a pending extension update at startup writes the VillageSQL system tables, which queued them for background statistics recalculation; the statistics thread's lock could race with startup's exclusive lock request and hang startup silently. The VillageSQL system tables now set `STATS_PERSISTENT=0`, as the `mysql.*` tables do. (`985e73c8ba3`, #1001)
- **`deterministic` read on pre-protocol-3 extensions** — The flag only exists as of `VEF_PROTOCOL_3`, and older function descriptors leave that byte uninitialized. The read that drives custom-type inference is now guarded on the protocol version. (`87622ecb9cd`, #865)
- **`UNINSTALL EXTENSION` commit failure** — The extension's shared object was unloaded before the transaction committed, so a failed commit rolled the metadata back but left the extension unusable. It is now unloaded only after the commit succeeds. (`3052598b8e3`, #872)
- **`INSTALL EXTENSION` and `ALTER EXTENSION ... AT RESTART` commit failure** — OK status was set before the transaction committed, so a failed commit raised an error over an already-set OK and aborted debug builds. (`746edebe59c`, #896)
- **Shared object leak on a failed install** — A failed `INSTALL EXTENSION` left the shared object mapped in the server process, with any capabilities it had populated still registered. (`a3223425319`, #904)
- **Shutdown ordering** — Extension state was destroyed before InnoDB shut down, leaving references into already-unloaded shared objects. Shutdown is now split so capabilities are torn down first and extension state after. (`f682003dbf5`, #911)
- **Storage cleanup on unload** — Adds an unload hook so extension storage is released. (`a57a0a8c195`, #906)
- **System variable callback deadlock** — The global system variable lock was held while a variable's change callback ran, and a callback that blocks could deadlock. The lock is now dropped for the callback, and a lifecycle mutex takes over the start/stop serialization the global lock had been providing. This had been flaking tests under the sanitizer runs. (`557a102cdaf`, #995)
- **Memory and resource leak fixes** — Cursor leak in the custom column insert path, `TypeDecoder` leaks in stored programs, an arena leak in the custom index SDK wrapper, and a use-after-free race in thread handle teardown. (`d9dcf3a724d`, #907; `ca2497dcbc1`, #869; `61b3aef6680`, #873; `e9a3064321e`, #861; `03701ec2406`, #866; `1bbad41e8db`, #855)

### Build & Compatibility

- **MySQL 8.4.11** — The mysql-8.4 codebase is now on upstream `mysql-8.4.11`. (`209da79328c`)
- **Release Docker images** — Publishing Docker images is now part of the release process, built one architecture at a time on its own runner, and a release workflow orchestrates those builds and pushes the images to Docker Hub together. (`b6d70466949`, #818; `a13a537f01f`, #973; `50da376e7b1`, #1025)
- **README corrections** — The stated build prerequisites were looser than what the build enforces (Clang 14 and CMake 3.19, not Clang 13 and CMake 3.16; Windows is not supported), two documented commands could not run as written, and two Known Limitations were broader than reality. The Rust SDK, its crate and build tool, and the Rust extension template are now named alongside the C++ ones. (`2e620ad5d7b`, #962; `927e57622c3`, #934)
- **Release artifact naming** — Server tarballs are named per codebase and platform, SDK tarballs per release version. (`0e7e6d669ee`, #803)
- **Build tooling consolidated under `villagesql/bld_tools/`** — The older top-level scripts (`build-ci.sh`, `test-ci.sh`, `make_villagesql_dev_server.sh`, `build_bundled_extension`, `setup_linux_build_env.sh`) are retired in favor of the `bld_tools` versions, which now carry a README, and build information is published as JSON. (`d823a3e6a96`, #888; `a2b40362189`, #886; `582e58f00c9`, #889; `c83946394f4`, #887; `3920398fec3`, #935; `af1938f0285`, #969)
- **Dev server socket override** — The dev server wrapper script accepts a custom socket path. (`b3096f2ace0`, #882)
- **Root password can be set as a hash in Docker** — `MYSQL_ROOT_PASSWORD_CACHING_SHA2_HASH_HEX` sets the root password from a hex-encoded `caching_sha2_password` digest, so the plaintext never has to reach the container; the prefix is validated at startup. The bootstrap server is now shell-backgrounded rather than daemonized, so it can be signalled and waited on instead of keeping the root password available, and `mysqlx` is disabled during bootstrap so nothing is listening on a port before the root password is set. (`0cdbb140cee`, #784)
- **`jq` is now needed to build the bundled extensions** — The bundled extension tooling reads its extension table as JSON. GitHub runners ship `jq`, so a plain Debian or macOS box was the only place this showed up. The build environment setup script installs it. (`7d445f7d157`, #975; `da881129dd0`, #988)
- **Build prerequisites and steps corrected** — The prerequisites named C++17 where the build compiles with `-std=c++20`, and Bison 3.0 where 3.0.4 is the floor. The clone step now says to clone into the home directory, which the later steps assume, and the note about `~` carves out `mysqld` option values such as `--datadir=`, where the shell leaves the tilde unexpanded and the server aborts on the literal character. The README also stops carrying its own copy of the apt and brew package lists, which had drifted from `villagesql/bld_tools/`, and points at the setup script instead. (`ea11e2bb5c2`, #945; `e5b6ed67650`, #993; `cf887bc37cf`, #963)
- **Contributor Guide overhaul** — The first set of changes from a rewrite of the contributor guide. (`3d0c834cda4`, #760)
- **Nightly and release CI reworked** — A nightly run now dispatches listener workflows, so one event can start several workflows with different inputs, and it drives the release-build Full Test Suite. Its run summary names the nightly tag it created rather than leaving `nightly.latest` ambiguous. Version and extension information moved into JSON for parsing, the CMake and Cargo extension builds share one framework, and the self-hosted jobs are consolidated onto Linux x86-64 runners. (`1055b3c761e`, #1005; `de2afc6adfc`, #1008; `f03c8298f8a`, #1009; `b0c08387ddf`, #1002; `29c178727b5`, #1006; `e6d077f0975`, #989; `87cd97def3c`, #986; `93d13d9d147`, #990)
- **Pull request label and merge checks** — A pull request is now checked for an `area/` and a `kind/` label. One carrying both an LGTM and an approval merges automatically on an hourly pass, and the LGTM is withdrawn when new changes arrive. (`a946ab5d922`, #997)

## Known Issues

- **`--initialize` with `--default_table_encryption=ON` aborts** — Upstream MySQL 8.4.11 (the fix for Bug#39114059) assumes the `mysql` tablespace is unencrypted during initialization. With `--default_table_encryption=ON` that assumption is false, so `mysqld --initialize` aborts with `ERROR 3825 Request to create 'unencrypted' table while using an 'encrypted' tablespace` and leaves the new data directory unusable. A fix is in progress on `dbentley/fix-initialize-encryption`.

## Community

Thanks to @samkhn for fixing a documentation comment that broke the Clang `-Wdocumentation` build. (`76dda4fe0b1`, #864)
