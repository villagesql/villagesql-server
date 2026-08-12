# VillageSQL 0.0.6

Draft release notes — the release commit will be recorded when 0.0.6 is tagged.

## What's New

### Authentication Extensions (preview)

- **Authentication capability** — Extensions can now provide a login method through the `vsql::preview::auth` capability, invoked on the pre-authentication handshake. Ships with the SDK surface for writing one. (`d7e0800ea87`, #762; `8533deb86a3`, #867; `7dfd0025c33`, #875)
- **Token-driven role activation** — An authentication extension can name the roles to activate for the session, so a JWT's claims can decide what the login starts with. Only roles already granted to the user are honored, so a token cannot escalate privileges. (`ddac93e3578`, #814)
- **Automatic user creation** — An authentication extension can create the account on first successful login. (`8500503f23d`, #903)
- **`IDENTIFIED WITH <method> BY '<password>'` is now rejected** — The `BY` clause is not meaningful for the current authentication extensions and was silently accepted before. (`18d36c72122`, #874)
- **Persisted authentication method fix** — Fixes a bug in how an extension-provided authentication method was persisted. (`4bbd3a832af`, #881)

### Extensions

- **MySQL services capability** — Extensions can consume MySQL component services through a new capability. This release exposes the consumer side only; providing services is not yet supported. (`4c56f3f00e1`, #954)
- **Extension init and deinit hooks** — `on_init()` and `on_deinit()` on the extension builder register a function to run extension-side at load and unload, for local setup such as selecting CPU-specific implementations or allocating extension-owned state. (`95d5f59a9b3`, #947)
- **Richer statement telemetry** — The statement-event capability now reports the performance-schema statement digest hash and the per-statement handler row-access counters (`read_key`, `read_next`, `read_rnd_next`, and the rest), which quantify the access method that the existing flags only flagged. (`de3d664cbae`, #921)
- **`EXTENSION_ADMIN` privilege** — Extension management is now gated by its own dynamic privilege rather than `SUPER`. In-place upgrades grant it to existing `SUPER` holders. (`9854529cb56`, #752; `9e0577bd0b6`, #941)
- **Rust extensions in nightly testing** — The nightly extension suite builds and tests Rust extensions alongside the C++ ones. (`c323f70848c`, #932; `f2e6b27ad9e`, #949)
- **`vsql_allow_preview_extensions` can no longer be changed with `SET GLOBAL`** — Both directions are now rejected. Previously turning it off was accepted and then silently reverted on restart. (`079184a2216`, #960)

### Custom Types

- **Duplicate names within one extension are rejected at install** — An extension registering two custom types, two VDFs, two index types, or two index profiles under the same name is now refused at `INSTALL EXTENSION` instead of being accepted. (`2074660c4f2`, #834; `3c6c6f70270`, #854; `c50dbe40ef5`, #918; `36860278e24`, #919)
- **Over-large types are rejected at install** — A type declaring a storage footprint wider than a column can hold used to fail later from MySQL's field machinery, either with a `ER_TOO_BIG_FIELDLENGTH` message that quoted a limit the user never chose, or by silently rewriting the column to a `BLOB` outside strict mode. The declared footprint is now checked at install time, capped at 65532 bytes. (`950945e1bb2`, #836)
- **`resolve_params` sizes are validated** — A resolved `persisted_length` may not exceed the declared `max_persisted_length`, and a resolved `max_decode_buffer_length` must be greater than zero. (`b69ca1a5aad`, #835)
- **Type builder invariants are checked at compile time** — Misconfigured custom types now fail to compile rather than failing at `INSTALL EXTENSION` or DDL time: `params<P>()` and `int_to_params()` require `resolve_params()`, variable-length types must not declare `persisted_length()`, `max_persisted_length()` is only valid for variable-length or parameterized types, and the two intrinsic-default forms are mutually exclusive. (`db340305af0`, #833)

### Stability

The server and the bundled extensions now run under ASAN, UBSAN and LSAN every night. Several of the fixes below came out of those runs. (`c14c14040f1`, #956; `95044fa29b1`, #880; `81dd34a866d`, #959)

- **Server deadlock on concurrent extension queries** — Concurrent queries calling an extension function over a custom-type table could deadlock the whole server, through a nested read lock on the victionary's writer-preference lock. (`4e599fe2a9c`, #913)
- **Dotted identifiers in the victionary** — Renaming a custom-type column failed, and crashed debug builds, when the database, table, or column name contained a dot. Victionary update lookups split the dot-joined key instead of using the key components. (`4967b3ffba4`, #981)
- **`deterministic` read on pre-protocol-3 extensions** — The flag only exists as of `VEF_PROTOCOL_3`, and older function descriptors leave that byte uninitialized. The read that drives custom-type inference is now guarded on the protocol version. (`87622ecb9cd`, #865)
- **`UNINSTALL EXTENSION` commit failure** — The extension's shared object was unloaded before the transaction committed, so a failed commit rolled the metadata back but left the extension unusable. It is now unloaded only after the commit succeeds. (`3052598b8e3`, #872)
- **`INSTALL EXTENSION` and `ALTER EXTENSION ... AT RESTART` commit failure** — OK status was set before the transaction committed, so a failed commit raised an error over an already-set OK and aborted debug builds. (`746edebe59c`, #896)
- **Shared object leak on a failed install** — A failed `INSTALL EXTENSION` left the shared object mapped in the server process, with any capabilities it had populated still registered. (`a3223425319`, #904)
- **Shutdown ordering** — Extension state was destroyed before InnoDB shut down, leaving references into already-unloaded shared objects. Shutdown is now split so capabilities are torn down first and extension state after. (`f682003dbf5`, #911)
- **Storage cleanup on unload** — Adds an unload hook so extension storage is released. (`a57a0a8c195`, #906)
- **Memory and resource leak fixes** — Cursor leak in the custom column insert path, `TypeDecoder` leaks in stored programs, an arena leak in the custom index SDK wrapper, and a use-after-free race in thread handle teardown. (`d9dcf3a724d`, #907; `ca2497dcbc1`, #869; `61b3aef6680`, #873; `e9a3064321e`, #861; `03701ec2406`, #866; `1bbad41e8db`, #855)

### Build & Compatibility

- **Release Docker images** — Publishing Docker images is now part of the release process, built one architecture at a time on its own runner. (`b6d70466949`, #818; `a13a537f01f`, #973)
- **README corrections** — The stated build prerequisites were looser than what the build enforces (Clang 14 and CMake 3.19, not Clang 13 and CMake 3.16; Windows is not supported), two documented commands could not run as written, and two Known Limitations were broader than reality. The Rust SDK, its crate and build tool, and the Rust extension template are now named alongside the C++ ones. (`2e620ad5d7b`, #962; `927e57622c3`, #934)
- **Release artifact naming** — Server tarballs are named per codebase and platform, SDK tarballs per release version. (`0e7e6d669ee`, #803)
- **Build tooling consolidated under `villagesql/bld_tools/`** — The older top-level scripts (`build-ci.sh`, `test-ci.sh`, `make_villagesql_dev_server.sh`, `build_bundled_extension`, `setup_linux_build_env.sh`) are retired in favor of the `bld_tools` versions, which now carry a README, and build information is published as JSON. (`d823a3e6a96`, #888; `a2b40362189`, #886; `582e58f00c9`, #889; `c83946394f4`, #887; `3920398fec3`, #935; `af1938f0285`, #969)
- **Release-mode compilation fix** — An assertion inside an `else` branch broke the release build under `-Werror=empty-body`. (`e834978b761`, #897)
- **Dev server socket override** — The dev server wrapper script accepts a custom socket path. (`b3096f2ace0`, #882)

## Community

Thanks to @samkhn for fixing a documentation comment that broke the Clang `-Wdocumentation` build. (`76dda4fe0b1`, #864)
