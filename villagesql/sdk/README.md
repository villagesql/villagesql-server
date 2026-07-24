# VillageSQL Extension Framework SDK

The VillageSQL Extension Framework (VEF) allows developers to extend
VillageSQL, including adding custom functions and types in a future-proof way.
Extensions are compiled as shared libraries (`.so`) and packaged as `.veb`
files that the server loads at runtime.

See `include/villagesql/extension.h` for the full C++ API and examples.

This SDK contains:

- Two Protocol Versions:
  - Stable (`/include` in the SDK bundle) - The most recent stable API.
    Extensions built against this API can be expected to work on any currently
    supported server version. It is copied from `villagesql/stable_sdk/v{N}`.
  - Unstable (`/include-dev` in the SDK bundle) - The in-development version of
    the API. The stable version is used by default for out of tree extensions.
    It is copied from `villagesql/sdk/include`.
    TODO(villagesql): Add documentation on how to develop extensions against
    unstable.
- CMake files - Used to build extension libraries and package veb files.
- A template project that can be the starting point for a VillageSQL Extension.

## Protocol Versions

The VEF uses an explicit protocol version to manage compatibility between the
server and extensions. Both sides declare the highest protocol version they
support; the lower of the two is used for the session:

```
negotiated_protocol = min(server_max_protocol, extension_max_protocol)
```

This means an extension built against an old protocol continues to work with a
new server, and a new extension gracefully degrades when loaded by an old
server.

### Current Protocol Versions

| Version          | Status         | Notes                                                                                                                                                                                              |
| ---------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `VEF_PROTOCOL_1` | Stable         | Base protocol (stable as of v0.0.1). Likely to be deprecated before beta.                                                                                                                        |
| `VEF_PROTOCOL_2` | Deprecated     | Was the unstable development version before being promoted to `VEF_PROTOCOL_3`. The server rejects extensions that declare this version, and no `stable_sdk/v2` snapshot exists.                  |
| `VEF_PROTOCOL_3` | Stable         | Stable as of v0.0.4. Adds the `deterministic` VDF attribute; VDF-based type operations (encode/decode/compare/hash and int_to_params/resolve_params name fields); a values pointer array; and the preview-capability system. |
| `VEF_PROTOCOL_4` | In development | Adds `max_result_length` on `vef_func_desc_t` so a STRING result column can be sized to the full value instead of the argument width.                                                             |

The enum in `villagesql/sdk/include/villagesql/abi/types.h` is the source of truth
for protocol status; keep this table in sync with it.

### Version numbering model

Protocol version parity encodes stability:

- **Odd versions are stable** (`VEF_PROTOCOL_1`, `_3`, ...). Each odd version has
  a frozen snapshot under `villagesql/stable_sdk/v{ODD}/` and is accepted by the
  server forever.
- **Even versions are unstable/in-development** (`VEF_PROTOCOL_2`, `_4`, ...).
  Only one even version — the server's current one — exists at a time. It has no
  snapshot, and its ABI/API may still change.

The server advertises a single "current" version, `vef_server_protocol_version`
in `villagesql/veb/veb_file.cc`. At load time it negotiates
`min(server_version, extension_version)` and then enforces the parity rule (also
in `veb_file.cc`):

- An **odd** (stable) extension version is always accepted.
- An **even** (unstable) extension version is accepted only if it exactly matches
  the server's current version; any older even version is rejected as obsolete.

There is currently **no minimum-supported-version floor** — every odd version
back to `VEF_PROTOCOL_1` is accepted.
TODO(villagesql-beta): Define a minimum supported stable version and enforce it
next to the parity check in `veb_file.cc`.

### Changing the ABI/API

Any changes to the ABI/API must be done in the Unstable/Development version of
the headers (`villagesql/sdk/include`), never in a frozen `stable_sdk/` snapshot.
New fields go at the **end** of the relevant descriptor struct, guarded by a
comment marking the minimum protocol required (e.g.,
`// protocol >= VEF_PROTOCOL_4`). See [API_ABI.md](API_ABI.md) for the full rules.

### Rolling the protocol version (stabilizing)

When the current unstable version `M` (even) is ready, it is **promoted** to the
next stable version `M+1` (odd), and a new unstable version `M+2` (even) is
opened for future work. A roll therefore adds *two* enum values, not one.

Worked example: PR #581 promoted the unstable `VEF_PROTOCOL_2` to stable
`VEF_PROTOCOL_3` and opened `VEF_PROTOCOL_4` as the new unstable version. Use its
diff as the reference for the full set of edit sites.

**1. Freeze the snapshot.** Stop changing `villagesql/sdk/` and copy its current
   contents to `villagesql/stable_sdk/v{M+1}/` (the new odd/stable snapshot).
   Even versions are never snapshotted, which is why there is no `stable_sdk/v2`.

**2. Add both enum values** to `vef_protocol_t` in
   `villagesql/sdk/include/villagesql/abi/types.h`: `VEF_PROTOCOL_{M+1}` (now
   stable) and `VEF_PROTOCOL_{M+2}` (new unstable). Mark the old even value `M` as
   deprecated/rejected.

**3. Bump the "current version" to `M+2`** everywhere it is hardcoded:
   - `villagesql/veb/veb_file.cc` — `vef_server_protocol_version` (the server's
     advertised max; also the value the parity check compares against).
   - `villagesql/sdk/include/villagesql/detail/vef_register.h` — the
     `reg.protocol` / `desc.protocol` the dev SDK advertises.
   - Per-feature minimum-protocol gates in the builder headers
     (`detail/func_builder.h`, `func_builder.h`, `type_builder.h`,
     `vsql/type_builder.h`, `vsql/extension_builder.h`, `vsql/var_args.h`) and any
     server-side gates (e.g. `sql/sql_udf.h`,
     `villagesql/schema/descriptor/type_context.cc`,
     `villagesql/services/capability_registry.cc`,
     `villagesql/sql/func_lookup.cc`).
   TODO(villagesql): Centralize the current-version constant so this step is a
   single edit instead of many.

**4. Update the build** — `villagesql/CMakeLists.txt` (`STABLE_SDK_DIR`) and
   `villagesql/cmake/VsqlExtension.cmake` to point the stable include path at
   `stable_sdk/v{M+1}`.

**5. Update the tests.** This is the long tail:
   - Clone the previous stable ABI suite into a new `mysql-test/suite/villagesql/abi_v{M+1}/`
     (binary- and source-compat tests + the SDK-finding include).
   - Update expectations in the `extension` / `extension_registration` suites and
     `std_data/min_protocol_extension.cc`, plus the affected gunit tests
     (`type_context-t.cc`, `type_descriptor-t.cc`, `validate-t.cc`).

**6. Update external repos** — bundled extensions and the extension template — to
   build against the new stable API, and remove `use_dev_unstable` from any repo
   that no longer needs an unstable-only feature (i.e. everything it used is now
   in the newly-stabilized `stable_sdk/v{M+1}`).

**7. Update docs** — the protocol status table above and any references in
   [API_ABI.md](API_ABI.md).

TODO(villagesql): Decide how/when preview headers are published into the stable
SDK snapshot (e.g. only at release time).

The `stable_sdk/` snapshots are the source of truth for what extensions compiled
against a given protocol version can expect. An extension built from a frozen
snapshot must continue to load on all future server versions.
