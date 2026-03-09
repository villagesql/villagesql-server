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

| Version          | Status         | Notes                                                                      |
| ---------------- | -------------- | -------------------------------------------------------------------------- |
| `VEF_PROTOCOL_1` | Stable         | Base protocol. Planned to be deprecated before beta.                       |
| `VEF_PROTOCOL_2` | In development | Adds `deterministic` flag, VDF-based type operations, parameterized types. |

### Changing the ABI/API

Any changes to the ABI/API must be done in the Unstable/Development version of the headers.
See [API_ABI.md](API_ABI.md) for more details on the API and ABI.

### Stabilizing a Protocol

When the functionality in the new protocol is ready, then it should be
stabilized, and a new protocol should be started.

1. Copy the now "stabilized" `villagesql/sdk/` to the appropriate subdirectory
   of `villagesql/stable_sdk/v{N}` where N is the existing protocol.
2. Add the new `VEF_PROTOCOL_{N+1}` value to `vef_protocol_t` in
   `villagesql/sdk/include/villagesql/abi/types.h`.
3. Add new fields to the end of the relevant descriptor struct(s), guarded by a
   comment marking the minimum protocol required (e.g.,
   `// protocol >= VEF_PROTOCOL_{N+1}`).
4. Update `VEF_GENERATE_ENTRY_POINTS` in `extension_builder.h` to advertise the
   new protocol.
5. Add corresponding API support in the builder headers.
6. Update the protocol status table above.

The `stable_sdk/` snapshots are the source of truth for what extensions compiled
against a given protocol version can expect. An extension built from a frozen
snapshot must continue to load on all future server versions.
