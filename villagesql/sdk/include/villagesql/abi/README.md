# VEF ABI Headers

These headers define the **binary interface** (ABI) between the VillageSQL
server and extension `.so` files. They are the contract enforced at load
time.

## Don't write code against these headers

If you are writing an extension, use the C++ API in `<villagesql/vsql.h>`
instead. The ABI headers define raw C structs and function-pointer
signatures (`vef_invalue_t`, `vef_vdf_func_t`, etc.) that are intentionally
minimal and ergonomically poor — the API is what makes them usable.

The ABI exists so:

- The server and extension `.so` files can be compiled separately and still
  agree on memory layout and calling conventions.
- An extension built against an older protocol version keeps working when
  loaded by a newer server, via protocol negotiation.

## Stability

The ABI's stability story is governed by **protocol versions**, not by the
SDK release cadence. See [../../API_ABI.md](../../API_ABI.md) for the full
distinction between API and ABI and how each evolves.

Concretely:

- `stable_sdk/v{N}/` snapshots are the frozen ABI surface for protocol N.
  An extension compiled against a frozen snapshot must continue to load on
  all future server versions.
- Headers under `abi/preview/` expose preview-capability ABIs and follow
  the preview-capability rules in [../preview/README.md](../preview/README.md).
