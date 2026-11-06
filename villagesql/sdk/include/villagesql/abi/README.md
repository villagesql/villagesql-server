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

## Type parameter text

Type parameter names and values must be valid UTF-8 without embedded NUL
because the ABI uses NUL-terminated strings. The server converts SQL input
from the connection charset to utf8mb4; extensions must supply UTF-8. The server
trims surrounding ASCII spaces and lowercases names and values. Names are sorted
and checked for duplicates using the fixed `utf8mb4_0900_ai_ci` collation (case-
and accent-insensitive). Extensions cannot configure this collation.

`SHOW CREATE TABLE` preserves supplementary characters in custom type parameters
when the client result charset supports them. MySQL's utf8mb3 dictionary display
fields remain limited; the separately stored parameter JSON retains the full text.

## STRING and BINARY values

A `VEF_TYPE_STRING` value is text in utf8mb4. From `VEF_PROTOCOL_4`, the server
converts a STRING argument from whatever charset it is in; an argument in the
binary charset must already be valid UTF-8, or the statement fails. An
extension at an older protocol gets an argument's bytes as they are. A STRING
result must be utf8mb4, and the server labels it `utf8mb4_bin`.

A `VEF_TYPE_BINARY` value (protocol >= `VEF_PROTOCOL_4`) is raw bytes. A BINARY
argument arrives exactly as the caller supplied it, whatever its charset, and a
BINARY result is labelled `binary`. Use it for data that is not text, such as a
hash or a serialized structure.

## Constant arguments in prerun

From `VEF_PROTOCOL_4`, a prerun sees the value of each constant argument in
`const_args`: an argument the server can evaluate before the first row, such
as a literal or `CAST(NULL AS SIGNED)`. A prepared-statement parameter or a
subquery is never a constant here. Each value is the `vef_invalue_t` the VDF
will receive for that argument, of any type, and `const_args[i]` is NULL for an
argument that is not a constant.

An argument's type is always the type it arrives as, so a NULL literal is a
STRING with `is_null` set. A prerun that admits NULL checks that rather than
admitting every STRING. An extension at an older protocol gets no
`const_args`. The older `deprecated_const_values` and
`deprecated_const_lengths` are never filled.
