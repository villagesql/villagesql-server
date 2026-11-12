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
- Compile-time checks in `unittest/gunit/villagesql/` pin the layout of every
  stable protocol: `abi_v{N}_check-t.cc` checks each frozen snapshot, and
  `abi_dev_check-t.cc` checks that these headers keep every stable offset.
  When you add a field to these headers, record it and its struct's new size
  in `abi_dev_layout.inc`. Never edit a stable `abi_v{N}_layout.inc`.
- Headers under `abi/preview/` expose preview-capability ABIs and follow
  the preview-capability rules in [../preview/README.md](../preview/README.md).

## Type parameter text

Type parameter names and values must be valid UTF-8 without embedded NUL
because the ABI uses NUL-terminated strings. The server converts SQL input
from the connection charset to utf8mb4; extensions must supply UTF-8. The server
trims surrounding ASCII spaces and lowercases names and values. Names are sorted
and checked for duplicates using the fixed `utf8mb4_0900_ai_ci` collation (case-
and accent-insensitive). Extensions cannot configure this collation.

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

A custom type may also declare `from_binary_vdf_name` (protocol >=
`VEF_PROTOCOL_4`), an encode with signature `(BINARY) -> CUSTOM(this type)`
that builds a value from its binary form, next to the required encode from
text. It is registered as the SQL function `TYPE::from_binary`. As with the
encode from text, a parameterized type's parameters are inferred from a
constant argument. The server uses it for any value in the binary charset that
becomes a value of the type: a hex or bit literal, a `_binary` string, a
parameter bound as a BLOB, or a `VARBINARY` column. A value in any other
charset is text, converted to utf8mb4 for the encode from text. For a type
without `from_binary`, a value in the binary charset is an error rather than
being read as text, so adding `from_binary` later never changes what existing
input means. This applies to the types of an extension built against protocol
>= 4. A type of an extension built against an earlier protocol, which cannot
declare `from_binary`, keeps receiving a value in the binary charset in its
encode from text, as its bytes.

The inverse is `to_binary_vdf_name` (protocol >= `VEF_PROTOCOL_4`), a decode
with signature `(CUSTOM(this type)) -> BINARY` that returns a value's binary
form, next to the required decode to text. It is registered as the SQL
function `TYPE::to_binary`. The server does not call it itself: a custom type
value is never converted implicitly to a string or to the binary charset, so
`TYPE::to_binary` and `TYPE::to_string` are how a value leaves in either form.
The binary form it returns reaches clients and other servers, so it is the same
on every platform: the type defines the byte order of its multi-byte numbers
(TVECTOR uses little-endian floats), whatever the byte order of the host.

## Constant arguments in prerun

From `VEF_PROTOCOL_4`, a prerun sees the value of each constant argument in
`const_args`: an argument the server can evaluate before the first row, such
as a literal, `CAST(NULL AS SIGNED)` or a prepared-statement parameter. A
subquery is never a constant here. Each value is the `vef_invalue_t` the VDF
will receive for that argument, of any type, and `const_args[i]` is NULL for an
argument that is not a constant.

An argument's type is always the type it arrives as, so a NULL literal is a
STRING with `is_null` set. A prerun that admits NULL checks that rather than
admitting every STRING. An extension at an older protocol gets no
`const_args`. The older `deprecated_const_values` and
`deprecated_const_lengths` are never filled.
