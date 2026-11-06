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

### SQL result collation

Scalar and aggregate VDF STRING results use the fixed `utf8mb4_bin` collation,
independent of argument and connection collations. This includes functions with
mixed input collations or no text arguments. The server does not automatically
convert the returned UTF-8 text back to an input's charset.

When `utf8mb4_bin` is used for comparison, case and accents are significant:
`'A'` differs from `'a'`, and `'e'` differs from `'é'`. Trailing spaces are ignored
in equality comparisons (`'a' = 'a '`), grouping, and DISTINCT. This does not
remove spaces from the returned text. Trailing spaces remain significant in LIKE.

The result has COERCIBLE derivation (4), the same collation precedence as a
string literal. Normal MySQL collation aggregation still applies: for example,
a column's collation has stronger precedence. A fixed result collation therefore
does not force every comparison involving the result to use `utf8mb4_bin`.

Callers can apply COLLATE to the result to select different comparison rules.
For a STRING-returning VDF named `my_extension.transform`, for example:

```sql
SELECT my_extension.transform('Hello') COLLATE utf8mb4_0900_ai_ci;
```

Applying COLLATE to an argument does not select the VDF result's collation.

### Assignment and client conversion

The result's internal charset is separate from its destination. MySQL converts
text to an existing column's charset on assignment and to `character_set_results`
when sending it to the client. Setting `character_set_results` to NULL or binary
disables that client conversion. Extensions must still return UTF-8; they should
not convert STRING results to the client's charset themselves. Characters that
the destination charset cannot represent cannot be preserved by conversion.

CREATE TABLE AS SELECT inherits the expression's charset/collation unless
overridden. Once stored, the value has column collation precedence rather than
the VDF expression's COERCIBLE precedence.

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
