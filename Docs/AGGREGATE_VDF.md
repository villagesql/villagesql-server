# Aggregate VDF Support

This document describes the design for adding aggregate function support to VDFs (VillageSQL Defined Functions). Today VDFs are scalar (per-row) functions. This extends them to support GROUP BY aggregation, similar to how MySQL supports both scalar UDFs and aggregate UDFs.

## Background

### How Scalar VDFs Work Today

VDFs are loaded from extension `.so` files via the VEF framework. Each extension's `vef_register()` returns a `vef_registration_t` containing `vef_func_desc_t` entries. Each descriptor has:

- `vdf` — main function pointer, called once per row
- `prerun` — optional, called once before the first row (allocates state in `user_data`)
- `postrun` — optional, called once after the last row (frees `user_data`)

During query parsing, `try_itemize_custom_vdf()` looks up the `FuncDescriptor` in VictionaryClient, calls `make_udf_func_from_vdf()` to create a `udf_func` wrapper with `type = UDFTYPE_FUNCTION`, then `Create_udf_func::create()` produces an `Item_func_udf_*` node.

At execution time, `udf_handler` detects `is_vdf()` and delegates to `vdf_handler`, which calls `prerun` once, then `marshal_args()` + `vdf()` per row, then `postrun` once.

The ABI (`types.h`) has an open issue comment: *"C) Support Aggregate functions."*

### How MySQL UDF Aggregates Work

MySQL aggregate UDFs extend the scalar UDF interface with two additional callbacks:

- `func_clear` — reset accumulator state for a new group
- `func_add` — accumulate one row's values into the state

The lifecycle per GROUP BY group is: `clear()` → `add()` × N rows → main function for the result. State is carried in `initid->ptr`.

On the Item side, `Item_udf_sum` extends `Item_sum` (not `Item_func`) and overrides `clear()`/`add()`. Type-specific subclasses (`Item_sum_udf_float`, `_int`, `_str`, `_decimal`) handle result retrieval. `Create_udf_func::create()` checks `udf->type == UDFTYPE_AGGREGATE` and instantiates `Item_sum_udf_*` instead of `Item_func_udf_*`.

## Design

### Detecting Aggregate vs Scalar

A VDF is an aggregate if and only if both `clear` and `add` are non-NULL in its `vef_func_desc_t`. No separate `is_aggregate` flag is needed. This is consistent with how `prerun`/`postrun` optionality works today.

It is an error for exactly one of `clear`/`add` to be set — both must be present or both must be NULL. This must be validated at extension registration time.

### ABI Changes (`villagesql/sdk/include/villagesql/abi/types.h`)

Add two new function pointer types:

```c
// Reset aggregate state for a new group.
// Called once at the start of each group. The extension should reset any
// accumulator state stored in args->user_data.
typedef void (*vef_vdf_clear_func_t)(vef_context_t *ctx, vef_vdf_args_t *args);

// Accumulate one row into the aggregate.
// Called once per row within a group. The extension reads values from args
// and updates its accumulator in args->user_data. If an error occurs during
// accumulation, write the message to result->error_msg and set
// result->type = VEF_RESULT_ERROR.
typedef void (*vef_vdf_add_func_t)(vef_context_t *ctx, vef_vdf_args_t *args,
                                    vef_vdf_result_t *result);
```

Add two fields to `vef_func_desc_t` (under VEF_PROTOCOL_2 or a new protocol version):

```c
// OPTIONAL: Set both to non-NULL to make this function an aggregate.
// When set, the main `vdf` callback becomes the "result" function, called
// once per group after all rows have been accumulated.
// It is an error to set exactly one of these; both must be present or absent.
vef_vdf_clear_func_t clear;
vef_vdf_add_func_t add;
```

### SDK Builder (`villagesql/sdk/include/villagesql/func_builder.h`)

Add `.clear<>()` and `.add<>()` methods to the function builder, or a `make_aggregate_func<>()` convenience builder. Usage:

```cpp
make_func<&my_result>("my_sum")
  .returns(INT)
  .param(INT)
  .clear<&my_clear>()
  .add<&my_add>()
  .prerun<&my_prerun>()
  .postrun<&my_postrun>()
  .build()
```

### FuncDescriptor (`villagesql/schema/descriptor/func_descriptor.h`)

No structural changes needed. `FuncDescriptor` already stores `const vef_func_desc_t *func_desc`, which will contain the new `clear`/`add` pointers.

### `make_udf_func_from_vdf()` (`villagesql/sql/func_lookup.cc`)

By this point, registration validation has already rejected the case where exactly one of `clear`/`add` is set, so we can rely on checking just one:

```cpp
udf->type = func_desc->clear ? UDFTYPE_AGGREGATE : UDFTYPE_FUNCTION;
```

This is the key change. `Create_udf_func::create()` already branches on `UDFTYPE_AGGREGATE` to create `Item_sum_udf_*` nodes instead of `Item_func_udf_*` — no changes needed there.

### `vdf_handler` (`villagesql/vdf/vdf_handler.h` and `.cc`)

Add two public methods:

- `clear()` — calls `func_desc->clear(&m_context, &m_vdf_args)`
- `add(bool *null_value)` — marshals current row args, calls `func_desc->add(&m_context, &m_vdf_args, &result)`, checks for errors

The existing `val_real()`, `val_int()`, `val_str()` methods already call the main `vdf` callback, which for aggregates becomes the result function. These should work unchanged.

### `udf_handler` delegation (`sql/item_func.cc`)

`udf_handler::clear()` and `udf_handler::add()` already exist for classic UDF aggregates. Add VDF branches:

```cpp
void udf_handler::clear() {
  if (m_vdf) {
    m_vdf->clear();
    return;
  }
  // existing classic UDF clear code...
}

void udf_handler::add(bool *null_value) {
  if (m_vdf) {
    m_vdf->add(null_value);
    return;
  }
  // existing classic UDF add code...
}
```

### Parse Tree (`villagesql/sql/parse_tree_items.cc`)

No changes needed. `try_itemize_custom_vdf()` and `try_itemize_unqualified_vdf()` call `Create_udf_func::create()`, which already handles the `UDFTYPE_AGGREGATE` branching.

### Extension Registration Validation

During extension loading (where `vef_registration_t` is processed), validate that `clear` and `add` are either both NULL or both non-NULL. If exactly one is set, reject the extension with an error message like:

```
Aggregate VDF 'func_name' in extension 'ext_name' must provide both clear and
add callbacks, or neither.
```

## Execution Lifecycle

For an aggregate VDF with a query like `SELECT ext.vdf_sum(col) FROM t GROUP BY g`:

1. **fix_fields**: `vdf_handler::fix_fields()` allocates buffers, calls `prerun` (extension allocates accumulator state in `user_data`)
2. **Per group**:
   - `clear()`: server calls `vdf_handler::clear()` → extension resets accumulator
   - `add()` × N rows: server calls `vdf_handler::add()` → extension marshals args and accumulates
   - `val_*()`: server calls `vdf_handler::val_int()` (or `val_real()`, `val_str()`) → extension's main `vdf` callback reads accumulator and returns result
3. **cleanup**: `vdf_handler::cleanup()` calls `postrun` (extension frees accumulator)

## Testing

### Test Extension

Create an aggregate test VDF (e.g., `vdf_sum` returning INT, `vdf_avg` returning REAL) in the test extension used by the VillageSQL test suite.

### MTR Tests (`mysql-test/suite/villagesql/`)

**Basic aggregation:**
- `SELECT ext.vdf_sum(col) FROM t GROUP BY g` — correct per-group results
- Whole-table aggregate (no GROUP BY): `SELECT ext.vdf_sum(col) FROM t`
- Empty table: `SELECT ext.vdf_sum(col) FROM t` where t has no rows

**GROUP BY variations:**
- Multiple groups with correct clear/add/result cycling
- Multiple aggregate VDFs in one query: `SELECT ext.vdf_sum(a), ext.vdf_avg(b) FROM t GROUP BY g`
- Mixed scalar and aggregate VDFs: `SELECT ext.vdf_sum(a), ext.scalar_func(g) FROM t GROUP BY g`

**SQL features:**
- HAVING clause: `SELECT g, ext.vdf_sum(a) FROM t GROUP BY g HAVING ext.vdf_sum(a) > 10`
- ORDER BY aggregate: `SELECT g, ext.vdf_sum(a) AS s FROM t GROUP BY g ORDER BY s`
- Window functions: `SELECT ext.vdf_sum(a) OVER (PARTITION BY g) FROM t`
- Subqueries: `SELECT * FROM t WHERE col > (SELECT ext.vdf_sum(a) FROM t2)`

**NULL handling:**
- Some NULL values in the aggregated column
- All NULL values in a group
- NULL in a non-aggregated argument

**Error handling:**
- Aggregate VDF that returns `VEF_RESULT_ERROR` during `add`
- Aggregate VDF that returns `VEF_RESULT_ERROR` during result

**Registration validation:**
- Extension with `add` set but `clear` NULL — must fail to load
- Extension with `clear` set but `add` NULL — must fail to load
- Extension with both `clear` and `add` NULL — loads as scalar (existing behavior)
- Extension with both `clear` and `add` set — loads as aggregate

**Qualified and unqualified calls:**
- `SELECT ext.vdf_sum(col) FROM t GROUP BY g` (qualified)
- `SELECT vdf_sum(col) FROM t GROUP BY g` (unqualified)
