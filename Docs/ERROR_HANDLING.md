# VillageSQL Error Handling Guidelines

This document outlines the strategy for error handling and reporting within the VillageSQL codebase and its integration with the MySQL server. The goal is to maintain a clear separation between internal error logging and user-facing error reporting, while ensuring robustness and consistency with MySQL's architecture.

## 1. Internal VillageSQL Code

**Scope:** Code residing within the `villagesql/` directory, implementing core logic (e.g., Schema, Types, System Tables), *excluding* top-level SQL command implementations.

*   **Use `LogVSQL(ERROR_LEVEL, ...)`:** For internal failures (e.g., system table write errors, logical inconsistencies, unexpected states), log the error details to the server error log.
*   **Return Failure:** Functions should return `true` (or a specific error code) to indicate failure to the caller.
*   **Do NOT use `my_error` / `villagesql_error`:** Avoid setting user-facing errors in the Diagnostics Area (DA) directly from deep internal logic. This keeps the internal code focused on mechanics and logging.
    *   **Exception:** Critical resource failures like Out Of Memory (OOM) *may* set `my_error(ER_OUTOFMEMORY)` immediately if recovery is impossible or standard practice dictates it.
*   **Pointer Validation:**
    *   Use C++ references (`T&`) instead of pointers (`T*`) for function arguments where the caller guarantees non-nullability.
    *   Remove redundant `nullptr` checks inside functions when references are used.
    *   **Skip null checks when upstream guarantees non-null:** If calling code (especially MySQL core) dereferences a pointer before passing it to your function, that pointer is guaranteed non-null. Example: if `open_tables()` uses `thd->lex` before calling your function with `thd`, you don't need to check if `thd` is null.
    *   Use `should_assert_if_null(ptr)` only at public API boundaries or where `nullptr` is a valid but unexpected runtime condition that must be guarded against in release builds.
    *   **Defensive checks are redundant:** Don't add null checks "just to be safe" if the caller's code would have already crashed with a null pointer before reaching your code.

## 2. Boundary Code (SQL Commands)

**Scope:** Code within `villagesql/` that implements MySQL interfaces directly, such as `Sql_cmd` subclasses (e.g., `villagesql/veb/sql_extension.cc`).

*   **Set User Errors:** Since these functions are the direct entry point for user SQL statements, they *must* set user-facing errors using `villagesql_error(...)` (which wraps `my_printf_error`) or standard `my_error(...)`.
*   **Return Failure:** Return `true` to the MySQL command dispatcher to indicate the statement failed.

## 3. Integration Points (MySQL Core)

**Scope:** Code outside `villagesql/` (e.g., `sql/sql_table.cc`, `sql/sql_base.cc`) that calls into VillageSQL functionality.

*   **Bridge the Gap:** When calling VillageSQL functions that only log errors (per Rule 1), callers in MySQL core are responsible for ensuring an error is set in the Diagnostics Area (DA) if the VillageSQL function returns failure.
    *   **Why this is needed:** Internal VillageSQL functions log to the error log but don't set user-facing errors. If MySQL code expects an error in the DA after a failure, we must set one.
    *   **Scenario A (Caller expects callee to set error):** If the surrounding MySQL code expects the called function to set an error (e.g., `rea_create_base_table`), and the VillageSQL function only logged it (per Rule 1), the caller **must** set a generic error.
        *   **Pattern:**
            ```cpp
            if (villagesql::SomeFunction(args)) {
              if (!thd->is_error()) villagesql_check_error_log();
              return true;
            }
            ```
        *   This sets a "Check error log for more info" error if no other error (like OOM) was already set by the VillageSQL function.
        *   **Real example:** See `rea_create_base_table()` in `sql/sql_table.cc` calling `MaybeUpdateColumnMetadata`
    *   **Scenario B (Caller handles error generically):** If the surrounding MySQL code has its own generic error reporting path for failures (e.g., `open_tables`), rely on that.
        *   **Pattern:** `LogVSQL(ERROR_LEVEL, "Context for failure");` (optional, for extra context)
        *   Do **not** call `villagesql_check_error_log()` if it would result in double error reporting or confuse the existing error handling flow.
    *   **Scenario C (Cleanup/Rollback):** If the failure triggers a cleanup path (e.g., transaction rollback) that implicitly handles the failure state or is expected to set its own error, do not add explicit error setting code.

## Summary Table

| Location | Action on Failure | Function to Use |
| :--- | :--- | :--- |
| **Internal `villagesql/`** | Log & Return Fail | `LogVSQL(...)` |
| **SQL Commands (`veb/`)** | Set User Error & Return Fail | `villagesql_error(...)` |
| **MySQL Core (Caller)** | Ensure DA is set | `villagesql_check_error_log()` (if needed) |

## Key Macros

*   **`LogVSQL(level, ...)`:** Logs to the MySQL error log.
*   **`villagesql_error(msg, ...)`:** Sets a user-facing error in the Diagnostics Area.
*   **`villagesql_check_error_log()`:** Helper that sets a generic "Check error log" user error.
*   **`should_assert_if_null(ptr)`:** Asserts in debug, returns true in release if null.
*   **`should_assert_if_true(cond)`:** Asserts in debug, returns true in release if condition is true.
*   **`should_assert_if_false(cond)`:** Asserts in debug, returns true in release if condition is false.

## Concrete Examples

### Example 1: Internal VillageSQL Function (Uses References)

```cpp
// villagesql/types/util.cc
bool MaybeUpdateColumnMetadata(THD &thd, const char *db_name,
                               const char *table_name,
                               List<Create_field> &create_fields) {
  // THD is a reference - no null check needed
  // Check other pointers
  if (should_assert_if_null(db_name) || should_assert_if_null(table_name)) {
    LogVSQL(ERROR_LEVEL, "Called with null parameters");
    return true;
  }

  // Internal failure - just log and return error
  if (vclient.columns().MarkForInsertion(thd, entry)) {
    LogVSQL(ERROR_LEVEL, "Failed to mark column for insertion");
    return true;  // Let caller handle error reporting to user
  }

  return false;
}
```

### Example 2: MySQL Core Caller (Bridges the Gap)

```cpp
// sql/sql_table.cc - rea_create_base_table()
// Note: thd is used extensively before this point (thd->dd_client(), etc.)
// so it's guaranteed non-null

if (villagesql::MaybeUpdateColumnMetadata(*thd, db, table_name, create_fields)) {
  // VillageSQL function only logged the error - we must set user error
  if (!thd->is_error()) villagesql_check_error_log();
  return true;
}
```

**Key points:**
- MySQL core passes `*thd` (dereferences pointer to reference)
- VillageSQL function only logs (per Rule 1)
- Caller checks `if (!thd->is_error())` and sets error if needed

### Example 3: Skipping Redundant Null Checks

**❌ Don't do this (redundant check):**
```cpp
bool SomeFunction(THD *thd, ...) {
  if (should_assert_if_null(thd)) {  // REDUNDANT!
    LogVSQL(ERROR_LEVEL, "THD is null");
    return true;
  }
  // ... rest of function
}

// Caller in MySQL core:
static bool mysql_function(THD *thd, ...) {
  thd->dd_client()->acquire(...);  // Would crash here if thd is null
  // ... 50 lines of code using thd-> ...

  if (villagesql::SomeFunction(thd, ...)) {  // thd already used above!
    return true;
  }
}
```

**✅ Do this instead:**
```cpp
bool SomeFunction(THD &thd, ...) {  // Use reference
  // No null check needed - caller guarantees non-null
  // ... rest of function
}

// Caller dereferences at the boundary:
static bool mysql_function(THD *thd, ...) {
  thd->dd_client()->acquire(...);  // Uses thd before our call

  if (villagesql::SomeFunction(*thd, ...)) {  // Dereference to reference
    if (!thd->is_error()) villagesql_check_error_log();
    return true;
  }
}
```

### Example 4: When NULL Checks ARE Needed

```cpp
// Public API boundary where nullptr might be passed
bool PublicAPI(THD *thd, ...) {
  if (should_assert_if_null(thd)) {  // VALID - this is an API boundary
    LogVSQL(ERROR_LEVEL, "THD is null");
    return true;
  }
  // ... call internal functions with THD& ...
}
```

Use null checks only when:
- Function is a public API that external code might call incorrectly
- There's no evidence upstream code has already dereferenced the pointer
- During early initialization where guarantees are unclear
