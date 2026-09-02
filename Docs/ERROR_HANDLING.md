# VillageSQL Error Handling Guidelines

This document outlines the strategy for error handling and reporting within the VillageSQL codebase and its integration with the MySQL server. The goal is to maintain a clear separation between internal error logging and user-facing error reporting, while ensuring robustness and consistency with MySQL's architecture.

## 1. Internal VillageSQL Code

**Scope:** Code residing within the `villagesql/` directory, implementing core logic (e.g., Schema, Types, System Tables), *excluding* top-level SQL command implementations.

*   **Use `LogVSQL(ERROR_LEVEL, ...)`:** For internal failures (e.g., system table write errors, logical inconsistencies, unexpected states), log the error details to the server error log.
*   **Return Failure:** Functions should return `true` (or a specific error code) to indicate failure to the caller.
*   **Do NOT use `my_error` / `villagesql_error`:** Avoid setting user-facing errors in the Diagnostics Area (DA) directly from deep internal logic. This keeps the internal code focused on mechanics and logging.
    *   **Exception:** Critical resource failures like Out Of Memory (OOM) may report immediately. See §4 for which allocations report themselves and how to report the rest without allocating.
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

## 4. Memory Allocation

**Scope:** Every allocation in `villagesql/`.

**Principle:** An allocation failure can only be reported where the code making
the allocation has an error channel. There are exactly three kinds of allocation
in this codebase, distinguished by who owns the call, and the rule follows from
that ownership - not from the size or purpose of the allocation.

| Allocation | Use | On failure |
| :--- | :--- | :--- |
| Statement lifetime | `new (mem_root) T` | Already reported - just return `true` |
| Ours, outlives the statement | `new (std::nothrow)`, or the `alloc.h` helpers for smart pointers | Null-check, then report per §1 / §2 |
| Inside an STL container | Nothing - it throws | Converted to a statement error at an exception boundary (§4.4) |

Rules 4.1-4.3 mean **no code inside `villagesql/` mentions exceptions.** §4.4 is
the only place they appear.

### 4.1 Statement lifetime: MEM_ROOT

Prefer a MEM_ROOT whenever the lifetime allows it. This is not a style
preference - it is the only allocation that is both nothrow and self-reporting,
so it needs no handling code at all:

*   `new (mem_root) T` returns `nullptr` instead of throwing (`include/my_alloc.h`).
*   Every MEM_ROOT constructed in a server translation unit installs
    `sql_alloc_error_handler` in its constructor, under
    `#if defined(MYSQL_SERVER)`. On failure it sets the Diagnostics Area to
    `ER_OUT_OF_RESOURCES` directly (`sql/sql_alloc_error_handler.cc`).

So after a failed MEM_ROOT allocation the DA is already set. **Do not call
`my_error` again.** `my_error` formats a message, which allocates, which is the
recursion `sql_alloc_error_handler` documents avoiding. Log context if useful
and return `true`.

### 4.2 Objects that outlive the statement

Where a MEM_ROOT will not do - anything cached in the victionary, or that must
survive the statement - allocate nothrow and check the result:

*   `new (std::nothrow) T(...)` for raw allocations (this is also upstream
    practice; `sql/` has ~270 uses).
*   `villagesql/include/alloc.h` for smart pointers: `make_shared_nothrow<T>()`,
    `make_unique_nothrow<T>()`, and `wrap_shared_nothrow(new (std::nothrow) T())`
    for types whose private constructor `make_shared` cannot reach.

`std::make_shared` has no nothrow overload and `std::allocate_shared` cannot
report failure without throwing, so the helpers absorb `std::bad_alloc` in one
place and return `nullptr`. Null-check with `should_assert_if_null()` and report
per §1 (internal: log and return) or §2 (SQL command: set a user error).

### 4.3 STL containers throw, and that cannot be changed

`std::string`, `std::vector`, and friends throw `std::bad_alloc` on allocation
failure. This is not fixable by choosing a different container or allocator:

*   The Allocator contract gives `allocate()` no way to signal failure other
    than throwing. An allocator returning `nullptr` leaves the container
    constructing objects at address zero.
*   MySQL's own allocators therefore throw too - `Mem_root_allocator`
    (`sql/mem_root_allocator.h`), `Malloc_allocator`, and `Stateless_allocator`
    all `throw std::bad_alloc()`. `dd::String_type` uses the last of these.

**Do not migrate away from `std::string` for allocation-safety reasons.** It
buys nothing, and MySQL relies on throwing containers throughout. Container
failures are handled at the boundary instead.

### 4.4 Exception boundaries

**A. Extension ABI - mandatory.** Any function whose address is handed to
extension code, i.e. the function pointers in the capability vtables under
`villagesql/services/`. An exception escaping one of these unwinds into a shared
object we did not compile, possibly built with `-fno-exceptions`, a different
compiler, or a different C++ runtime. Wrap the body, following the pattern used
by every service implementation in `sql/server_component/`:

```cpp
try {
  // ... implementation ...
  return false;
} catch (...) {
  // Set the DA without allocating; see below.
  return true;
}
```

This rule is ABI hygiene and applies regardless of OOM policy.

**B. Our own SQL commands - recommended.** The `Sql_cmd` implementations in
`villagesql/veb/` (§2). We own the whole statement from dispatch to return, so a
catch here turns an OOM in our own DDL into a clean statement error rather than a
crash. Requires that everything it covers be exception-safe: RAII locks (the
victionary's `ReadLockGuard` / `WriteLockGuard` qualify) and no owning raw
pointers left dangling on unwind.

**C. Everywhere else - no catch.** Functions in `villagesql/` called from inside
MySQL statement execution (Item evaluation, table open, ALTER paths) stay
exception-ignorant. Our frames are a thin slice of a stack whose MySQL frames are
not exception-safe, so catching only in ours gives partial coverage and false
confidence. MySQL's own equivalent code relies on `std::terminate` there; match
it.

Report OOM at a boundary the way `sql_alloc_error_handler` does - set the error
status directly rather than calling `my_error`, since formatting allocates:

```cpp
if (thd != nullptr && !thd->is_error())
  thd->get_stmt_da()->set_error_status(thd, ER_OUT_OF_RESOURCES);
```

*(A `villagesql_report_oom()` helper for this is not yet in the tree; add it with
the first conversion.)*

### 4.5 Calibration

On Linux with default overcommit (`vm.overcommit_memory=0`) a small `malloc`
does not return `nullptr` - the OOM killer arrives first, and none of the above
triggers. These rules matter on macOS, on Windows, under cgroup limits with
overcommit disabled, and for allocations large enough to fail outright. Write
them correctly, but do not expect test coverage to come from a Linux CI run.

## Summary Table

| Location | Action on Failure | Function to Use |
| :--- | :--- | :--- |
| **Internal `villagesql/`** | Log & Return Fail | `LogVSQL(...)` |
| **SQL Commands (`veb/`)** | Set User Error & Return Fail | `villagesql_error(...)` |
| **MySQL Core (Caller)** | Ensure DA is set | `villagesql_check_error_log()` (if needed) |
| **Any allocation** | MEM_ROOT, else nothrow + null check | `new (mem_root)`, `new (std::nothrow)`, `alloc.h` helpers |

## Key Macros

*   **`LogVSQL(level, ...)`:** Logs to the MySQL error log.
*   **`villagesql_error(msg, ...)`:** Sets a user-facing error in the Diagnostics Area.
*   **`villagesql_check_error_log()`:** Helper that sets a generic "Check error log" user error.
*   **`make_shared_nothrow<T>(...)` / `make_unique_nothrow<T>(...)` / `wrap_shared_nothrow(ptr)`:** Nothrow smart-pointer allocation (`villagesql/include/alloc.h`); return `nullptr` on failure.
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
