// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// =============================================================================
// VEF PREVIEW ABI HEADER — UNSTABLE BINARY INTERFACE
// =============================================================================
// This header is both:
//   - an ABI header — extension authors should use the C++ API in
//     <villagesql/vsql.h>, not these raw types. See villagesql/abi/README.md.
//   - a preview capability — API and ABI may change or be removed without
//     notice. See villagesql/preview/README.md.
// =============================================================================

#ifndef VILLAGESQL_ABI_PREVIEW_SESSION_VAR_H
#define VILLAGESQL_ABI_PREVIEW_SESSION_VAR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::session_var"
//
// Lets extensions declare per-session (THD-local) system variables — the
// functional equivalent of a plugin's MYSQL_THDVAR_*. Each connection has its
// own value, set with SET SESSION, with the descriptor's def_val as the global
// default. Read the caller's per-session value from extension code with the
// get_session_* functions.
//
// This is the session-scoped sibling of the global "vsql::sys_var" capability.
// A variable's scope is fixed when it is registered: use this capability for
// per-session variables, vsql::sys_var for global ones.
//
// Session values are resolved against the current connection thread, so the
// get_session_* functions must be called on that thread (a VDF or callback
// running on the connection), never from a background thread worker.

#define VEF_PREVIEW_SESSION_VAR_NAME "vsql::session_var"

// Capability ABI version compiled into this SDK snapshot.
#define VEF_PREVIEW_SESSION_VAR_ABI_VERSION 1

// Session variable value type. Only INT and STR are supported in v1.
typedef enum {
  VEF_SESSION_VAR_INT = 0,
  VEF_SESSION_VAR_STR = 1,
} vef_session_var_type_t;

typedef struct {
  // Variable name (without extension prefix). Encoded using UTF-8.
  const char *name;

  // Human-readable description shown in SHOW VARIABLES.
  const char *comment;

  vef_session_var_type_t type;

  // Type-specific default and constraints. Only the field matching type is
  // used. The def_val becomes the global default; each session starts from it
  // until it runs SET SESSION.
  union {
    struct {
      long long def_val;
      long long min_val;
      long long max_val;
    } integer;
    struct {
      const char *def_val;
    } str;
  };
} vef_session_var_desc_t;

// Descriptor list passed from extension to server at populate time. Its address
// also serves as the calling extension's identity token: the SDK passes it back
// as the `cap` argument to the read functions below, and the server maps it to
// the extension that registered these variables. The extension therefore never
// names itself — its identity lives only server-side.
typedef struct {
  const vef_session_var_desc_t *const *vars;
  uint32_t var_count;
} vef_session_var_descriptor_list_t;

// Reads the caller's session value of an INT session variable — the equivalent
// of a plugin's THDVAR(thd, var). Resolves the value for the current
// connection thread.
//
// cap:  the caller's descriptor-list pointer (identity token; see
//       vef_session_var_descriptor_list_t). The SDK supplies this; it scopes
//       the lookup to the variables the calling extension registered.
// name: variable name without the extension prefix
// out:  on success, set to the current session value
//
// Returns false on success, true on error (e.g. variable not found, wrong
// type, or no connection thread).
typedef bool (*vef_session_var_get_int_func_t)(const void *cap,
                                               const char *name,
                                               long long *out);

// Reads the caller's session value of a STRING session variable. Same thread
// rules as vef_session_var_get_int_func_t. `cap` is the identity token as for
// get_int.
//
// val:     on success, set to a newly allocated null-terminated string; caller
//          must free with free()
// val_len: on success, set to the string length (excluding null terminator)
//
// Returns false on success, true on error.
typedef bool (*vef_session_var_get_str_func_t)(const void *cap,
                                               const char *name, void **val,
                                               size_t *val_len);

// Opaque handle to a resolved INT session variable, obtained from
// resolve_int_handle and read with read_int. The handle captures the
// variable's per-session storage offset, which is the same on every connection
// and stable for the life of the registration, so one handle is valid on every
// connection thread and should be resolved once (at load or first use) and
// reused — never re-resolved per query. Free it with free_handle.
//
// Reading through a stale handle stays memory-safe but may return a stale
// value. The per-session storage offset is never reclaimed (the server only
// ever grows its offset space), so read_int keeps returning a valid, readable
// long long from that slot even after the owning extension is unregistered —
// it is simply the last value the slot held, not a live variable. A subsequent
// reinstall of the same extension may place the variable at a different offset,
// so a handle from before the reinstall then reads an unrelated slot: still
// safe to read, but semantically meaningless. Resolve a fresh handle after any
// reinstall, and for reads of a variable your extension does not own use the
// name-keyed get_session_int, which re-resolves on every call.
//
// There is intentionally no handle/read fast path for STRING variables. An INT
// value lives inline in the per-session slot, so an offset load reads it
// directly; a STRING slot holds a pointer to a separately allocated buffer, and
// a lock-free read would chase that pointer — which a concurrent SET SESSION
// (which frees and replaces the buffer) or an UNINSTALL (which frees it) can
// turn into a use-after-free. Read strings with get_session_str, which copies
// the value out under the server's lock.
typedef struct vef_session_var_handle vef_session_var_handle_t;

// Resolves an INT session variable to a reusable handle. This is the slow path
// (it resolves the variable by name once); call it at load time or on first
// use, then read through the handle. Typically used for a variable the
// extension itself declared.
//
// cap:        the caller's descriptor-list pointer (identity token; see
//             vef_session_var_descriptor_list_t)
// name:       variable name without the extension prefix
// out_handle: on success, set to a non-null handle the caller owns and must
//             release with free_handle
//
// Returns false on success, true on error (variable not found, not INT, or not
// session-scoped).
typedef bool (*vef_session_var_resolve_int_handle_func_t)(
    const void *cap, const char *name, vef_session_var_handle_t **out_handle);

// Reads the caller's current per-session value through a resolved handle — the
// hot path, and the equivalent of a plugin's THDVAR(thd, var). Lock-free in
// steady state: a per-thread base-pointer + offset load against the current
// connection thread, with no name lookup and no global lock, so concurrent
// reads from many threads touch only their own per-session storage and never
// contend.
//
// Must be called on the connection thread whose value is wanted (a VDF or
// callback running on the connection), never from a background thread worker.
//
// out: on success, set to the current session value.
//
// Returns false on success, true on error (null handle or no connection
// thread).
typedef bool (*vef_session_var_read_int_func_t)(
    const vef_session_var_handle_t *handle, long long *out);

// Releases a handle returned by resolve_int_handle. Passing null is a no-op.
typedef void (*vef_session_var_free_handle_func_t)(
    vef_session_var_handle_t *handle);

typedef struct {
  // Capability ABI version. Always the first field in every capability vtable.
  uint32_t version;

  // version >= 1: read the caller's per-session value by name (re-resolves on
  // every call). Suited to occasional reads of any session variable.
  vef_session_var_get_int_func_t get_session_int;
  vef_session_var_get_str_func_t get_session_str;

  // version >= 1: resolve-once handle fast path for repeated INT reads
  // (typically the extension's own variables). Resolve once with
  // resolve_int_handle, then read_int lock-free per query; free_handle when
  // done.
  vef_session_var_resolve_int_handle_func_t resolve_int_handle;
  vef_session_var_read_int_func_t read_int;
  vef_session_var_free_handle_func_t free_handle;
} vef_preview_session_var_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_SESSION_VAR_H
