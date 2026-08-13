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

// Descriptor list passed from extension to server at populate time.
typedef struct {
  const vef_session_var_desc_t *const *vars;
  uint32_t var_count;
} vef_session_var_descriptor_list_t;

// Reads the caller's session value of an INT session variable — the equivalent
// of a plugin's THDVAR(thd, var). Resolves the value for the current
// connection thread.
//
// component_name: extension name (e.g. "vsql_my_ext")
// name:           variable name without the extension prefix
// out:            on success, set to the current session value
//
// Returns false on success, true on error (e.g. variable not found, wrong
// type, or no connection thread).
typedef bool (*vef_session_var_get_int_func_t)(const char *component_name,
                                               const char *name,
                                               long long *out);

// Reads the caller's session value of a STRING session variable. Same thread
// rules as vef_session_var_get_int_func_t.
//
// val:     on success, set to a newly allocated null-terminated string; caller
//          must free with free()
// val_len: on success, set to the string length (excluding null terminator)
//
// Returns false on success, true on error.
typedef bool (*vef_session_var_get_str_func_t)(const char *component_name,
                                               const char *name, void **val,
                                               size_t *val_len);

typedef struct {
  // Capability ABI version. Always the first field in every capability vtable.
  uint32_t version;

  // version >= 1: read the caller's per-session value.
  vef_session_var_get_int_func_t get_session_int;
  vef_session_var_get_str_func_t get_session_str;
} vef_preview_session_var_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_SESSION_VAR_H
