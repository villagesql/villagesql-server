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

#ifndef VILLAGESQL_ABI_PREVIEW_SYS_VAR_H
#define VILLAGESQL_ABI_PREVIEW_SYS_VAR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::sys_var"
//
// Allows extensions to expose configurable system variables via SET GLOBAL /
// SELECT @@global. Declare a SysVarCapability, populate it with brace-init
// descriptors, and pass it to .with() on the extension builder.
//
// The server registers the declared variables when the extension loads and
// unregisters them when it unloads.

#define VEF_PREVIEW_SYS_VAR_NAME "vsql::sys_var"

// Capability ABI version compiled into this SDK snapshot.
#define VEF_PREVIEW_SYS_VAR_ABI_VERSION 1

// System variable value type.
typedef enum {
  VEF_VAR_BOOL = 0,
  VEF_VAR_INT = 1,
  VEF_VAR_DOUBLE = 2,
  VEF_VAR_STR = 3,
} vef_var_type_t;

// Passed to on_change callbacks when a variable is SET.
typedef struct {
  // Name of the variable that changed (without extension prefix).
  const char *var_name;
  vef_var_type_t type;
  // Only the field matching type is valid.
  union {
    bool bool_val;
    long long int_val;
    double dbl_val;
    // For VEF_VAR_STR: points to the newly allocated string. Valid for the
    // duration of the callback; do not retain the pointer.
    const char *str_val;
  };
} vef_sys_var_change_t;

// Callback invoked after the server writes a new value for a system variable.
typedef void (*vef_sys_var_on_change_func_t)(const vef_sys_var_change_t *);

typedef struct {
  // Variable name (without extension prefix). Encoded using UTF-8.
  const char *name;

  // Human-readable description shown in SHOW VARIABLES.
  const char *comment;

  vef_var_type_t type;

  // Optional. Called after the server writes a new value. May be NULL.
  vef_sys_var_on_change_func_t on_change;

  // Type-specific storage and constraints. Only the field matching type is
  // used.
  union {
    struct {
      bool *value_ptr;
      bool def_val;
    } boolean;
    struct {
      long long *value_ptr;
      long long def_val;
      long long min_val;
      long long max_val;
    } integer;
    struct {
      double *value_ptr;
      double def_val;
      double min_val;
      double max_val;
    } dbl;
    struct {
      char **value_ptr;
      const char *def_val;
    } str;
  };
} vef_sys_var_desc_t;

// Descriptor list passed from extension to server at populate time.
typedef struct {
  const vef_sys_var_desc_t *const *vars;
  uint32_t var_count;
} vef_sys_var_descriptor_list_t;

// Reads a system variable registered by any extension.
//
// component_name: extension name (e.g. "vsql_my_ext")
// name:           variable name without the extension prefix (e.g. "threshold")
// val:            on success, set to a newly allocated buffer holding the value
//                 as a null-terminated string; caller must free with free()
// val_len:        on success, set to the string length (excluding null
// terminator)
//
// Returns false on success, true on error.
typedef bool (*vef_sys_var_get_func_t)(const char *component_name,
                                       const char *name, void **val,
                                       size_t *val_len);

// Sets a system variable to a string value.
//
// component_name: extension name (e.g. "vsql_my_ext")
// name:           variable name without the extension prefix
// scope:          nullptr  → update running value only (GLOBAL, not persisted)
//                 "PERSIST"      → update running value AND write to
//                                  mysqld-auto.cnf (survives restart)
//                 "PERSIST_ONLY" → write to mysqld-auto.cnf only; running
//                                  value unchanged (takes effect on restart)
// val:            new value as a null-terminated string
//
// Returns false on success, true on error.
typedef bool (*vef_sys_var_set_func_t)(const char *component_name,
                                       const char *name, const char *scope,
                                       const char *val);

typedef struct {
  // Capability ABI version. Always the first field in every capability vtable.
  uint32_t version;

  // Read/write access to system variables from extension code.
  // Set by the server during capability registration; call through these
  // pointers from extension VDFs or callbacks.
  vef_sys_var_get_func_t get;
  vef_sys_var_set_func_t set;
} vef_preview_sys_var_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_SYS_VAR_H
