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

#ifndef VILLAGESQL_ABI_PREVIEW_STATUS_VAR_H
#define VILLAGESQL_ABI_PREVIEW_STATUS_VAR_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::status_var"
//
// Allows extensions to expose read-only counters and gauges via SHOW STATUS /
// performance_schema.global_status. Declare a StatusVarCapability, populate it
// with add_int() / add_double() descriptors, and pass
// it to .with() on the extension builder.
//
// The server registers the declared variables when the extension loads and
// unregisters them when it unloads.

#define VEF_PREVIEW_STATUS_VAR_NAME "vsql::status_var"

// Capability ABI version compiled into this SDK snapshot.
#define VEF_PREVIEW_STATUS_VAR_ABI_VERSION 1

// Status variable type. Unlike system variables (which are configurable),
// status variables are read-only counters and gauges exposed via SHOW STATUS.
typedef enum {
  VEF_STATUS_VAR_INT = 0,     // long long counter/gauge (shown as unsigned)
  VEF_STATUS_VAR_DOUBLE = 1,  // double gauge
} vef_status_var_type_t;

typedef struct {
  // Variable name (without extension prefix). Encoded using UTF-8.
  const char *name;

  vef_status_var_type_t type;

  // Pointer to storage in the extension .so. Must remain valid for the
  // lifetime of the extension. The extension writes to this; the server reads
  // it at SHOW STATUS time.
  union {
    long long *integer_ptr;
    double *double_ptr;
  };
} vef_status_var_desc_t;

// Descriptor list passed from extension to server at populate time.
// The extension keeps this struct alive for its entire lifetime; the server
// reads it only during on_populate and stores its own copies of the data.
typedef struct {
  // Array of pointers to status variable descriptors. Must remain valid for
  // the lifetime of the extension.
  const vef_status_var_desc_t *const *vars;
  uint32_t var_count;
} vef_status_var_descriptor_list_t;

typedef struct {
  // Capability ABI version. Always the first field in every capability vtable.
  uint32_t version;

  // version >= 1: no extension-callable functions needed; the server reads the
  // descriptor list from the extension's StatusVarCapability at populate time.
} vef_preview_status_var_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_STATUS_VAR_H
