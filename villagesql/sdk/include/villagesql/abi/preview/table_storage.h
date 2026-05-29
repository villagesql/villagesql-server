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

#ifndef VILLAGESQL_ABI_PREVIEW_TABLE_STORAGE_H
#define VILLAGESQL_ABI_PREVIEW_TABLE_STORAGE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::preview::table_storage"
//
// Provides server-owned hidden table storage for extension data. The first
// intended use is custom index storage: the server owns table lifecycle,
// locking, transaction participation, metadata identity, and handler access;
// the extension owns the row layout and index algorithm.
//
// Capability name: VEF_PREVIEW_TABLE_STORAGE_NAME

#define VEF_PREVIEW_TABLE_STORAGE_NAME "vsql::preview::table_storage"
#define VEF_PREVIEW_TABLE_STORAGE_ABI_VERSION 1

typedef struct vef_table_storage_t vef_table_storage_t;
typedef struct vef_table_storage_handle_t vef_table_storage_handle_t;
typedef struct vef_table_storage_cursor_t vef_table_storage_cursor_t;

typedef enum {
  VEF_TABLE_STORAGE_COL_BYTES = 1,
  VEF_TABLE_STORAGE_COL_UINT64 = 2,
  VEF_TABLE_STORAGE_COL_INT64 = 3,
} vef_table_storage_col_type_t;

typedef struct {
  const char *name;
  vef_table_storage_col_type_t type;
  uint32_t max_length;
  bool nullable;
} vef_table_storage_col_def_t;

// A secondary (non-PK) index on the hidden table. Used by extensions
// that need to seek to a row by something other than the PK — e.g. the
// entry-point lookup for a graph-structured index ("scan by `layer`
// descending, take the first row, capture its ref").
typedef struct {
  const char *name;  // extension-chosen identifier
  const uint32_t
      *column_indices;  // indices into vef_table_storage_def_t::columns
  uint32_t column_count;
  bool unique;
} vef_table_storage_index_def_t;

typedef struct vef_table_storage_def_t {
  uint32_t version;
  const char *logical_name;
  const vef_table_storage_col_def_t *columns;
  uint32_t column_count;
  const uint32_t *primary_key_columns;
  uint32_t primary_key_column_count;
  const vef_table_storage_index_def_t *secondary_indexes;
  uint32_t secondary_index_count;
} vef_table_storage_def_t;

typedef struct {
  const unsigned char *data;
  uint32_t length;
  bool is_null;
} vef_table_storage_value_t;

typedef enum {
  VEF_TABLE_STORAGE_LOCK_READ = 1,
  VEF_TABLE_STORAGE_LOCK_WRITE = 2,
} vef_table_storage_lock_t;

typedef enum {
  VEF_TABLE_STORAGE_SCAN_FULL = 1,
  VEF_TABLE_STORAGE_SCAN_PRIMARY_KEY = 2,
  VEF_TABLE_STORAGE_SCAN_SECONDARY_INDEX = 3,
} vef_table_storage_scan_type_t;

typedef enum {
  VEF_TABLE_STORAGE_SCAN_DIR_ASC = 1,
  VEF_TABLE_STORAGE_SCAN_DIR_DESC = 2,
} vef_table_storage_scan_direction_t;

typedef struct {
  uint32_t version;
  vef_table_storage_scan_type_t scan_type;
  const vef_table_storage_value_t *key_values;
  uint32_t key_value_count;
  uint32_t limit;
  // For SECONDARY_INDEX scans: which index, in which direction. The
  // index name must match a vef_table_storage_index_def_t::name from
  // the descriptor. key_values, when non-empty, is the seek-to prefix;
  // an empty key_values seeks to the first/last row in the chosen
  // direction.
  const char *secondary_index_name;
  vef_table_storage_scan_direction_t direction;
} vef_table_storage_scan_t;

typedef bool (*vef_table_storage_create_fn)(const vef_table_storage_def_t *def,
                                            vef_table_storage_t **table,
                                            char *error_msg,
                                            uint32_t error_msg_len);

typedef bool (*vef_table_storage_drop_fn)(vef_table_storage_t *table,
                                          char *error_msg,
                                          uint32_t error_msg_len);

typedef bool (*vef_table_storage_open_fn)(vef_table_storage_t *table,
                                          vef_table_storage_lock_t lock,
                                          vef_table_storage_handle_t **handle,
                                          char *error_msg,
                                          uint32_t error_msg_len);

typedef void (*vef_table_storage_close_fn)(vef_table_storage_handle_t *handle);

typedef bool (*vef_table_storage_insert_fn)(
    vef_table_storage_handle_t *handle, const vef_table_storage_value_t *values,
    uint32_t value_count, char *error_msg, uint32_t error_msg_len);

typedef bool (*vef_table_storage_delete_fn)(
    vef_table_storage_handle_t *handle,
    const vef_table_storage_value_t *primary_key_values,
    uint32_t primary_key_value_count, char *error_msg, uint32_t error_msg_len);

// Mutate an existing row in place: look up by PK, overwrite all
// non-PK column values, and write back. The row's identity (its PK,
// and therefore its engine row ref / gref) is preserved. Used by
// extensions that need to mark/unmark rows (e.g. tombstones in graph
// indexes) or maintain mutable side-state on existing rows (e.g.
// HNSW neighbor lists) without touching identity.
//
// new_values must include all columns in declaration order (same
// shape as insert), but the PK columns within it are ignored — the
// PK is identified by primary_key_values.
typedef bool (*vef_table_storage_update_fn)(
    vef_table_storage_handle_t *handle,
    const vef_table_storage_value_t *primary_key_values,
    uint32_t primary_key_value_count,
    const vef_table_storage_value_t *new_values, uint32_t new_value_count,
    char *error_msg, uint32_t error_msg_len);

typedef bool (*vef_table_storage_scan_begin_fn)(
    vef_table_storage_handle_t *handle, const vef_table_storage_scan_t *scan,
    vef_table_storage_cursor_t **cursor, bool *eof, char *error_msg,
    uint32_t error_msg_len);

typedef bool (*vef_table_storage_scan_next_fn)(
    vef_table_storage_cursor_t *cursor, bool *eof, char *error_msg,
    uint32_t error_msg_len);

typedef bool (*vef_table_storage_scan_fetch_fn)(
    vef_table_storage_cursor_t *cursor, vef_table_storage_value_t *values,
    uint32_t value_count, char *error_msg, uint32_t error_msg_len);

typedef void (*vef_table_storage_scan_end_fn)(
    vef_table_storage_cursor_t *cursor);

// Return the ref length the handle's underlying engine uses for row
// references. Extensions store grefs as opaque blobs of this length;
// columns intended to hold grefs should be sized accordingly. Stable
// for the lifetime of the handle.
typedef bool (*vef_table_storage_ref_length_fn)(
    vef_table_storage_handle_t *handle, uint32_t *ref_length_out,
    char *error_msg, uint32_t error_msg_len);

// Return opaque ref bytes for the row the cursor is currently
// positioned on (i.e., the row whose columns scan_fetch would return).
// The returned pointer is valid until the next scan_next / scan_seek
// / scan_end on this cursor; extensions that need to persist the ref
// (e.g. into a stored column) must copy the bytes. The byte length
// equals vef_table_storage_ref_length_fn for this handle.
typedef bool (*vef_table_storage_scan_position_fn)(
    vef_table_storage_cursor_t *cursor, const unsigned char **ref_out,
    uint32_t *ref_len_out, char *error_msg, uint32_t error_msg_len);

// Open a single-row cursor positioned on the row identified by `ref`.
// The cursor behaves like a SCAN_FULL cursor with one entry: caller
// invokes scan_fetch to read columns, then scan_end. `eof_out` is
// false if the row was found, true if no row matches the ref (e.g.
// concurrent delete). Extensions follow links between hidden-table
// rows (graph traversal, linked list, etc.) by capturing refs via
// scan_position and replaying them here.
typedef bool (*vef_table_storage_scan_seek_fn)(
    vef_table_storage_handle_t *handle, const unsigned char *ref,
    uint32_t ref_len, vef_table_storage_cursor_t **cursor_out, bool *eof_out,
    char *error_msg, uint32_t error_msg_len);

typedef struct {
  uint32_t version;

  vef_table_storage_create_fn create;
  vef_table_storage_drop_fn drop;
  vef_table_storage_open_fn open;
  vef_table_storage_close_fn close;
  vef_table_storage_insert_fn insert;
  vef_table_storage_delete_fn delete_row;
  vef_table_storage_update_fn update_row;
  vef_table_storage_scan_begin_fn scan_begin;
  vef_table_storage_scan_next_fn scan_next;
  vef_table_storage_scan_fetch_fn scan_fetch;
  vef_table_storage_scan_end_fn scan_end;
  vef_table_storage_ref_length_fn ref_length;
  vef_table_storage_scan_position_fn scan_position;
  vef_table_storage_scan_seek_fn scan_seek;
} vef_preview_table_storage_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_TABLE_STORAGE_H
