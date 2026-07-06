/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

// =============================================================================
// VEF PREVIEW ABI HEADER — UNSTABLE BINARY INTERFACE
// =============================================================================
// This header is both:
//   - an ABI header — extension authors should use the C++ API in
//     <villagesql/vsql.h>, not these raw types. See villagesql/abi/README.md.
//   - a preview capability — API and ABI may change or be removed without
//     notice. See villagesql/preview/README.md.
// =============================================================================

#ifndef VILLAGESQL_ABI_INDEX_H_
#define VILLAGESQL_ABI_INDEX_H_

#include <stdbool.h>
#include <stdint.h>
#include "../types.h"
#include "storage.h"

#ifdef __cplusplus
extern "C" {
#endif

// Index access capabilities (used by optimizer)
typedef uint32_t vef_index_cap_t;
// Zero value for initialization; not a valid capabilities value for a
// registered index (at least one capability must be set).
#define VEF_INDEX_CAP_NONE ((vef_index_cap_t)0)
// Supports exact-match lookup by full key value.
#define VEF_INDEX_CAP_POINT_LOOKUP ((vef_index_cap_t)1u << 0)
// Supports range scans with BEGIN and/or END bounds.
#define VEF_INDEX_CAP_RANGE_SCAN ((vef_index_cap_t)1u << 1)
// Supports scanning in descending key order.
#define VEF_INDEX_CAP_REVERSE_SCAN ((vef_index_cap_t)1u << 2)
// Index scan returns results in key order, allowing the optimizer to satisfy
// ORDER BY on the index columns without a separate sort step.
#define VEF_INDEX_CAP_ORDER_BY ((vef_index_cap_t)1u << 3)
// Supports index scan for K nearest neighbours.
// ORDER BY distance_function(index_column, reference_key) LIMIT K;
#define VEF_INDEX_CAP_KNN ((vef_index_cap_t)1u << 4)

// Physical storage properties (used by InnoDB).
// Every index entry must carry a back-reference to the data it indexes so the
// server can resolve an index hit back to the row. The extension declares which
// reference type(s) it stores; the server chooses the appropriate one at
// index-creation time. At least one of HAS_ROW_REF or HAS_COLUMN_REF must be
// set.
typedef uint32_t vef_index_storage_t;
// Zero value for initialization; not a valid storage_props for a registered
// index (at least one of HAS_ROW_REF or HAS_COLUMN_REF must be set).
#define VEF_INDEX_STORAGE_NONE ((vef_index_storage_t)0)
// Index entry stores a stable column reference supplied by the server. The
// server provides this reference when the column has a stable physical address
// (e.g. a reference from VECTOR column storage). Allows row lookup without
// storing the full PK.
#define VEF_INDEX_STORAGE_HAS_COLUMN_REF ((vef_index_storage_t)1u << 0)
// Index entry stores the primary key of the indexed row. The server resolves
// an index hit by performing a primary key lookup with the stored value.
#define VEF_INDEX_STORAGE_HAS_ROW_REF ((vef_index_storage_t)1u << 1)
// Ability to re-locate an index entry using a stable reference token.
// Typically used to locate index key for internal operations like purge
// when index doesn't support point key lookup.
// key_ref uniqueness: every index entry that has not yet been physically
// removed by purge must have a unique key_ref. Mark-deleted entries are still
// live in this sense and must retain their unique key_ref until purged.
// The extension must guarantee this invariant.
#define VEF_INDEX_STORAGE_REF_LOOKUP ((vef_index_storage_t)1u << 2)

// Opaque index descriptor reference from MySQL/InnoDB
typedef void *vef_index_ref_t;

// Naming convention for function pointer typedefs in this header:
//   _fn      - callback provided by the server and called by the extension
//   _func_t  - function provided by the extension and called by the server

// Get the maximum storage length in bytes for a key column.
// key_pos is the 0-based column index within the key (index key if
// is_primary=false, primary key if is_primary=true).
typedef uint32_t (*vef_index_max_key_len_fn)(vef_index_ref_t index_ref,
                                             uint32_t key_pos, bool is_primary);

// Resolve a column reference to column data.
// The caller pre-allocates col_data; the server fills in its data pointer and
// length to reference the stored column value.
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_index_col_ref_to_data_fn)(vef_index_ref_t index_ref,
                                             vef_storage_col_ref_t col_ref,
                                             vef_storage_col_data_t *col_data,
                                             char *error_msg,
                                             uint32_t error_msg_len);

// Derive a stable column reference from column data.
// col_data must describe a value that was previously inserted into column
// storage; the server computes the stable col_ref for that stored value.
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_index_col_data_to_ref_fn)(vef_index_ref_t index_ref,
                                             vef_storage_col_data_t col_data,
                                             vef_storage_col_ref_t *col_ref,
                                             char *error_msg,
                                             uint32_t error_msg_len);

// Call a helper function from the index profile by its registered ID.
// An index profile is declared by the extension when registering an index type.
// It enumerates helper functions (e.g. distance, compare) each identified by a
// uint32_t fn_id. This callback is the server's generic dispatcher for those
// functions. The layouts of args (input array of nargs elements) and result
// (output buffer) are defined by the specific function registered at fn_id;
// the extension determines these conventions when it registers the profile.
// Profile functions are infallible.
typedef void (*vef_index_profile_fn)(vef_index_ref_t index_ref,
                                     uint32_t key_pos, uint32_t fn_id,
                                     const void *const *args, uint32_t nargs,
                                     void *result);

// Index context passed by the server to every extension function. The pointer
// remains valid for the lifetime of the loaded index storage; the extension
// may cache it inside its storage or cursor state.
typedef struct {
  // Version for future extensibility
  uint32_t version;

  // Opaque index descriptor reference from MySQL/InnoDB.
  vef_index_ref_t index_ref;

  // Number of index key columns
  uint32_t num_key_columns;

  // Number of primary key columns
  uint32_t num_primary_key_columns;

  // Index profile function call interface. Always non-NULL.
  vef_index_profile_fn profile_fn;

  // Index profile helper call interface. Always non-NULL.
  vef_index_profile_fn helper_fn;

  // Get maximum key storage length. Always non-NULL.
  vef_index_max_key_len_fn key_len_fn;

  // Convert a column reference to column data. Always non-NULL when
  // VEF_INDEX_STORAGE_HAS_COLUMN_REF is set in storage_props; otherwise NULL.
  vef_index_col_ref_to_data_fn col_ref_to_data_fn;

  // Derive a stable column reference from column data. Always non-NULL when
  // VEF_INDEX_STORAGE_HAS_COLUMN_REF is set in storage_props; otherwise NULL.
  vef_index_col_data_to_ref_fn col_data_to_ref_fn;

  // Pointer to the options struct filled by parse() at CREATE INDEX time.
  // Valid only during the create() call; NULL for all other calls and when
  // the index type declared no options.
  const void *options;

} vef_index_ctx_t;

// Index scan type. Determines the structure of the keys array in
// vef_index_scan_desc_t:
//
//   POINT: Exact-match lookup. The keys array contains exactly one entry of
//          type BEGIN with include_key=true and key_columns != NULL. The
//          extension must match only entries equal to that key value.
//
//   RANGE: Range scan. The keys array contains a BEGIN entry (lower bound) and
//          an END entry (upper bound) in any order. Either may be unbounded
//          (key_columns == NULL). include_key on each entry controls whether
//          the bound is inclusive or exclusive.
//
//   KNN:   K nearest neighbour scan. The keys array contains exactly one entry
//          of type KNN_QUERY carrying the query vector. limit in the descriptor
//          specifies K.
typedef uint32_t vef_index_scan_type_t;
#define VEF_INDEX_SCAN_TYPE_POINT ((vef_index_scan_type_t)0)
#define VEF_INDEX_SCAN_TYPE_RANGE ((vef_index_scan_type_t)1)
#define VEF_INDEX_SCAN_TYPE_KNN ((vef_index_scan_type_t)2)

// Index scan key type.
// BEGIN and END are value-space bounds, not traversal-order positions.
// BEGIN is always the lower value bound; END is always the upper value bound.
// The `reverse` field in vef_index_scan_desc_t controls traversal direction
// within those fixed bounds: a reverse scan starts at the END bound and
// advances toward BEGIN using PREV. The extension must account for `reverse`
// when positioning the cursor in scan_begin, but the meaning of BEGIN/END
// does not change.
typedef uint32_t vef_index_scan_key_type_t;
#define VEF_INDEX_SCAN_KEY_TYPE_BEGIN ((vef_index_scan_key_type_t)0)
#define VEF_INDEX_SCAN_KEY_TYPE_END ((vef_index_scan_key_type_t)1)
#define VEF_INDEX_SCAN_KEY_TYPE_KNN_QUERY ((vef_index_scan_key_type_t)2)

typedef struct {
  // Version for future extensibility
  uint32_t version;

  // Key type determines which fields below are meaningful:
  //   BEGIN / END:  range bound (lower / upper value bound respectively)
  //   KNN_QUERY:    the query vector to find K nearest neighbours of
  vef_index_scan_key_type_t type;

  // Number of key columns.
  // For BEGIN/END: may be a prefix of the full index key.
  // For KNN_QUERY: number of columns in the query vector; must not be 0.
  uint32_t num_key_columns;

  // Key data for each column.
  // For BEGIN/END: NULL means unbounded on that side:
  //   BEGIN with key_columns == NULL -> no lower value bound (-INF)
  //   END with key_columns == NULL   -> no upper value bound (+INF)
  //   These semantics are value-space and do not change with `reverse`: a
  //   reverse scan with a NULL END key still starts at +INF and advances
  //   downward. When NULL, num_key_columns must be 0 and include_key is
  //   ignored.
  // For KNN_QUERY: must not be NULL.
  vef_storage_col_data_t *key_columns;

  // Whether the range bound is inclusive. Applies to BEGIN/END only.
  // true -> GE (BEGIN) or LE (END); false -> GT (BEGIN) or LT (END).
  // Ignored for KNN_QUERY and when key_columns is NULL.
  uint8_t include_key;

} vef_index_scan_key_t;

// Index scan descriptor.
typedef struct {
  // Version for future extensibility
  uint32_t version;

  // Scan type: point lookup, range scan, or KNN. The server sets this so the
  // extension can dispatch without inspecting the key array.
  vef_index_scan_type_t scan_type;

  // Scan direction. false = ascending (BEGIN -> END), true = descending
  // (END -> BEGIN). Does not change the value-space meaning of BEGIN/END keys.
  // Ignored for VEF_INDEX_SCAN_TYPE_KNN.
  uint8_t reverse;

  // Maximum number of rows the server will read from this scan; 0 means
  // unlimited. For VEF_INDEX_SCAN_TYPE_KNN this is K. For other scan types
  // it signals that the server will call scan_end after at most limit rows,
  // which the extension may use to optimize internal structures (e.g. avoid
  // materializing more results than needed).
  uint32_t limit;

  // Number of keys in the keys array.
  uint32_t num_keys;
  // Array of num_keys scan keys describing the bounds or query for this scan.
  vef_index_scan_key_t *keys;

} vef_index_scan_desc_t;

// Cursor operation types
typedef uint32_t vef_index_cursor_op_t;
#define VEF_INDEX_CURSOR_OP_NEXT ((vef_index_cursor_op_t)0)
#define VEF_INDEX_CURSOR_OP_PREV ((vef_index_cursor_op_t)1)

// Opaque cursor type owned by extension.
typedef void *vef_index_cursor_ref_t;

// Create index storage.
// Parameters:
//   index_ctx   - Index context
//   space_ref   - Tablespace in which to create the index storage
//   trx_ref     - Transaction under which the creation runs
//   arena_ctx   - Opaque arena handle passed to arena_alloc
//   arena_alloc - Allocator for the storage context; memory is freed when
//                 storage is dropped
//   storage     - Output: newly created storage context
//   error_msg   - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_create_func_t)(
    const vef_index_ctx_t *index_ctx, vef_storage_space_ref_t space_ref,
    vef_storage_trx_ref_t trx_ref, vef_storage_arena_t *arena_ctx,
    vef_storage_arena_func_t arena_alloc, vef_storage_ctx_t **storage,
    char *error_msg, uint32_t error_msg_len);

// Drop index storage.
// Parameters:
//   index_ctx - Index context
//   storage   - Storage context to drop
//   trx_ref   - Transaction under which the drop runs
//   error_msg - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_drop_func_t)(const vef_index_ctx_t *index_ctx,
                                           vef_storage_ctx_t *storage,
                                           vef_storage_trx_ref_t trx_ref,
                                           char *error_msg,
                                           uint32_t error_msg_len);

// Load existing index storage from a storage reference.
// Parameters:
//   index_ctx   - Index context
//   storage_ref - Reference to the existing storage to load
//   arena_ctx   - Opaque arena handle passed to arena_alloc
//   arena_alloc - Allocator for the storage context; memory is freed when
//                 storage is dropped
//   storage     - Output: loaded storage context
//   error_msg   - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_load_func_t)(
    const vef_index_ctx_t *index_ctx, vef_storage_ref_t storage_ref,
    vef_storage_arena_t *arena_ctx, vef_storage_arena_func_t arena_alloc,
    vef_storage_ctx_t **storage, char *error_msg, uint32_t error_msg_len);

// Unloads an index storage context.
// Called before a storage context is destroyed, such as during cache eviction,
// table close, or server shutdown. This callback should release any resources
// associated with the storage context.
// Parameters:
//   index_ctx - Index context
//   storage   - Storage context being unloaded
//   error_msg - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_unload_func_t)(const vef_index_ctx_t *index_ctx,
                                             vef_storage_ctx_t *storage,
                                             char *error_msg,
                                             uint32_t error_msg_len);

// Insert an index entry for the given key and primary key data.
// Parameters:
//   index_ctx   - Index context
//   storage     - Storage context to insert into
//   trx_ref     - Transaction performing the insert
//   key_columns - Array of index_ctx->num_key_columns index key columns
//   pkey_columns - Array of index_ctx->num_primary_key_columns primary key
//                  columns for the row
//   key_ref     - Output: opaque reference to the newly inserted index entry.
//                 Valid only if VEF_INDEX_STORAGE_REF_LOOKUP capability is set.
//                 The reference remains stable across mini-transactions and
//                 transactions, and continues to identify the same index entry
//                 after mark_delete, until the entry is physically removed by
//                 purge. After a successful purge of the entry, the reference
//                 becomes invalid and must not be used.
//   error_msg   - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_insert_func_t)(
    const vef_index_ctx_t *index_ctx, vef_storage_ctx_t *storage,
    vef_storage_trx_ref_t trx_ref, vef_storage_col_data_t *key_columns,
    vef_storage_col_data_t *pkey_columns, vef_storage_col_ref_t *key_ref,
    char *error_msg, uint32_t error_msg_len);

// Mark or unmark an index entry as deleted by the transaction.
// Parameters:
//   index_ctx    - Index context
//   storage      - Storage context
//   trx_ref      - Transaction performing the operation
//   key_ref      - Reference to the index entry to mark.
//                  Used only if VEF_INDEX_STORAGE_REF_LOOKUP capability is set.
//   key_columns  - Array of index_ctx->num_key_columns index key columns
//   pkey_columns - Array of index_ctx->num_primary_key_columns primary key
//                  columns for the row
//   delete_mark  - true to mark as deleted, false to unmark
//   error_msg    - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_mark_delete_func_t)(
    const vef_index_ctx_t *index_ctx, vef_storage_ctx_t *storage,
    vef_storage_trx_ref_t trx_ref, vef_storage_col_ref_t *key_ref,
    vef_storage_col_data_t *key_columns, vef_storage_col_data_t *pkey_columns,
    bool delete_mark, char *error_msg, uint32_t error_msg_len);

// Remove an index entry only if it has the matching transaction reference.
// Parameters:
//   index_ctx    - Index context
//   storage      - Storage context
//   trx_ref      - Transaction to match with the record to purge
//   key_ref      - Reference to the index entry to purge.
//                  Used only if VEF_INDEX_STORAGE_REF_LOOKUP capability is set.
//   key_columns  - Array of index_ctx->num_key_columns index key columns
//   pkey_columns - Array of index_ctx->num_primary_key_columns primary key
//                  columns for the row
//   error_msg    - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_purge_func_t)(
    const vef_index_ctx_t *index_ctx, vef_storage_ctx_t *storage,
    vef_storage_trx_ref_t trx_ref, vef_storage_col_ref_t *key_ref,
    vef_storage_col_data_t *key_columns, vef_storage_col_data_t *pkey_columns,
    char *error_msg, uint32_t error_msg_len);

// Begin an index scan with the given scan descriptor.
// The cursor is created and immediately positioned at the first record in
// traversal order within the scan bounds: the BEGIN bound for a forward scan,
// the END bound for a reverse scan. The caller must not call scan_position
// before the first scan_fetch; the cursor is already positioned on return.
// The caller owns the cursor and must release it with scan_end, even if eof
// is true on return. On error (returns true), cursor is set to NULL and
// scan_end must not be called.
// Parameters:
//   index_ctx - Index context
//   storage   - Storage context to scan
//   mctx      - Mini-transaction context
//   scan_desc - Scan parameters (keys, direction, limit)
//   cursor    - Output: newly created and positioned cursor
//   eof       - Output: true if no records match the scan bounds. On success
//               (returns false), the cursor is still created and must be
//               released with scan_end regardless of eof.
//   error_msg - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_scan_begin_func_t)(
    const vef_index_ctx_t *index_ctx, vef_storage_ctx_t *storage,
    vef_storage_mtr_ref_t mctx, const vef_index_scan_desc_t *scan_desc,
    vef_index_cursor_ref_t *cursor, bool *eof, char *error_msg,
    uint32_t error_msg_len);

// Advance the cursor using the given operation.
// Parameters:
//   cursor    - Cursor to operate on
//   op        - Cursor operation (next, prev)
//   eof       - Output: true if the scan has reached the end
//   error_msg - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_scan_position_func_t)(
    vef_index_cursor_ref_t cursor, vef_index_cursor_op_t op, bool *eof,
    char *error_msg, uint32_t error_msg_len);

// Fetch key and primary key data at the current cursor position.
// The caller pre-allocates the key_columns and pkey_columns arrays of
// vef_storage_col_data_t structs. The extension fills in each struct's data
// pointer and length to reference its internal page data directly; no copy is
// made. The data pointers are valid only while the cursor's mini-transaction
// remains active; they must not be read after scan_save or scan_end.
// Parameters:
//   cursor       - Cursor to read from
//   key_ref      - Output: reference to the current index entry.
//                  Valid only if VEF_INDEX_STORAGE_REF_LOOKUP capability is
//                  set. Follows the same stability guarantees as the key_ref
//                  output of insert: stable across mini-transactions and
//                  transactions until the entry is physically removed by purge.
//   key_columns  - Caller-allocated array of index_ctx->num_key_columns
//                  vef_storage_col_data_t structs; the extension sets each
//                  data pointer and length to the key data at this position
//   pkey_columns - Caller-allocated array of
//                  index_ctx->num_primary_key_columns vef_storage_col_data_t
//                  structs; the extension sets each data pointer and length
//                  to the primary key data for the current row
//   error_msg    - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_scan_fetch_func_t)(
    vef_index_cursor_ref_t cursor, vef_storage_col_ref_t *key_ref,
    vef_storage_col_data_t *key_columns, vef_storage_col_data_t *pkey_columns,
    char *error_msg, uint32_t error_msg_len);

// Save the current cursor position before the active mini-transaction commits.
// The server calls this when it is about to commit the mini-transaction the
// cursor is associated with. The cursor must record enough logical state to
// relocate the same position later. Page latches are released implicitly by
// the mini-transaction commit; the cursor does not need to release them.
// No mctx parameter is needed because the cursor is detaching from, not
// acquiring, a mini-transaction.
// Parameters:
//   cursor    - Cursor to save
//   error_msg - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_scan_save_func_t)(vef_index_cursor_ref_t cursor,
                                                char *error_msg,
                                                uint32_t error_msg_len);

// Restore a previously saved cursor position under a new mini-transaction.
// The server calls this after starting a new mini-transaction to resume the
// scan. The cursor must re-acquire any page latches needed to continue from
// the position saved by the last scan_save call.
// Parameters:
//   cursor    - Cursor to restore
//   mctx      - Newly started mini-transaction to attach to
//   eof       - Output: true if the saved position is now past the end
//   error_msg - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_scan_restore_func_t)(
    vef_index_cursor_ref_t cursor, vef_storage_mtr_ref_t mctx, bool *eof,
    char *error_msg, uint32_t error_msg_len);

// End the scan and free all cursor resources.
// Parameters:
//   cursor - Pointer to the cursor to destroy; set to NULL on return.
typedef void (*vef_type_index_scan_end_func_t)(vef_index_cursor_ref_t *cursor);

// One key=value parameter from a WITH (...) clause on a custom index.
// Both strings are null-terminated. Numeric literal values (e.g. M = 16) are
// represented as their decimal string ("16").
typedef struct {
  const char *key;
  const char *value;
} vef_index_param_t;

// Parse and validate WITH (...) parameters at CREATE INDEX time.
// The server calls this before creating the index entry. The extension fills
// options_out (server-allocated, options_size bytes) with the validated result,
// which the server then passes as index_ctx->options to create().
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_index_parse_func_t)(const vef_index_param_t *params,
                                            uint32_t count, void *options_out,
                                            char *error_msg,
                                            uint32_t error_msg_len);

// Index type interface version constants.
#define VEF_INDEX_TYPE_INTF_VERSION_1 1
#define VEF_INDEX_TYPE_INTF_VERSION VEF_INDEX_TYPE_INTF_VERSION_1

// Index interface provided by an extension for custom index storage.
// The extension sets version to the index interface version it implements.
// The server must check version before accessing any field introduced in
// later versions. The server reads `version` before accessing any field to
// determine which fields are present. New fields must always be appended at
// the end so that older structs remain a valid prefix.
// All function pointers except parse must be non-NULL.
typedef struct {
  // Version for future extensibility
  uint32_t version;

  // Bitmask of optimizer capabilities this index supports (VEF_INDEX_CAP_*).
  // The server reads this before invoking any scan or DML functions to
  // determine which query plans are valid for this index type.
  vef_index_cap_t capabilities;

  // Bitmask of physical storage properties this index provides
  // (VEF_INDEX_STORAGE_*). The server reads this before invoking any DML
  // functions to know which outputs (key_ref, etc.) are populated.
  vef_index_storage_t storage_props;

  // Life cycle functions
  vef_type_index_create_func_t create;
  vef_type_index_drop_func_t drop;
  vef_type_index_load_func_t load;

  // Automatically populated by the SDK's IndexBuilder.
  // Extensions cannot currently override this callback.
  vef_type_index_unload_func_t unload;

  // DML functions
  vef_type_index_insert_func_t insert;
  // TODO(villagesql-indexing): Add bulk insert interface.
  vef_type_index_mark_delete_func_t mark_delete;
  vef_type_index_purge_func_t purge;

  // Scan functions
  vef_type_index_scan_begin_func_t scan_begin;
  vef_type_index_scan_position_func_t scan_position;
  vef_type_index_scan_fetch_func_t scan_fetch;
  vef_type_index_scan_save_func_t scan_save;
  vef_type_index_scan_restore_func_t scan_restore;
  vef_type_index_scan_end_func_t scan_end;

  // WITH (...) parameter parsing. Set options_size to sizeof(Options) and
  // parse to the validation function. Both must be zero/NULL together; either
  // both are set or neither is.
  uint32_t options_size;
  vef_type_index_parse_func_t parse;

  // TODO(villagesql-indexing): Add signatures for the index functions and
  // helper functions (keyed by fn_id) that this index type expects. The server
  // can use these to validate that a profile binding this index type declares
  // compatible function signatures.

} vef_type_index_intf_t;

// ===========================================================================
// vsql::preview::index_type capability
// ===========================================================================
//
// Registers custom index type implementations. Pass a
// vef_preview_index_type_ext_desc_t as
// vef_required_capability_t.capability_config when requiring this capability.
//
// Capability name: VEF_PREVIEW_INDEX_TYPE_NAME

#define VEF_PREVIEW_INDEX_TYPE_NAME "vsql::preview::index_type"

#define VEF_PREVIEW_INDEX_TYPE_ABI_VERSION_1 1
#define VEF_PREVIEW_INDEX_TYPE_ABI_VERSION VEF_PREVIEW_INDEX_TYPE_ABI_VERSION_1

// One index type entry: name plus a pointer to the interface struct.
// Both pointers must remain valid for the lifetime of the extension.
typedef struct {
  const char *name;
  const vef_type_index_intf_t *intf;
} vef_index_type_reg_t;

// Extension descriptor for vsql::preview::index_type.
typedef struct {
  // Must be set to VEF_PREVIEW_INDEX_TYPE_ABI_VERSION.
  uint32_t version;
  uint32_t count;
  // Flat array of count entries. NULL when count is zero.
  const vef_index_type_reg_t *types;
} vef_preview_index_type_ext_desc_t;

// Minimal vtable for vsql::preview::index_type.
typedef struct {
  uint32_t version;
} vef_preview_index_type_t;

// ===========================================================================
// vsql::preview::index_profile capability
// ===========================================================================
//
// Registers index profiles. Each profile binds a custom type to an index type
// and declares the helper functions (distance, compare, etc.) used by the
// index storage implementation. The server registers those functions as SQL
// VDFs automatically when the profile is loaded.
//
// Pass a vef_preview_index_profile_ext_desc_t as
// vef_required_capability_t.capability_config when requiring this capability.
//
// Capability name: VEF_PREVIEW_INDEX_PROFILE_NAME

#define VEF_PREVIEW_INDEX_PROFILE_NAME "vsql::preview::index_profile"

#define VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION_1 1
#define VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION \
  VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION_1

// One function binding within an index profile.
// fn_id is the identifier used by the index storage implementation when it
// calls vef_index_profile_fn in the index context. The remaining fields carry
// the VDF metadata the server uses to register the function as a SQL function.
typedef struct {
  uint32_t fn_id;
  const char *name;
  vef_protocol_t protocol;
  vef_vdf_func_t vdf;
  vef_signature_t signature;
  uint8_t is_deterministic;
} vef_index_profile_fn_binding_t;

// One registered index profile entry.
typedef struct {
  const char *name;
  // Name of the custom type this profile applies to.
  const char *type_name;
  // Name of the index type (vef_index_type_reg_t.name) that implements this
  // profile's storage.
  const char *index_type_name;
  // User-visible SQL functions. The optimizer pattern-matches calls to these
  // functions in queries and substitutes an index scan.
  uint32_t function_count;
  // Pointer to a flat array of function_count bindings. NULL when
  // function_count is zero.
  const vef_index_profile_fn_binding_t *functions;
  // Helper functions invoked only by the index implementation via
  // vef_index_ctx_t.helper_fn. fn_ids are independent of function fn_ids.
  uint32_t helper_count;
  // Pointer to a flat array of helper_count bindings. NULL when
  // helper_count is zero.
  const vef_index_profile_fn_binding_t *helpers;
  // Bitmask of VEF_INDEX_ORDERING_* flags indicating supported scan directions.
  uint8_t ordering;
  // 1 if this is the default profile for the type when no profile is named at
  // CREATE INDEX time.
  uint8_t default_for_type;
} vef_index_profile_reg_t;

// Bitmask values for vef_index_profile_reg_t.ordering.
#define VEF_INDEX_ORDERING_NONE 0x00
#define VEF_INDEX_ORDERING_ASC 0x01
#define VEF_INDEX_ORDERING_DESC 0x02

// Extension descriptor for vsql::preview::index_profile.
// TODO(villagesql-indexing): Consider changing profiles to
// const vef_index_profile_reg_t ** so vef_index_profile_reg_t can grow
// without changing the array stride.
typedef struct {
  // Must be set to VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION.
  uint32_t version;
  uint32_t count;
  // Flat array of count entries. NULL when count is zero.
  const vef_index_profile_reg_t *profiles;
} vef_preview_index_profile_ext_desc_t;

// Minimal vtable for vsql::preview::index_profile.
typedef struct {
  uint32_t version;
} vef_preview_index_profile_t;

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // VILLAGESQL_ABI_INDEX_H_
