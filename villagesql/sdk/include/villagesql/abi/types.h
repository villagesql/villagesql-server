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

#ifndef VILLAGESQL_ABI_TYPES_H_
#define VILLAGESQL_ABI_TYPES_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Protocol Versioning
//
// The protocol is incremented when the binary layout or function signatures of
// the ABI is changed.
//
// Each versioned struct contains the `protocol` as the first member.  Every
// new version of the struct will be a strict superset of the previous versions
// of the structure.  Therefore a binary or extension supporting an older
// version can understand that subset of the structure.
//
// During registration the mysqld binary will pass the highest protocol it
// supports (i.e.  the protocol it was built with) in `vef_register_arg`.  The
// extension can read and understand all fields up to the min(mysqld_protocol,
// extension_protocol)
//
// The returned `vef_registration` has the highest protocol it was built with.
// And has all the fields required by the min(mysqld_protocol,
// extension_protocol) version specified.
//
// Structs that are used as inline fields in another struct cannot be
// versioned. When new version of an inline field us needed, a new field will
// be added with the new type will be added to a new version of the containing
// struct.
//
// Extension Lifecycle
// ===================
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                        REGISTRATION FLOW                           │
//   └─────────────────────────────────────────────────────────────────────┘
//
//     ┌──────────┐                                    ┌──────────────────┐
//     │  mysqld  │                                    │  extension.so    │
//     └────┬─────┘                                    └────────┬─────────┘
//          │                                                   │
//          │  1. dlopen("extension.so")                        │
//          │──────────────────────────────────────────────────>│
//          │                                                   │
//          │  2. dlsym("vef_register")                         │
//          │──────────────────────────────────────────────────>│
//          │                                                   │
//          │  3. vef_register(&register_arg)                   │
//          │   ┌─────────────────────────────┐                 │
//          │   │ vef_register_arg_t:         │                 │
//          │   │   protocol (negotiation)    │                 │
//          │   │   mysql_version             │                 │
//          │   │   vef_version               │                 │
//          │   └─────────────────────────────┘                 │
//          │──────────────────────────────────────────────────>│
//          │                                                   │
//          │                     4. return vef_registration_t* │
//          │   ┌─────────────────────────────┐                 │
//          │   │ vef_registration_t:         │                 │
//          │   │   protocol                  │                 │
//          │   │   extension_name/version    │                 │
//          │   │   funcs[] -> vef_func_desc  │                 │
//          │   │   types[] -> vef_type_desc  │                 │
//          │   └─────────────────────────────┘                 │
//          │<──────────────────────────────────────────────────│
//          │                                                   │
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                      FUNCTION INVOCATION FLOW                       │
//   └─────────────────────────────────────────────────────────────────────┘
//
//     ┌──────────┐                                    ┌──────────────────┐
//     │  mysqld  │                                    │  extension.so    │
//     └────┬─────┘                                    └────────┬─────────┘
//          │                                                   │
//          │  SQL: SELECT my_func(col1, col2)                  │
//          │                                                   │
//          │  1. vef_prerun_func_t(ctx, args, result)          │
//          │   ┌─────────────────────────────┐                 │
//          │   │ vef_prerun_args_t:          │                 │
//          │   │   arg_count, arg_types      │                 │
//          │   │   const_values/lengths      │                 │
//          │   │                             │                 │
//          │   │ vef_prerun_result_t:        │                 │
//          │   │   user_data (set by callee) │                 │
//          │   │   result_buffer_size        │                 │
//          │   └─────────────────────────────┘                 │
//          │──────────────────────────────────────────────────>│
//          │                                                   │
//          │  2. vef_vdf_func_t(ctx, args, result)             │
//          │   ┌─────────────────────────────┐                 │
//          │   │ vef_vdf_args_t:             │                 │
//          │   │   user_data (from prerun)   │                 │
//          │   │   value_count               │                 │
//          │   │   values[] -> vef_invalue_t │                 │
//          │   └─────────────────────────────┘                 │
//          │──────────────────────────────────────────────────>│
//          │                  (repeated for each row)          │
//          │                                                   │
//          │                  3. writes result to result->buf  │
//          │   ┌─────────────────────────────┐                 │
//          │   │ vef_vdf_result_t:           │                 │
//          │   │   type = IS_VALUE/NULL/ERR  │                 │
//          │   │   actual_len                │                 │
//          │   │   buf (caller-provided)     │                 │
//          │   │   alt_buf (optional, callee │                 │
//          │   │     can return own pointer) │                 │
//          │   └─────────────────────────────┘                 │
//          │<─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│
//          │                                                   │
//          │  4. vef_postrun_func_t(ctx, args, result)         │
//          │   ┌─────────────────────────────┐                 │
//          │   │ vef_postrun_args_t:         │                 │
//          │   │   user_data (for cleanup)   │                 │
//          │   └─────────────────────────────┘                 │
//          │──────────────────────────────────────────────────>│
//          │                                                   │
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                       UNREGISTRATION FLOW                           │
//   └─────────────────────────────────────────────────────────────────────┘
//
//     ┌──────────┐                                    ┌──────────────────┐
//     │  mysqld  │                                    │  extension.so    │
//     └────┬─────┘                                    └────────┬─────────┘
//          │                                                   │
//          │  1. vef_unregister(&unregister_arg, registration) │
//          │──────────────────────────────────────────────────>│
//          │                                                   │
//          │                       2. extension frees memory   │
//          │                          allocated in register    │
//          │                                                   │
//          │  3. dlclose("extension.so")                       │
//          │──────────────────────────────────────────────────>│
//          │                                                   │
//

typedef enum : unsigned int {
  VEF_PROTOCOL_0,  // Not used
  VEF_PROTOCOL_1,  // Stable as of v0.0.1, likely to be deprecated.
  VEF_PROTOCOL_2,  // Under development, not stable. Adds:
                   // + Add deterministic VDF attribute.
                   // + Add encode/decode/compare/hash VDF name fields to
                   //   vef_type_desc_t.
                   // + Add int_to_params and resolve_params VDF name fields to
                   //   vef_type_desc_t.
                   // + Replace vef_vdf_args_t.values_v1 flat array with
                   //   vef_vdf_args_t.values pointer array (allows
                   //   vef_invalue_t to grow in future protocol versions).
                   // + Preview capability system: extensions declare named
                   //   capabilities they require in vef_registration_t;
                   //   the server populates their function pointers before
                   //   vef_register() returns.
                   //   (vef_required_capability_t, required_capabilities,
                   //   required_capability_count in vef_registration_t)
} vef_protocol_t;

// Max length of error messages in caller-provided buffers.
#define VEF_MAX_ERROR_LEN 512

// NB - this file is in C so that it can more easily be used in languages other
// than C++.
//
// Open issues:
//   A) How do we handle encodings and collations?
//   B) Resizing the buffer can probably be done in a smarter way.
typedef struct {
  unsigned int major;
  unsigned int minor;
  unsigned int patch;

  // Owned by whoever owns this struct.
  const char *extra;
} vef_version_t;

// Context passed to all function calls (prerun, vdf, postrun)
//
typedef struct {
  // protocol version being used
  vef_protocol_t protocol;

  // We foresee adding logger or distributed trace information in this context
} vef_context_t;

typedef struct {
  // protocol >= VEF_PROTOCOL_1
  vef_protocol_t protocol;

  vef_version_t mysql_version;
  vef_version_t vef_version;
} vef_register_arg_t;

typedef struct {
  // protocol >= VEF_PROTOCOL_1
  vef_protocol_t protocol;
} vef_unregister_arg_t;

// Type identifiers for VEF values (parameters and return types)
typedef enum : int {
  VEF_TYPE_STRING = 0,
  VEF_TYPE_REAL = 1,
  VEF_TYPE_INT = 2,
  VEF_TYPE_CUSTOM = 3

  // TODO(villagesql-ga): Do we want to support DECIMAL?
} vef_type_id;

// Snapshot of vef_invalue_t as of VEF_PROTOCOL_1. Used as the element type of
// vef_vdf_args_t.values_v1 to preserve the correct stride when a v2 extension
// is called by a v1 server. Do NOT add fields here.
typedef struct {
  vef_type_id type;
  bool is_null;

  union {
    struct {
      size_t str_len;
      const char *str_value;
    };
    struct {
      size_t bin_len;
      const unsigned char *bin_value;
    };
    double real_value;
    long long int_value;
  };
} vef_invalue_v1_t;

// Type parameters for a concrete type instantiation (e.g., dimension=1536).
// Keys are sorted alphabetically; values are in the same order so that
// keys[i] pairs with values[i]. count == 0 for non-parameterized or
// non-custom types.
typedef struct {
  unsigned int count;
  const char *const *keys;
  const char *const *values;
} vef_type_params_t;

// Server-supplied output channel for at-parse-time (fix_fields) inference of
// from_string parameters from a constant string literal. The SDK wrapper
// writes the canonical "k=v,k=v" form of the inferred params here when the
// extension's parameterized from_string transitions MaybeParams<P> from
// unknown to known during the call.
//
// Used only on the constant-string inference path. NULL on the normal
// row-time path; see vef_vdf_result_t::out_type_params.
//
// Overflow contract (snprintf-style): the wrapper always sets actual_len to
// the number of bytes that *would* have been written. If actual_len exceeds
// max_buf_len, overflow is true and the contents of buf are undefined; the
// caller should re-invoke with a heap-allocated buffer sized to
// actual_len + 1 and try again.
typedef struct {
  // INPUT: caller-supplied buffer for the canonical "k=v,k=v" params string.
  char *buf;
  // INPUT: capacity of buf in bytes.
  size_t max_buf_len;
  // OUTPUT: number of bytes that would have been written (no NUL). 0 means
  // no params were inferred (e.g., the extension's from_string did not call
  // p.set(), the call errored, or the type does not register
  // params_to_strings).
  size_t actual_len;
  // OUTPUT: true if actual_len > max_buf_len. When true, buf is not safe to
  // read; the caller should retry with a larger buffer.
  bool overflow;
} vef_inferred_type_params_t;

// Input value for VDF function arguments.
// The `type` field indicates which union member to read.
// Check `is_null` first - if true, the value is SQL NULL.
typedef struct {
  vef_type_id type;
  bool is_null;

  union {
    // For TYPE_STRING: human-readable text
    struct {
      size_t str_len;
      const char *str_value;
    };

    // For TYPE_CUSTOM: binary data (persisted format)
    struct {
      size_t bin_len;
      const unsigned char *bin_value;

      // protocol >= VEF_PROTOCOL_2
      // Read-only: the extension must not overwrite these parameters.
      // TODO(villagesql-beta): Optimize this to pass a token so that the
      // extension can cache these values in a language-specific way.
      vef_type_params_t type_params;
    };

    // For TYPE_REAL
    double real_value;

    // For TYPE_INT
    long long int_value;
  };
} vef_invalue_t;

typedef enum : int {
  // `buf` contains the serialized value
  VEF_RESULT_VALUE = 0,

  // The result value is NULL
  VEF_RESULT_NULL = 1,

  // The result is a row-level issue: execution continues, NULL is returned for
  // this row, and a warning is added to the statement's warning list.
  // Use for bad input data that should not abort the query.
  VEF_RESULT_WARNING = 2,

  // The result is a fatal error: statement execution is aborted immediately.
  // Use for corrupt stored data, programming errors, or other conditions that
  // make it unsafe to continue.
  VEF_RESULT_ERROR = 3,
} vef_return_value_type_t;

// Result from a VDF call
//
// For STRING return type: use str_buf/max_str_len/alt_str_buf
// For CUSTOM return type: use bin_buf/max_bin_len/alt_bin_buf
// For REAL return type: use real_value
// For INT return type: use int_value
typedef struct {
  // protocol >= VEF_PROTOCOL_1
  vef_return_value_type_t type;

  // The actual length of the result (set iff type is IS_VALUE for
  // STRING/CUSTOM) If type is IS_BUF_TOO_SMALL, this indicates the required
  // buffer size. For REAL and INT types, this field is unused.
  size_t actual_len;

  // Caller-provided buffer for error message (size VEF_MAX_ERROR_LEN).
  // Write a null-terminated string here if type == IS_ERROR.
  char *error_msg;

  union {
    // For STRING type: caller-provided buffer for text result
    struct {
      // Buffer where the string result should be written
      char *str_buf;

      // Size of str_buf in bytes
      size_t max_str_len;

      // Callee can return a different pointer than str_buf, e.g., if it already
      // has the result in its memory and wants to avoid a copy. To do this,
      // set *alt_str_buf to point to the result. If *alt_str_buf is set:
      //   - str_buf is ignored (caller reads from *alt_str_buf instead)
      //   - actual_len must still be set
      // The callee retains ownership of the memory; the pointer must remain
      // valid until the next call (either the next row invocation or the
      // postrun hook). Set to NULL by caller before the call.
      //
      // NB: VDF's should not allocate on each call. You can use the user_data
      // field in the vef_prerun_result_t/vef_vdf_args_t/vef_postrun_args_t
      // to store memory across all rows if you need extra allocations.
      char **alt_str_buf;
    };

    // For CUSTOM type: caller-provided buffer for binary result
    struct {
      // Buffer where the binary result should be written
      unsigned char *bin_buf;

      // Size of bin_buf in bytes
      size_t max_bin_len;

      // Callee can return a different pointer than bin_buf. Same semantics
      // as alt_str_buf above.
      unsigned char **alt_bin_buf;

      // protocol >= VEF_PROTOCOL_2
      // Read-only: the extension must not overwrite these parameters.
      vef_type_params_t type_params;
    };

    // For REAL return type
    double real_value;

    // For INT return type
    long long int_value;
  };

  // protocol >= VEF_PROTOCOL_2
  //
  // Optional out-channel for constant-string from_string inference at
  // fix_fields time. NULL on the normal row-time path. When non-NULL AND the
  // extension's parameterized from_string transitions MaybeParams<P> from
  // unknown to known, the SDK wrapper writes the inferred params as a
  // canonical "k=v,k=v" string via the type's registered params_to_strings
  // callback. See vef_inferred_type_params_t for the overflow contract.
  vef_inferred_type_params_t *out_type_params;
} vef_vdf_result_t;

// =============================================================================
// VDF - VillageSQL Defined Function (vef_vdf_func_t)
// =============================================================================
//
// Called for each row. This is the main function that performs the computation.
//
// Function execution order per statement:
//   1. vef_prerun_func_t  - (optional) called once before the first row
//   2. vef_vdf_func_t     - (required) called for each row
//   3. vef_postrun_func_t - (optional) called once after the last row
//
// Extension authors should not have to write prerun/postrun functions
// in most cases. For people familiar with MySQL UDF's, this might sound odd,
// but vef_func_desc_t supports passing information from the extension
// to the framework. This allows the framework to perform work like
// type-checking and buffer allocation without requiring an extra call.
// The C++ SDK allows extension authors to use these features without
// having to understand the low-level ABI defined here.

typedef struct {
  // user_data from prerun_result (NULL if no prerun was called)
  void *user_data;

  // Number of input values
  unsigned int value_count;

  // Check ctx->protocol to determine which union member to read.
  union {
    // protocol == VEF_PROTOCOL_1: flat array of value_count vef_invalue_v1_t.
    vef_invalue_v1_t *values_v1;

    // protocol >= VEF_PROTOCOL_2: array of value_count pointers to
    // vef_invalue_t. Using a pointer array decouples extensions from
    // vef_invalue_t's binary layout, allowing the struct to grow in future
    // protocol versions.
    vef_invalue_t **values;
  };
} vef_vdf_args_t;

typedef struct {
  vef_type_id id;

  // Only set if id == TYPE_CUSTOM
  // Just the type name, without the extension name; must refer to a type
  // defined in this extension.
  const char *custom_type;
} vef_type_t;

typedef struct {
  unsigned int param_count;
  const vef_type_t *params;

  vef_type_t return_type;
} vef_signature_t;

typedef void (*vef_vdf_func_t)(vef_context_t *ctx, vef_vdf_args_t *args,
                               vef_vdf_result_t *result);

// =============================================================================
// Prerun Function (vef_prerun_func_t)
// =============================================================================
//
// Called once before the first row. Use this to:
//   - Allocate resources and store state in result->user_data
//   - Request a specific buffer size via result->result_buffer_size
//
// Memory lifetime: The caller owns all arrays in args (arg_types, const_values,
// const_lengths). The callee must copy any values it needs to retain before
// the function returns. The caller may overwrite or free this memory after the
// function returns.
//
// The user_data pointer allows the extension to maintain state across calls.
// Memory allocated in prerun should be freed in postrun.

typedef struct {
  // Number of arguments that will be passed to each vdf call
  unsigned int arg_count;

  // Type of each argument. Array has arg_count elements.
  vef_type_t *arg_types;

  // For each argument: non-NULL if the argument is a constant, NULL otherwise.
  // If non-NULL, points to the constant's serialized value.
  // Array has arg_count elements.
  char **const_values;

  // Length of each constant value. Only valid where const_values[i] != NULL.
  // Array has arg_count elements.
  size_t *const_lengths;
} vef_prerun_args_t;

typedef struct {
  // Result type: IS_VALUE on success, IS_ERROR on failure
  vef_return_value_type_t type;

  // Caller-provided buffer for error message (size VEF_MAX_ERROR_LEN).
  // Write a null-terminated string here if type == IS_ERROR.
  char *error_msg;

  // Requested result buffer size (0 = use default from type)
  size_t result_buffer_size;

  // Extension-allocated state. Set this to pass data to vdf and postrun.
  // Caller initializes to NULL.
  void *user_data;
} vef_prerun_result_t;

typedef void (*vef_prerun_func_t)(vef_context_t *ctx, vef_prerun_args_t *args,
                                  vef_prerun_result_t *result);

// =============================================================================
// Postrun Function (vef_postrun_func_t)
// =============================================================================
//
// Called once after the last row. Use this to free any resources stored in
// args->user_data.

typedef struct {
  // user_data from prerun_result (for cleanup)
  void *user_data;
} vef_postrun_args_t;

typedef struct {
  // Reserved for future use
} vef_postrun_result_t;

typedef void (*vef_postrun_func_t)(vef_context_t *ctx, vef_postrun_args_t *args,
                                   vef_postrun_result_t *result);

// =============================================================================
// Aggregate Functions
// =============================================================================
//
// When a VDF is registered as an aggregate (by setting both clear and
// accumulate), the main `vdf` function pointer changes role: instead of being
// called per row, it becomes the "result" function called once per group after
// all rows have been accumulated. It should read the final state from
// args->user_data and write the group's output value.

// Reset aggregate state for a new group.
// Called once at the start of each group. The extension should reset any
// accumulator state stored in args->user_data.
typedef void (*vef_vdf_clear_func_t)(vef_context_t *ctx, vef_vdf_args_t *args);

// Accumulate one row into the aggregate.
// Called once per row within a group. The extension reads values from args
// and updates its accumulator in args->user_data. If an error occurs during
// accumulation, write the message to result->error_msg and set
// result->type = VEF_RESULT_ERROR.
typedef void (*vef_vdf_accumulate_func_t)(vef_context_t *ctx,
                                          vef_vdf_args_t *args,
                                          vef_vdf_result_t *result);

// =============================================================================
// Function and Type Descriptors
// =============================================================================

typedef struct {
  // protocol >= VEF_PROTOCOL_1
  vef_protocol_t protocol;

  // Encoded using UTF-8
  const char *name;

  vef_signature_t *signature;

  // Main function pointer. For scalar VDFs this is called once per row. For
  // aggregates (clear and accumulate both set), this becomes the result
  // function, called once per group to produce the final output value.
  vef_vdf_func_t vdf;

  // Optional functions (called once per statement execution)
  vef_prerun_func_t prerun;
  vef_postrun_func_t postrun;

  // Minimum buffer size requested for string results (0 = use default)
  size_t buffer_size;

  // protocol >= VEF_PROTOCOL_2
  // If true, the function always returns the same result for the same inputs
  // and has no side effects. The optimizer may use this to cache results.
  bool deterministic;

  // OPTIONAL: Set both to non-NULL to make this function an aggregate.
  //
  // Aggregate VDFs use three callbacks:
  //   1. clear       – resets accumulator state at the start of each group
  //   2. accumulate  – called once per row to fold it into the accumulator
  //   3. vdf (above) – becomes the "result" function, called once per group
  //                    after all rows have been accumulated; it reads the
  //                    final accumulator state and writes the output value
  //
  // State is managed via user_data in vef_vdf_args_t, typically allocated in
  // prerun and freed in postrun.
  //
  // It is an error to set exactly one of these; both must be present or absent.
  vef_vdf_clear_func_t clear;
  vef_vdf_accumulate_func_t accumulate;
} vef_func_desc_t;

// =============================================================================
// Type Function Signatures (for custom types)
// =============================================================================
//
// These signatures match TypeDescriptor in type_descriptor.h

// Encode: Convert string representation to internal binary representation
// Parameters:
//   buffer      - Output buffer for binary data
//   buffer_size - Size of output buffer
//   from        - Input string data
//   from_len    - Length of input string
//   length      - Output: actual bytes written, or SIZE_MAX to return SQL NULL
// Returns: false on success, true on error
typedef bool (*vef_encode_func_t)(unsigned char *buffer, size_t buffer_size,
                                  const char *from, size_t from_len,
                                  size_t *length);

// Decode: Convert internal binary representation to string representation
// Parameters:
//   buffer      - Input binary data
//   buffer_size - Size of input data
//   to          - Output string buffer
//   to_size     - Size of output buffer
//   to_length   - Output: actual characters written
// Returns: false on success, true on error
typedef bool (*vef_decode_func_t)(const unsigned char *buffer,
                                  size_t buffer_size, char *to, size_t to_size,
                                  size_t *to_length);

// Compare: Compare two values in binary representation
// Parameters:
//   data1, len1 - First value
//   data2, len2 - Second value
// Returns: <0 if data1 < data2, 0 if equal, >0 if data1 > data2
typedef int (*vef_compare_func_t)(const unsigned char *data1, size_t len1,
                                  const unsigned char *data2, size_t len2);

// Hash: Compute hash of a value in binary representation
// Parameters:
//   data, len - Value to hash
// Returns: hash value
typedef size_t (*vef_hash_func_t)(const unsigned char *data, size_t len);

// Maximum length of the serialized "key=value,key=value,..." string used
// by int_to_params and resolve_params VDFs.
#define VEF_MAX_TYPE_PARAMS_STRING_LEN 1024

typedef struct {
  // protocol >= VEF_PROTOCOL_1
  vef_protocol_t protocol;

  // Encoded using UTF-8
  const char *name;

  // Expected size for fixed-length binary storage. While the encode
  // function may return a smaller length (e.g., 0 to indicate error),
  // this defines the standard persisted footprint for the type.
  int64_t persisted_length;

  // Maximum size of the string representation (for decode output buffer)
  int64_t max_decode_buffer_length;

  // REQUIRED
  vef_encode_func_t encode_func;
  vef_decode_func_t decode_func;
  vef_compare_func_t compare_func;

  // OPTIONAL (NULL if not provided)
  vef_hash_func_t hash_func;

  // protocol >= VEF_PROTOCOL_2

  // OPTIONAL: Names of VDFs (from this extension's funcs[]) to use as
  // encode/decode/compare/hash implementations. When set, the named VDF is
  // used instead of the corresponding _func pointer above; exactly one of the
  // two must be set for required operations (encode, decode, compare). The
  // named VDF must have the matching signature:
  //   encode_vdf_name: (STRING) -> CUSTOM(this type)
  //   decode_vdf_name: (CUSTOM(this type)) -> STRING
  //   compare_vdf_name: (CUSTOM(this type), CUSTOM(this type)) -> INT
  //   hash_vdf_name: (CUSTOM(this type)) -> INT
  // The named VDFs are also registered as callable SQL functions.
  const char *encode_vdf_name;
  const char *decode_vdf_name;
  const char *compare_vdf_name;
  const char *hash_vdf_name;  // OPTIONAL (like hash_func)

  // OPTIONAL: Names of VDFs (from this extension's funcs[]) to use as
  // int_to_params/resolve_params implementations. The SDK provides wrapper
  // templates (IntToParamsWrapper, ResolveParamsWrapper) so extension authors
  // write against a clean std::map-based C++ API and the SDK generates
  // VDF-compatible wrappers that handle serialization.
  // The named VDFs must have the matching signature:
  //   int_to_params_vdf_name: (INT) -> STRING
  //   resolve_params_vdf_name: (STRING) -> STRING
  const char *int_to_params_vdf_name;
  const char *resolve_params_vdf_name;

  // OPTIONAL: Name of a VDF (from this extension's funcs[]) that produces the
  // intrinsic default binary value for this type. The named VDF must have
  // signature (INT) -> STRING, where the INT argument is the resolved
  // persisted_length in bytes (allowing variable-size types to produce the
  // correct number of bytes). NULL means the type has no intrinsic default.
  const char *intrinsic_default_vdf_name;

  // OPTIONAL: A string literal to encode as the intrinsic default value.
  // The server runs the type's encode function on this string to produce the
  // binary default. Ignored if intrinsic_default_vdf_name is set. NULL means
  // no string default is provided; the server falls back to encode("").
  // Only used when protocol >= VEF_PROTOCOL_2.
  const char *intrinsic_default_str;

  // Upper bound on persisted_length across all valid parameterizations of
  // this type. Required for parameterized types; ignored for non-parameterized
  // types (where persisted_length already gives the answer). Placed at the
  // end of the struct so that adding it does not shift earlier offsets,
  // preserving binary compatibility for v1 extensions.
  //
  // Used only on the fix_fields-time constant-string inference path: the
  // server doesn't yet know the parameters (those are about to be inferred),
  // so it cannot consult resolve_params to size the encode buffer. It
  // allocates max_persisted_length bytes, runs from_string with
  // MaybeParams<P> unknown, then trims the result to actual_len.
  //
  // Example: for SVECTOR with max dimension 3072, this is
  //   sizeof(vef_storage_ref_t) + 3072 * sizeof(float).
  int64_t max_persisted_length;
} vef_type_desc_t;

// Forward declaration so vef_required_capability_t can reference it.
typedef struct vef_registration_t vef_registration_t;

// A single capability request in vef_registration_t.required_capabilities.
// The extension sets name, vtable_dest, abi_type_hash, and min_version. If the
// capability is registered and passes all server-side checks, the server
// writes the vtable pointer directly to *vtable_dest before vef_register
// returns.
//
// Server-side compatibility logic (ABI hash check, min_version floor, and the
// option to override both per capability) lives in cap_compat_fn in
// capability_registry.h and is not visible to extension authors.
typedef struct {
  // Capability name, e.g. "vsql::preview::ping". Must remain valid for the
  // lifetime of the extension (use a string literal).
  const char *name;
  // Address of the abi-pointer slot inside the extension's capability
  // wrapper. The server writes the vtable pointer here on success. Must
  // remain valid for the lifetime of the extension.
  void **vtable_dest;
  // Compile-time hash of the ABI struct type, computed via
  // villagesql::detail::abi_type_hash<AbiType>(). The server compares this
  // against its own hash for the same name to detect ABI struct mismatches.
  size_t abi_type_hash;
  // Minimum capability ABI version the extension requires. The server reads
  // the version field from its vtable and fails loading if it is less than
  // this value. Set to the VEF_PREVIEW_*_ABI_VERSION constant the extension
  // was compiled against.
  uint32_t min_version;
  // Optional. Capability-specific descriptor supplied by the extension to the
  // server. Its type is capability-specific. NULL for capabilities that do not
  // need it. Must remain valid for the lifetime of the extension.
  const void *extension_data;
  // Compile-time hash of the descriptor struct type pointed to by
  // extension_data, computed via
  // villagesql::detail::abi_type_hash<DescriptorType>(). 0 if extension_data
  // is NULL. The server compares this against its own hash to detect
  // descriptor ABI mismatches.
  size_t descriptor_abi_hash;
} vef_required_capability_t;

typedef struct vef_registration_t {
  // protocol >= VEF_PROTOCOL_1
  vef_protocol_t protocol;

  // If the register call failed, provide a useful debugging message.
  char *error_msg;

  // Deprecated: extension name and version are now sourced from the VEB
  // manifest. These fields exist only to preserve struct layout for v1
  // binary compatibility. New extensions set them to nullptr.
  const char *deprecated_extension_version;
  vef_version_t sdk_version;
  const char *deprecated_extension_name;

  unsigned int func_count;
  vef_func_desc_t **funcs;

  unsigned int type_count;
  vef_type_desc_t **types;

  // protocol >= VEF_PROTOCOL_2
  // Preview capabilities required by this extension. Each entry names a
  // capability the extension needs (e.g. "vsql::ping"). The server populates
  // the capability struct pointed to by each entry before vef_register()
  // returns. If a capability is unavailable or there is an ABI struct
  // mismatch, loading the extension fails with an error.
  unsigned int required_capability_count;
  const vef_required_capability_t *required_capabilities;
} vef_registration_t;

// The returned objects can be freed when the registration is passed to the
// unregister function.
typedef vef_registration_t *(*vef_register_func_t)(
    vef_register_arg_t *const arg);

typedef void (*vef_unregister_func_t)(vef_unregister_arg_t *const arg,
                                      vef_registration_t *registration);

// Expected export names for extension entry points
#define VEF_REGISTER_FUNC_NAME "vef_register"
#define VEF_UNREGISTER_FUNC_NAME "vef_unregister"

#endif  // VILLAGESQL_ABI_TYPES_H_
