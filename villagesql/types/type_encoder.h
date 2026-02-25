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

#ifndef VILLAGESQL_TYPES_TYPE_ENCODER_H_
#define VILLAGESQL_TYPES_TYPE_ENCODER_H_

#include <cstddef>

#include "sql_string.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

struct MEM_ROOT;

namespace villagesql {

// TypeEncoder holds per-Field (or per-Item) encoding state, lazily allocated
// from the owning object's mem_root on first encode and reused for all
// subsequent encodes within the same lifetime.
//
// It pre-allocates a scratch buffer (persisted_length bytes) and a String
// wrapper once, eliminating per-row allocations during INSERT/UPDATE.
//
// For Fields the buffer is owned by TABLE::mem_root and freed when the TABLE
// is evicted. For Items it is owned by thd->mem_root and freed at query end;
// Item::cleanup() nulls the pointer between PS re-executions so the next
// execution lazily re-allocates on the fresh thd->mem_root.
//
// The buffer is a scratch buffer: written by the encode function and
// immediately consumed by the caller (e.g. via field->store()). The caller
// MUST copy the encoded bytes before the next encode() call.
class TypeEncoder {
 public:
  // Construct from mem_root: pre-fills VDF/fn_ state. Does not allocate the
  // encode buffer; call Init() before encode(). mem_root must outlive this.
  TypeEncoder(const TypeContext *tc, MEM_ROOT &mem_root);

  // Allocate the encode buffer from mem_root_. Must be called once before
  // encode(). On OOM calls my_error and returns false.
  bool Init();

  // Disable copy and move: TypeEncoder is tied to its allocated buffer and
  // the pre-filled VDF structs contain internal self-pointers.
  TypeEncoder(const TypeEncoder &) = delete;
  TypeEncoder &operator=(const TypeEncoder &) = delete;
  TypeEncoder(TypeEncoder &&) = delete;
  TypeEncoder &operator=(TypeEncoder &&) = delete;

  // Encode 'from' into the pre-allocated buffer_/result_.
  // Returns the encoded String* on success, nullptr on error (is_valid is
  // false for encoding errors, true for OOM with my_error already called).
  // The returned String* is valid until the next encode() call.
  String *encode(const String &from, bool &is_valid);

 private:
  MEM_ROOT *mem_root_{nullptr};  // owning mem_root, used for overflow growth
  char *buffer_{nullptr};        // pre-allocated from mem_root_
  size_t buffer_size_{0};        // = tc->persisted_length()
  String result_;                // reused String wrapper pointing into buffer_

  // Overflow path: reused when VDF output exceeds buffer_size_ (rare).
  uchar *overflow_buf_{nullptr};
  size_t overflow_buf_size_{0};
  String overflow_result_;

  // fn_ path: function pointer (non-null when not using a VDF)
  vef_encode_func_t fn_{nullptr};

  // VDF path: pre-filled call structures. Constant fields are set once in the
  // constructor; only input_[0].str_len, input_[0].str_value,
  // vdf_result_.type, vdf_result_.actual_len, and alt_bin_buf_ are updated
  // per encode() call.
  const vef_func_desc_t *vdf_{nullptr};
  vef_context_t ctx_{};
  vef_invalue_t input_[1]{};
  vef_vdf_args_t vdf_args_{};
  vef_vdf_result_t vdf_result_{};
  char error_msg_[VEF_MAX_ERROR_LEN]{};
  unsigned char *alt_bin_buf_{nullptr};
};

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_TYPE_ENCODER_H_
