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

#ifndef VILLAGESQL_TYPES_TYPE_DECODER_H_
#define VILLAGESQL_TYPES_TYPE_DECODER_H_

#include <cstddef>
#include <optional>

#include "my_inttypes.h"
#include "sql_string.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/types/special_vdf_call.h"

struct MEM_ROOT;

namespace villagesql {

// TypeDecoder holds per-Field (or per-Item) decoding state, lazily allocated
// from the owning object's mem_root on first decode and reused for all
// subsequent decodes within the same lifetime.
//
// It pre-allocates a scratch buffer (max_decode_buffer_length bytes),
// eliminating per-row allocations during SELECT.
//
// For Fields the buffer is owned by TABLE_SHARE::mem_root and freed when the
// TABLE_SHARE is evicted. For Items it is owned by thd->mem_root and freed at
// query end; Item::cleanup() nulls the pointer between PS re-executions so the
// next execution lazily re-allocates on the fresh thd->mem_root.
//
// The buffer is a scratch buffer: decode() writes into it and sets the
// caller-provided String to point at it. The caller MUST NOT hold the String
// past the next decode() call.
class TypeDecoder {
 public:
  // Construct from mem_root: pre-fills VDF/fn_ state. Does not allocate the
  // decode buffer; call Init() before decode(). mem_root must outlive this.
  TypeDecoder(const TypeContext &tc, MEM_ROOT &mem_root);

  // Allocate the decode buffer from mem_root_. Must be called once before
  // decode(). On OOM calls my_error and returns true.
  bool Init();

  // Disable copy and move: TypeDecoder is tied to its allocated buffer and
  // the pre-filled VDF structs contain internal self-pointers.
  TypeDecoder(const TypeDecoder &) = delete;
  TypeDecoder &operator=(const TypeDecoder &) = delete;
  TypeDecoder(TypeDecoder &&) = delete;
  TypeDecoder &operator=(TypeDecoder &&) = delete;

  // Decode binary data in [data, data+len) into the pre-allocated buffer_ and
  // set out to point at the result. Returns true on success, false on error
  // (is_valid is false for invalid data, true for OOM with my_error already
  // called). out is valid until the next decode() call.
  bool decode(const uchar *data, size_t len, String *out, bool &is_valid);

 private:
  MEM_ROOT *mem_root_{nullptr};  // owning mem_root used for buffers
  char *buffer_{nullptr};        // pre-allocated from mem_root_
  size_t buffer_size_{0};        // = tc->max_decode_buffer_length()

  // fn_ path: function pointer (non-null when not using a VDF)
  vef_decode_func_t fn_{nullptr};

  // VDF path: SpecialVdfCall owns ctx/inputs/vdf_args, error_msg, and alt_buf.
  // The output buffer (buffer_) is passed per decode() call.
  std::optional<SpecialVdfCall<StringResult, CustomArg>> vdf_call_{};
};

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_TYPE_DECODER_H_
