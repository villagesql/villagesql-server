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

#include "villagesql/types/type_decoder.h"

#include <cassert>
#include <cstring>

#include "my_alloc.h"
#include "my_base.h"
#include "my_inttypes.h"
#include "mysqld_error.h"
#include "sql_string.h"
#include "template_utils.h"
#include "villagesql/include/error.h"

namespace villagesql {

TypeDecoder::TypeDecoder(const TypeContext &tc, MEM_ROOT &mem_root)
    : mem_root_(&mem_root),
      buffer_size_(static_cast<size_t>(tc.max_decode_buffer_length())) {
  assert(buffer_size_ > 0);

  const DecodeOp &op = tc.decode_op();
  if (op.vdf() != nullptr) {
    vdf_call_.emplace(op.vdf());
    const auto &params = op.parameters();
    vdf_call_->init(TypeParameterSlice{},
                    TypeParameterSlice(params.count(), params.key_data(),
                                       params.value_data()));
  } else {
    fn_ = op.fn();
  }
}

bool TypeDecoder::Init() {
  buffer_ = new (mem_root_) char[buffer_size_];
  if (!buffer_) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), buffer_size_);
    return true;
  }
  return false;
}

DecodeResult TypeDecoder::decode(const uchar *data, size_t len, String *out) {
  if (vdf_call_.has_value()) {
    auto r = vdf_call_->invoke(BinarySlice{data, len}, buffer_, buffer_size_);
    if (!r) {
      last_error_msg_ = vdf_call_->error_msg();
      return DecodeResult::kInvalidData;
    }

    if (vdf_call_->alt_str_buf() != nullptr) {
      // TODO(villagesql-general): support caller supplied buffers.
      last_error_msg_ = "VDF provided unsupported alternate output buffer";
      return DecodeResult::kExtensionError;
    }

    if (*r > buffer_size_) {
      last_error_msg_ = "decoded length exceeds buffer";
      return DecodeResult::kExtensionError;
    }

    out->set(buffer_, *r, &my_charset_utf8mb4_bin);
  } else {
    // TODO(villagesql-beta): remove raw func pointer
    assert(fn_ != nullptr);
    size_t decoded_length = 0;
    if (fn_(data, len, buffer_, buffer_size_, &decoded_length)) {
      last_error_msg_ = "decode function reported invalid data";
      return DecodeResult::kInvalidData;
    }

    if (should_assert_if_false(decoded_length <= buffer_size_)) {
      last_error_msg_ = "decoded length exceeds buffer";
      return DecodeResult::kExtensionError;
    }

    out->set(buffer_, decoded_length, &my_charset_utf8mb4_bin);
  }

  return DecodeResult::kSuccess;
}

}  // namespace villagesql
