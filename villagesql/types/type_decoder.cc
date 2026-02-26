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
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/types/type_op.h"

namespace villagesql {

TypeDecoder::TypeDecoder(const TypeContext &tc, MEM_ROOT &mem_root)
    : mem_root_(&mem_root),
      buffer_size_(static_cast<size_t>(tc.max_decode_buffer_length())) {
  assert(buffer_size_ > 0);

  const DecodeOp &op = tc.descriptor()->decode_op();
  if (op.vdf() != nullptr) {
    vdf_ = op.vdf();
    assert(vdf_->prerun == nullptr && vdf_->postrun == nullptr);
    ctx_.protocol = VEF_PROTOCOL_2;
    input_[0].type = VEF_TYPE_CUSTOM;
    input_[0].is_null = false;
    vdf_args_.user_data = nullptr;
    vdf_args_.value_count = 1;
    vdf_args_.values = input_;
    vdf_result_.error_msg = error_msg_;
    vdf_result_.max_str_len = buffer_size_;
    vdf_result_.alt_str_buf = &alt_str_buf_;
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
  if (vdf_ != nullptr) {
    vdf_result_.str_buf = buffer_;
  }
  return false;
}

bool TypeDecoder::decode(const uchar *data, size_t len, String *out,
                         bool &is_valid) {
  is_valid = true;

  if (vdf_ != nullptr) {
    input_[0].bin_len = len;
    input_[0].bin_value = data;
    vdf_result_.type = VEF_RESULT_VALUE;
    vdf_result_.actual_len = 0;
    alt_str_buf_ = nullptr;

    vdf_->vdf(&ctx_, &vdf_args_, &vdf_result_);

    if (vdf_result_.type != VEF_RESULT_VALUE) {
      // TODO(villagesql-beta): log errors.
      is_valid = false;
      return true;
    }

    if (alt_str_buf_ != nullptr) {
      // TODO(villagesql-beta): support caller supplied buffers.
      return true;
    }
    const size_t actual_len = vdf_result_.actual_len;
    if (actual_len > buffer_size_) {
      return true;
    }

    out->set(buffer_, actual_len, &my_charset_utf8mb4_bin);
  } else {
    // TODO(villagesql-beta): remove raw func pointer
    assert(fn_ != nullptr);
    size_t decoded_length = 0;
    if (fn_(data, len, buffer_, buffer_size_, &decoded_length)) {
      is_valid = false;
      return true;
    }
    // the buffer rather than silently returning invalid.
    if (should_assert_if_false(decoded_length <= buffer_size_)) {
      return true;
    }

    out->set(buffer_, decoded_length, &my_charset_utf8mb4_bin);
  }

  return false;
}

}  // namespace villagesql
