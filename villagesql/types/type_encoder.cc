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

#include "villagesql/types/type_encoder.h"

#include <cassert>
#include <cstring>

#include "my_alloc.h"
#include "my_base.h"
#include "mysqld_error.h"
#include "sql_string.h"
#include "template_utils.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/types/type_op.h"

namespace villagesql {

TypeEncoder::TypeEncoder(const TypeContext *tc, MEM_ROOT &mem_root)
    : mem_root_(&mem_root),
      buffer_size_(static_cast<size_t>(tc->persisted_length())) {
  assert(tc != nullptr);
  assert(buffer_size_ > 0);

  const EncodeOp &op = tc->descriptor()->encode_op();
  if (op.vdf() != nullptr) {
    vdf_ = op.vdf();
    assert(vdf_->prerun == nullptr && vdf_->postrun == nullptr);
    ctx_.protocol = VEF_PROTOCOL_2;
    input_[0].type = VEF_TYPE_STRING;
    input_[0].is_null = false;
    vdf_args_.user_data = nullptr;
    vdf_args_.value_count = 1;
    vdf_args_.values = input_;
    vdf_result_.error_msg = error_msg_;
    vdf_result_.max_bin_len = buffer_size_;
    vdf_result_.alt_bin_buf = &alt_bin_buf_;
  } else {
    fn_ = op.fn();
  }
}

bool TypeEncoder::Init() {
  buffer_ = new (mem_root_) char[buffer_size_];
  if (should_assert_if_null(buffer_)) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), buffer_size_);
    return true;
  }
  if (vdf_ != nullptr) {
    vdf_result_.bin_buf = pointer_cast<uchar *>(buffer_);
  }
  return false;
}

String *TypeEncoder::encode(const String &from, bool &is_valid) {
  is_valid = true;

  if (vdf_ != nullptr) {
    input_[0].str_len = from.length();
    input_[0].str_value = from.ptr();
    vdf_result_.type = VEF_RESULT_VALUE;
    vdf_result_.actual_len = 0;
    alt_bin_buf_ = nullptr;

    vdf_->vdf(&ctx_, &vdf_args_, &vdf_result_);

    if (vdf_result_.type != VEF_RESULT_VALUE) {
      // TODO(villagesql-beta): handle errors
      is_valid = false;
      return nullptr;
    }

    const size_t actual_len = vdf_result_.actual_len;

    if (alt_bin_buf_ != nullptr) {
      // VDF used its own buffer (output exceeded buffer_size_). Grow
      // overflow_buf_ if needed and reuse it across rows.
      if (actual_len > overflow_buf_size_) {
        auto *new_buf = new (mem_root_) uchar[actual_len];
        if (should_assert_if_null(new_buf)) {
          my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), actual_len);
          return nullptr;
        }
        overflow_buf_ = new_buf;
        overflow_buf_size_ = actual_len;
      }
      if (actual_len > 0) memcpy(overflow_buf_, alt_bin_buf_, actual_len);
      overflow_result_.set(pointer_cast<const char *>(overflow_buf_),
                           actual_len, &my_charset_bin);
      return &overflow_result_;
    }

    // TODO(villagesql-beta): report an error or warning when the VDF overruns
    // the buffer rather than silently returning invalid.
    if (should_assert_if_false(actual_len <= buffer_size_)) {
      is_valid = false;
      return nullptr;
    }
    result_.set(buffer_, actual_len, &my_charset_bin);
  } else {
    assert(fn_ != nullptr);
    size_t actual_length = 0;
    if (fn_(pointer_cast<uchar *>(buffer_), buffer_size_, from.ptr(),
            from.length(), &actual_length)) {
      is_valid = false;
      return nullptr;
    }
    // TODO(villagesql-beta): report an error or warning when the fn_ overruns
    // the buffer rather than silently returning invalid.
    if (should_assert_if_false(actual_length <= buffer_size_)) {
      is_valid = false;
      return nullptr;
    }
    result_.set(buffer_, actual_length, &my_charset_bin);
  }

  return &result_;
}

}  // namespace villagesql
