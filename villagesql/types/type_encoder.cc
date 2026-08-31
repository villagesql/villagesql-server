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

namespace villagesql {

TypeEncoder::TypeEncoder(const TypeContext *tc, MEM_ROOT &mem_root)
    : mem_root_(&mem_root),
      // Fixed-length types encode into persisted_length bytes; variable-length
      // types encode into a buffer sized to the type's max_persisted_length
      // upper bound and set the actual length per value.
      buffer_size_(static_cast<size_t>(tc->field_buffer_length())) {
  assert(tc != nullptr);
  assert(buffer_size_ > 0);

  const EncodeOp &op = tc->encode_op();
  if (op.vdf() != nullptr) {
    vdf_call_.emplace(op.vdf());
    const auto &params = op.parameters();
    vdf_call_->init(TypeParameterSlice(params.count(), params.key_data(),
                                       params.value_data()),
                    NoInitData{});
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
  return false;
}

String *TypeEncoder::encode(const String &from, bool &is_valid) {
  is_valid = true;

  if (vdf_call_.has_value()) {
    auto r =
        vdf_call_->invoke(from, pointer_cast<uchar *>(buffer_), buffer_size_);
    if (!r) {
      // TODO(villagesql-general): log warnings for errors
      is_valid = false;
      return nullptr;
    }

    if (vdf_call_->alt_bin_buf() != nullptr) {
      // TODO(villagesql-beta): support the extension returning alternate
      // buffers
      is_valid = false;
      return nullptr;
    }

    // TODO(villagesql-beta): report an error or warning when the VDF overruns
    // the buffer rather than silently returning invalid
    if (*r > buffer_size_) {
      is_valid = false;
      return nullptr;
    }

    result_.set(buffer_, *r, &my_charset_bin);
  } else {
    // TODO(villagesql-beta): Remove suport for these raw functions
    assert(fn_ != nullptr);
    size_t actual_length = 0;
    if (fn_(pointer_cast<uchar *>(buffer_), buffer_size_, from.ptr(),
            from.length(), &actual_length)) {
      is_valid = false;
      return nullptr;
    }
    if (should_assert_if_false(actual_length <= buffer_size_)) {
      is_valid = false;
      return nullptr;
    }
    result_.set(buffer_, actual_length, &my_charset_bin);
  }

  return &result_;
}

}  // namespace villagesql
