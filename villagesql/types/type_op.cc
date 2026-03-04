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

#include "villagesql/types/type_op.h"

#include <cstdlib>
#include <cstring>
#include <string>

#include "my_alloc.h"
#include "my_base.h"
#include "mysqld_error.h"
#include "sql_string.h"
#include "template_utils.h"
#include "villagesql/include/error.h"

namespace villagesql {

bool DecodeOp::invoke(const unsigned char *data, size_t len,
                      int64_t buffer_hint, MEM_ROOT &mem_root,
                      String *output_buffer, bool &is_valid) const {
  assert(output_buffer);
  is_valid = true;

  if (vdf_ != nullptr) {
    const vef_func_desc_t *fd = vdf_;
    assert(fd->prerun == nullptr && fd->postrun == nullptr);
    vef_context_t ctx = {VEF_PROTOCOL_2};
    const size_t buffer_size = static_cast<size_t>(buffer_hint);

    char *buffer = new (&mem_root) char[buffer_size];
    if (should_assert_if_null(buffer)) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), buffer_size);
      return true;
    }

    vef_invalue_t input[1];
    input[0].type = VEF_TYPE_CUSTOM;
    input[0].is_null = false;
    input[0].bin_len = len;
    input[0].bin_value = data;

    vef_vdf_args_t vdf_args = {nullptr, 1, input};

    char error_msg[VEF_MAX_ERROR_LEN] = {};
    char *alt_str_buf = nullptr;
    vef_vdf_result_t vdf_result = {};
    vdf_result.type = VEF_RESULT_VALUE;
    vdf_result.error_msg = error_msg;
    vdf_result.str_buf = buffer;
    vdf_result.max_str_len = buffer_size;
    vdf_result.actual_len = 0;
    vdf_result.alt_str_buf = &alt_str_buf;

    fd->vdf(&ctx, &vdf_args, &vdf_result);

    if (vdf_result.type != VEF_RESULT_VALUE) {
      is_valid = false;
      return true;
    }

    const char *result_data = (alt_str_buf != nullptr) ? alt_str_buf : buffer;
    const size_t decoded_length = vdf_result.actual_len;
    if (alt_str_buf != nullptr) {
      buffer = new (&mem_root) char[decoded_length];
      if (should_assert_if_null(buffer)) {
        my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), decoded_length);
        return true;
      }
      memcpy(buffer, result_data, decoded_length);
      result_data = buffer;
    }

    output_buffer->set(result_data, decoded_length, &my_charset_utf8mb4_bin);
    return false;
  }

  assert(fn_ != nullptr);
  assert(buffer_hint > 0);
  char *buffer = new (&mem_root) char[buffer_hint];
  if (should_assert_if_null(buffer)) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), buffer_hint);
    return true;
  }

  size_t decoded_length = 0;
  if (fn_(data, len, buffer, buffer_hint, &decoded_length)) {
    is_valid = false;
    return true;
  }

  output_buffer->set(buffer, decoded_length, &my_charset_utf8mb4_bin);
  return false;
}

int CompareOp::invoke(const unsigned char *data1, size_t len1,
                      const unsigned char *data2, size_t len2) const {
  if (fn_ != nullptr) {
    return fn_(data1, len1, data2, len2);
  }

  assert(vdf_ != nullptr);
  const vef_func_desc_t *fd = vdf_;
  assert(fd->prerun == nullptr && fd->postrun == nullptr);
  vef_context_t ctx = {VEF_PROTOCOL_2};

  vef_invalue_t inputs[2];
  inputs[0].type = VEF_TYPE_CUSTOM;
  inputs[0].is_null = false;
  inputs[0].bin_len = len1;
  inputs[0].bin_value = data1;
  inputs[1].type = VEF_TYPE_CUSTOM;
  inputs[1].is_null = false;
  inputs[1].bin_len = len2;
  inputs[1].bin_value = data2;

  vef_vdf_args_t vdf_args = {nullptr, 2, inputs};

  char error_msg[VEF_MAX_ERROR_LEN] = {};
  vef_vdf_result_t vdf_result = {};
  vdf_result.type = VEF_RESULT_VALUE;
  vdf_result.error_msg = error_msg;
  vdf_result.int_value = 0;

  fd->vdf(&ctx, &vdf_args, &vdf_result);

  if (vdf_result.type != VEF_RESULT_VALUE) {
    LogVSQL(ERROR_LEVEL, "compare VDF '%s' returned error: %s", fd->name,
            error_msg);
    return 0;
  }

  return static_cast<int>(vdf_result.int_value);
}

size_t HashOp::invoke(const unsigned char *data, size_t len) const {
  if (fn_ != nullptr) {
    return fn_(data, len);
  }

  assert(vdf_ != nullptr);
  const vef_func_desc_t *fd = vdf_;
  assert(fd->prerun == nullptr && fd->postrun == nullptr);
  vef_context_t ctx = {VEF_PROTOCOL_2};

  vef_invalue_t input[1];
  input[0].type = VEF_TYPE_CUSTOM;
  input[0].is_null = false;
  input[0].bin_len = len;
  input[0].bin_value = data;

  vef_vdf_args_t vdf_args = {nullptr, 1, input};

  char error_msg[VEF_MAX_ERROR_LEN] = {};
  vef_vdf_result_t vdf_result = {};
  vdf_result.type = VEF_RESULT_VALUE;
  vdf_result.error_msg = error_msg;
  vdf_result.int_value = 0;

  fd->vdf(&ctx, &vdf_args, &vdf_result);

  if (vdf_result.type != VEF_RESULT_VALUE) {
    LogVSQL(ERROR_LEVEL, "hash VDF '%s' returned error: %s", fd->name,
            error_msg);
    return 0;
  }

  return static_cast<size_t>(vdf_result.int_value);
}

bool IntToParamsOp::invoke(int64_t value, std::string *result,
                           char *error_msg) const {
  assert(vdf_ != nullptr);
  const vef_func_desc_t *fd = vdf_;
  assert(fd->prerun == nullptr && fd->postrun == nullptr);
  vef_context_t ctx = {VEF_PROTOCOL_2};

  vef_invalue_t input[1];
  input[0].type = VEF_TYPE_INT;
  input[0].is_null = false;
  input[0].int_value = value;

  vef_vdf_args_t vdf_args = {nullptr, 1, input};

  char str_buffer[VEF_MAX_TYPE_PARAMS_STRING_LEN];
  char *alt_str_buf = nullptr;
  vef_vdf_result_t vdf_result = {};
  vdf_result.type = VEF_RESULT_VALUE;
  vdf_result.error_msg = error_msg;
  vdf_result.str_buf = str_buffer;
  vdf_result.max_str_len = sizeof(str_buffer);
  vdf_result.actual_len = 0;
  vdf_result.alt_str_buf = &alt_str_buf;

  fd->vdf(&ctx, &vdf_args, &vdf_result);

  if (vdf_result.type == VEF_RESULT_ERROR) {
    return true;
  }
  if (vdf_result.type != VEF_RESULT_VALUE) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "int_to_params VDF returned unexpected result type");
    return true;
  }

  const char *result_data = (alt_str_buf != nullptr) ? alt_str_buf : str_buffer;
  *result = std::string(result_data, vdf_result.actual_len);
  return false;
}

bool ResolveParamsOp::invoke(const std::string &params_str,
                             ResolvedTypeParams *result,
                             char *error_msg) const {
  assert(vdf_ != nullptr);
  const vef_func_desc_t *fd = vdf_;
  assert(fd->prerun == nullptr && fd->postrun == nullptr);
  vef_context_t ctx = {VEF_PROTOCOL_2};

  vef_invalue_t input[1];
  input[0].type = VEF_TYPE_STRING;
  input[0].is_null = false;
  input[0].str_len = params_str.size();
  input[0].str_value = params_str.c_str();

  vef_vdf_args_t vdf_args = {nullptr, 1, input};

  char str_buffer[VEF_MAX_TYPE_PARAMS_STRING_LEN];
  char *alt_str_buf = nullptr;
  vef_vdf_result_t vdf_result = {};
  vdf_result.type = VEF_RESULT_VALUE;
  vdf_result.error_msg = error_msg;
  vdf_result.str_buf = str_buffer;
  vdf_result.max_str_len = sizeof(str_buffer);
  vdf_result.actual_len = 0;
  vdf_result.alt_str_buf = &alt_str_buf;

  fd->vdf(&ctx, &vdf_args, &vdf_result);

  if (vdf_result.type == VEF_RESULT_ERROR) {
    return true;
  }
  if (vdf_result.type != VEF_RESULT_VALUE) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params VDF returned unexpected result type");
    return true;
  }

  const char *result_data = (alt_str_buf != nullptr) ? alt_str_buf : str_buffer;
  std::string output(result_data, vdf_result.actual_len);

  // Parse "persisted_length,max_decode_buffer_length"
  size_t comma = output.find(',');
  if (comma == std::string::npos) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params VDF returned invalid format: expected "
             "'persisted_length,max_decode_buffer_length'");
    return true;
  }
  char *endptr = nullptr;
  result->persisted_length = strtoll(output.c_str(), &endptr, 10);
  if (endptr != output.c_str() + comma) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params VDF returned invalid persisted_length");
    return true;
  }
  result->max_decode_buffer_length =
      strtoll(output.c_str() + comma + 1, &endptr, 10);
  if (*endptr != '\0') {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params VDF returned invalid max_decode_buffer_length");
    return true;
  }

  return false;
}

bool IntrinsicDefaultOp::invoke(int64_t buffer_size, unsigned char *buffer,
                                size_t *length, char *error_msg) const {
  assert(vdf_ != nullptr);
  const vef_func_desc_t *fd = vdf_;
  assert(fd->prerun == nullptr && fd->postrun == nullptr);
  vef_context_t ctx = {VEF_PROTOCOL_2};

  vef_invalue_t input[1];
  input[0].type = VEF_TYPE_INT;
  input[0].is_null = false;
  input[0].int_value = buffer_size;

  vef_vdf_args_t vdf_args = {nullptr, 1, input};

  vef_vdf_result_t vdf_result = {};
  vdf_result.type = VEF_RESULT_VALUE;
  vdf_result.error_msg = error_msg;
  vdf_result.bin_buf = buffer;
  vdf_result.max_bin_len = static_cast<size_t>(buffer_size);
  vdf_result.actual_len = 0;

  fd->vdf(&ctx, &vdf_args, &vdf_result);

  if (vdf_result.type == VEF_RESULT_ERROR) {
    return true;
  }
  if (vdf_result.type != VEF_RESULT_VALUE) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "intrinsic_default VDF returned unexpected result type");
    return true;
  }

  *length = vdf_result.actual_len;
  return false;
}

}  // namespace villagesql
