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
#include "villagesql/types/special_vdf_call.h"

namespace villagesql {

int CompareOp::invoke(const unsigned char *data1, size_t len1,
                      const unsigned char *data2, size_t len2) const {
  if (fn_ != nullptr) {
    return fn_(data1, len1, data2, len2);
  }

  SpecialVdfCall<IntResult, CustomArg, CustomArg> call(vdf_);
  call.init();
  auto result = call.invoke(BinarySlice{data1, len1}, BinarySlice{data2, len2});
  if (!result) {
    LogVSQL(ERROR_LEVEL, "compare VDF '%s' returned error: %s", call.name(),
            call.error_msg());
    return 0;
  }
  return static_cast<int>(*result);
}

size_t HashOp::invoke(const unsigned char *data, size_t len) const {
  if (fn_ != nullptr) {
    return fn_(data, len);
  }

  SpecialVdfCall<IntResult, CustomArg> call(vdf_);
  call.init();
  auto result = call.invoke(BinarySlice{data, len});
  if (!result) {
    LogVSQL(ERROR_LEVEL, "hash VDF '%s' returned error: %s", call.name(),
            call.error_msg());
    return 0;
  }
  return static_cast<size_t>(*result);
}

bool IntToParamsOp::invoke(int64_t value, std::string *result,
                           char *error_msg) const {
  char str_buffer[VEF_MAX_TYPE_PARAMS_STRING_LEN];
  SpecialVdfCall<StringResult, IntArg> call(vdf_);
  call.init();
  auto r = call.invoke(value, str_buffer, sizeof(str_buffer));
  if (!r) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "%s", call.error_msg());
    return true;
  }

  const char *result_data =
      call.alt_str_buf() != nullptr ? call.alt_str_buf() : str_buffer;
  *result = std::string(result_data, *r);
  return false;
}

bool ResolveParamsOp::invoke(const std::string &params_str,
                             ResolvedTypeParams *result,
                             char *error_msg) const {
  char str_buffer[VEF_MAX_TYPE_PARAMS_STRING_LEN];
  SpecialVdfCall<StringResult, StringArg> call(vdf_);
  call.init();
  auto r = call.invoke(StringSlice{params_str.c_str(), params_str.size()},
                       str_buffer, sizeof(str_buffer));
  if (!r) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "%s", call.error_msg());
    return true;
  }

  const char *result_data =
      call.alt_str_buf() != nullptr ? call.alt_str_buf() : str_buffer;
  std::string output(result_data, *r);

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
  SpecialVdfCall<BinaryResult, IntArg> call(vdf_);
  call.init();
  auto r = call.invoke(buffer_size, buffer, static_cast<size_t>(buffer_size));
  if (!r) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "%s", call.error_msg());
    return true;
  }

  *length = *r;
  return false;
}

}  // namespace villagesql
