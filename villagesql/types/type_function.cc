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

#include "villagesql/types/type_function.h"

#include <cstdlib>
#include <cstring>
#include <string>

#include "villagesql/types/special_vdf_call.h"

namespace villagesql {

bool IntToParamsFunction::invoke(int64_t value, std::string *result,
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

bool ResolveParamsFunction::invoke(const std::string &params_str,
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

bool IntrinsicDefaultFunction::invoke(const vef_type_params_t &type_params,
                                      unsigned char *buffer, size_t buffer_size,
                                      size_t *length, char *error_msg) const {
  SpecialVdfCall<CustomResult> call(vdf_);
  call.init(TypeParameterSlice(type_params.count, type_params.keys,
                               type_params.values));
  auto r = call.invoke(buffer, buffer_size);
  if (!r) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "%s", call.error_msg());
    return true;
  }

  *length = *r;
  return false;
}

}  // namespace villagesql
