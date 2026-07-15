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
#include <string_view>

#include "villagesql/types/special_vdf_call.h"

namespace villagesql {

namespace {

// Parses the optional rewritten-params section that mutating resolve_params VDF
// appends after peristed_length,max_decode_buffer_length fields:
// "<byte-len>[,key=value,...]". Section is the text following the ',' that
// introduces it, and points into a NUL-terminated buffer. On success writes
// the params string to *params (empty when the length is 0) and returns false;
// on malformed input writes error_msg and returns true.
bool parse_rewritten_params(std::string_view section, std::string *params,
                            char *error_msg) {
  char *len_end = nullptr;
  long long params_len = strtoll(section.data(), &len_end, 10);
  if (len_end == section.data() || params_len < 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params VDF returned invalid rewritten params length");
    return true;
  }

  params->clear();
  if (params_len == 0) return false;

  size_t consumed = static_cast<size_t>(len_end - section.data());
  if (*len_end != ',') {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params VDF returned malformed rewritten params");
    return true;
  }
  consumed++;  // the ',' separating the length from the params bytes
  if (section.size() - consumed < static_cast<size_t>(params_len)) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params VDF returned rewritten params shorter than its "
             "declared length");
    return true;
  }
  params->assign(section.data() + consumed, static_cast<size_t>(params_len));
  return false;
}

}  // namespace

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
                                   ResolvedTypeParams *result, char *error_msg,
                                   std::string *rewritten_params) const {
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

  // Parse "persisted_length,max_decode_buffer_length" with an optional trailing
  // rewritten-params section from the mutating resolve_params overload:
  //   "persisted_length,max_decode_buffer_length,<byte-len>[,key=value,...]"
  // The byte length makes the params section self-delimiting, so it reads
  // exactly that many bytes and further trailing fields could be added after
  // them without ambiguity.
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
  // endptr now points at the terminator: '\0' for the two-field (const) form,
  // or ',' introducing the rewritten-params section for the mutating overload.
  if (*endptr != '\0' && *endptr != ',') {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params VDF returned invalid max_decode_buffer_length");
    return true;
  }
  // max_decode_buffer_length sizes the decode scratch buffer, so it must be
  // positive for every parameterization, regardless of the type's length kind.
  if (result->max_decode_buffer_length <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params resolved a non-positive max_decode_buffer_length "
             "(%lld); it must be > 0",
             static_cast<long long>(result->max_decode_buffer_length));
    return true;
  }
  if (*endptr == '\0') return false;

  // Rewritten-params section from the mutating overload:
  // "<byte-len>[,key=value,...]", starting just past this comma.
  std::string_view section(
      endptr + 1,
      output.size() - static_cast<size_t>(endptr + 1 - output.c_str()));
  std::string params;
  if (parse_rewritten_params(section, &params, error_msg)) return true;
  if (rewritten_params != nullptr) *rewritten_params = std::move(params);

  return false;
}

bool IntrinsicDefaultFunction::invoke(const vef_type_params_t &type_params,
                                      std::string *result,
                                      char *error_msg) const {
  char str_buffer[VEF_MAX_TYPE_PARAMS_STRING_LEN];
  SpecialVdfCall<StringResult> call(vdf_);
  call.init(TypeParameterSlice(type_params.count, type_params.keys,
                               type_params.values));
  auto r = call.invoke(str_buffer, sizeof(str_buffer));
  if (!r) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "%s", call.error_msg());
    return true;
  }

  const char *result_data =
      call.alt_str_buf() != nullptr ? call.alt_str_buf() : str_buffer;
  *result = std::string(result_data, *r);
  return false;
}

}  // namespace villagesql
