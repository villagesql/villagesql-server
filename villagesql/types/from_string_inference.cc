// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#include "villagesql/types/from_string_inference.h"

#include <cstring>
#include <vector>

#include "my_sys.h"
#include "mysqld_error.h"
#include "sql/derror.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/victionary_client.h"

namespace villagesql {

namespace {

// Default stack-buffer size for the inferred-params canonical-string channel.
// Realistic params canonical strings are well under this (e.g., TVECTOR's
// "dimension=4096,type=double" is ~33 bytes). Overflow falls through to a
// heap-allocated retry; second-attempt overflow is a wrapper bug.
constexpr size_t kStackParamsBufLen = 256;

}  // namespace

bool LookupTypeForInference(std::string_view extension_name,
                            std::string_view type_name,
                            TypeInferenceSnapshot *out) {
  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return true;

  TypeDescriptorKeyPrefix prefix{std::string(type_name),
                                 std::string(extension_name)};

  auto guard = vclient.get_read_lock();
  std::vector<const TypeDescriptor *> results =
      vclient.type_descriptors().get_prefix_committed(prefix);
  if (results.size() != 1) return true;
  out->is_parameterized = results[0]->is_parameterized();
  out->max_persisted_length = results[0]->max_persisted_length();
  return false;
}

bool InferFromStringConstant(THD * /*thd*/, int64_t max_persisted_length,
                             const vef_func_desc_t *encode_vdf,
                             std::string_view input_string,
                             TypeParameters *out_inferred,
                             std::string *out_encoded_bytes) {
  // Pre-conditions: the caller should have already verified that the type is
  // parameterized and that the VDF is the type's encode VDF. We still guard
  // defensively so a bad call doesn't crash.
  if (encode_vdf == nullptr || encode_vdf->vdf == nullptr) {
    return true;
  }
  if (max_persisted_length <= 0) {
    // Type didn't declare max_persisted_length; inference path is disabled
    // for this type. Caller falls through to ambiguity error.
    return true;
  }

  // Buffer for the encoded binary value. Sized to the type's declared upper
  // bound; the wrapper trims via actual_len.
  // TODO(villagesql-performance): we should consider putting this in the thd's
  // mem_root instead and perhaps reusing the buffer, since the
  // max_persisted_length can be relatively large.
  std::vector<unsigned char> bin_buf(static_cast<size_t>(max_persisted_length));

  // Workspace for inferred params (stack first, heap on overflow).
  char stack_params_buf[kStackParamsBufLen];
  vef_inferred_type_params_t inferred_workspace{};
  inferred_workspace.buf = stack_params_buf;
  inferred_workspace.max_buf_len = sizeof(stack_params_buf);

  // Build the single STRING input arg holding the constant literal.
  // TODO(villagesql): change this to use the encoding wrapper.
  vef_invalue_t arg{};
  arg.type = VEF_TYPE_STRING;
  arg.is_null = false;
  arg.str_value = input_string.data();
  arg.str_len = input_string.size();
  vef_invalue_t *arg_ptr = &arg;

  vef_vdf_args_t vdf_args{};
  vdf_args.value_count = 1;
  vdf_args.values = &arg_ptr;

  char err_msg[VEF_MAX_ERROR_LEN] = {0};
  vef_vdf_result_t result{};
  result.type = VEF_RESULT_VALUE;
  result.error_msg = err_msg;
  result.bin_buf = bin_buf.data();
  result.max_bin_len = bin_buf.size();
  // Empty input type_params signals the wrapper to construct MaybeParams<P>
  // in the unknown state.
  result.type_params = {0, nullptr, nullptr};
  result.out_type_params = &inferred_workspace;

  vef_context_t ctx{};
  ctx.protocol = VEF_PROTOCOL_3;

  encode_vdf->vdf(&ctx, &vdf_args, &result);

  // Retry on params-string overflow. The wrapper's actual_len tells us the
  // exact size needed; on the second pass it must fit.
  std::vector<char> heap_params_buf;
  if (inferred_workspace.overflow) {
    heap_params_buf.resize(inferred_workspace.actual_len + 1);
    inferred_workspace.buf = heap_params_buf.data();
    inferred_workspace.max_buf_len = heap_params_buf.size();
    inferred_workspace.actual_len = 0;
    inferred_workspace.overflow = false;
    // Reset result for the second call.
    result.type = VEF_RESULT_VALUE;
    result.actual_len = 0;
    err_msg[0] = '\0';
    // Make a second call, with a larger buffer.
    encode_vdf->vdf(&ctx, &vdf_args, &result);
    if (inferred_workspace.overflow) {
      villagesql_error(
          "internal: inferred params buffer overflow on retry for %s", MYF(0),
          encode_vdf->name);
      return true;
    }
  }

  switch (result.type) {
    case VEF_RESULT_VALUE:
      break;  // proceed below
    case VEF_RESULT_NULL:
      // NULL input → nothing to infer from. Caller's ambiguity path handles
      // this.
      return true;
    case VEF_RESULT_WARNING:
    case VEF_RESULT_ERROR:
      // The literal couldn't be encoded. For constant-string inference at
      // fix_fields, surface this as a fix_fields error regardless of whether
      // the wrapper reported it as a warning or hard error — there is no row
      // to skip and INSERT IGNORE shouldn't silently mask a malformed literal
      // here.
      villagesql_error("error encountered during %s inference: %s", MYF(0),
                       encode_vdf->name,
                       err_msg[0] != '\0' ? err_msg : "unknown error");
      return true;
  }

  // VEF_RESULT_VALUE path. If the wrapper didn't write any inferred params,
  // the extension either has no params_to_strings registered or its
  // from_string did not call p.set(). Either way, we can't bind a
  // TypeContext from this call; fall back to ambiguity.
  if (inferred_workspace.actual_len == 0) {
    return true;
  }

  *out_inferred = TypeParameters(
      std::string(inferred_workspace.buf, inferred_workspace.actual_len));

  const unsigned char *out_bytes =
      result.alt_bin_buf != nullptr ? *result.alt_bin_buf : result.bin_buf;
  out_encoded_bytes->assign(reinterpret_cast<const char *>(out_bytes),
                            result.actual_len);
  return false;
}

}  // namespace villagesql
