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

#include "villagesql/vdf/vdf_handler.h"

#include <type_traits>

#include "lex_string.h"
#include "my_sys.h"
#include "mysql/strings/m_ctype.h"
#include "sql/current_thd.h"
#include "sql/derror.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_udf.h"
#include "sql_string.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/types/from_string_inference.h"
#include "villagesql/types/util.h"

namespace villagesql {
namespace vdf {

// Returns true when `arg` is an input the from_string-inference path can
// safely pre-execute at fix_fields time: a constant-for-execution Item whose
// val_str() has no side effects. Today this covers string literals and
// read-only user variables (e.g. @v after `SET @v = '[...]'`).
static bool is_inferrable_string_const(Item *arg) {
  if (!arg->const_for_execution()) return false;
  if (arg->type() == Item::STRING_ITEM) return true;
  if (arg->type() == Item::FUNC_ITEM &&
      down_cast<Item_func *>(arg)->functype() == Item_func::GUSERVAR_FUNC) {
    return true;
  }
  return false;
}

// Default result-buffer size for STRING/CUSTOM returns when the extension does
// not declare its own buffer_size. Most JSON/text results fit in 1 KiB, so
// this avoids a reallocation in the common case.
static constexpr size_t kDefaultResultBufferSize = 1024;

// Result-buffer growth quantum. Reallocations round up to a multiple of this
// so a value that creeps larger row-over-row settles after one grow rather
// than reallocating repeatedly. Must be a power of two.
static constexpr size_t kResultBufferQuantum = 1024;

static size_t RoundUpResultBuffer(size_t needed) {
  return (needed + (kResultBufferQuantum - 1)) & ~(kResultBufferQuantum - 1);
}

vdf_handler::vdf_handler(udf_func *u_d) : m_udf(u_d) {}

bool vdf_handler::returns_string() const {
  return m_udf && m_udf->vdf_func_desc &&
         m_udf->vdf_func_desc->signature->return_type.id == VEF_TYPE_STRING;
}

bool vdf_handler::MaybeResizeBuffer(size_t needed) {
  if (needed <= m_result_buffer_size) return false;
  const size_t new_size = RoundUpResultBuffer(needed);
  char *buf = pointer_cast<char *>((*THR_MALLOC)->Alloc(new_size));
  if (buf == nullptr) return true;
  m_result_buffer = buf;
  m_result_buffer_size = new_size;
  return false;
}

bool vdf_handler::fix_fields(THD *thd [[maybe_unused]],
                             Item_result_field *func [[maybe_unused]],
                             uint arg_count, Item **arguments,
                             String *buffers) {
  m_args = arguments;
  m_buffers = buffers;
  m_arg_count = arg_count;

  // Allocate invalues array for argument marshaling
  if (arg_count > 0) {
    if (m_udf->vdf_protocol >= VEF_PROTOCOL_3) {
      m_invalues = pointer_cast<vef_invalue_t *>(
          (*THR_MALLOC)->Alloc(sizeof(vef_invalue_t) * arg_count));
      if (!m_invalues) return true;
      m_invalues_ptrs = pointer_cast<vef_invalue_t **>(
          (*THR_MALLOC)->Alloc(sizeof(vef_invalue_t *) * arg_count));
      if (!m_invalues_ptrs) return true;
      for (uint i = 0; i < arg_count; i++) {
        m_invalues_ptrs[i] = &m_invalues[i];
      }
    } else {
      m_invalues_v1 = pointer_cast<vef_invalue_v1_t *>(
          (*THR_MALLOC)->Alloc(sizeof(vef_invalue_v1_t) * arg_count));
      if (!m_invalues_v1) return true;
    }
  }

  // Allocate error message buffer (always needed)
  m_error_msg = pointer_cast<char *>((*THR_MALLOC)->Alloc(VEF_MAX_ERROR_LEN));
  if (!m_error_msg) return true;
  m_error_msg[0] = '\0';

  // Allocate result buffer only for types that need it (STRING/CUSTOM)
  const vef_type_id return_type =
      m_udf->vdf_func_desc->signature->return_type.id;
  if (return_type == VEF_TYPE_STRING || return_type == VEF_TYPE_CUSTOM) {
    size_t needed = m_udf->vdf_func_desc->buffer_size > 0
                        ? m_udf->vdf_func_desc->buffer_size
                        : kDefaultResultBufferSize;
    // STRING-returning VDFs whose argument is a custom type (e.g. the type's
    // to_string / decode VDF) emit output whose size scales with the input
    // value, bounded by max_decode_buffer_length on the argument's
    // TypeContext.  Raise the buffer to fit so that, for example,
    // SVECTOR::to_string(v) has room to decode a wide vector.  prerun can
    // still grow the buffer further below if it requests more, and val_str
    // grows it at row time if the VDF reports its output did not fit.
    if (return_type == VEF_TYPE_STRING) {
      for (uint i = 0; i < arg_count; i++) {
        const auto *tc = m_args[i]->get_type_context();
        if (tc == nullptr) continue;
        needed = std::max(needed,
                          static_cast<size_t>(tc->max_decode_buffer_length()));
      }
    }
    if (MaybeResizeBuffer(needed)) return true;
  }

  m_context.protocol = m_udf->vdf_protocol;
  m_vdf_args.user_data = nullptr;
  m_vdf_args.value_count = static_cast<int>(arg_count);
  if (m_udf->vdf_protocol >= VEF_PROTOCOL_3) {
    m_vdf_args.values = m_invalues_ptrs;
  } else {
    m_vdf_args.values_v1 = m_invalues_v1;
  }

  // Validate and convert VDF arguments (custom type handling).
  // We resolve unknown type params from sibling args by default; we then infer
  // return type params from the args, as written into return_params.
  const vef_signature_t *signature = m_udf->vdf_func_desc->signature;
  villagesql::TypeParameters return_params;
  if (signature != nullptr &&
      villagesql::ValidateAndConvertVDFArguments(
          thd, m_udf->name.str, to_string_view(m_udf->extension_name),
          arg_count, m_args, signature, &return_params)) {
    return true;
  }

  // Constant-string from_string inference.
  //
  // If the return type's params are still unknown after type disambiguation
  // rules and the call matches `<TYPE>::from_string(<string-const>)` with a
  // deterministic VDF, we pre-execute the encode VDF here to learn the params
  // from the input string. The SDK wrapper writes the inferred canonical
  // params back through vef_inferred_type_params_t; we bind them to
  // return_params so SetVDFReturnTypeContext below populates the outer call's
  // return TypeContext. The VDF re-runs at row time on the original argument,
  // this time with known params, and encodes normally.
  //
  // "String const" here covers string literals and user variables — anything
  // is_inferrable_string_const() accepts.
  // The deterministic flag only exists as of VEF_PROTOCOL_3; older descriptors
  // leave that byte uninitialized, so guard the read on the protocol version.
  if (signature != nullptr && return_params.empty() &&
      signature->return_type.id == VEF_TYPE_CUSTOM &&
      m_udf->vdf_func_desc->protocol >= VEF_PROTOCOL_3 &&
      m_udf->vdf_func_desc->deterministic && arg_count == 1 &&
      is_inferrable_string_const(arguments[0])) {
    TypeInferenceSnapshot snap;
    if (!LookupTypeForInference(to_string_view(m_udf->extension_name),
                                signature->return_type.custom_type, &snap) &&
        snap.is_parameterized && snap.max_persisted_length > 0) {
      String tmp;
      String *val = arguments[0]->val_str(&tmp);
      if (val != nullptr && !arguments[0]->null_value) {
        TypeParameters inferred;
        // We will discard the encoded bytes when we infer the parameters so as
        // not to handle this as a special case when the data would have
        // encoded. That is, the buffer presented then would have pre-encoded
        // data, and thus try to encode it again, resulting in corrupted data.
        // The cost of this simpler code execution flow is a single call to the
        // encode function.
        std::string encoded_bytes;
        if (!villagesql::InferFromStringConstant(
                thd, snap.max_persisted_length, m_udf->vdf_func_desc,
                std::string_view(val->ptr(), val->length()), &inferred,
                &encoded_bytes)) {
          return_params = std::move(inferred);
        } else if (current_thd->is_error()) {
          // InferFromStringConstant surfaced a real error (malformed literal,
          // wrapper bug, etc.).
          return true;
        }
        // Else: inference produced no params (e.g., type didn't register
        // params_to_strings). Fall through; SetVDFReturnTypeContext below
        // will receive an empty return_params, and downstream code will
        // surface the existing ambiguity error if needed.
      }
    }
  }

  // Call prerun if present
  if (m_udf->vdf_func_desc->prerun) {
    vef_prerun_args_t prerun_args{};
    prerun_args.arg_count = arg_count;
    prerun_args.const_values = nullptr;
    prerun_args.const_lengths = nullptr;

    // Populate arg_types from Item types
    vef_type_t *arg_types = nullptr;
    if (arg_count > 0) {
      arg_types = pointer_cast<vef_type_t *>(
          (*THR_MALLOC)->Alloc(sizeof(vef_type_t) * arg_count));
      if (!arg_types) return true;
      for (uint i = 0; i < arg_count; i++) {
        auto *tc = m_args[i]->get_type_context();
        if (tc != nullptr) {
          arg_types[i].id = VEF_TYPE_CUSTOM;
          arg_types[i].custom_type = tc->type_name().c_str();
        } else {
          switch (m_args[i]->result_type()) {
            case REAL_RESULT:
              arg_types[i].id = VEF_TYPE_REAL;
              break;
            case INT_RESULT:
              arg_types[i].id = VEF_TYPE_INT;
              break;
            default:
              arg_types[i].id = VEF_TYPE_STRING;
              break;
          }
          arg_types[i].custom_type = nullptr;
        }
      }
    }
    prerun_args.arg_types = arg_types;

    char error_msg[VEF_MAX_ERROR_LEN] = {0};
    vef_prerun_result_t prerun_result{};
    prerun_result.type = VEF_RESULT_VALUE;
    prerun_result.error_msg = error_msg;
    prerun_result.result_buffer_size = 0;
    prerun_result.user_data = nullptr;

    m_udf->vdf_func_desc->prerun(&m_context, &prerun_args, &prerun_result);

    if (prerun_result.type == VEF_RESULT_WARNING ||
        prerun_result.type == VEF_RESULT_ERROR) {
      my_error(ER_CANT_INITIALIZE_UDF, MYF(0), m_udf->name.str,
               error_msg[0] ? error_msg : "prerun failed");
      return true;
    }

    // Store user_data for subsequent calls
    m_vdf_args.user_data = prerun_result.user_data;

    // Handle buffer size request
    if (MaybeResizeBuffer(prerun_result.result_buffer_size)) return true;
  }

  // Set return type_context if this VDF returns a custom type. Pass the
  // return_params inferred from args via the call to
  // ValidateAndConvertVDFArguments.
  if (signature != nullptr && signature->return_type.id == VEF_TYPE_CUSTOM) {
    villagesql::SetVDFReturnTypeContext(thd,
                                        to_string_view(m_udf->extension_name),
                                        signature, func, &return_params);
    m_return_type_context = func->get_type_context();

    // CUSTOM-returning VDFs (e.g. the type's from_string / encode VDF) emit
    // output that fits the resolved return type's field. For a
    // fixed-length or parameter-resolved type that is persisted_length, unknown
    // until SetVDFReturnTypeContext above resolves it from return_params; for a
    // variable-length type it is the type's max_persisted_length upper bound.
    // field_buffer_length() returns the right one. Grow the buffer to fit so
    // that, for example, SVECTOR::from_string('[…1024 floats…]') has room to
    // encode a wide vector. Mirrors the input-side growth done above for
    // STRING-returning VDFs. prerun may still have grown the buffer further;
    // keep the max.
    if (m_return_type_context != nullptr) {
      const int64_t buffer_len = m_return_type_context->field_buffer_length();
      assert(!m_return_type_context->is_variable_length() || buffer_len > 0);
      if (buffer_len > 0 &&
          MaybeResizeBuffer(static_cast<size_t>(buffer_len))) {
        return true;
      }
    }
  }

  m_active = true;
  return false;
}

void vdf_handler::clear() {
  m_udf->vdf_func_desc->clear(&m_context, &m_vdf_args);
}

void vdf_handler::accumulate(bool *null_value) {
  marshal_args();
  vef_vdf_result_t result{};
  result.type = VEF_RESULT_VALUE;
  m_error_msg[0] = '\0';
  result.error_msg = m_error_msg;
  m_udf->vdf_func_desc->accumulate(&m_context, &m_vdf_args, &result);
  switch (result.type) {
    case VEF_RESULT_VALUE:
      *null_value = false;
      return;
    case VEF_RESULT_WARNING:
      push_warning_printf(
          current_thd, Sql_condition::SL_WARNING, ER_UDF_ERROR,
          "VDF error in function '%s': %s", m_udf->name.str,
          m_error_msg[0] != '\0' ? m_error_msg : "unknown error");
      *null_value = true;
      return;
    case VEF_RESULT_ERROR:
      my_printf_error(ER_UDF_ERROR, "VDF error in function '%s': %s", MYF(0),
                      m_udf->name.str,
                      m_error_msg[0] != '\0' ? m_error_msg : "unknown error");
      *null_value = true;
      return;
    default:
      *null_value = false;
      return;
  }
}

void vdf_handler::cleanup() {
  // Call postrun if VDF was active and postrun exists
  if (m_active && m_udf->vdf_func_desc->postrun) {
    vef_postrun_args_t postrun_args{};
    postrun_args.user_data = m_vdf_args.user_data;
    vef_postrun_result_t postrun_result{};
    m_udf->vdf_func_desc->postrun(&m_context, &postrun_args, &postrun_result);
  }
  m_active = false;
}

template <typename InvalueType>
static void marshal_args_typed(const vef_signature_t *sig, uint value_count,
                               Item **args, String *buffers,
                               InvalueType *invalues) {
  for (unsigned int i = 0; i < value_count; i++) {
    Item *arg_item = args[i];
    vef_type_id param_type;
    if (sig->params != nullptr && i < sig->param_count) {
      param_type = sig->params[i].id;
    } else {
      // Varargs: no per-slot signature, so infer the type from the Item.
      // ValidateAndConvertVDFArguments already rejects arg-count mismatches
      // for fixed-arity functions, so this branch only fires for varargs.
      auto *tc = arg_item->get_type_context();
      if (tc != nullptr) {
        param_type = VEF_TYPE_CUSTOM;
      } else {
        switch (arg_item->result_type()) {
          case REAL_RESULT:
            param_type = VEF_TYPE_REAL;
            break;
          case INT_RESULT:
            param_type = VEF_TYPE_INT;
            break;
          default:
            param_type = VEF_TYPE_STRING;
            break;
        }
      }
    }
    invalues[i].type = param_type;

    switch (param_type) {
      case VEF_TYPE_INT: {
        longlong val = arg_item->val_int();
        invalues[i].is_null = arg_item->null_value;
        invalues[i].int_value = val;
        break;
      }
      case VEF_TYPE_REAL: {
        double val = arg_item->val_real();
        invalues[i].is_null = arg_item->null_value;
        invalues[i].real_value = val;
        break;
      }
      case VEF_TYPE_STRING: {
        String *arg_str = arg_item->val_str(&buffers[i]);
        if (arg_item->null_value || arg_str == nullptr) {
          invalues[i].is_null = true;
          invalues[i].str_value = nullptr;
          invalues[i].str_len = 0;
        } else {
          invalues[i].is_null = false;
          invalues[i].str_value = arg_str->ptr();
          invalues[i].str_len = arg_str->length();
        }
        break;
      }
      case VEF_TYPE_CUSTOM:
      default: {
        String *arg_str = arg_item->val_str(&buffers[i]);

        // Only set the type parameters in protocol 2 or greater.
        if constexpr (!std::is_same_v<InvalueType, vef_invalue_v1_t>) {
          const auto *tc = arg_item->get_type_context();
          if (tc != nullptr) {
            const auto &params = tc->parameters();
            invalues[i].type_params = {params.count(), params.key_data(),
                                       params.value_data()};
          } else {
            invalues[i].type_params = {0, nullptr, nullptr};
          }
        }
        if (arg_item->null_value || arg_str == nullptr) {
          invalues[i].is_null = true;
          invalues[i].bin_value = nullptr;
          invalues[i].bin_len = 0;
        } else {
          invalues[i].is_null = false;
          invalues[i].bin_value =
              reinterpret_cast<const unsigned char *>(arg_str->ptr());
          invalues[i].bin_len = arg_str->length();
        }
        break;
      }
    }
  }
}

void vdf_handler::marshal_args() {
  const vef_signature_t *sig = m_udf->vdf_func_desc->signature;
  if (m_udf->vdf_protocol >= VEF_PROTOCOL_3) {
    marshal_args_typed(sig, m_vdf_args.value_count, m_args, m_buffers,
                       m_invalues);
  } else {
    marshal_args_typed(sig, m_vdf_args.value_count, m_args, m_buffers,
                       m_invalues_v1);
  }
}

template <typename T>
bool vdf_handler::invoke_numeric(T *out_value, bool *null_value) {
  marshal_args();

  // Set up result structure (numeric types use the union, not the buffer)
  vef_vdf_result_t result{};
  result.type = VEF_RESULT_VALUE;
  m_error_msg[0] = '\0';
  result.error_msg = m_error_msg;

  // Call the VDF function
  m_udf->vdf_func_desc->vdf(&m_context, &m_vdf_args, &result);

  // Handle result
  switch (result.type) {
    case VEF_RESULT_VALUE:
      if constexpr (std::is_same_v<T, double>) {
        *out_value = result.real_value;
      } else {
        *out_value = result.int_value;
      }
      *null_value = false;
      return true;
    case VEF_RESULT_NULL:
      *null_value = true;
      return false;
    case VEF_RESULT_WARNING:
      push_warning_printf(
          current_thd, Sql_condition::SL_WARNING, ER_UDF_ERROR,
          "VDF error in function '%s': %s", m_udf->name.str,
          m_error_msg[0] != '\0' ? m_error_msg : "unknown error");
      *null_value = true;
      return false;
    case VEF_RESULT_ERROR:
      my_printf_error(ER_UDF_ERROR, "VDF error in function '%s': %s", MYF(0),
                      m_udf->name.str,
                      m_error_msg[0] != '\0' ? m_error_msg : "unknown error");
      *null_value = true;
      return false;
  }
  *null_value = true;
  return false;
}

// Explicit template instantiations
template bool vdf_handler::invoke_numeric<double>(double *, bool *);
template bool vdf_handler::invoke_numeric<longlong>(longlong *, bool *);

double vdf_handler::val_real(bool *null_value) {
  double result = 0.0;
  invoke_numeric(&result, null_value);
  return result;
}

longlong vdf_handler::val_int(bool *null_value) {
  longlong result = 0LL;
  invoke_numeric(&result, null_value);
  return result;
}

String *vdf_handler::result_string(const char *res, size_t res_length,
                                   String *str, String *save_str,
                                   const CHARSET_INFO *charset) {
  String *res_str = nullptr;
  if (res == str->ptr()) {
    res_str = str;
    res_str->length(res_length);
    res_str->set_charset(charset);
  } else {
    res_str = save_str;
    res_str->set(res, res_length, charset);
  }
  return res_str;
}

String *vdf_handler::val_str(String *str, String *save_str,
                             const char *func_name,
                             const CHARSET_INFO *charset) {
  marshal_args();

  const vef_type_id return_type =
      m_udf->vdf_func_desc->signature->return_type.id;
  const bool is_binary = (return_type == VEF_TYPE_CUSTOM);

  // Call the VDF, growing the result buffer and retrying once if the VDF
  // reports (via actual_len) that its output did not fit. The SDK result
  // wrappers follow an snprintf-style contract: they copy what fits but set
  // actual_len to the full size that *would* have been written. One retry
  // always suffices because actual_len tells us exactly how large the buffer
  // must be. marshal_args() stays outside the loop: the re-invocation runs on
  // the already-marshaled arguments (aggregates re-serialize their accumulated
  // state; deterministic scalars reproduce the same bytes).
  vef_vdf_result_t result{};
  for (int attempt = 0; attempt < 2; ++attempt) {
    result = vef_vdf_result_t{};
    result.type = VEF_RESULT_VALUE;
    result.actual_len = 0;
    m_error_msg[0] = '\0';
    result.error_msg = m_error_msg;

    if (is_binary) {
      result.bin_buf = reinterpret_cast<unsigned char *>(m_result_buffer);
      result.max_bin_len = m_result_buffer_size;
      result.alt_bin_buf = nullptr;
      if (m_return_type_context != nullptr) {
        const auto &params = m_return_type_context->parameters();
        result.type_params = {params.count(), params.key_data(),
                              params.value_data()};
      } else {
        result.type_params = {0, nullptr, nullptr};
      }
    } else {
      result.str_buf = m_result_buffer;
      result.max_str_len = m_result_buffer_size;
      result.alt_str_buf = nullptr;
    }

    m_udf->vdf_func_desc->vdf(&m_context, &m_vdf_args, &result);

    if (result.type != VEF_RESULT_VALUE ||
        result.actual_len <= m_result_buffer_size) {
      break;
    }

    // The output overflowed our buffer. Grow to fit and retry once.
    if (attempt == 1) {
      // A well-behaved VDF reports a stable size, so the grown buffer must
      // fit. Guard against a pathological VDF rather than loop forever.
      my_printf_error(ER_UDF_ERROR,
                      "VDF result for function '%s' did not fit after resize",
                      MYF(0), func_name);
      return nullptr;
    }
    // Mirror MySQL's built-in string functions (e.g. CONCAT, REPEAT): a result
    // larger than max_allowed_packet is a row-level warning and NULL, not a
    // fatal error that aborts the statement.
    if (result.actual_len > current_thd->variables.max_allowed_packet) {
      push_warning_printf(
          current_thd, Sql_condition::SL_WARNING,
          ER_WARN_ALLOWED_PACKET_OVERFLOWED,
          ER_THD(current_thd, ER_WARN_ALLOWED_PACKET_OVERFLOWED), func_name,
          current_thd->variables.max_allowed_packet);
      return nullptr;
    }
    if (MaybeResizeBuffer(result.actual_len)) return nullptr;
  }

  // Handle result
  switch (result.type) {
    case VEF_RESULT_VALUE: {
      char *res;
      if (is_binary) {
        res = result.alt_bin_buf ? reinterpret_cast<char *>(*result.alt_bin_buf)
                                 : reinterpret_cast<char *>(result.bin_buf);
      } else {
        res = result.alt_str_buf ? *result.alt_str_buf : result.str_buf;
      }
      return result_string(res, result.actual_len, str, save_str, charset);
    }
    case VEF_RESULT_NULL:
      return nullptr;
    case VEF_RESULT_WARNING:
      push_warning_printf(
          current_thd, Sql_condition::SL_WARNING, ER_UDF_ERROR,
          "VDF error in function '%s': %s", func_name,
          m_error_msg[0] != '\0' ? m_error_msg : "unknown error");
      return nullptr;
    case VEF_RESULT_ERROR:
      my_printf_error(ER_UDF_ERROR, "VDF error in function '%s': %s", MYF(0),
                      func_name,
                      m_error_msg[0] != '\0' ? m_error_msg : "unknown error");
      return nullptr;
  }
  return nullptr;
}

}  // namespace vdf
}  // namespace villagesql
