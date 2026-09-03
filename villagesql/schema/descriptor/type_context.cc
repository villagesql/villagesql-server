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

#include "villagesql/schema/descriptor/type_context.h"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <utility>
#include <vector>

#include "mysql/strings/m_ctype.h"
#include "sql/strfunc.h"
#include "template_utils.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/identifier_names.h"
#include "villagesql/types/special_vdf_call.h"

namespace villagesql {

TypeContext::TypeContext(const TypeContextKey &key,
                         const TypeDescriptor *descriptor)
    : descriptor_(descriptor), key_(key) {
  assert(descriptor);
  assert(descriptor->key() == key.descriptor_key());

  // Build bound Ops from the descriptor's TypeFunctions + our parameters.
  // Functions may be absent for key-only descriptors (used in tests).
  if (descriptor_->has_encode_fn())
    encode_op_.emplace(descriptor_->encode_fn(), key_.parameters());
  if (descriptor_->has_decode_fn())
    decode_op_.emplace(descriptor_->decode_fn(), key_.parameters());
  if (descriptor_->has_compare_fn())
    compare_op_.emplace(descriptor_->compare_fn(), key_.parameters());
  if (descriptor_->hash_fn().has_value())
    hash_op_.emplace(*descriptor_->hash_fn(), key_.parameters());

  resolve_cached_values();
}

bool TypeContext::init_intrinsic_default(std::string &error_out) {
  error_out.clear();

  // Protocol-1 types do not define intrinsic defaults; MySQL's built-in default
  // handling is used instead. Nothing to initialize.
  if (descriptor_->protocol() < VEF_PROTOCOL_3) return false;

  // Pre-encode the intrinsic default value. Returns false (success) when a
  // default is stored or when none is needed. Returns true (failure) when the
  // type cannot produce its required default; this is fatal for both
  // fixed-length and variable-length types.
  //
  // Sources tried in order:
  // 1. intrinsic_default_fn VDF: returns a string, which the server converts.
  // 2. intrinsic_default_str: encode the extension-supplied string literal.
  // 3. encode(""): encode the empty string.
  //
  // Parameterized types declared without parameters (is_unknown) cannot encode
  // a default yet: both the storage size and the encode behavior depend on the
  // parameters supplied at column-definition time. Skip them.
  if (is_unknown()) return false;

  // Buffer capacity to encode the default into. Fixed-length and
  // parameter-resolved types have an exact persisted_length and must encode to
  // exactly that many bytes. Variable-length types have no fixed size: encode
  // into the field's max capacity (max_persisted_length) and accept whatever
  // non-empty length the encoder produces (e.g. an empty bitmap/array).
  const bool variable_length = is_variable_length();
  const int64_t capacity =
      variable_length ? field_buffer_length() : persisted_length_;

  // No known storage size (e.g. a fixed-length type whose parameter resolution
  // failed). Nothing to encode a default against.
  if (capacity <= 0) return false;

  const size_t storage_size = static_cast<size_t>(capacity);
  std::vector<unsigned char> buffer(storage_size);
  size_t encoded_length = 0;

  // A fixed-length encode must produce exactly storage_size bytes. A
  // variable-length encode may produce anything in 1..storage_size.
  const auto length_ok = [&](size_t len) {
    return variable_length ? (len > 0 && len <= storage_size)
                           : (len == storage_size);
  };
  const std::string size_expectation =
      variable_length
          ? "expected 1.." + std::to_string(storage_size) + " bytes"
          : "expected persisted_length=" + std::to_string(storage_size);

  // Sources 1, 2, and 3 all produce a string that is then converted.
  // Source 1 calls the intrinsic_default VDF to get the string.
  // Source 2 uses the extension-supplied literal.
  // Source 3 falls back to the empty string.
  std::string input_str;
  if (descriptor_->intrinsic_default_fn().has_value()) {
    const auto &params = parameters();
    vef_type_params_t tp = {params.count(), params.key_data(),
                            params.value_data()};
    char error_msg[VEF_MAX_ERROR_LEN] = {};
    if (descriptor_->intrinsic_default_fn()->invoke(tp, &input_str,
                                                    error_msg)) {
      error_out =
          std::string("intrinsic_default function failed: ") + error_msg;
      return true;
    }
  } else if (descriptor_->intrinsic_default_str().has_value()) {
    input_str = *descriptor_->intrinsic_default_str();
  }

  if (!encode_op_.has_value()) {
    error_out = "no encode function available";
    return true;
  }

  // TODO(villagesql-general): consolidate this encode call with TypeEncoder to
  // avoid duplicating the vdf/fn dispatch logic.
  const EncodeOp &op = encode_op_.value();
  if (op.vdf() != nullptr) {
    SpecialVdfCall<CustomResult, StringArg> vdf_call(op.vdf());
    const auto &params = op.parameters();
    vdf_call.init(TypeParameterSlice(params.count(), params.key_data(),
                                     params.value_data()),
                  NoInitData{});
    auto r =
        vdf_call.invoke(StringSlice(input_str.c_str(), input_str.size()),
                        pointer_cast<uchar *>(buffer.data()), storage_size);
    if (r && length_ok(*r)) {
      buffer.resize(*r);
      intrinsic_default_buffer_ = std::move(buffer);
      intrinsic_default_size_ = *r;
      return false;
    }
    if (!r) {
      error_out = "from_string VDF failed to encode intrinsic default input '" +
                  input_str + "' (" + size_expectation + ")";
    } else {
      error_out = "from_string VDF encoded intrinsic default input '" +
                  input_str + "' to " + std::to_string(*r) + " bytes, " +
                  size_expectation;
    }
  } else if (op.fn() != nullptr) {
    bool fn_failed = op.fn()(buffer.data(), storage_size, input_str.c_str(),
                             input_str.size(), &encoded_length);
    if (!fn_failed && length_ok(encoded_length)) {
      buffer.resize(encoded_length);
      intrinsic_default_buffer_ = std::move(buffer);
      intrinsic_default_size_ = encoded_length;
      return false;
    }
    if (fn_failed) {
      error_out = "encode function failed for intrinsic default input '" +
                  input_str + "' (" + size_expectation + ")";
    } else {
      error_out = "encode function encoded intrinsic default input '" +
                  input_str + "' to " + std::to_string(encoded_length) +
                  " bytes, " + size_expectation;
    }
  } else {
    error_out =
        "encode op has neither VDF nor function for intrinsic default "
        "input '" +
        input_str + "'";
  }
  return true;
}

void TypeContext::resolve_cached_values() {
  // Build qualified_base_name_ once: "ext.type" (no parameters)
  qualified_base_name_ = descriptor_->qualified_base_name();

  // Build qualified_name_: "ext.type", "ext.type(N)", or
  // "ext.type('k1=v1,k2=v2,...')".
  // When int_to_params is available, try each integer-valued parameter and,
  // after filling defaults via resolve_params, check if the result reproduces
  // our exact params. If so, use the shorter TYPE(N) form. Otherwise fall back
  // to TYPE('k=v,...').
  qualified_name_ = descriptor_->qualified_base_name();
  if (!key_.parameters().empty()) {
    bool used_shorthand = false;
    if (descriptor_->int_to_params_fn().has_value()) {
      for (unsigned int i = 0; i < key_.parameters().count(); i++) {
        const char *val = key_.parameters().value_data()[i];
        char *end = nullptr;
        errno = 0;
        int64_t n = strtoll(val, &end, 10);
        if (errno != 0 || end == val || *end != '\0') continue;
        std::string result;
        char err[VEF_MAX_ERROR_LEN] = {0};
        if (!descriptor_->int_to_params_fn()->invoke(n, &result, err)) {
          // Check whether TYPE(N) reproduces the stored params. Responsibility
          // for filling in defaults is now split between int_to_params and
          // resolve_params: int_to_params (should) emits only the pair derived
          // from N, so we run resolve_params on its output here to fill the
          // rest before comparing. A type providing int_to_params must also
          // provide resolve_params (enforced at registration), so it is always
          // present here.
          assert(descriptor_->resolve_params_fn().has_value());
          ResolvedTypeParams tmp = {};
          char rerr[VEF_MAX_ERROR_LEN] = {0};
          std::string canonical = result;
          if (descriptor_->resolve_params_fn()->invoke(result, &tmp, rerr,
                                                       &canonical))
            continue;
          TypeParameters candidate = TypeParameters::from_raw(canonical);
          if (candidate == key_.parameters()) {
            qualified_name_ += "(";
            qualified_name_ += std::to_string(n);
            qualified_name_ += ")";
            used_shorthand = true;
            break;
          }
        }
      }
    }
    if (!used_shorthand) {
      qualified_name_ += "('";
      qualified_name_ += key_.parameters().str();
      qualified_name_ += "')";
    }
  }

  if (!key_.parameters().empty() &&
      descriptor_->resolve_params_fn().has_value()) {
    // Variable-length type with parameters: resolve via callback
    ResolvedTypeParams resolved = {};
    char error_msg[VEF_MAX_ERROR_LEN] = {0};
    if (descriptor_->resolve_params_fn()->invoke(key_.parameters().str(),
                                                 &resolved, error_msg)) {
      // resolve_params failed — fall back to descriptor's base values.
      // This can happen if parameters stored on disk no longer validate
      // (e.g., extension upgraded). The type will be unusable until the
      // column is altered, but we won't crash.
      // TODO(villagesql-production): how should we actually handle this error?
      LogVSQL(WARNING_LEVEL,
              "resolve_params failed for type '%s' (params: %s): %s. "
              "Falling back to descriptor defaults.",
              descriptor_->qualified_base_name().c_str(),
              key_.parameters().str().c_str(), error_msg);
      persisted_length_ = descriptor_->persisted_length();
      max_decode_buffer_length_ = descriptor_->max_decode_buffer_length();
      return;
    }
    persisted_length_ = resolved.persisted_length;
    max_decode_buffer_length_ = resolved.max_decode_buffer_length;
  } else {
    // Fixed-length type or bare variable-length type without parameters
    persisted_length_ = descriptor_->persisted_length();
    max_decode_buffer_length_ = descriptor_->max_decode_buffer_length();
  }
}

void TypeParameters::build_entries() {
  keys_.clear();
  values_.clear();
  c_keys_.clear();
  c_values_.clear();
  if (str_.empty()) return;

  // Parse "key=value" pairs separated by commas. Keys are already sorted
  // alphabetically in canonical form.
  size_t start = 0;
  while (start < str_.size()) {
    size_t comma = str_.find(',', start);
    if (comma == std::string::npos) comma = str_.size();
    std::string pair = str_.substr(start, comma - start);
    size_t eq = pair.find('=');
    if (eq != std::string::npos) {
      keys_.push_back(pair.substr(0, eq));
      values_.push_back(pair.substr(eq + 1));
    } else {
      keys_.push_back(std::move(pair));
      values_.push_back(std::string());
    }
    start = comma + 1;
  }
  c_keys_.reserve(keys_.size());
  for (const auto &k : keys_) {
    c_keys_.push_back(k.c_str());
  }
  c_values_.reserve(values_.size());
  for (const auto &v : values_) {
    c_values_.push_back(v.c_str());
  }
}

TypeParameters TypeParameters::from_raw(const std::string_view raw) {
  if (raw.empty()) return TypeParameters();

  // Parse "k=v,k=v,..." into pairs, sort by lowercased key, lowercase values,
  // re-serialize. Use the same charset for both lowercasing and sort order.
  const CHARSET_INFO *cs = type_parameter_collation();
  std::vector<std::pair<std::string, std::string>> pairs;
  size_t start = 0;
  // <= visits an empty token after a trailing comma, so it becomes a nameless
  // parameter. This does not cause an out-of-bounds read: when start == size(),
  // find() returns npos and substr() only errors out for pos > size().
  while (start <= raw.size()) {
    size_t comma = raw.find(',', start);
    if (comma == std::string_view::npos) comma = raw.size();
    size_t eq = raw.find('=', start);
    // A token with no '=' names a parameter but supplies no value. Keep it
    // with an empty value.
    const bool has_value = eq != std::string_view::npos && eq < comma;
    std::string_view key = has_value ? raw.substr(start, eq - start)
                                     : raw.substr(start, comma - start);
    std::string_view value =
        has_value ? raw.substr(eq + 1, comma - eq - 1) : std::string_view();

    // Trim whitespace
    auto trim = [](std::string_view &s) {
      size_t b = s.find_first_not_of(' ');
      size_t e = s.find_last_not_of(' ');
      s = (b == std::string::npos) ? "" : s.substr(b, e - b + 1);
    };
    trim(key);
    trim(value);
    std::string key_str{key};
    std::string value_str{value};
    // Lowercase. casedn() dereferences &s.front(), so it must not be called on
    // an empty string (e.g. a valueless token's empty value).
    if (!key_str.empty()) key_str = casedn(cs, std::move(key_str));
    if (!value_str.empty()) value_str = casedn(cs, std::move(value_str));

    pairs.emplace_back(std::move(key_str), std::move(value_str));
    start = comma + 1;
  }

  // Sort by key using the same charset collation used for lowercasing.
  std::sort(pairs.begin(), pairs.end(), [cs](const auto &a, const auto &b) {
    return my_strnncoll(cs, pointer_cast<const uint8_t *>(a.first.data()),
                        a.first.size(),
                        pointer_cast<const uint8_t *>(b.first.data()),
                        b.first.size()) < 0;
  });

  // Re-serialize
  std::string canonical;
  for (size_t i = 0; i < pairs.size(); i++) {
    if (i > 0) canonical += ',';
    canonical += pairs[i].first;
    canonical += '=';
    canonical += pairs[i].second;
  }

  return TypeParameters(std::move(canonical));
}

std::string TypeParameters::to_json() const {
  if (str_.empty()) return std::string("{}");

  // Convert "k1=v1,k2=v2" → {"k1":"v1","k2":"v2"}
  std::string json = "{";
  size_t start = 0;
  bool first = true;
  while (start < str_.size()) {
    size_t comma = str_.find(',', start);
    if (comma == std::string::npos) comma = str_.size();
    size_t eq = str_.find('=', start);
    if (eq != std::string::npos && eq < comma) {
      if (!first) json += ',';
      json += '"';
      json += str_.substr(start, eq - start);
      json += "\":\"";
      json += str_.substr(eq + 1, comma - eq - 1);
      json += '"';
      first = false;
    }
    start = comma + 1;
  }
  json += '}';
  return json;
}

TypeParameters TypeParameters::from_json(const std::string &json) {
  if (json.empty() || json == "{}") return TypeParameters();

  // Parse {"k1":"v1","k2":"v2"} → "k1=v1,k2=v2"
  // Simple parser: skip '{', find "key":"value" pairs, skip '}'
  std::string canonical;
  size_t pos = json.find('{');
  if (pos == std::string::npos) return TypeParameters();
  pos++;

  bool first = true;
  while (pos < json.size()) {
    // Skip whitespace and commas
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == ',')) pos++;
    if (pos >= json.size() || json[pos] == '}') break;

    // Expect "key"
    if (json[pos] != '"') break;
    pos++;
    size_t key_end = json.find('"', pos);
    if (key_end == std::string::npos) break;
    std::string key = json.substr(pos, key_end - pos);
    pos = key_end + 1;

    // Skip ':'
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == ':')) pos++;

    // Expect "value"
    if (pos >= json.size() || json[pos] != '"') break;
    pos++;
    size_t val_end = json.find('"', pos);
    if (val_end == std::string::npos) break;
    std::string value = json.substr(pos, val_end - pos);
    pos = val_end + 1;

    if (!first) canonical += ',';
    canonical += key;
    canonical += '=';
    canonical += value;
    first = false;
  }

  // Canonicalize so key order and casing match what from_raw() produces.
  return from_raw(canonical);
}

}  // namespace villagesql
