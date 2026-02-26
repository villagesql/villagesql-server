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

#include <vector>

#include "my_rapidjson_size_t.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "villagesql/include/error.h"

namespace villagesql {

void TypeContext::resolve_cached_values() {
  // Build qualified_name_ once: "ext.type" or "ext.type(v1,v2,...)"
  // TODO(villagesql): This needs to be updated to support both TYPE(N) and
  // TYPE('k1=v1,k2=v2,...') syntax, and to preserve parameter order as defined
  // by the extension (currently alphabetical due to std::map). This will be
  // revisited when SHOW CREATE TABLE support is added.
  qualified_name_ = descriptor_->qualified_base_name();
  if (!key_.parameters().empty()) {
    qualified_name_ += "(";
    const char *delim = "";
    for (const auto &[k, v] : key_.parameters().params()) {
      qualified_name_ += delim;
      qualified_name_ += v;
      delim = ",";
    }
    qualified_name_ += ")";
  }

  if (!key_.parameters().empty() && descriptor_->resolve_params() != nullptr) {
    // Variable-length type with parameters: resolve via callback
    const auto &param_map = key_.parameters().params();
    std::vector<vef_type_param_t> params;
    params.reserve(param_map.size());
    for (const auto &[k, v] : param_map) {
      params.push_back({k.c_str(), v.c_str()});
    }
    vef_type_resolved_params_t resolved = {};
    char error_msg[VEF_MAX_ERROR_LEN] = {0};
    if (descriptor_->resolve_params()(params.data(), params.size(), &resolved,
                                      error_msg)) {
      // resolve_params failed — fall back to descriptor's base values.
      // This can happen if parameters stored on disk no longer validate
      // (e.g., extension upgraded). The type will be unusable until the
      // column is altered, but we won't crash.
      // TODO(villagesql-beta): how should we actually handle this error?
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

std::string TypeParameters::to_json() const {
  if (params_.empty()) return std::string("{}");

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

  writer.StartObject();
  for (const auto &[key, value] : params_) {
    writer.Key(key.c_str(), static_cast<rapidjson::SizeType>(key.length()));
    writer.String(value.c_str(),
                  static_cast<rapidjson::SizeType>(value.length()));
  }
  writer.EndObject();

  return std::string(buffer.GetString(), buffer.GetLength());
}

TypeParameters TypeParameters::from_json(const std::string &json) {
  if (json.empty() || json == "{}") return TypeParameters();

  rapidjson::Document doc;
  doc.Parse(json.c_str());

  if (doc.HasParseError() || !doc.IsObject()) return TypeParameters();

  ParamMap params;
  for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
    if (it->name.IsString() && it->value.IsString()) {
      params.emplace(
          std::string(it->name.GetString(), it->name.GetStringLength()),
          std::string(it->value.GetString(), it->value.GetStringLength()));
    }
  }

  return TypeParameters(std::move(params));
}

}  // namespace villagesql
