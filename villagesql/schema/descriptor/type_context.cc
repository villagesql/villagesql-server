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
#include <utility>
#include <vector>

#include "mysql/strings/m_ctype.h"
#include "sql/strfunc.h"
#include "villagesql/include/error.h"

namespace villagesql {

void TypeContext::resolve_cached_values() {
  // Build qualified_base_name_ once: "ext.type" (no parameters)
  qualified_base_name_ = descriptor_->qualified_base_name();

  // Build qualified_name_ once: "ext.type" or "ext.type('k1=v1,k2=v2,...')"
  // TODO(villagesql-beta): add special case for TYPE(N) when only a single
  // integer parameter is present (i.e. int_to_params was used).
  qualified_name_ = descriptor_->qualified_base_name();
  if (!key_.parameters().empty()) {
    qualified_name_ += "('";
    qualified_name_ += key_.parameters().str();
    qualified_name_ += "')";
  }

  if (!key_.parameters().empty() &&
      descriptor_->resolve_params_op().has_value()) {
    // Variable-length type with parameters: resolve via callback
    ResolvedTypeParams resolved = {};
    char error_msg[VEF_MAX_ERROR_LEN] = {0};
    if (descriptor_->resolve_params_op()->invoke(key_.parameters().str(),
                                                 &resolved, error_msg)) {
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

TypeParameters TypeParameters::from_raw(const std::string &raw) {
  if (raw.empty()) return TypeParameters();

  // Parse "k=v,k=v,..." into pairs, sort by lowercased key, lowercase values,
  // re-serialize. Use the same charset for both lowercasing and sort order.
  // TODO(villagesql-beta): refactor parsing to use std::string_view.
  const CHARSET_INFO *cs = &my_charset_utf8mb4_0900_ai_ci;
  std::vector<std::pair<std::string, std::string>> pairs;
  size_t start = 0;
  while (start < raw.size()) {
    size_t comma = raw.find(',', start);
    if (comma == std::string::npos) comma = raw.size();
    size_t eq = raw.find('=', start);
    if (eq == std::string::npos || eq >= comma) {
      start = comma + 1;
      continue;
    }
    std::string key = raw.substr(start, eq - start);
    std::string value = raw.substr(eq + 1, comma - eq - 1);

    // Trim whitespace
    auto trim = [](std::string &s) {
      size_t b = s.find_first_not_of(' ');
      size_t e = s.find_last_not_of(' ');
      s = (b == std::string::npos) ? "" : s.substr(b, e - b + 1);
    };
    trim(key);
    trim(value);

    // Lowercase
    key = casedn(cs, std::move(key));
    value = casedn(cs, std::move(value));

    pairs.emplace_back(std::move(key), std::move(value));
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

  return TypeParameters(std::move(canonical));
}

}  // namespace villagesql
