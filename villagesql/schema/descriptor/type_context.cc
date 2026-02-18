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

#include "my_rapidjson_size_t.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace villagesql {

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
