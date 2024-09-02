/*
  Copyright (c) 2024, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#include "mrs/database/helper/bind.h"

#ifdef RAPIDJSON_NO_SIZETYPEDEFINE
#include "my_rapidjson_size_t.h"
#endif

#include <rapidjson/document.h>

#include "helper/json/to_sqlstring.h"
#include "helper/mysql_numeric_value.h"
#include "mrs/http/error.h"

namespace mrs {
namespace database {

using DataTypeInText = ::helper::DataTypeInText;

void MysqlBind::fill_mysql_bind_for_out(DataType data_type) {
  allocate_bind_buffer(data_type);
}

void MysqlBind::fill_mysql_bind_impl(const std::string &value,
                                     DataType data_type) {
  if (data_type == DataType::typeBoolean) {
    using namespace std::string_literals;
    bool value_bool = false;
    static const std::string k_false{"\0", 1};
    static const std::string k_true{"\1", 1};

    auto value_type = ::helper::get_type_inside_text(value);
    switch (value_type) {
      case DataTypeInText::kDataInteger:
        value_bool = std::atoi(value.c_str()) > 0;
        break;
      case DataTypeInText::kDataString: {
        if (value == "true") {
          value_bool = 1;
        } else if (value == "false") {
          value_bool = 0;
        } else
          throw http::Error(HttpStatusCode::BadRequest,
                            "Not allowed value:"s + value +
                                ", for one of boolean parameters");

      } break;

      default:
        throw http::Error(
            HttpStatusCode::BadRequest,
            "Not allowed value:"s + value + ", for one of parameters");
    }
    allocate_for_blob(value_bool ? k_true : k_false);
    return;
  }

  // Transfer other types as strings.
  allocate_for_string(value);
}

enum_field_types MysqlBind::to_mysql_type(
    mrs::database::entry::Field::DataType pdt) {
  using Pdt = mrs::database::entry::Field::DataType;
  switch (pdt) {
    case Pdt::typeString:
      return MYSQL_TYPE_STRING;
    case Pdt::typeInt:
      return MYSQL_TYPE_LONG;
    case Pdt::typeDouble:
      return MYSQL_TYPE_DOUBLE;
    case Pdt::typeBoolean:
      return MYSQL_TYPE_TINY_BLOB;
    case Pdt::typeLong:
      return MYSQL_TYPE_LONGLONG;
    case Pdt::typeTimestamp:
      return MYSQL_TYPE_TIMESTAMP;

    default:
      return MYSQL_TYPE_NULL;
  }
}

const std::string &MysqlBind::to_string(const std::string &value) {
  return value;
}

std::string MysqlBind::to_string(const rapidjson::Value &value) {
  std::stringstream stream;
  ::helper::json::to_stream(stream, value, "1", "0");
  return stream.str();
}

MYSQL_BIND *MysqlBind::allocate_for_blob(const std::string &value) {
  auto result = allocate_for(value);
  result->buffer_type = MYSQL_TYPE_BLOB;
  return result;
}
MYSQL_BIND *MysqlBind::allocate_for_string(const std::string &value) {
  return allocate_for(value);
}

MYSQL_BIND *MysqlBind::allocate_for(const std::string &value) {
  auto bind = allocate_bind_buffer(DataType::typeString);
  if (value.length() + 1 > bind->buffer_length) {
    using namespace std::string_literals;
    throw http::Error(
        HttpStatusCode::BadRequest,
        "'in-out' parameter is too long, the internal buffer is "s +
            std::to_string(bind->buffer_length) + " bytes long.");
  }
  memcpy(reinterpret_cast<char *>(bind->buffer), value.c_str(), value.length());
  auto length = new unsigned long;
  lengths_.emplace_back(length);
  bind->length = length;
  *bind->length = value.length();

  return bind;
}

MYSQL_BIND *MysqlBind::allocate_bind_buffer(DataType data_type) {
  auto bind = &parameters.emplace_back();

  // Please note that the var-string maximums size is 2^16-1 characters.
  // We are not allowing LARGETEXT, here. It would be too much for the
  // network transfer.
  const int k_var_string_maximum_size = 0xFFFF;  // 2^16-1
  const int k_tiny_blob_maximum_size = 255;

  std::unique_ptr<char[]> &buffer = buffers_.emplace_back();
  bind->buffer_type = to_mysql_type(data_type);

  switch (bind->buffer_type) {
    case MYSQL_TYPE_STRING:
      buffer.reset(new char[bind->buffer_length = k_var_string_maximum_size]);
      break;

    case MYSQL_TYPE_LONGLONG:
      buffer.reset(new char[bind->buffer_length = sizeof(uint64_t)]);
      break;

    case MYSQL_TYPE_DOUBLE:
      buffer.reset(new char[bind->buffer_length = sizeof(double)]);
      break;

    case MYSQL_TYPE_TINY_BLOB:
      buffer.reset(new char[bind->buffer_length = k_tiny_blob_maximum_size]);
      break;

    case MYSQL_TYPE_LONG:
      buffer.reset(new char[bind->buffer_length = sizeof(uint32_t)]);
      break;

    case MYSQL_TYPE_TIMESTAMP:
      buffer.reset(new char[bind->buffer_length = k_var_string_maximum_size]);
      break;

    default:
      assert(nullptr && "should not happen");
  }

  bind->buffer = buffer.get();

  return bind;
}

}  // namespace database
}  // namespace mrs
