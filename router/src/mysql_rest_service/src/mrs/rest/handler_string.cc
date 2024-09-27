/*
 Copyright (c) 2021, 2024, Oracle and/or its affiliates.

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

#include "mrs/rest/handler_string.h"

#include <map>
#include <string>
#include <vector>

#include "mysql/harness/logging/logging.h"
#include "mysql/harness/string_utils.h"

#include "helper/media_type.h"
#include "mrs/database/query_entry_content_file.h"
#include "mrs/database/query_factory.h"
#include "mrs/http/error.h"
#include "mrs/rest/request_context.h"
#include "mysqlrouter/base64.h"

IMPORT_LOG_FUNCTIONS()

using Type = mrs::interface::RestHandler::HttpResult::Type;

namespace mrs {
namespace rest {

using namespace std::string_literals;

using HttpResult = Handler::HttpResult;

template <typename T>
static std::string as_string(const std::vector<T> &v) {
  return std::string(v.begin(), v.end());
}

HandlerString::HandlerString(const std::string &path,
                             const std::string &content,
                             mrs::interface::AuthorizeManager *auth_manager)
    : Handler("", {"^"s + path + "$"}, std::string{}, auth_manager),
      path_{path},
      content_{content} {
  mysql_harness::Path p{path_};
  type_ = helper::get_media_type_from_extension(
      mysql_harness::make_lower(p.extension()).c_str());

  if (!helper::is_text_type(type_)) {
    content_ = as_string(Base64::decode(content_));
  }
}

UniversalId HandlerString::get_service_id() const { return {}; }
UniversalId HandlerString::get_db_object_id() const { return {}; }
UniversalId HandlerString::get_schema_id() const { return {}; }

Handler::Authorization HandlerString::requires_authentication() const {
  return Authorization::kNotNeeded;
}

uint32_t HandlerString::get_access_rights() const {
  using Op = mrs::database::entry::Operation::Values;
  return Op::valueRead;
}

void HandlerString::authorization(rest::RequestContext *) {}

HttpResult HandlerString::handle_get(rest::RequestContext *) {
  return {content_, type_};
}

HttpResult HandlerString::handle_delete(rest::RequestContext *) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

HttpResult HandlerString::handle_post(rest::RequestContext *,
                                      const std::vector<uint8_t> &) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

HttpResult HandlerString::handle_put(rest::RequestContext *) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

}  // namespace rest
}  // namespace mrs
