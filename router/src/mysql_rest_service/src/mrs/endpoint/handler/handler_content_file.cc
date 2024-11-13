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

#include "mrs/endpoint/handler/handler_content_file.h"

#include <map>
#include <vector>

#include "mysql/harness/filesystem.h"
#include "mysql/harness/string_utils.h"

#include "helper/digester/md5_digest.h"
#include "helper/media_type.h"
#include "helper/string/hex.h"
#include "mrs/database/query_entry_content_file.h"
#include "mrs/database/query_factory.h"
#include "mrs/endpoint/handler/url_paths.h"
#include "mrs/endpoint/handler/utilities.h"
#include "mrs/http/error.h"
#include "mrs/rest/request_context.h"

using MySQLSession = collector::MysqlCacheManager::Object;
using CachedObject = collector::MysqlCacheManager::CachedObject;
using Type = mrs::interface::RestHandler::HttpResult::Type;
using HttpResult = mrs::rest::Handler::HttpResult;
using MysqlCacheManager = collector::MysqlCacheManager;
using MySQLConnection = collector::MySQLConnection;
using Authorization = mrs::rest::Handler::Authorization;

static CachedObject get_session(
    MySQLSession session, MysqlCacheManager *cache_manager,
    MySQLConnection type = MySQLConnection::kMySQLConnectionMetadataRO) {
  if (session) return CachedObject(nullptr, true, session);

  return cache_manager->get_instance(type, false);
}

namespace mrs {
namespace endpoint {
namespace handler {

namespace {

std::vector<std::string> get_regex_paths(
    std::weak_ptr<ContentFileEndpoint> endpoint_wptr) {
  auto endpoint = lock(endpoint_wptr);
  assert(endpoint && "Endpoint must be valid/lockable at object creation");

  return regex_path_content_file(endpoint->get_url_path());
}

}  // namespace

HandlerContentFile::HandlerContentFile(
    std::weak_ptr<ContentFileEndpoint> endpoint,
    mrs::interface::AuthorizeManager *auth_manager,
    collector::MysqlCacheManager *cache, mrs::ResponseCache *response_cache)
    : Handler(get_endpoint_host(endpoint), get_regex_paths(endpoint),
              get_endpoint_options(lock(endpoint)), auth_manager),
      endpoint_{endpoint},
      cache_{cache} {
  auto endpoint_file = lock(endpoint);
  auto endpoint_set = lock_parent(endpoint_file);
  assert(endpoint_set && "Parent must be available.");
  entry_file_ = endpoint_file->get();
  entry_set_ = endpoint_set->get();

  helper::digester::Md5Digest version_calculation;
  version_calculation.update(std::to_string(time(nullptr)));
  version_calculation.update(std::to_string(entry_file_->size));
  version_ = helper::string::hex<std::string, helper::string::CStringHex>(
      version_calculation.finalize());

  if (response_cache) {
    response_cache_ =
        std::make_shared<FileEndpointResponseCache>(response_cache);
  }
}

UniversalId HandlerContentFile::get_service_id() const {
  return entry_set_->service_id;
}

UniversalId HandlerContentFile::get_db_object_id() const { return {}; }

UniversalId HandlerContentFile::get_schema_id() const { return {}; }

Authorization HandlerContentFile::requires_authentication() const {
  bool auth = entry_file_->requires_authentication ||
              entry_set_->requires_authentication;
  return auth ? Authorization::kRequires : Authorization::kNotNeeded;
}

uint32_t HandlerContentFile::get_access_rights() const {
  return mrs::database::entry::Operation::valueRead;
}

void HandlerContentFile::authorization(rest::RequestContext *ctxt) {
  throw_unauthorize_when_check_auth_fails(ctxt);
}

HttpResult HandlerContentFile::handle_get(rest::RequestContext *ctxt) {
  mysql_harness::Path path{entry_file_->request_path};
  auto if_not_matched =
      ctxt->request->get_input_headers().find_cstr("If-None-Match");

  if (if_not_matched && version_ == if_not_matched)
    throw http::Error(HttpStatusCode::NotModified);

  if (response_cache_) {
    auto entry = response_cache_->lookup_file(entry_file_->id);
    if (entry) {
      return {entry->data, entry->media_type.value(), version_};
    }
  }

  auto result_type = helper::get_media_type_from_extension(
      mysql_harness::make_lower(path.extension()).c_str());

  auto session = get_session(ctxt->sql_session_cache.get(), cache_,
                             MySQLConnection::kMySQLConnectionMetadataRO);

  if (nullptr == session.get())
    throw http::Error(HttpStatusCode::InternalError);

  mrs::database::QueryEntryContentFile query_content_file;
  query_content_file.query_file(session.get(), entry_file_->id);

  if (response_cache_) {
    response_cache_->create_file_entry(entry_file_->id,
                                       query_content_file.result, result_type);
  }

  return {std::move(query_content_file.result), result_type, version_};
}

HttpResult HandlerContentFile::handle_delete(rest::RequestContext *) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

HttpResult HandlerContentFile::handle_post(rest::RequestContext *,
                                           const std::vector<uint8_t> &) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

HttpResult HandlerContentFile::handle_put(rest::RequestContext *) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

}  // namespace handler
}  // namespace endpoint
}  // namespace mrs
