/*
  Copyright (c) 2022, 2024, Oracle and/or its affiliates.

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

#include "mrs/endpoint/handler_factory.h"

#include "mrs/endpoint/db_service_endpoint.h"
#include "mrs/endpoint/handler/authentication/handler_authorize_auth_apps.h"
#include "mrs/endpoint/handler/authentication/handler_authorize_completed.h"
#include "mrs/endpoint/handler/authentication/handler_authorize_login.h"
#include "mrs/endpoint/handler/authentication/handler_authorize_logout.h"
#include "mrs/endpoint/handler/authentication/handler_authorize_status.h"
#include "mrs/endpoint/handler/authentication/handler_authorize_user.h"
#include "mrs/endpoint/handler/handler_content_file.h"
#include "mrs/endpoint/handler/handler_db_object_function.h"
#include "mrs/endpoint/handler/handler_db_object_metadata.h"
#include "mrs/endpoint/handler/handler_db_object_metadata_catalog.h"
#include "mrs/endpoint/handler/handler_db_object_sp.h"
#include "mrs/endpoint/handler/handler_db_object_table.h"
#include "mrs/endpoint/handler/handler_db_schema_metadata.h"
#include "mrs/endpoint/handler/handler_db_schema_metadata_catalog.h"
#include "mrs/endpoint/handler/handler_db_service_metadata.h"
#include "mrs/endpoint/handler/handler_redirection.h"
#include "mrs/endpoint/handler/handler_string.h"
#include "mrs/endpoint/handler/utilities.h"
#include "mrs/endpoint/url_host_endpoint.h"

namespace mrs {
namespace endpoint {

using namespace mrs::endpoint::handler;

using HandlerPtr = std::unique_ptr<HandlerFactory::Handler>;

static std::string get_regex_path_authnetication(
    const std::shared_ptr<mrs::database::entry::DbService> &service) {
  std::string auth_path = service->auth_path.has_value()
                              ? service->auth_path.value()
                              : "/authentication";
  return std::string("^") + service->url_context_root + auth_path;
}

static std::string get_path_redirect(
    const std::shared_ptr<mrs::database::entry::DbService> &service) {
  std::string auth_path = service->auth_path.has_value()
                              ? service->auth_path.value()
                              : "/authentication";

  return service->url_context_root + auth_path + "/completed";
}

HandlerFactory::HandlerFactory(AuthorizeManager *auth_manager,
                               GtidManager *gtid_manager,
                               MysqlCacheManager *cache_manager,
                               ResponseCache *response_cache,
                               ResponseCache *file_cache)
    : auth_manager_{auth_manager},
      gtid_manager_{gtid_manager},
      cache_manager_{cache_manager},
      response_cache_{response_cache},
      file_cache_{file_cache} {}

HandlerPtr HandlerFactory::create_db_schema_metadata_catalog_handler(
    EndpointBasePtr endpoint) {
  using namespace mrs::endpoint::handler;

  auto db_schema_endpoint =
      std::dynamic_pointer_cast<DbSchemaEndpoint>(endpoint);

  return std::make_unique<HandlerDbSchemaMetadataCatalog>(db_schema_endpoint,
                                                          auth_manager_);
}

HandlerPtr HandlerFactory::create_db_object_handler(EndpointBasePtr endpoint) {
  using namespace mrs::endpoint::handler;
  using DbObjectLite = mrs::database::entry::DbObject;

  auto db_object_endpoint =
      std::dynamic_pointer_cast<DbObjectEndpoint>(endpoint);
  assert(db_object_endpoint && "Object must be castable.");

  auto entry_ = db_object_endpoint->get();

  switch (entry_->type) {
    case DbObjectLite::k_objectTypeTable:
      return std::make_unique<HandlerDbObjectTable>(
          db_object_endpoint, auth_manager_, gtid_manager_, cache_manager_,
          response_cache_, entry_->option_cache_ttl_ms.value_or(0));
    case DbObjectLite::k_objectTypeProcedure:
      return std::make_unique<HandlerDbObjectSP>(
          db_object_endpoint, auth_manager_, gtid_manager_, cache_manager_,
          response_cache_, entry_->option_cache_ttl_ms.value_or(0));
    case DbObjectLite::k_objectTypeFunction:
      return std::make_unique<HandlerDbObjectFunction>(
          db_object_endpoint, auth_manager_, gtid_manager_, cache_manager_,
          response_cache_, entry_->option_cache_ttl_ms.value_or(0));
  }

  assert(false && "all cases must be handled inside the switch.");
  return {};
}

HandlerPtr HandlerFactory::create_db_object_metadata_catalog_handler(
    EndpointBasePtr endpoint) {
  auto db_object_endpoint =
      std::dynamic_pointer_cast<DbObjectEndpoint>(endpoint);
  assert(db_object_endpoint && "Object must be castable.");

  return std::make_unique<HandlerDbObjectMetadataCatalog>(db_object_endpoint,
                                                          auth_manager_);
}

HandlerPtr HandlerFactory::create_db_service_metadata_handler(
    EndpointBasePtr endpoint) {
  auto db_service_endpoint =
      std::dynamic_pointer_cast<DbServiceEndpoint>(endpoint);
  assert(db_service_endpoint && "Object must be castable.");

  return std::make_unique<HandlerDbServiceMetadata>(db_service_endpoint,
                                                    auth_manager_);
}

HandlerPtr HandlerFactory::create_db_schema_metadata_handler(
    EndpointBasePtr endpoint) {
  auto db_schema_endpoint =
      std::dynamic_pointer_cast<DbSchemaEndpoint>(endpoint);
  assert(db_schema_endpoint && "Object must be castable.");

  return std::make_unique<HandlerDbSchemaMetadata>(db_schema_endpoint,
                                                   auth_manager_);
}

HandlerPtr HandlerFactory::create_db_object_metadata_handler(
    EndpointBasePtr endpoint) {
  auto db_object_endpoint =
      std::dynamic_pointer_cast<DbObjectEndpoint>(endpoint);
  assert(db_object_endpoint && "Object must be castable.");

  return std::make_unique<HandlerDbObjectMetadata>(db_object_endpoint,
                                                   auth_manager_);
}

HandlerPtr HandlerFactory::create_content_file(EndpointBasePtr endpoint) {
  auto content_file_endpoint =
      std::dynamic_pointer_cast<ContentFileEndpoint>(endpoint);
  assert(content_file_endpoint && "Object must be castable.");

  return std::make_unique<HandlerContentFile>(
      content_file_endpoint, auth_manager_, cache_manager_, file_cache_);
}

HandlerPtr HandlerFactory::create_string_handler(
    const UniversalId &service_id, bool requires_authentication, const Uri &,
    const std::string &path, const std::string &file_name,
    const std::string &file_content, bool is_index) {
  return std::make_unique<HandlerString>(service_id, requires_authentication,
                                         path, file_name, file_content,
                                         is_index, auth_manager_);
}

HandlerPtr HandlerFactory::create_redirection_handler(
    const UniversalId &service_id, bool requires_authentication, const Uri &url,
    const std::string &path, const std::string &file_name,
    const std::string &redirection_path, const bool pernament) {
  std::string whole_path = path;
  if (!file_name.empty()) {
    whole_path += "/" + file_name;
  }
  return std::make_unique<HandlerRedirection>(
      service_id, requires_authentication, get_endpoint_host(url), whole_path,
      file_name, redirection_path, auth_manager_, pernament);
}

HandlerPtr HandlerFactory::create_authentication_login(
    EndpointBasePtr endpoint) {
  auto db_service_endpoint =
      std::dynamic_pointer_cast<DbServiceEndpoint>(endpoint);
  assert(db_service_endpoint && "Object must be castable.");
  if (!db_service_endpoint)  // in case when we add new object type at level of
    return {};               // db-service.

  auto url_host_endpoint = std::dynamic_pointer_cast<UrlHostEndpoint>(
      db_service_endpoint->get_parent_ptr());
  assert(url_host_endpoint && "UrlHost must be castable.");
  if (!url_host_endpoint)  // in case when we add new object type at level of
    return {};             // root object (url-host)

  auto entry = db_service_endpoint->get();
  auto url_host_entry = url_host_endpoint->get();

  auto regex_path = get_regex_path_authnetication(entry) + "/login$$";
  auto redirect_path = get_path_redirect(entry);

  return std::make_unique<HandlerAuthorizeLogin>(
      url_host_entry->name, entry->id, regex_path,
      entry->options.value_or(std::string{}), redirect_path, auth_manager_);
}

HandlerPtr HandlerFactory::create_authentication_logout(
    EndpointBasePtr endpoint) {
  auto db_service_endpoint =
      std::dynamic_pointer_cast<DbServiceEndpoint>(endpoint);
  assert(db_service_endpoint && "Object must be castable.");
  if (!db_service_endpoint)  // in case when we add new object type at level of
    return {};               // db-service.

  auto url_host_endpoint = std::dynamic_pointer_cast<UrlHostEndpoint>(
      db_service_endpoint->get_parent_ptr());
  assert(url_host_endpoint && "UrlHost must be castable.");
  if (!url_host_endpoint)  // in case when we add new object type at level of
    return {};             // root object (url-host)

  auto entry = db_service_endpoint->get();
  auto url_host_entry = url_host_endpoint->get();

  auto regex_path = get_regex_path_authnetication(entry) + "/logout$";

  return std::make_unique<HandlerAuthorizeLogout>(
      url_host_entry->name, entry->id, regex_path,
      entry->options.value_or(std::string{}), auth_manager_);
}

HandlerPtr HandlerFactory::create_authentication_completed(
    EndpointBasePtr endpoint) {
  auto db_service_endpoint =
      std::dynamic_pointer_cast<DbServiceEndpoint>(endpoint);
  assert(db_service_endpoint && "Object must be castable.");
  if (!db_service_endpoint)  // in case when we add new object type at level of
    return {};               // db-service.

  auto url_host_endpoint = std::dynamic_pointer_cast<UrlHostEndpoint>(
      db_service_endpoint->get_parent_ptr());
  assert(url_host_endpoint && "UrlHost must be castable.");
  if (!url_host_endpoint)  // in case when we add new object type at level of
    return {};             // root object (url-host)

  auto entry = db_service_endpoint->get();
  auto url_host_entry = url_host_endpoint->get();

  auto regex_path = get_regex_path_authnetication(entry) + "/completed";

  return std::make_unique<HandlerAuthorizeCompleted>(
      url_host_entry->name, entry->id, regex_path,
      entry->options.value_or(std::string{}),
      entry->auth_completed_page_content.value_or(std::string{}),
      auth_manager_);
}

HandlerPtr HandlerFactory::create_authentication_user(
    EndpointBasePtr endpoint) {
  auto db_service_endpoint =
      std::dynamic_pointer_cast<DbServiceEndpoint>(endpoint);
  assert(db_service_endpoint && "Object must be castable.");
  if (!db_service_endpoint)  // in case when we add new object type at level of
    return {};               // db-service.

  auto url_host_endpoint = std::dynamic_pointer_cast<UrlHostEndpoint>(
      db_service_endpoint->get_parent_ptr());
  assert(url_host_endpoint && "UrlHost must be castable.");
  if (!url_host_endpoint)  // in case when we add new object type at level of
    return {};             // root object (url-host)

  auto entry = db_service_endpoint->get();
  auto url_host_entry = url_host_endpoint->get();

  auto regex_path = get_regex_path_authnetication(entry) + "/user";

  return std::make_unique<HandlerAuthorizeUser>(
      url_host_entry->name, entry->id, regex_path,
      entry->options.value_or(std::string{}), auth_manager_);
}

HandlerPtr HandlerFactory::create_authentication_auth_apps(
    EndpointBasePtr endpoint) {
  auto db_service_endpoint =
      std::dynamic_pointer_cast<DbServiceEndpoint>(endpoint);
  assert(db_service_endpoint && "Object must be castable.");
  if (!db_service_endpoint)  // in case when we add new object type at level of
    return {};               // db-service.

  auto url_host_endpoint = std::dynamic_pointer_cast<UrlHostEndpoint>(
      db_service_endpoint->get_parent_ptr());
  assert(url_host_endpoint && "UrlHost must be castable.");
  if (!url_host_endpoint)  // in case when we add new object type at level of
    return {};             // root object (url-host)

  auto entry = db_service_endpoint->get();
  auto url_host_entry = url_host_endpoint->get();

  auto regex_path = get_regex_path_authnetication(entry) + "/authApps$";
  auto redirect_path = get_path_redirect(entry);

  return std::make_unique<HandlerAuthorizeAuthApps>(
      url_host_entry->name, entry->id, regex_path,
      entry->options.value_or(std::string{}), redirect_path, auth_manager_);
}

HandlerPtr HandlerFactory::create_authentication_status(
    EndpointBasePtr endpoint) {
  auto db_service_endpoint =
      std::dynamic_pointer_cast<DbServiceEndpoint>(endpoint);
  assert(db_service_endpoint && "Object must be castable.");
  if (!db_service_endpoint)  // in case when we add new object type at level of
    return {};               // db-service.

  auto url_host_endpoint = std::dynamic_pointer_cast<UrlHostEndpoint>(
      db_service_endpoint->get_parent_ptr());
  assert(url_host_endpoint && "UrlHost must be castable.");
  if (!url_host_endpoint)  // in case when we add new object type at level of
    return {};             // root object (url-host)

  auto entry = db_service_endpoint->get();
  auto url_host_entry = url_host_endpoint->get();

  auto regex_path = get_regex_path_authnetication(entry) + "/status$";

  return std::make_unique<HandlerAuthorizeStatus>(
      url_host_entry->name, entry->id, regex_path,
      entry->options.value_or(std::string{}), auth_manager_);
}

}  // namespace endpoint
}  // namespace mrs
