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

#include "mrs/endpoint/handler/handler_content_file.h"
#include "mrs/endpoint/handler/handler_db_object_function.h"
#include "mrs/endpoint/handler/handler_db_object_metadata_catalog.h"
#include "mrs/endpoint/handler/handler_db_object_sp.h"
#include "mrs/endpoint/handler/handler_db_object_table.h"
#include "mrs/endpoint/handler/handler_db_schema_metadata_catalog.h"
#include "mrs/endpoint/handler/handler_redirection.h"
#include "mrs/endpoint/handler/handler_string.h"
#include "mrs/endpoint/handler/utilities.h"

namespace mrs {
namespace endpoint {

using namespace mrs::endpoint::handler;

using HandlerPtr = std::unique_ptr<HandlerFactory::Handler>;

HandlerFactory::HandlerFactory(AuthorizeManager *auth_manager,
                               GtidManager *gtid_manager,
                               MysqlCacheManager *cache_manager)
    : auth_manager_{auth_manager},
      gtid_manager_{gtid_manager},
      cache_manager_{cache_manager} {}

HandlerPtr HandlerFactory::create_schema_metadata_catalog_handler(
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
          db_object_endpoint, auth_manager_, gtid_manager_, cache_manager_);
    case DbObjectLite::k_objectTypeProcedure:
      return std::make_unique<HandlerDbObjectSP>(
          db_object_endpoint, auth_manager_, gtid_manager_, cache_manager_);
    case DbObjectLite::k_objectTypeFunction:
      return std::make_unique<HandlerDbObjectFunction>(
          db_object_endpoint, auth_manager_, gtid_manager_, cache_manager_);
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

HandlerPtr HandlerFactory::create_content_file(EndpointBasePtr endpoint) {
  auto content_file_endpoint =
      std::dynamic_pointer_cast<ContentFileEndpoint>(endpoint);
  assert(content_file_endpoint && "Object must be castable.");

  return std::make_unique<HandlerContentFile>(content_file_endpoint,
                                              auth_manager_, cache_manager_);
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

}  // namespace endpoint
}  // namespace mrs
