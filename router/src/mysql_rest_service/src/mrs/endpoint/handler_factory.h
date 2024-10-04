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

#ifndef ROUTER_SRC_REST_MRS_SRC_MRS_ENDPOINT_HANDLER_HANDLER_FACTORY_H_
#define ROUTER_SRC_REST_MRS_SRC_MRS_ENDPOINT_HANDLER_HANDLER_FACTORY_H_

#include <memory>

#include "collector/mysql_cache_manager.h"
#include "mrs/interface/authorize_manager.h"
#include "mrs/interface/handler_factory.h"
#include "mrs/rest/response_cache.h"

namespace mrs {
namespace endpoint {

class HandlerFactory : public mrs::interface::HandlerFactory {
 public:
  using MysqlCacheManager = collector::MysqlCacheManager;

 public:
  HandlerFactory(AuthorizeManager *auth_manager, GtidManager *gtid_manager,
                 MysqlCacheManager *cache_manager,
                 ResponseCache *response_cache);

  std::unique_ptr<Handler> create_db_service_metadata_handler(
      EndpointBasePtr db_service_endpoint) override;

  std::unique_ptr<Handler> create_db_schema_metadata_catalog_handler(
      EndpointBasePtr db_shema_endpoint) override;
  std::unique_ptr<Handler> create_db_schema_metadata_handler(
      EndpointBasePtr db_service_endpoint) override;

  std::unique_ptr<Handler> create_db_object_handler(
      EndpointBasePtr db_object_endpoint) override;
  std::unique_ptr<Handler> create_db_object_metadata_catalog_handler(
      EndpointBasePtr db_object_endpoint) override;
  std::unique_ptr<Handler> create_db_object_metadata_handler(
      EndpointBasePtr db_object_endpoint) override;

  std::unique_ptr<Handler> create_content_file(
      EndpointBasePtr db_object_endpoint) override;

  std::unique_ptr<Handler> create_string_handler(
      const UniversalId &service_id, bool requires_authentication,
      const Uri &url, const std::string &path, const std::string &file_name,
      const std::string &file_content, bool is_index) override;
  std::unique_ptr<Handler> create_redirection_handler(
      const UniversalId &service_id, bool requires_authentication,
      const Uri &url, const std::string &path, const std::string &file_name,
      const std::string &redirection_path, const bool pernament) override;

 private:
  AuthorizeManager *auth_manager_;
  GtidManager *gtid_manager_;
  MysqlCacheManager *cache_manager_;
  ResponseCache *response_cache_;
};

}  // namespace endpoint
}  // namespace mrs

#endif  // ROUTER_SRC_REST_MRS_SRC_MRS_ENDPOINT_HANDLER_HANDLER_FACTORY_H_
