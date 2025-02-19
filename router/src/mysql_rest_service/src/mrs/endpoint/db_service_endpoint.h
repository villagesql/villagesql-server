/*
  Copyright (c) 2024, 2025, Oracle and/or its affiliates.

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

#ifndef ROUTER_SRC_MYSQL_REST_SERVICE_SRC_MRS_ENDPOINT_DB_SERVICE_ENDPOINT_H_
#define ROUTER_SRC_MYSQL_REST_SERVICE_SRC_MRS_ENDPOINT_DB_SERVICE_ENDPOINT_H_

#include <memory>

#include "mrs/database/entry/db_service.h"
#include "mrs/endpoint/option_endpoint.h"
#include "mrs/interface/handler_factory.h"

#ifdef HAVE_GRAALVM_PLUGIN
#include "router/src/graalvm/include/mysqlrouter/graalvm_context_pool.h"
#endif

namespace mrs {

namespace file_system {
class DbServiceFileSystem;
}
namespace endpoint {

class UrlHostEndpoint;

class DbServiceEndpoint : public OptionEndpoint {
 public:
  using Parent = OptionEndpoint;
  using DbService = mrs::database::entry::DbService;
  using DbServicePtr = std::shared_ptr<DbService>;
  using HandlerFactoryPtr = std::shared_ptr<mrs::interface::HandlerFactory>;
  using DataType = DbService;

 public:
  DbServiceEndpoint(const DbService &entry,
                    EndpointConfigurationPtr configuration,
                    HandlerFactoryPtr factory);

  UniversalId get_id() const override;
  UniversalId get_parent_id() const override;
  Uri get_url() const override;
  std::optional<std::string> get_options() const override;

  const DbServicePtr get() const;
  void set(const DbService &entry, EndpointBasePtr parent);

  void set_debug_enabled(bool value);
  bool is_debug_enabled() const;

#ifdef HAVE_GRAALVM_PLUGIN
  std::shared_ptr<graalvm::Pooled_context> get_scripting_context();
#endif

 private:
  void update() override;
  void activate_public() override;
  void deactivate() override;
  std::string get_extra_update_data() override;
  EnabledType get_this_node_enabled_level() const override;
  std::string get_my_url_path_part() const override;
  std::string get_my_url_part() const override;
  bool does_this_node_require_authentication() const override;

#ifdef HAVE_GRAALVM_PLUGIN
  std::shared_ptr<file_system::DbServiceFileSystem> get_file_system();
  const std::vector<std::string> &get_content_set_scripts();
#endif

  DbServicePtr entry_;
  std::vector<HandlerPtr> url_handlers_;
  bool debug_enabled_ = false;

#ifdef HAVE_GRAALVM_PLUGIN
  std::shared_ptr<file_system::DbServiceFileSystem> file_system_;
  std::optional<std::vector<std::string>> content_set_scripts_;
  size_t context_pool_size_ = 8;
  std::string content_set_path_;
#endif
};

}  // namespace endpoint
}  // namespace mrs

#endif  // ROUTER_SRC_MYSQL_REST_SERVICE_SRC_MRS_ENDPOINT_DB_SERVICE_ENDPOINT_H_
