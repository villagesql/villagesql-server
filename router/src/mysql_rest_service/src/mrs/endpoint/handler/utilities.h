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

#ifndef ROUTER_SRC_MYSQL_REST_SERVICE_SRC_MRS_ENDPOINT_HANDLER_UTILITIES_H_
#define ROUTER_SRC_MYSQL_REST_SERVICE_SRC_MRS_ENDPOINT_HANDLER_UTILITIES_H_

#include <cassert>
#include <memory>
#include <optional>
#include <string>

#include "mrs/endpoint/content_file_endpoint.h"
#include "mrs/endpoint/content_set_endpoint.h"
#include "mrs/endpoint/db_object_endpoint.h"
#include "mrs/endpoint/db_schema_endpoint.h"
#include "mrs/endpoint/db_service_endpoint.h"
#include "mrs/http/error.h"

namespace mrs {
namespace endpoint {

namespace handler {

const uint64_t k_default_items_on_page = 25;

template <typename Type>
std::shared_ptr<Type> lock_or_throw_unavail(std::weak_ptr<Type> &endpoint) {
  auto result = endpoint.lock();

  if (!result) throw http::Error(HttpStatusCode::ServiceUnavailable);

  return result;
}

template <typename Type>
std::shared_ptr<Type> lock(std::weak_ptr<Type> &endpoint) {
  auto result = endpoint.lock();

  assert(result &&
         "The weak_ptr should be not expired, when calling any Handler "
         "constructor.");

  return result;
}

inline std::string get_endpoint_host(const ::http::base::Uri &url) {
  auto result = url.get_host();
  if (!result.empty()) {
    auto port = url.get_port();
    if (-1 != port) {
      return result + ":" + std::to_string(port);
    }
  }
  return result;
}

inline std::string get_endpoint_host(
    std::weak_ptr<mrs::interface::EndpointBase> wp) {
  auto endpoint = lock(wp);
  if (!endpoint) return {};

  return get_endpoint_host(endpoint->get_url());
}

// inline std::string get_endpoint_host(
//     std::weak_ptr<mrs::endpoint::DbObjectEndpoint> wp) {
//   auto endpoint = lock(wp);
//   if (!endpoint) return {};
//
//   return get_endpoint_host(endpoint->get_url());
// }

inline std::shared_ptr<DbSchemaEndpoint> lock_parent(
    std::shared_ptr<DbObjectEndpoint> &endpoint) {
  auto parent = endpoint->get_parent_ptr();
  if (!parent) return {};

  return std::dynamic_pointer_cast<DbSchemaEndpoint>(parent);
}

inline std::shared_ptr<DbServiceEndpoint> lock_parent(
    std::shared_ptr<DbSchemaEndpoint> &endpoint) {
  auto parent = endpoint->get_parent_ptr();
  if (!parent) return {};

  return std::dynamic_pointer_cast<DbServiceEndpoint>(parent);
}

inline std::shared_ptr<ContentSetEndpoint> lock_parent(
    std::shared_ptr<ContentFileEndpoint> &endpoint) {
  auto parent = endpoint->get_parent_ptr();
  if (!parent) return {};

  return std::dynamic_pointer_cast<ContentSetEndpoint>(parent);
}

inline std::optional<std::string> get_endpoint_options(
    const std::shared_ptr<DbServiceEndpoint> &endpoint) {
  return endpoint->get()->options;
}

inline std::optional<std::string> get_endpoint_options(
    const std::shared_ptr<DbSchemaEndpoint> &endpoint) {
  const auto &option = endpoint->get()->options;
  if (option.has_value()) return option;

  auto parent =
      std::dynamic_pointer_cast<DbServiceEndpoint>(endpoint->get_parent_ptr());
  if (!parent) return {};

  return get_endpoint_options(parent);
}

inline std::optional<std::string> get_endpoint_options(
    const std::shared_ptr<DbObjectEndpoint> &endpoint) {
  const auto &option = endpoint->get()->options;
  if (option.has_value()) return option;

  auto parent =
      std::dynamic_pointer_cast<DbSchemaEndpoint>(endpoint->get_parent_ptr());
  if (!parent) return {};

  return get_endpoint_options(parent);
}

inline std::optional<std::string> get_endpoint_options(
    const std::shared_ptr<ContentSetEndpoint> &endpoint) {
  const auto &option = endpoint->get()->options;
  if (option.has_value()) return option;

  auto parent =
      std::dynamic_pointer_cast<DbServiceEndpoint>(endpoint->get_parent_ptr());
  if (!parent) return {};

  return get_endpoint_options(parent);
}

inline std::optional<std::string> get_endpoint_options(
    const std::shared_ptr<ContentFileEndpoint> &endpoint) {
  const auto &option = endpoint->get()->options;
  if (option.has_value()) return option;

  auto parent =
      std::dynamic_pointer_cast<ContentSetEndpoint>(endpoint->get_parent_ptr());
  if (!parent) return {};

  return get_endpoint_options(parent);
}

}  // namespace handler
}  // namespace endpoint
}  // namespace mrs

#endif /* ROUTER_SRC_MYSQL_REST_SERVICE_SRC_MRS_ENDPOINT_HANDLER_UTILITIES_H_ \
        */
