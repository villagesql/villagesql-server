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

#include "mrs/endpoint/db_service_endpoint.h"

#include <mutex>
#include <set>
#include <string>

#include "mrs/endpoint/url_host_endpoint.h"
#include "mrs/router_observation_entities.h"

namespace mrs {
namespace endpoint {

using DbService = DbServiceEndpoint::DbService;
using DbServicePtr = DbServiceEndpoint::DbServicePtr;
using UniversalId = DbServiceEndpoint::UniversalId;
using EndpointConfiguration = DbServiceEndpoint::EndpointConfiguration;
using EnabledType = DbServiceEndpoint::EnabledType;

namespace {

enum UsedProtocol {
  k_usedProtocolNone,
  k_usedProtocolHttp,
  k_usedProtocolHttps
};

const std::string k_http = "HTTP";
const std::string k_https = "HTTPS";

UsedProtocol get_properly_configured_protocol(
    const std::set<std::string> &protocols,
    const EndpointConfiguration *configuration) {
  if (0 == protocols.count(configuration->does_server_support_https() ? k_https
                                                                      : k_http))
    return k_usedProtocolNone;

  if (configuration->does_server_support_https()) return k_usedProtocolHttps;

  return k_usedProtocolHttp;
}

void add_protocol_to_host(UsedProtocol protocol, ::http::base::Uri *uri) {
  switch (protocol) {
    case k_usedProtocolHttp:
      uri->set_scheme("http");
      return;
    case k_usedProtocolHttps:
      uri->set_scheme("https");
      return;
    default:
      return;
  }
}

}  // namespace

DbServiceEndpoint::DbServiceEndpoint(const DbService &entry,
                                     EndpointConfigurationPtr configuration,
                                     HandlerFactoryPtr factory)
    : OptionEndpoint(entry.id, configuration, factory),
      entry_{std::make_shared<DbService>(entry)} {}

UniversalId DbServiceEndpoint::get_id() const { return entry_->id; }

UniversalId DbServiceEndpoint::get_parent_id() const {
  return entry_->url_host_id;
}

const DbServicePtr DbServiceEndpoint::get() const { return entry_; }

void DbServiceEndpoint::set(const DbService &entry, EndpointBasePtr parent) {
  auto lock = std::unique_lock<std::shared_mutex>(endpoints_access_);
  entry_ = std::make_shared<DbService>(entry);
  change_parent(parent);
  changed();
}

void DbServiceEndpoint::update() {
  Parent::update();
  observability::EntityCounter<kEntityCounterUpdatesServices>::increment();
}

void DbServiceEndpoint::activate_public() {
  url_handlers_.clear();
  auto this_ep = shared_from_this();

  url_handlers_.push_back(
      factory_->create_db_service_metadata_handler(this_ep));
  url_handlers_.push_back(factory_->create_authentication_login(this_ep));
  url_handlers_.push_back(factory_->create_authentication_logout(this_ep));
  url_handlers_.push_back(factory_->create_authentication_completed(this_ep));
  url_handlers_.push_back(factory_->create_authentication_user(this_ep));
  url_handlers_.push_back(factory_->create_authentication_auth_apps(this_ep));
  url_handlers_.push_back(factory_->create_authentication_status(this_ep));
  url_handlers_.push_back(factory_->create_db_service_openapi_handler(this_ep));
}

void DbServiceEndpoint::deactivate() { url_handlers_.clear(); }

EnabledType DbServiceEndpoint::get_this_node_enabled_level() const {
  return entry_->enabled;
}

std::string DbServiceEndpoint::get_my_url_path_part() const {
  return entry_->url_context_root;
}

std::string DbServiceEndpoint::get_my_url_part() const {
  return entry_->url_context_root;
}

DbServiceEndpoint::Uri DbServiceEndpoint::get_url() const {
  auto parent = get_parent_ptr();
  if (parent) {
    auto parent_url = parent->get_url();
    if (!parent_url.empty()) {
      auto protocol = get_properly_configured_protocol(entry_->url_protocols,
                                                       configuration_.get());
      add_protocol_to_host(protocol, &parent_url);

      parent_url.set_path(parent_url.get_path() + get_my_url_part());
      return parent_url;
    }
  }

  Uri result;
  result.set_path(get_my_url_part());
  return result;
}

bool DbServiceEndpoint::does_this_node_require_authentication() const {
  return false;
}

std::optional<std::string> DbServiceEndpoint::get_options() const {
  return entry_->options;
}

std::string DbServiceEndpoint::get_extra_update_data() {
  return std::string(", published:") + (entry_->published ? "yes" : "no");
}

}  // namespace endpoint
}  // namespace mrs
