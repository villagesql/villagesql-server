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

#include "mrs/endpoint/db_service_endpoint.h"

#include <rapidjson/document.h>
#include <mutex>
#include <set>
#include <string>

#include "mrs/endpoint/content_set_endpoint.h"
#include "mrs/endpoint/handler/helper/utils_proto.h"
#include "mrs/endpoint/url_host_endpoint.h"
#include "mrs/router_observation_entities.h"
#ifdef HAVE_GRAALVM_PLUGIN
#include "mrs/file_system/db_service_file_system.h"
#include "mysqlrouter/graalvm_component.h"
#endif

namespace mrs {
namespace endpoint {

using DbService = DbServiceEndpoint::DbService;
using DbServicePtr = DbServiceEndpoint::DbServicePtr;
using UniversalId = DbServiceEndpoint::UniversalId;
using EndpointConfiguration = DbServiceEndpoint::EndpointConfiguration;
using EnabledType = DbServiceEndpoint::EnabledType;

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

void DbServiceEndpoint::set_debug_enabled(bool value) {
  debug_enabled_ = value;

#ifdef HAVE_GRAALVM_PLUGIN
  if (debug_enabled_) {
    get_scripting_context();
  } else {
    auto &instance = graalvm::GraalVMComponent::get_instance();
    instance.stop_debug_context(get()->id.to_string());
  }
#endif
}

bool DbServiceEndpoint::is_debug_enabled() const { return debug_enabled_; }

void DbServiceEndpoint::set(const DbService &entry, EndpointBasePtr parent) {
  auto lock = std::unique_lock<std::shared_mutex>(endpoints_access_);
  entry_ = std::make_shared<DbService>(entry);
  change_parent(parent);
  changed();
}

void DbServiceEndpoint::on_updated_content_set() {
#ifdef HAVE_GRAALVM_PLUGIN
  content_set_scripts_.reset();
#endif
}

#ifdef HAVE_GRAALVM_PLUGIN
std::shared_ptr<file_system::DbServiceFileSystem>
DbServiceEndpoint::get_file_system() {
  if (!file_system_) {
    file_system_ = std::make_shared<file_system::DbServiceFileSystem>(this);
  }

  return file_system_;
}

bool DbServiceEndpoint::get_content_set_scripts() {
  bool updated = false;
  if (!content_set_scripts_.has_value()) {
    updated = true;
    std::vector<std::string> scripts;

    for (const auto &child : get_children()) {
      auto content_set_ep =
          std::dynamic_pointer_cast<ContentSetEndpoint>(child);

      // We only care about content set childs
      if (!content_set_ep || !content_set_ep->get_options().has_value()) {
        continue;
      }

      content_set_ep->get_content_set_scripts(&scripts);

      // TODO(rennox): this should be turned int a global function if multiple
      // content sets per service are allowed
      content_set_path_ = content_set_ep->get_url().join();
    }

    content_set_scripts_ = std::move(scripts);
  }

  return updated;
}

namespace {
std::optional<uint64_t> get_pool_size(const std::string &options) {
  if (!options.empty()) {
    rapidjson::Document doc;
    doc.Parse(options.data(), options.size());

    if (doc.IsObject() && doc.HasMember("jitExecutor")) {
      const auto &node = doc["jitExecutor"];

      if (node.HasMember("poolSize")) {
        return node["poolSize"].GetUint();
      }
    }
  }

  return {};
}
}  // namespace

const std::vector<std::string> &DbServiceEndpoint::get_isolate_params() {
  std::vector<std::string> params;
  if (!isolate_args_.has_value()) {
    if (entry_->options.has_value()) {
      rapidjson::Document doc;
      doc.Parse((*entry_->options).data(), (*entry_->options).size());

      if (doc.IsObject()) {
        if (doc.HasMember("polyglotIsolateParams") &&
            doc["polyglotIsolateParams"].IsArray()) {
          for (auto &item : doc["polyglotIsolateParams"].GetArray()) {
            if (item.IsString()) {
              params.push_back(item.GetString());
            }
          }
        }
      }
    }
  }

  isolate_args_ = params;

  return *isolate_args_;
}

std::shared_ptr<graalvm::IGraalvm_context_handle>
DbServiceEndpoint::get_scripting_context() {
  auto &instance = graalvm::GraalVMComponent::get_instance();
  const auto id = get()->id.to_string();

  auto globals = shcore::make_dict();
  globals->emplace("contentSetPath", shcore::Value(content_set_path_));

  auto reset_context = get_content_set_scripts();

  return instance.get_context(
      id, get_pool_size(get_options().value_or("")).value_or(8),
      get_file_system(), *content_set_scripts_, globals,
      debug_enabled_ ? get_configuration()->get_debug_port() : "",
      get_isolate_params(), reset_context);
}
#endif

void DbServiceEndpoint::update() {
  Parent::update();
  observability::EntityCounter<kEntityCounterUpdatesServices>::increment();
}

void DbServiceEndpoint::activate_public() {
  url_handlers_.clear();
  auto this_ep = shared_from_this();

  url_handlers_.push_back(
      factory_->create_db_service_metadata_handler(this_ep));
  if (!get_configuration()->get_debug_port().empty()) {
    url_handlers_.push_back(factory_->create_db_service_debug_handler(this_ep));
  }
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
  using namespace ::mrs::endpoint::handler;
  auto parent = get_parent_ptr();
  if (parent) {
    auto parent_url = parent->get_url();
    if (!parent_url.empty()) {
      auto protocol = get_properly_configured_used_protocol(
          entry_->url_protocols, configuration_.get());
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
