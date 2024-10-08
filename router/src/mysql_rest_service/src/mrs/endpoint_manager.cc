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

#include "endpoint_manager.h"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <vector>

#include "endpoint/handler/handler_db_schema_metadata_catalog.h"
#include "mysql/harness/logging/logging.h"

#include "helper/container/map.h"
#include "helper/container/to_string.h"
#include "helper/json/rapid_json_to_struct.h"
#include "helper/json/text_to.h"
#include "helper/string/contains.h"
#include "helper/to_string.h"
#include "mrs/endpoint/content_file_endpoint.h"
#include "mrs/endpoint/content_set_endpoint.h"
#include "mrs/endpoint/db_object_endpoint.h"
#include "mrs/endpoint/db_schema_endpoint.h"
#include "mrs/endpoint/handler_factory.h"
#include "mrs/rest/handler_string.h"

IMPORT_LOG_FUNCTIONS()

namespace mrs {

namespace cvt {

using std::to_string;
static const std::string &to_string(const std::string &str) { return str; }

}  // namespace cvt

namespace {

bool g_logging{true};

class PluginOptions {
 public:
  std::map<std::string, std::string> default_content;
};

class ParsePluginOptions
    : public helper::json::RapidReaderHandlerToStruct<PluginOptions> {
 public:
  template <typename ValueType>
  void handle_object_value(const std::string &key, const ValueType &vt) {
    //    log_debug("handle_object_value key:%s, v:%s", key.c_str(),
    //              cvt::to_string(vt).c_str());
    static const std::string kHttpContent = "defaultContent.";
    using std::to_string;

    if (helper::starts_with(key, kHttpContent)) {
      result_.default_content[key.substr(kHttpContent.length())] =
          cvt::to_string(vt);
    }
  }

  template <typename ValueType>
  void handle_value(const ValueType &vt) {
    const auto &key = get_current_key();
    if (is_object_path()) {
      handle_object_value(key, vt);
    }
  }

  bool String(const Ch *v, rapidjson::SizeType v_len, bool) override {
    handle_value(std::string{v, v_len});
    return true;
  }

  bool RawNumber(const Ch *v, rapidjson::SizeType v_len, bool) override {
    handle_value(std::string{v, v_len});
    return true;
  }

  bool Bool(bool v) override {
    handle_value(v);
    return true;
  }
};

PluginOptions parse_json_options(const std::string &options) {
  return helper::json::text_to_handler<ParsePluginOptions>(options);
}

class EndpointConfiguration : public mrs::interface::EndpointConfiguration {
 public:
  EndpointConfiguration(const bool is_ssl) : is_ssl_{is_ssl} {}

  bool does_server_support_https() const override { return is_ssl_; }

 private:
  bool is_ssl_;
};

}  // namespace

using UniversalId = mrs::database::entry::UniversalId;
using DbObject = mrs::database::entry::DbObject;
using DbSchema = mrs::database::entry::DbSchema;
using ContentSet = mrs::database::entry::ContentSet;
using ContentFile = mrs::database::entry::ContentFile;
using DbService = mrs::database::entry::DbService;
using UrlHost = mrs::database::entry::UrlHost;
using EndpointBase = EndpointManager::EndpointBase;
using EndpointBasePtr = EndpointManager::EndpointBasePtr;
using EndpointFactory = EndpointManager::EndpointFactory;

EndpointManager::EndpointManager(collector::MysqlCacheManager *cache,
                                 const bool is_ssl,
                                 mrs::interface::AuthorizeManager *auth_manager,
                                 mrs::GtidManager *gtid_manager,
                                 EndpointFactoryPtr endpoint_factory)
    : cache_{cache},
      is_ssl_{is_ssl},
      auth_manager_{auth_manager},
      gtid_manager_{gtid_manager},
      endpoint_factory_{endpoint_factory} {
  if (!endpoint_factory_) {
    auto handler_factory = std::make_shared<mrs::endpoint::HandlerFactory>(
        auth_manager_, gtid_manager_, cache_);
    auto configuration = std::make_shared<EndpointConfiguration>(is_ssl);
    endpoint_factory_ =
        std::make_shared<EndpointFactory>(handler_factory, configuration);
  }
}

void EndpointManager::configure(const std::optional<std::string> &options) {
  update_options(options.value_or("{}"));

  // std::map<UniversalId, EndpointBasePtr>
  for (auto &[_, endpoint] : hold_host_endpoints_) {
    auto url_host_ep =
        std::dynamic_pointer_cast<mrs::endpoint::UrlHostEndpoint>(endpoint);
    if (!url_host_ep) {
      assert(false && "Should not happen.");
      continue;
    }
    auto parent = url_host_ep->get_parent_ptr();
    auto entry = *url_host_ep->get();
    entry.options = options;
    url_host_ep->set(entry, parent);
  }
}

const UniversalId &get_parent_id(const UrlHost &) {
  static UniversalId empty{};
  return empty;
}

const UniversalId &get_parent_id(const DbService &service) {
  return service.url_host_id;
}

const UniversalId &get_parent_id(const ContentFile &content_file) {
  return content_file.content_set_id;
}

const UniversalId &get_parent_id(const ContentSet &content_set) {
  return content_set.service_id;
}

const UniversalId &get_parent_id(const DbSchema &schema) {
  return schema.service_id;
}

const UniversalId &get_parent_id(const DbObject &object) {
  return object.schema_id;
}

template <typename HoldingContainer>
std::weak_ptr<EndpointBase> get_object_by_id(HoldingContainer *holding,
                                             const UniversalId &id) {
  if (id.empty()) return {};

  std::weak_ptr<EndpointBase> out_ptr;
  helper::container::get_value(*holding, id, &out_ptr);

  return out_ptr;
}

void log_debug_db_entry_impl(const UrlHost &host) {
  log_debug("UrlHost id=%s", host.id.to_string().c_str());
  log_debug("UrlHost deleted=%s", helper::to_string(host.deleted).c_str());
  log_debug("UrlHost name=%s", host.name.c_str());
}

void log_debug_db_entry_impl(const DbService &service) {
  log_debug("DbService id=%s", service.id.to_string().c_str());
  log_debug("DbService root=%s", service.url_context_root.c_str());
  log_debug("DbService deleted=%s", helper::to_string(service.deleted).c_str());
  log_debug("DbService protocols=%s",
            helper::container::to_string(service.url_protocols).c_str());
  log_debug("DbService enabled=%s", helper::to_string(service.enabled).c_str());
  log_debug("DbService host_id=%s", service.url_host_id.to_string().c_str());
  log_debug("DbService in_developement=%s",
            helper::to_string(service.in_development).c_str());
}

void log_debug_db_entry_impl(const DbSchema &schema) {
  log_debug("DbSchema id=%s", schema.id.to_string().c_str());
  log_debug("DbSchema deleted=%s", helper::to_string(schema.deleted).c_str());
  log_debug("DbSchema name=%s", schema.name.c_str());
  log_debug("DbSchema enabled=%s", helper::to_string(schema.enabled).c_str());
  if (!schema.options.has_value()) {
    log_debug("DbSchema options=NONE");
  } else {
    log_debug("DbSchema options=%s",
              helper::to_string(schema.options.value()).c_str());
  }
}

void log_debug_db_entry_impl(const DbObject &obj) {
  log_debug("DbObject id=%s", obj.id.to_string().c_str());
  log_debug("DbObject deleted=%s", helper::to_string(obj.deleted).c_str());
  log_debug("DbObject name=%s", obj.name.c_str());
  log_debug("DbObject enabled=%s", helper::to_string(obj.enabled).c_str());
}

void log_debug_db_entry_impl(const ContentSet &content_set) {
  log_debug("ContentSet id=%s", content_set.id.to_string().c_str());
  log_debug("ContentSet deleted=%s",
            helper::to_string(content_set.deleted).c_str());
  log_debug("ContentSet request_path=%s", content_set.request_path.c_str());
  log_debug("ContentSet enabled=%s",
            helper::to_string(content_set.enabled).c_str());
}

void log_debug_db_entry_impl(const ContentFile &content_file) {
  log_debug("ContentFile id=%s", content_file.id.to_string().c_str());
  log_debug("ContentFile deleted=%s",
            helper::to_string(content_file.deleted).c_str());
  log_debug("ContentFile request_path=%s", content_file.request_path.c_str());
  log_debug("ContentFile enabled=%s",
            helper::to_string(content_file.enabled).c_str());
}

template <typename DbType>
void log_debug_db_entry(const DbType &type) {
  if (g_logging) {
    log_debug("Entry:");
    log_debug_db_entry_impl(type);
  }
}

template <typename Target, typename ChangedContainer, typename HoldingContainer>
void process_endpoints(
    EndpointFactory *factory, ChangedContainer &in, HoldingContainer *holder,
    std::map<UniversalId, std::shared_ptr<EndpointBase>> *out) {
  using TargetContainer = std::map<UniversalId, std::shared_ptr<EndpointBase>>;
  for (const auto &new_entry : in) {
    typename TargetContainer::iterator it;
    log_debug_db_entry(new_entry);
    if (out) it = out->find(new_entry.id);
    auto target = get_object_by_id(holder, new_entry.id).lock();
    auto parent = get_object_by_id(holder, get_parent_id(new_entry)).lock();

    if (new_entry.deleted) {
      if (!target) continue;

      auto ptr = target->get_parent_ptr();
      if (ptr) {
        ptr->remove_child_endpoint(target->get_id());
      }

      holder->erase(new_entry.id);
      if (out && it != out->end()) out->erase(it);

      continue;
    }

    if (!target) {
      // The object register itself at parent.
      // Thus if out->insert is not called, the shared-pointer is
      // still referenced by the parent.
      auto ptr = factory->create_object(new_entry, parent);
      //      auto new_target_ptr = std::dynamic_pointer_cast<Target>(ptr);
      //
      //      assert(new_target_ptr &&
      //             "Wrong type specified in Target template argument, please
      //             select " "same as in overload.");

      if (out) {
        out->insert(std::make_pair(new_entry.id, ptr));
      }
      (*holder)[new_entry.id] = ptr;

      continue;
    }

    if (auto endpoint_target = dynamic_cast<Target *>(target.get()))
      endpoint_target->set(new_entry, parent);
    else
      target->set_parent(parent);
  }
}

void EndpointManager::update(const std::vector<UrlHost> &hosts) {
  if (hosts.size()) {
    log_debug("Endpoint Manager: Number of updated host entries:%i",
              static_cast<int>(hosts.size()));
  }
  process_endpoints<mrs::endpoint::UrlHostEndpoint>(
      endpoint_factory_.get(), hosts, &endpoints_, &hold_host_endpoints_);
}

void EndpointManager::update(const std::vector<DbService> &services) {
  if (services.size()) {
    log_debug("Endpoint Manager: Number of updated service entries:%i",
              static_cast<int>(services.size()));
  }
  process_endpoints<mrs::endpoint::DbServiceEndpoint>(
      endpoint_factory_.get(), services, &endpoints_, nullptr);
}

void EndpointManager::update(const std::vector<DbSchema> &schema) {
  if (schema.size()) {
    log_debug("Endpoint Manager: Number of updated schema entries:%i",
              static_cast<int>(schema.size()));
  }
  process_endpoints<mrs::endpoint::DbSchemaEndpoint>(
      endpoint_factory_.get(), schema, &endpoints_, nullptr);
}

void EndpointManager::update(const std::vector<DbObject> &obj) {
  if (obj.size()) {
    log_debug("Endpoint Manager: Number of updated object entries:%i",
              static_cast<int>(obj.size()));
  }
  process_endpoints<mrs::endpoint::DbObjectEndpoint>(endpoint_factory_.get(),
                                                     obj, &endpoints_, nullptr);
}

void EndpointManager::update(const std::vector<ContentSet> &set) {
  if (set.size()) {
    log_debug("Endpoint Manager: Number of updated content-set entries:%i",
              static_cast<int>(set.size()));
  }
  process_endpoints<mrs::endpoint::ContentSetEndpoint>(
      endpoint_factory_.get(), set, &endpoints_, nullptr);
}

void EndpointManager::update(const std::vector<ContentFile> &files) {
  if (files.size()) {
    log_debug("Endpoint Manager: Number of updated content-file entries:%i",
              static_cast<int>(files.size()));
  }
  process_endpoints<mrs::endpoint::ContentFileEndpoint>(
      endpoint_factory_.get(), files, &endpoints_, nullptr);
}

void EndpointManager::update_options(const std::string &options) {
  auto opt = parse_json_options(options);

  custom_paths_.clear();

  for (auto [k, v] : opt.default_content) {
    custom_paths_.push_back(
        std::make_shared<rest::HandlerString>(k, v, auth_manager_));
  }
}

void EndpointManager::clear() {
  endpoints_.clear();
  hold_host_endpoints_.clear();

  custom_paths_.clear();
}

}  // namespace mrs
