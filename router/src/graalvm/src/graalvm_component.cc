/*
  Copyright (c) 2024, 2025, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is designed to work with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have either included with
  the program or referenced in the documentation.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#include "mysqlrouter/graalvm_component.h"

#include "my_rapidjson_size_t.h"

#include <rapidjson/document.h>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "graalvm_common_context.h"
#include "graalvm_javascript_context.h"
#include "graalvm_service_handlers.h"
#include "mysqlrouter/graalvm_context_handle.h"
#include "mysqlrouter/polyglot_file_system.h"
#include "utils/polyglot_utils.h"

namespace graalvm {

GraalVMComponent &GraalVMComponent::get_instance() {
  static GraalVMComponent instance;

  return instance;
}

GraalVMComponent::~GraalVMComponent() {
  m_inactive_context_handlers.clear();
  m_service_context_handlers.clear();
}

namespace {
std::optional<uint64_t> get_value(const rapidjson::Value &value,
                                  const char *name) {
  if (value.HasMember(name) && value[name].IsUint64()) {
    return value[name].GetUint64();
  }

  return {};
}

}  // namespace

void GraalVMComponent::update_global_config(const std::string &options) {
  Global_config config;
  if (!options.empty()) {
    rapidjson::Document doc;
    doc.Parse(options.data(), options.size());
    if (doc.IsObject() && doc.HasMember("jitExecutor")) {
      const auto &jit_executor = doc["jitExecutor"];
      if (jit_executor.IsObject()) {
        config.maximum_ram_size = get_value(jit_executor, "maximumRamUsage");
        config.maximum_idle_time = get_value(jit_executor, "maximumIdleTime");
        config.default_pool_size = get_value(jit_executor, "defaultPoolSize");
      }
    }
  }

  if (m_global_config != config) {
    std::unique_lock<std::mutex> lock(m_context_creation);
    m_global_config = config;
    update_active_contexts();
  }
}

void GraalVMComponent::stop_debug_context(const std::string &service_id) {
  auto it = m_service_context_handlers.find(service_id);
  if (it != m_service_context_handlers.end()) {
    it->second->release_debug_context();
  }
}

void GraalVMComponent::update_active_contexts(
    const std::pair<std::string, std::shared_ptr<IGraalvm_service_handlers>>
        &replacement) {
  auto get_pool_size =
      [this](const std::shared_ptr<IGraalvm_service_handlers> &handler) {
        return handler->pool_size().value_or(
            m_global_config.default_pool_size.value_or(8));
      };

  std::unordered_map<std::string, std::shared_ptr<IGraalvm_service_handlers>>
      all_context_handlers = std::move(m_service_context_handlers);

  uint64_t total_pool = 0;
  for (const auto &it : all_context_handlers) {
    // Adds the existing context handler to be discarded
    it.second->teardown();
    m_inactive_context_handlers.push_back(it.second);

    // Creates an updated context handler if applicable
    if (replacement.first != it.first &&
        (!m_global_config.maximum_idle_time.has_value() ||
         static_cast<uint64_t>(it.second->idle_time().count()) <
             *m_global_config.maximum_idle_time)) {
      // Adds to the global count of pool items
      total_pool += get_pool_size(it.second);

      // Creates a new handler from the existing one
      auto source_handler =
          std::dynamic_pointer_cast<Graalvm_service_handlers>(it.second);
      m_service_context_handlers.emplace(
          it.first,
          std::make_shared<Graalvm_service_handlers>(*source_handler.get()));
    }
  }

  // Adds the replacement handler to the total pool size
  if (!replacement.first.empty()) {
    total_pool += get_pool_size(replacement.second);

    m_service_context_handlers.emplace(std::move(replacement));
  }

  // Now updates the memory limit for each active handler and starts it
  if (m_global_config.maximum_ram_size.has_value()) {
    uint64_t mem_per_pool_item = *m_global_config.maximum_ram_size / total_pool;

    for (const auto &it : m_service_context_handlers) {
      it.second->set_max_heap_size(mem_per_pool_item *
                                   get_pool_size(it.second));
    }
  }

  for (const auto &it : m_service_context_handlers) {
    it.second->init();
  }
}

std::shared_ptr<IGraalvm_context_handle> GraalVMComponent::get_context(
    const std::string &service_id, const Graalvm_service_handler_config &config,
    const std::string &debug_port, bool reset_context) {
  std::unique_lock<std::mutex> lock(m_context_creation);

  while (true) {
    try {
      auto it = m_service_context_handlers.find(service_id);
      if (it == m_service_context_handlers.end() || reset_context) {
        update_active_contexts(
            {service_id, std::make_shared<Graalvm_service_handlers>(config)});

        return m_service_context_handlers.at(service_id)
            ->get_context(debug_port);
      }

      return it->second->get_context(debug_port);
    } catch (const std::runtime_error &) {
      // If failed to create a context, then let's try re-creating the whole
      // pool, if this failed on a brand new pool, then there's nothing else to
      // be done
      if (!reset_context) {
        reset_context = true;
      } else {
        break;
      }
    }
  }

  return {};
}

}  // namespace graalvm