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

#include <memory>
#include <mutex>
#include <string>

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

GraalVMComponent::~GraalVMComponent() { m_service_context_handlers.clear(); }

void GraalVMComponent::stop_debug_context(const std::string &service_id) {
  auto it = m_service_context_handlers.find(service_id);
  if (it != m_service_context_handlers.end()) {
    it->second->release_debug_context();
  }
}

std::shared_ptr<IGraalvm_context_handle> GraalVMComponent::get_context(
    const std::string &service_id, size_t context_pool_size,
    const std::shared_ptr<shcore::polyglot::IFile_system> &fs,
    const std::vector<std::string> &module_files,
    const shcore::Dictionary_t &globals, const std::string &debug_port,
    const std::vector<std::string> &isolate_args, bool reset_context) {
  std::unique_lock<std::mutex> lock(m_context_creation);
  auto it = m_service_context_handlers.find(service_id);

  if (it != m_service_context_handlers.end() && reset_context) {
    it->second->teardown();
    m_inactive_context_handlers.push_back(it->second);
    m_service_context_handlers.erase(it);
    it = m_service_context_handlers.end();
  }

  if (it == m_service_context_handlers.end()) {
    it = m_service_context_handlers
             .emplace(service_id, std::make_shared<Graalvm_service_handlers>(
                                      context_pool_size, fs, module_files,
                                      globals, isolate_args))
             .first;

    it->second->init();
  }

  return it->second->get_context(debug_port);
}

}  // namespace graalvm