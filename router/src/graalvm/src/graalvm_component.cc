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

#include "router/src/graalvm/include/mysqlrouter/graalvm_common_context.h"
#include "router/src/graalvm/include/mysqlrouter/graalvm_javascript_context.h"
#include "router/src/graalvm/src/file_system/polyglot_file_system.h"
#include "router/src/graalvm/src/utils/polyglot_utils.h"

namespace graalvm {

GraalVMComponent &GraalVMComponent::get_instance() {
  static GraalVMComponent instance;

  return instance;
}

GraalVMComponent::~GraalVMComponent() { m_service_context_handlers.clear(); }

std::shared_ptr<Pooled_context> GraalVMComponent::get_context(
    const std::string &service_id, size_t context_pool_size,
    const std::shared_ptr<shcore::polyglot::IFile_system> &fs,
    const std::vector<std::string> &module_files,
    const shcore::Dictionary_t &globals) {
  std::unique_lock<std::mutex> lock(m_context_creation);
  auto it = m_service_context_handlers.find(service_id);
  if (it != m_service_context_handlers.end()) {
    return it->second->get_context();
  }

  it = m_service_context_handlers
           .emplace(service_id,
                    std::make_shared<Context_pool>(context_pool_size, fs,
                                                   module_files, globals))
           .first;

  return it->second->get_context();
}

}  // namespace graalvm