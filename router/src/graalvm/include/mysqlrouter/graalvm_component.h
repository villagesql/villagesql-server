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

#ifndef ROUTER_SRC_GRAALVM_INCLUDE_MYSQLROUTER_GRAALVM_COMPONENT_H_
#define ROUTER_SRC_GRAALVM_INCLUDE_MYSQLROUTER_GRAALVM_COMPONENT_H_

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "mysqlrouter/graalvm_context_handle.h"
#include "mysqlrouter/graalvm_plugin_export.h"
#include "mysqlrouter/graalvm_value.h"
#include "mysqlrouter/polyglot_file_system.h"

namespace graalvm {

/** Interface defining central location for the handlers associated to a
 * database service
 */
class IGraalvm_service_handlers {
 public:
  virtual ~IGraalvm_service_handlers() = default;
  virtual std::shared_ptr<IGraalvm_context_handle> get_context(
      const std::string &debug_port = "") = 0;

  virtual void release_debug_context() = 0;

  virtual void init() = 0;
  virtual void teardown() = 0;
};

/**
 * Registry of graal contexts to be used by each service.
 *
 * NOTE: The original idea, was to have a pool of contexts on which the service
 * module files would be loaded once and then shared across the contexts in the
 * pool. The script end points would be getting a context from the pool use it
 * and the release it. However, the main pre-requisite for that is that the
 * context could be reset to the original state, which is NOT possible in Graal.
 *
 * Suggestion from the Graal Team
 *
 * By default, each context would internally create an engine which would hold
 * the resources used in the context. However, it is possible to use a common
 * engine to enable the sharing of the resources  across contexts (i.e.
 * including parsed source code). Following this approach the context pool is
 * not needed since we would simply create/release the context on demand and it
 * would use the shared resources from the engine.
 *
 * Even this is the current implementation, expectation was that the module
 * files would be loaded only ONCE but that's not the case, they get reloaded on
 * every created context, even the shared engine is used.
 *
 * This class holds a registry of service ids vs ContextHandlers (who keep the
 * shared engine) and allows creating a context using the shared engine.
 */
class GRAALVM_PLUGIN_EXPORT GraalVMComponent {
 public:
  static GraalVMComponent &get_instance();
  ~GraalVMComponent();

  GraalVMComponent(GraalVMComponent const &) = delete;
  void operator=(GraalVMComponent const &) = delete;

  GraalVMComponent(GraalVMComponent &&) = delete;
  void operator=(GraalVMComponent &&) = delete;

  void stop_debug_context(const std::string &service_id);

  std::shared_ptr<IGraalvm_context_handle> get_context(
      const std::string &service_id, size_t context_pool_size,
      const std::shared_ptr<shcore::polyglot::IFile_system> &fs,
      const std::vector<std::string> &module_files,
      const shcore::Dictionary_t &globals = {},
      const std::string &debug_port = "",
      const std::vector<std::string> &isolate_args = {},
      bool reset_context = false);

 private:
  GraalVMComponent() = default;
  std::mutex m_context_creation;

  std::unordered_map<std::string, std::shared_ptr<IGraalvm_service_handlers>>
      m_service_context_handlers;

  std::vector<std::shared_ptr<IGraalvm_service_handlers>>
      m_inactive_context_handlers;
};

}  // namespace graalvm

#endif  // ROUTER_SRC_GRAALVM_INCLUDE_MYSQLROUTER_GRAALVM_COMPONENT_H_
