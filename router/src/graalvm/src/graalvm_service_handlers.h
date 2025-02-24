/*
 * Copyright (c) 2025, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is designed to work with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have either included with
 * the program or referenced in the documentation.
 *
 * This program is distributed in the hope that it will be useful,  but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See
 * the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

#ifndef ROUTER_SRC_GRAALVM_SRC_GRAALVM_SERVICE_HANDLERS_H_
#define ROUTER_SRC_GRAALVM_SRC_GRAALVM_SERVICE_HANDLERS_H_

#include "graalvm_context_pool.h"
#include "graalvm_debug_context_handle.h"
#include "mysqlrouter/graalvm_component.h"

#include <memory>
#include <vector>

namespace graalvm {

/**
 * Contains the handlers to be used to attend JavaScript processing requests for
 * a specific service.
 */
class Graalvm_service_handlers : public IGraalvm_service_handlers {
 public:
  Graalvm_service_handlers(
      size_t size, const std::shared_ptr<shcore::polyglot::IFile_system> &fs,
      const std::vector<std::string> &module_files,
      const shcore::Dictionary_t &globals = {},
      const std::vector<std::string> &isolate_args = {});
  ~Graalvm_service_handlers() override;

  std::shared_ptr<IGraalvm_context_handle> get_context(
      const std::string &debug_port = "") override;

  void release_debug_context() override;

  void init() override;
  void teardown() override;

  const std::shared_ptr<shcore::polyglot::IFile_system> &file_system() const {
    return m_fs;
  }

  const std::vector<std::string> &module_files() const {
    return m_module_files;
  }

  const shcore::Dictionary_t &globals() const { return m_globals; }

 private:
  void init_common_context();

  std::unique_ptr<GraalVMCommonContext> m_common_context;
  std::shared_ptr<IGraalvm_context_handle> m_debug_context;
  std::shared_ptr<Context_pool> m_context_pool;

  size_t m_pool_size;
  std::shared_ptr<shcore::polyglot::IFile_system> m_fs;
  std::vector<std::string> m_module_files;
  shcore::Dictionary_t m_globals;
  std::vector<std::string> m_isolate_args;
};

}  // namespace graalvm

#endif  // ROUTER_SRC_GRAALVM_SRC_GRAALVM_SERVICE_HANDLERS_H_