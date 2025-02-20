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

#include "graalvm_service_handlers.h"

#include "mysql/harness/logging/logging.h"

namespace graalvm {

IMPORT_LOG_FUNCTIONS()

Graalvm_service_handlers::Graalvm_service_handlers(
    size_t size, const std::shared_ptr<shcore::polyglot::IFile_system> &fs,
    const std::vector<std::string> &module_files,
    const shcore::Dictionary_t &globals,
    const std::vector<std::string> &isolate_args)
    : m_fs{fs}, m_module_files{module_files}, m_globals{globals} {
  m_common_context = std::make_unique<GraalVMCommonContext>(
      m_fs, m_module_files, m_globals, isolate_args);

  if (m_common_context->start()) {
    m_context_pool =
        std::make_shared<Context_pool>(size, m_common_context.get());
  } else {
    throw std::runtime_error(m_common_context->error());
  }
}

Graalvm_service_handlers::~Graalvm_service_handlers() {
  m_common_context.reset();
}

std::shared_ptr<IGraalvm_context_handle> Graalvm_service_handlers::get_context(
    const std::string &debug_port) {
  if (m_common_context->got_fatal_error()) {
    log_error("A fatal error prevents the usage of scripting endpoints");
    return nullptr;
  }

  if (debug_port.empty()) {
    return m_context_pool->get_context();
  }

  if (!m_debug_context) {
    m_debug_context = std::make_shared<Debug_context_handle>(
        debug_port, m_common_context.get());
  }

  return m_debug_context;
}

void Graalvm_service_handlers::release_debug_context() {
  m_debug_context.reset();
}

}  // namespace graalvm
