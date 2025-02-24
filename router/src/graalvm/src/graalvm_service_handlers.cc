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

#include <memory>

#include "mysql/harness/logging/logging.h"

namespace graalvm {

IMPORT_LOG_FUNCTIONS()

Graalvm_service_handlers::Graalvm_service_handlers(
    size_t size, const std::shared_ptr<shcore::polyglot::IFile_system> &fs,
    const std::vector<std::string> &module_files,
    const shcore::Dictionary_t &globals,
    const std::vector<std::string> &isolate_args)
    : m_pool_size{size},
      m_fs{fs},
      m_module_files{module_files},
      m_globals{globals},
      m_isolate_args{isolate_args} {}

void Graalvm_service_handlers::init() {
  init_common_context();

  if (m_common_context->start()) {
    m_context_pool =
        std::make_shared<Context_pool>(m_pool_size, m_common_context.get());
  } else {
    throw std::runtime_error(m_common_context->error());
  }
}

void Graalvm_service_handlers::init_common_context() {
  m_common_context = std::make_unique<GraalVMCommonContext>(
      m_fs, m_module_files, m_globals, m_isolate_args);
}

Graalvm_service_handlers::~Graalvm_service_handlers() {
  if (m_teardown_thread) {
    m_teardown_thread->join();
    m_teardown_thread.reset();
  } else {
    do_tear_down();
  }
}

void Graalvm_service_handlers::teardown() {
  m_teardown_thread = std::make_unique<std::thread>(
      &Graalvm_service_handlers::do_tear_down, this);
}

void Graalvm_service_handlers::do_tear_down() {
  m_context_pool->teardown();
  m_context_pool.reset();
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
