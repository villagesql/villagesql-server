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

#include "jit_executor_service_handlers.h"

#include <memory>
#include <vector>

#include "mysql/harness/logging/logging.h"

namespace jit_executor {

IMPORT_LOG_FUNCTIONS()

ServiceHandlers::ServiceHandlers(const ServiceHandlerConfig &config)
    : m_config{config} {}

ServiceHandlers::ServiceHandlers(const ServiceHandlers &other)
    : ServiceHandlers(other.m_config) {}

bool ServiceHandlers::init() {
  init_common_context();

  if (m_common_context->start()) {
    m_context_pool =
        std::make_shared<ContextPool>(pool_size(), m_common_context.get());

    return true;
  }

  return false;
}

void ServiceHandlers::init_common_context() {
  std::vector<std::string> isolate_args;
  // Using a default of 1024 MB if nothing else is configured
  auto max_heap_size = m_config.max_heap_size.value_or(1024);
  isolate_args.push_back("-Xmx" + std::to_string(max_heap_size) + "m");
  m_common_context = std::make_unique<CommonContext>(
      m_config.fs, m_config.module_files, m_config.globals, isolate_args);
}

ServiceHandlers::~ServiceHandlers() {
  if (m_teardown_thread) {
    m_teardown_thread->join();
    m_teardown_thread.reset();
  } else {
    do_tear_down();
  }
}

void ServiceHandlers::teardown() {
  m_teardown_thread =
      std::make_unique<std::thread>(&ServiceHandlers::do_tear_down, this);
}

void ServiceHandlers::do_tear_down() {
  if (m_context_pool) {
    m_context_pool->teardown();
    m_context_pool.reset();
  }
  m_common_context.reset();
}

std::chrono::seconds ServiceHandlers::idle_time() const {
  auto now = std::chrono::system_clock::now();
  auto diff = m_last_used_time - now;
  return std::chrono::duration_cast<std::chrono::seconds>(diff);
}

uint64_t ServiceHandlers::pool_size() const {
  return m_config.pool_size.value_or(m_config.default_pool_size);
}

void ServiceHandlers::set_max_heap_size(uint64_t size) {
  m_config.max_heap_size = size;
}

void ServiceHandlers::set_default_pool_size(uint64_t size) {
  m_config.default_pool_size = size;
}

std::shared_ptr<IContextHandle> ServiceHandlers::get_context(
    const std::string &debug_port) {
  if (m_common_context->got_fatal_error()) {
    throw std::runtime_error(
        "A fatal error prevents the usage of scripting endpoints");
  }

  m_last_used_time = std::chrono::system_clock::now();

  if (debug_port.empty()) {
    return m_context_pool->get_context();
  }

  if (!m_debug_context) {
    m_debug_context = std::make_shared<DebugContextHandle>(
        debug_port, m_common_context.get());
  }

  return m_debug_context;
}

void ServiceHandlers::release_debug_context() { m_debug_context.reset(); }

}  // namespace jit_executor
