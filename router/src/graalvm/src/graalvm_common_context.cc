/*
 * Copyright (c) 2024, Oracle and/or its affiliates.
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

#include "router/src/graalvm/include/mysqlrouter/graalvm_common_context.h"

#include <iostream>

#include "mysql/harness/logging/logging.h"
#include "router/src/graalvm/include/mysqlrouter/graalvm_javascript.h"
#include "router/src/graalvm/src/file_system/polyglot_file_system.h"
#include "router/src/graalvm/src/utils/polyglot_utils.h"

namespace graalvm {

GraalVMCommonContext::GraalVMCommonContext(
    const std::shared_ptr<shcore::polyglot::IFile_system> &fs,
    const std::vector<std::string> &module_files,
    const shcore::Dictionary_t &globals)
    : m_file_system{fs}, m_module_files{module_files}, m_globals{globals} {}

void GraalVMCommonContext::fatal_error() {
  // Something really bad happened, so we simply ensure it is indicated on the
  // screen.
  std::cout << "Polyglot: FATAL error occurred" << std::endl;
}

void GraalVMCommonContext::flush() {}

void GraalVMCommonContext::log(const char *bytes, size_t length) {
  mysql_harness::logging::log_debug("Polyglot: %.*s", static_cast<int>(length),
                                    bytes);
}

poly_engine GraalVMCommonContext::create_engine() {
  poly_engine engine = nullptr;

  // NOTE: this commented code is to create the engine through the builder which
  // would give us flexibility over its configuration, i.e. enable tracing of
  // code sharing

  // poly_engine_builder builder;
  // shcore::polyglot::throw_if_error(poly_create_engine_builder, m_thread,
  //                                  nullptr, 0, &builder);

  // shcore::polyglot::throw_if_error(
  //     poly_engine_builder_allow_experimental_options, m_thread, builder,
  //     true);

  // shcore::polyglot::throw_if_error(poly_engine_builder_option, m_thread,
  //                                  builder, "engine.TraceCodeSharing",
  //                                  "true");

  // shcore::polyglot::throw_if_error(poly_engine_builder_option, m_thread,
  //                                  builder, "engine.TraceSourceCache",
  //                                  "true");

  // shcore::polyglot::throw_if_error(poly_engine_builder_option, m_thread,
  //                                  builder, "engine.TraceCompilation",
  //                                  "true");

  // shcore::polyglot::throw_if_error(poly_engine_builder_option, m_thread,
  //                                  builder, "engine.TraceCompilationDetails",
  //                                  "true");

  // shcore::polyglot::throw_if_error(poly_engine_builder_build, m_thread,
  // builder,
  //                                  &engine);

  shcore::polyglot::throw_if_error(poly_create_engine, m_thread, &engine);

  return engine;
}

void GraalVMCommonContext::initialize() {
  Polyglot_common_context::initialize();

  m_base_context = std::make_shared<GraalVMJavaScript>(this);
  m_base_context->initialize(m_file_system);
  for (const auto &module : m_module_files) {
    std::string code = "import('" + module + "')";
    auto source = m_base_context->create_source(module, code);
    m_cached_sources.emplace_back(
        shcore::polyglot::Store(m_base_context->thread(), source));
    poly_value result;
    m_base_context->eval(m_cached_sources.back().get(), &result);
  }
}

void GraalVMCommonContext::finalize() {
  m_cached_sources.clear();
  if (m_base_context) {
    m_base_context->finalize();
    m_base_context.reset();
  }
  Polyglot_common_context::finalize();
}

}  // namespace graalvm