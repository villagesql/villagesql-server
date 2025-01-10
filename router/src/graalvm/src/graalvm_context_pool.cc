/*
 * Copyright (c) 2024, 2025, Oracle and/or its affiliates.
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

#include "router/src/graalvm/include/mysqlrouter/graalvm_context_pool.h"

#include <memory>
#include <string>
#include <vector>

#include "mysql/harness/logging/logging.h"
#include "router/src/graalvm/include/mysqlrouter/graalvm_common_context.h"
#include "router/src/graalvm/include/mysqlrouter/graalvm_javascript_context.h"

namespace graalvm {

IMPORT_LOG_FUNCTIONS()

Context_pool::Context_pool(
    size_t size, const std::shared_ptr<shcore::polyglot::IFile_system> &fs,
    const std::vector<std::string> &module_files,
    const shcore::Dictionary_t &globals)
    : m_fs{fs}, m_module_files{module_files}, m_globals{globals} {
  m_common_context =
      std::make_unique<GraalVMCommonContext>(fs, module_files, globals);

  m_common_context->start();

  m_pool = std::make_unique<Pool<IGraalVMContext *>>(
      size,
      [this]() -> GraalVMJavaScriptContext * {
        if (m_common_context->got_fatal_error()) {
          log_error("A fatal error prevents the usage of scripting endpoints");
          return nullptr;
        }

        return std::make_unique<GraalVMJavaScriptContext>(
                   m_common_context.get(), m_fs, m_globals)
            .release();
      },
      [](IGraalVMContext *ctx) { delete ctx; });
}

Context_pool::~Context_pool() {
  teardown();

  m_common_context.reset();
}

std::shared_ptr<Pooled_context> Context_pool::get_context() {
  auto ctx = m_pool->get();

  if (ctx) {
    return std::make_shared<Pooled_context>(this, ctx);
  }

  return {};
}

void Context_pool::release(IGraalVMContext *ctx) { m_pool->release(ctx); }

}  // namespace graalvm