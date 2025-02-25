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

#include "graalvm_context_pool.h"
#include "graalvm_common_context.h"
#include "graalvm_javascript_context.h"

#include <memory>
#include <string>
#include <vector>

#include "graalvm_common_context.h"
#include "graalvm_javascript_context.h"

namespace graalvm {

Context_pool::Context_pool(size_t size, GraalVMCommonContext *common_context)
    : m_common_context{common_context} {
  m_pool = std::make_unique<Pool<IGraalVMContext *>>(
      size,
      [this]() -> GraalVMJavaScriptContext * {
        return std::make_unique<GraalVMJavaScriptContext>(m_common_context)
            .release();
      },
      [](IGraalVMContext *ctx) { delete ctx; });
}

Context_pool::~Context_pool() { teardown(); }

std::shared_ptr<Pooled_context> Context_pool::get_context() {
  auto ctx = m_pool->get();

  if (ctx) {
    return std::make_shared<Pooled_context>(this, ctx);
  }

  return {};
}

void Context_pool::release(IGraalVMContext *ctx) {
  if (ctx->got_memory_error()) {
    m_pool->on_memory_error(ctx);
  } else {
    m_pool->release(ctx);
  }
}

}  // namespace graalvm