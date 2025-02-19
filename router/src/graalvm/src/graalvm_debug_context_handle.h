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

#ifndef ROUTER_SRC_GRAALVM_INCLUDE_MYSQLROUTER_GRAALVM_DEBUG_CONTEXT_HANDLER_H_
#define ROUTER_SRC_GRAALVM_INCLUDE_MYSQLROUTER_GRAALVM_DEBUG_CONTEXT_HANDLER_H_

#include <memory>

#include "graalvm_common_context.h"
#include "mysqlrouter/graalvm_context_handle.h"
#include "mysqlrouter/polyglot_file_system.h"

namespace graalvm {

class Debug_context_handle : public IGraalvm_context_handle {
 public:
  Debug_context_handle(const std::string &debug_port,
                       GraalVMCommonContext *common_context);

  ~Debug_context_handle() override = default;

  IGraalVMContext *get() override { return m_ctx.get(); }

 private:
  std::unique_ptr<IGraalVMContext> m_ctx;
};

}  // namespace graalvm
#endif  // ROUTER_SRC_GRAALVM_INCLUDE_MYSQLROUTER_GRAALVM_DEBUG_CONTEXT_HANDLER_H_