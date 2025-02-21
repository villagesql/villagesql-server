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

#ifndef ROUTER_SRC_GRAALVM_SRC_GRAALVM_JAVASCRIPT_CONTEXT_H_
#define ROUTER_SRC_GRAALVM_SRC_GRAALVM_JAVASCRIPT_CONTEXT_H_

#include <string>
#include <vector>

#include "graalvm_common_context.h"
#include "graalvm_javascript.h"
#include "mysqlrouter/graalvm_callbacks.h"
#include "mysqlrouter/graalvm_common.h"
#include "mysqlrouter/graalvm_context.h"
#include "mysqlrouter/graalvm_db_interface.h"

namespace graalvm {

using IFile_system = shcore::polyglot::IFile_system;
using Dictionary_t = shcore::Dictionary_t;

class GraalVMJavaScriptContext : public IGraalVMContext {
 public:
  GraalVMJavaScriptContext(GraalVMCommonContext *common_context,
                           const std::string &debug_port = "");
  ~GraalVMJavaScriptContext() override { m_language->stop(); }

  std::string execute(const std::string &module, const std::string &object,
                      const std::string &function,
                      const std::vector<Value> &parameters, int timeout,
                      ResultType result_type,
                      const Global_callbacks &global_callbacks) override;

 private:
  std::shared_ptr<GraalVMJavaScript> m_language;
};

}  // namespace graalvm

#endif  // ROUTER_SRC_GRAALVM_SRC_GRAALVM_JAVASCRIPT_CONTEXT_H_
