/*
  Copyright (c) 2024, Oracle and/or its affiliates.

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

#ifndef ROUTER_SRC_GRAALVM_INCLUDE_MYSQLROUTER_GRAALVM_JAVASCRIPT_H_
#define ROUTER_SRC_GRAALVM_INCLUDE_MYSQLROUTER_GRAALVM_JAVASCRIPT_H_

#include <condition_variable>
#include <memory>  // shared_ptr
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "router/src/graalvm/include/mysqlrouter/graalvm_common.h"
#include "router/src/graalvm/src/file_system/polyglot_file_system.h"
#include "router/src/graalvm/src/languages/polyglot_javascript.h"
#include "router/src/graalvm/src/utils/native_value.h"

namespace graalvm {

using Value = shcore::Value;
using ResultType = shcore::ResultType;
using Dictionary_t = shcore::Dictionary_t;
using Polyglot_error = shcore::polyglot::Polyglot_error;
using IFile_system = shcore::polyglot::IFile_system;
/**
 * MRS JavaScript Implementation
 *
 * Starts the JavaScript engine in a thread for execution of code from the
 * MRS end points. A threaded version is needed to support the JavaScript
 * Promise resolution to get the final result.
 *
 * To achieve these two global functions are exposed: synch_return and
 * synch_error, such function would be used on the promise resolution by
 * executing:
 *   promise.then(value => synch_return(value), error=>synch_error(err0r))
 */
class GraalVMJavaScript : public shcore::polyglot::Java_script_interface {
 public:
  using Java_script_interface::Java_script_interface;
  ~GraalVMJavaScript() override = default;

  void start(const std::shared_ptr<IFile_system> &fs = {},
             const Dictionary_t &predefined_globals = {});
  void stop();

  std::string execute(const std::string &code,
                      ResultType result_type = ResultType::Json);

  std::string get_parameter_string(const std::vector<Value> &parameters) const;

  /**
   * Wraps a call to poly_context_eval
   */
  int64_t eval(poly_reference source, poly_value *result) const;

  /**
   * Creates a Source object
   */
  poly_value create_source(const std::string &source,
                           const std::string &code_str) const;

 private:
  void run();

  Value native_object(poly_value object);
  Value native_array(poly_value object);
  Value to_native_object(poly_value object,
                         const std::string &class_name) override;
  void output_handler(const char *bytes, size_t length) override;
  void error_handler(const char *bytes, size_t length) override;

  std::string create_result(const Value &result,
                            const std::string &status = "ok");
  std::string create_result(const shcore::polyglot::Polyglot_error &error);

  // Every global function exposed to JavaScript requires:
  // - The function implementation
  // - The function metadata
  poly_value synch_return(const std::vector<poly_value> &args);
  struct Synch_return {
    static const constexpr char *name = "synch_return";
    static const constexpr std::size_t argc = 1;
    static const constexpr auto callback = &GraalVMJavaScript::synch_return;
  };

  poly_value synch_error(const std::vector<poly_value> &args);
  struct Synch_error {
    static const constexpr char *name = "synch_error";
    static const constexpr std::size_t argc = 1;
    static const constexpr auto callback = &GraalVMJavaScript::synch_error;
  };

  // To control the statement execution, the execution thread will be in wait
  // state until a statement arrives
  std::thread m_execution_thread;
  std::mutex m_process_mutex;
  std::condition_variable m_process_condition;

  // The caller thread will be waiting for the final response to be available
  std::mutex m_result_mutex;
  std::condition_variable m_result_condition;
  bool m_done = false;

  Dictionary_t m_predefined_globals;

  std::optional<std::string> m_code;
  std::optional<std::string> m_result;

  ResultType m_result_type;
};

}  // namespace graalvm

#endif  // ROUTER_SRC_GRAALVM_INCLUDE_MYSQLROUTER_GRAALVM_JAVASCRIPT_H_
