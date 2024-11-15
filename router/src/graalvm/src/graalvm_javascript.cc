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

#include "mysqlrouter/graalvm_javascript.h"

#include <memory>
#include <mutex>
#include <string>

#include "router/src/graalvm/include/mysqlrouter/graalvm_common.h"
#include "router/src/graalvm/src/file_system/polyglot_file_system.h"
#include "router/src/graalvm/src/languages/polyglot_javascript.h"
#include "router/src/graalvm/src/utils/polyglot_utils.h"
#include "router/src/graalvm/src/utils/utils_string.h"

namespace graalvm {

using Value_type = shcore::Value_type;
using Scoped_global = shcore::polyglot::Scoped_global;

std::string GraalVMJavaScript::create_result(const Value &result,
                                             const std::string &status) {
  // If it is a native object, it could well be a wrapper for an language
  // exception, so it is thrown, processed and handled as such
  if (result.get_type() == Value_type::Object) {
    auto object = result.as_object();
    if (object->is_exception()) {
      try {
        object->throw_exception();
      } catch (const Polyglot_error &error) {
        return create_result(error);
      }
    }
  }

  if (m_result_type == ResultType::Json) {
    shcore::JSON_dumper dumper;
    dumper.start_object();
    dumper.append_string("status");
    dumper.append_string(status);
    dumper.append_value("result", result);
    dumper.end_object();

    return dumper.str();
  } else {
    return result.descr(true);
  }
}

std::string GraalVMJavaScript::create_result(const Polyglot_error &error) {
  if (m_result_type == ResultType::Json) {
    shcore::JSON_dumper dumper;
    dumper.start_object();
    dumper.append_string("status");
    dumper.append_string("error");

    dumper.append_string("message");
    dumper.append_string(error.message());

    if (error.type().has_value()) {
      dumper.append_string("type");
      dumper.append_string(*error.type());
    }

    if (error.code().has_value()) {
      dumper.append_string("code");
      dumper.append_int64(*error.code());
    }

    if (error.line().has_value()) {
      dumper.append_string("line");
      dumper.append_int64(*error.line());
    }

    if (error.column().has_value()) {
      dumper.append_string("column");
      dumper.append_int64(*error.column());
    }

    if (!error.backtrace().empty()) {
      dumper.append_string("backtrace");
      dumper.start_array();
      for (const auto &frame : error.backtrace()) {
        dumper.append_string(frame);
      }
      dumper.end_array();
    }

    dumper.end_object();

    return dumper.str();
  } else {
    return error.format(true);
  }
}

void GraalVMJavaScript::start(const std::shared_ptr<IFile_system> &fs,
                              const Dictionary_t &predefined_globals) {
  m_file_system = fs;
  m_predefined_globals = predefined_globals;
  m_execution_thread = std::thread(&GraalVMJavaScript::run, this);
}

void GraalVMJavaScript::stop() {
  {
    std::lock_guard lock(m_process_mutex);
    m_done = true;
  }
  m_process_condition.notify_one();
  m_execution_thread.join();
}

int64_t GraalVMJavaScript::eval(poly_reference source,
                                poly_value *result) const {
  return poly_context_eval_source(thread(), context(), source, result);
}

poly_value GraalVMJavaScript::create_source(const std::string &source,
                                            const std::string &code_str) const {
  poly_value builder;
  shcore::polyglot::throw_if_error(poly_create_source_builder, thread(),
                                   get_language_id(), source.c_str(),
                                   code_str.c_str(), &builder);

  shcore::polyglot::throw_if_error(poly_source_builder_set_mime_type, thread(),
                                   builder, "application/javascript+module");

  poly_value poly_source;
  shcore::polyglot::throw_if_error(poly_source_builder_build, thread(), builder,
                                   &poly_source);

  return poly_source;
}

void GraalVMJavaScript::run() {
  initialize(m_file_system);

  for (const auto &it : (*m_predefined_globals)) {
    set_global(it.first, it.second);
  }

  set_global_function(
      "synch_return",
      shcore::polyglot::polyglot_handler_fixed_args<GraalVMJavaScript,
                                                    Synch_return>,
      this);

  set_global_function(
      "synch_error",
      shcore::polyglot::polyglot_handler_fixed_args<GraalVMJavaScript,
                                                    Synch_error>,
      this);

  while (!m_done) {
    std::unique_lock<std::mutex> lock(m_process_mutex);
    m_process_condition.wait(lock,
                             [this]() { return m_code.has_value() || m_done; });

    if (m_done) {
      continue;
    }

    m_result.reset();
    poly_value result = nullptr;

    try {
      // TODO(rennox): Should we specify a different source?
      if (const auto rc =
              Java_script_interface::eval("(internal)", *m_code, &result);
          rc != poly_ok) {
        throw Polyglot_error(thread(), rc);
      }

      // If it is a Promise, it needs to be resolved and the value will come
      // from synch_return
      if (std::string class_name;
          result && is_object(result, &class_name) && class_name == "Promise") {
        Scoped_global resolver(this, result);
        resolver.execute(
            "<<global>>.then(value=> synch_return(value), error => "
            "synch_error(error))");
      } else {
        {
          std::lock_guard lock(m_result_mutex);
          m_result = create_result(convert(result));
        }
        m_result_condition.notify_one();
      }
    } catch (const Polyglot_error &error) {
      {
        std::lock_guard lock(m_result_mutex);
        m_result = create_result(error);
      }
      m_result_condition.notify_one();
    }

    m_code.reset();
  }

  finalize();
}

Value GraalVMJavaScript::native_array(poly_value object) {
  int64_t array_size{0};
  if (const auto rc = poly_value_get_array_size(thread(), object, &array_size);
      rc != poly_ok) {
    throw Polyglot_error(thread(), rc);
  }

  auto narray = std::make_shared<Value::Array_type>();
  narray->resize(array_size);

  for (int32_t c = array_size, i = 0; i < c; i++) {
    poly_value item;
    if (const auto rc =
            poly_value_get_array_element(thread(), object, i, &item);
        rc != poly_ok) {
      throw Polyglot_error(thread(), rc);
    }

    (*narray)[i] = convert(item);
  }

  return Value(std::move(narray));
}

Value GraalVMJavaScript::native_object(poly_value object) {
  auto keys = shcore::polyglot::get_member_keys(thread(), context(), object);

  auto dict = shcore::make_dict();
  for (const auto &key : keys) {
    poly_value value;
    if (auto rc = poly_value_get_member(thread(), object, key.c_str(), &value);
        rc != poly_ok) {
      throw Polyglot_error(thread(), rc);
    }

    dict->set(key, convert(value));
  }

  return Value(std::move(dict));
}

Value GraalVMJavaScript::to_native_object(poly_value object,
                                          const std::string &class_name) {
  if (class_name == "Array") {
    return native_array(object);
  } else if (class_name == "Object") {
    return native_object(object);
    // } else if (class_name == "Function") {
    //   return native_function(object);
  } else if (class_name == "Error") {
    poly_value poly_cause;
    if (auto rc = poly_value_get_member(thread(), object, "cause", &poly_cause);
        rc != poly_ok) {
      throw Polyglot_error(thread(), rc);
    }

    Value cause = convert(poly_cause);

    if (!cause.is_null() && cause.get_type() != Value_type::Map) {
      poly_value poly_message;
      if (const auto rc =
              poly_value_get_member(thread(), object, "message", &poly_message);
          rc != poly_ok) {
        throw Polyglot_error(thread(), rc);
      }
      cause = convert(poly_message);
    }

    return cause;
  }

  return Java_script_interface::to_native_object(object, class_name);
}

void GraalVMJavaScript::output_handler(const char *bytes, size_t length) {
  std::cout << std::string(bytes, length);
}

void GraalVMJavaScript::error_handler(const char *bytes, size_t length) {
  std::cerr << std::string(bytes, length);
}

std::string GraalVMJavaScript::get_parameter_string(
    const std::vector<Value> &parameters) const {
  std::string parameter_string;

  for (const auto &param : parameters) {
    if (!parameter_string.empty()) {
      parameter_string += ",";
    }

    switch (param.get_type()) {
      case Value_type::Undefined:
        parameter_string += "undefined";
        break;
      case Value_type::Null:
        parameter_string += "null";
        break;
        // TODO(rennox): Binary conversion...
      case Value_type::String:
        parameter_string += shcore::quote_string(param.descr(), '`');
        break;
      default:
        parameter_string += param.descr();
    }
  }

  return parameter_string;
}

std::string GraalVMJavaScript::execute(const std::string &code,
                                       ResultType result_type) {
  {
    std::lock_guard lock(m_process_mutex);
    m_result_type = result_type;
    m_code = code;
  }
  m_process_condition.notify_one();

  std::unique_lock<std::mutex> lock(m_result_mutex);
  m_result_condition.wait(lock);
  return *m_result;
}

poly_value GraalVMJavaScript::synch_return(
    const std::vector<poly_value> &args) {
  std::string class_name;

  // If we got a promise (i.e. chained one) we wait for it to be resolved too
  if (args[0] && is_object(args[0], &class_name) && class_name == "Promise") {
    Scoped_global resolver(this, args[0]);
    resolver.execute(
        "<<global>>.then(value=> synch_return(value), error => "
        "synch_error(error))");
  } else {
    try {
      std::lock_guard lock(m_result_mutex);
      // If we got a module, it is automatically resolved as a Polyglot_object
      // i.e. import('<module-path>')
      if (class_name == "[object Module]") {
        m_result = create_result(to_native_object(args[0], class_name));
      } else {
        m_result = create_result(convert(args[0]));
      }
    } catch (const Polyglot_error &error) {
      m_result = create_result(error);
    } catch (const std::exception &error) {
      m_result = create_result(Value(error.what()), "error");
    }

    m_result_condition.notify_one();
  }

  return nullptr;
}

poly_value GraalVMJavaScript::synch_error(const std::vector<poly_value> &args) {
  std::lock_guard lock(m_result_mutex);
  try {
    m_result = create_result(convert(args[0]), "error");
  } catch (const Polyglot_error &error) {
    m_result = create_result(error);
  } catch (const std::exception &error) {
    m_result = create_result(Value(error.what()), "error");
  }
  m_result_condition.notify_one();

  return nullptr;
}

}  // namespace graalvm