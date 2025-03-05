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

#include "jit_executor_javascript.h"

#include <array>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "include/my_thread.h"
#include "languages/polyglot_javascript.h"
#include "mysql/harness/logging/logging.h"
#include "mysqlrouter/jit_executor_common.h"
#include "mysqlrouter/jit_executor_db_interface.h"
#include "mysqlrouter/polyglot_file_system.h"
#include "objects/polyglot_date.h"
#include "objects/polyglot_session.h"
#include "utils/polyglot_error.h"
#include "utils/polyglot_utils.h"
#include "utils/utils_general.h"
#include "utils/utils_string.h"

namespace jit_executor {

IMPORT_LOG_FUNCTIONS()

using shcore::polyglot::Date;
using Value_type = shcore::Value_type;
using Scoped_global = shcore::polyglot::Scoped_global;

void JavaScript::create_result(const Value &result, const std::string &status) {
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

    std::lock_guard lock(m_result_mutex);
    m_result = dumper.str();
  } else {
    std::lock_guard lock(m_result_mutex);
    m_result = result.descr(true);
  }
  m_result_condition.notify_one();
}

void JavaScript::create_result(const Polyglot_error &error) {
  std::string result;
  if (shcore::str_beginswith(error.message(),
                             "Garbage-collected heap size exceeded")) {
    m_memory_error = true;
  } else {
    m_is_error = true;
  }
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

    std::lock_guard lock(m_result_mutex);
    m_result = dumper.str();
  } else {
    std::lock_guard lock(m_result_mutex);
    m_result = error.format(true);
  }
  m_result_condition.notify_one();
}

void JavaScript::start(const std::shared_ptr<IFile_system> &fs,
                       const Dictionary_t &predefined_globals) {
  m_file_system = fs;
  m_predefined_globals = predefined_globals;
  m_execution_thread = std::thread(&JavaScript::run, this);
  std::unique_lock<std::mutex> lock(m_process_mutex);
  m_process_condition.wait(lock, [this]() {
    return m_initialized || !m_initialization_error.empty();
  });

  if (!m_initialization_error.empty()) {
    throw std::runtime_error(m_initialization_error);
  }
}

void JavaScript::stop() {
  {
    std::lock_guard lock(m_process_mutex);
    m_done = true;
  }
  m_process_condition.notify_one();
  m_execution_thread.join();
}

int64_t JavaScript::eval(poly_reference source, poly_value *result) const {
  return poly_context_eval_source(thread(), context(), source, result);
}

poly_value JavaScript::create_source(const std::string &source,
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

void JavaScript::run() {
  my_thread_self_setname("JitExecutor-Proc");
  try {
    initialize(m_file_system);
    m_initialized = true;

    if (m_predefined_globals) {
      for (const auto &it : (*m_predefined_globals)) {
        set_global(it.first, it.second);
      }
    }

    set_global_function(
        "synch_return",
        shcore::polyglot::polyglot_handler_fixed_args<JavaScript, Synch_return>,
        this);

    set_global_function(
        "synch_error",
        shcore::polyglot::polyglot_handler_fixed_args<JavaScript, Synch_error>,
        this);

    set_global_function(
        "getSession",
        shcore::polyglot::native_handler_variable_args<JavaScript, Get_session>,
        this);

    set_global_function(
        "getCurrentMrsUserId",
        shcore::polyglot::polyglot_handler_no_args<JavaScript,
                                                   Get_current_mrs_user_id>,
        this);

    set_global_function(
        "getContentSetPath",
        shcore::polyglot::native_handler_fixed_args<JavaScript,
                                                    Get_content_set_path>,
        this);

    if (const auto rc = Java_script_interface::eval(
            "(internal)::resolver",
            R"(new Function ("prom", "prom.then(value => synch_return(value)).catch(error => synch_error(error));");)",
            &m_promise_resolver);
        rc != poly_ok) {
      throw Polyglot_error(thread(), rc);
    }
  } catch (const Polyglot_error &err) {
    m_done = true;
    m_initialization_error = err.format(true);
  }

  m_process_condition.notify_one();

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
      if (const auto rc =
              Java_script_interface::eval("(internal)", *m_code, &result);
          rc != poly_ok) {
        throw Polyglot_error(thread(), rc);
      }

      // If it is a Promise, it needs to be resolved and the value will come
      // from synch_return
      if (std::string class_name;
          result && is_object(result, &class_name) && class_name == "Promise") {
        resolve_promise(result);
      } else {
        create_result(convert(result));
      }
    } catch (const Polyglot_error &error) {
      create_result(error);
    } catch (const std::exception &error) {
      create_result(Value(error.what()), "error");
    }

    m_code.reset();
  }

  if (m_initialized) {
    finalize();
  }
}

Value JavaScript::native_array(poly_value object) {
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

Value JavaScript::native_object(poly_value object) {
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

Value JavaScript::to_native_object(poly_value object,
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

void JavaScript::output_handler(const char *bytes, size_t length) {
  log_info("%.*s", static_cast<int>(length), bytes);
}

void JavaScript::error_handler(const char *bytes, size_t length) {
  log_error("%.*s", static_cast<int>(length), bytes);
}

poly_value JavaScript::from_native_object(const Object_bridge_t &object) const {
  poly_value result = nullptr;
  if (object && object->class_name() == "Date") {
    std::shared_ptr<Date> date = std::static_pointer_cast<Date>(object);

    // 0 date values can come from MySQL but they're not supported by the JS
    // Date object, so we convert them to null
    if (date->has_date() && date->get_year() == 0 && date->get_month() == 0 &&
        date->get_day() == 0) {
      shcore::polyglot::throw_if_error(poly_create_null, thread(), context(),
                                       &result);
    } else if (!date->has_date()) {
      // there's no Time object in JS and we can't use Date to represent time
      // only
      std::string t;
      result = poly_string(date->append_descr(t));
    } else {
      auto source = shcore::str_format(
          "new Date(%d, %d, %d, %d, %d, %d, %d)", date->get_year(),
          date->get_month() - 1, date->get_day(), date->get_hour(),
          date->get_min(), date->get_sec(), date->get_usec() / 1000);

      Scoped_global builder(this);
      result = builder.execute(source);
    }
  }

  return result;
}

std::string JavaScript::get_parameter_string(
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

std::string JavaScript::execute(const std::string &code, int timeout,
                                ResultType result_type,
                                const GlobalCallbacks &global_callbacks) {
  clear_is_terminating();

  auto ms_timeout = std::chrono::milliseconds{timeout};

  // Ensures a clean result...
  m_result.reset();
  m_is_error = false;

  {
    m_global_callbacks = &global_callbacks;
    std::lock_guard lock(m_process_mutex);
    m_result_type = result_type;
    m_code = code;
  }

  shcore::Scoped_callback clean_resources([this]() {
    if (m_session) {
      m_session->reset();
    }
    m_session.reset();
    m_global_callbacks = nullptr;
  });

  m_process_condition.notify_one();

  {
    std::unique_lock<std::mutex> lock(m_result_mutex);

    if (!m_debug_port.empty()) {
      m_result_condition.wait(lock, [this]() { return m_result.has_value(); });
      if (m_memory_error) {
        throw MemoryError(*m_result);
      } else if (m_is_error) {
        throw std::runtime_error(*m_result);
      }
      return *m_result;
    } else {
      if (m_result_condition.wait_for(
              lock, ms_timeout, [this]() { return m_result.has_value(); })) {
        if (m_memory_error) {
          throw MemoryError(*m_result);
        } else if (m_is_error) {
          throw std::runtime_error(*m_result);
        }
        return *m_result;
      }
    }
  }

  // Timeout ocurred, the termination logic should be executed
  if (global_callbacks.interrupt) {
    global_callbacks.interrupt();
  }
  terminate();

  // Now we wait for the termination to be complete
  std::unique_lock<std::mutex> lock(m_result_mutex);
  m_result_condition.wait(lock, [this]() { return m_result.has_value(); });

  throw TimeoutError("Timeout");
}

poly_value JavaScript::synch_return(const std::vector<poly_value> &args) {
  std::string class_name;

  // If we got a promise (i.e. chained one) we wait for it to be resolved too
  if (args[0] && is_object(args[0], &class_name) && class_name == "Promise") {
    resolve_promise(args[0]);
  } else {
    try {
      // If we got a module, it is automatically resolved as a Polyglot_object
      // i.e. import('<module-path>')
      if (class_name == "[object Module]") {
        create_result(to_native_object(args[0], class_name));
      } else {
        create_result(convert(args[0]));
      }
    } catch (const Polyglot_error &error) {
      create_result(error);
    } catch (const std::exception &error) {
      create_result(Value(error.what()), "error");
    }
  }

  return nullptr;
}

poly_value JavaScript::synch_error(const std::vector<poly_value> &args) {
  try {
    create_result(convert(args[0]), "error");
  } catch (const Polyglot_error &error) {
    create_result(error);
  } catch (const std::exception &error) {
    create_result(Value(error.what()), "error");
  }

  return nullptr;
}

void JavaScript::resolve_promise(poly_value promise) {
  std::array<poly_value, 1> args{promise};

  if (const auto rc = poly_value_execute(thread(), m_promise_resolver,
                                         args.data(), args.size(), nullptr);
      rc != poly_ok) {
    throw Polyglot_error(thread(), rc);
  }
}

shcore::Value JavaScript::get_session(const std::vector<shcore::Value> &args) {
  assert(m_global_callbacks->get_session);

  std::lock_guard lock(m_result_mutex);
  try {
    bool read_only = true;
    if (args.size() > 1) {
      throw std::runtime_error(shcore::str_format(
          "getSession(bool readOnly) takes up to 1 argument"));
    } else if (!args.empty()) {
      read_only = args[0].as_bool();
    }

    return shcore::Value(std::make_shared<shcore::polyglot::Session>(
        m_global_callbacks->get_session(read_only)));

  } catch (const Polyglot_error &error) {
    create_result(error);
  } catch (const std::exception &error) {
    create_result(Value(error.what()), "error");
  }
  m_result_condition.notify_one();

  return shcore::Value();
}

poly_value JavaScript::get_current_mrs_user_id() {
  assert(m_global_callbacks->get_current_mrs_user_id);

  std::lock_guard lock(m_result_mutex);
  try {
    std::optional<std::string> user_id;
    user_id = m_global_callbacks->get_current_mrs_user_id();

    if (user_id.has_value()) {
      return convert(Value(*user_id));
    } else {
      return undefined();
    }
  } catch (const Polyglot_error &error) {
    create_result(error);
  } catch (const std::exception &error) {
    create_result(Value(error.what()), "error");
  }
  m_result_condition.notify_one();

  return nullptr;
}

shcore::Value JavaScript::get_content_set_path(
    const std::vector<shcore::Value> &args) {
  assert(m_global_callbacks->get_content_set_path);

  return shcore::Value(
      m_global_callbacks->get_content_set_path(args[0].as_string()));
}

}  // namespace jit_executor