/*
 Copyright (c) 2021, 2024, Oracle and/or its affiliates.

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License, version 2.0,
 as published by the Free Software Foundation.

 This program is also distributed with certain software (including
 but not limited to OpenSSL) that is licensed under separate terms,
 as designated in a particular file or component or in included license
 documentation.  The authors of MySQL hereby grant you an additional
 permission to link the program and your derivative works with the
 separately licensed software that they have included with MySQL.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#include "mrs/endpoint/handler/handler_db_object_sp.h"

#include <string>

#include "mysql/harness/logging/logging.h"

#include "helper/container/generic.h"
#include "helper/http/url.h"
#include "helper/json/jvalue.h"
#include "helper/json/rapid_json_iterator.h"
#include "helper/json/text_to.h"
#include "helper/json/to_sqlstring.h"
#include "helper/json/to_string.h"
#include "helper/media_detector.h"
#include "helper/mysql_numeric_value.h"
#include "mrs/database/helper/bind.h"
#include "mrs/database/query_rest_sp.h"
#include "mrs/database/query_rest_sp_media.h"
#include "mrs/endpoint/handler/utilities.h"
#include "mrs/http/error.h"
#include "mrs/rest/request_context.h"
#include "mrs/router_observation_entities.h"

IMPORT_LOG_FUNCTIONS()

namespace mrs {
namespace endpoint {
namespace handler {

namespace {

// CLANG doesn't allow capture, already captured variable.
// Instead using lambda let use class (llvm-issue #48582).
class CompareFieldName {
 public:
  CompareFieldName(const std::string &k) : key_{k} {}

  bool operator()(const mrs::database::entry::Field &f) const {
    return f.name == key_;
  }

 private:
  const std::string &key_;
};

std::string get_endpoint_url(
    std::weak_ptr<mrs::endpoint::DbObjectEndpoint> &wp) {
  auto locked = lock_or_throw_unavail(wp);
  return locked->get_url().join();
}

}  // namespace

using HttpResult = mrs::rest::Handler::HttpResult;
using CachedObject = collector::MysqlCacheManager::CachedObject;
using Url = helper::http::Url;
using Authorization = mrs::rest::Handler::Authorization;

HttpResult HandlerDbObjectSP::handle_delete(
    [[maybe_unused]] rest::RequestContext *ctxt) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

std::string to_string(rapidjson::Value *v) {
  if (v->IsString()) {
    return std::string{v->GetString(), v->GetStringLength()};
  }

  return helper::json::to_string(v);
}

using DataType = mrs::database::entry::ColumnType;

mysqlrouter::sqlstring get_sql_format(DataType type) {
  using namespace helper;
  switch (type) {
    case DataType::BINARY:
      return mysqlrouter::sqlstring("FROM_BASE64(?)");

    case DataType::GEOMETRY:
      return mysqlrouter::sqlstring("ST_GeomFromGeoJSON(?)");

    case DataType::VECTOR:
      return mysqlrouter::sqlstring("STRING_TO_VECTOR(?)");

    case DataType::JSON:
      return mysqlrouter::sqlstring("CAST(? as JSON)");

    default: {
    }
  }

  return mysqlrouter::sqlstring("?");
}

mysqlrouter::sqlstring to_sqlstring(const std::string &value, DataType type) {
  using namespace helper;
  auto v = get_type_inside_text(value);
  switch (type) {
    case DataType::BOOLEAN:
      if (kDataInteger == v) return mysqlrouter::sqlstring{value.c_str()};
      return mysqlrouter::sqlstring("?") << value;

    case DataType::DOUBLE:
      if (kDataString == v) return mysqlrouter::sqlstring("?") << value;
      return mysqlrouter::sqlstring{value.c_str()};

    case DataType::INTEGER:
      if (kDataString == v) return mysqlrouter::sqlstring("?") << value;
      return mysqlrouter::sqlstring{value.c_str()};

    case DataType::BINARY:
      return mysqlrouter::sqlstring("FROM_BASE64(?)") << value;

    case DataType::GEOMETRY:
      return mysqlrouter::sqlstring("ST_GeomFromGeoJSON(?)") << value;

    case DataType::VECTOR:
      return mysqlrouter::sqlstring("STRING_TO_VECTOR(?)") << value;

    case DataType::JSON:
      return mysqlrouter::sqlstring("CAST(? as JSON)") << value;

    case DataType::STRING:
      return mysqlrouter::sqlstring("?") << value;

    case DataType::UNKNOWN:
      // Lets handle it by function return.
      break;
  }

  assert(nullptr && "Shouldn't happen");
  return {};
}

static HttpResult handler_mysqlerror(const mysqlrouter::MySQLSession::Error &e,
                                     database::QueryRestSP *db) {
  static const std::string k_state_with_user_defined_error = "45000";
  if (!db->get_sql_state()) throw e;

  auto sql_state = db->get_sql_state();
  log_debug("While handling SP, received a mysql-error with state: %s",
            sql_state);
  if (k_state_with_user_defined_error != sql_state) {
    throw e;
  }
  // 5000 is the offset for HTTPStatus errors,
  // Still first HTTP status begins with 100 code,
  // because of that we are validating the value
  // not against 5000, but 5100.
  if (e.code() < 5100 || e.code() >= 5600) {
    throw e;
  }
  helper::json::MapObject map{{"message", e.message()}};
  HttpResult::HttpStatus status = e.code() - 5000;
  try {
    HttpStatusCode::get_default_status_text(status);
  } catch (...) {
    throw e;
  }
  auto json = helper::json::to_string(map);
  log_debug("SP - generated custom HTTPstats + message:%s", json.c_str());
  return HttpResult(status, std::move(json), HttpResult::Type::typeJson);
}

HandlerDbObjectSP::HandlerDbObjectSP(
    std::weak_ptr<DbObjectEndpoint> endpoint,
    mrs::interface::AuthorizeManager *auth_manager,
    mrs::GtidManager *gtid_manager, collector::MysqlCacheManager *cache,
    mrs::ResponseCache *response_cache,
    mrs::database::SlowQueryMonitor *slow_monitor)
    : HandlerDbObjectTable{endpoint, auth_manager,   gtid_manager,
                           cache,    response_cache, slow_monitor} {}

HttpResult HandlerDbObjectSP::handle_put(rest::RequestContext *ctxt) {
  using namespace std::string_literals;
  using namespace helper::json::sql;

  auto session = get_session(ctxt, collector::kMySQLConnectionUserdataRW);

  auto url = get_endpoint_url(endpoint_);
  auto &input_buffer = ctxt->request->get_input_buffer();
  auto size = input_buffer.length();
  auto request_body = input_buffer.pop_front(size);

  if (response_cache_) {
    auto entry = response_cache_->lookup_routine(
        ctxt->request->get_uri(),
        {reinterpret_cast<const char *>(request_body.data()),
         request_body.size()});
    if (entry) {
      Counter<kEntityCounterRestReturnedItems>::increment(entry->items);
      return {std::string{entry->data}};
    }
  }
  rapidjson::Document doc;
  doc.Parse(reinterpret_cast<const char *>(request_body.data()),
            request_body.size());

  if (!doc.IsObject()) throw http::Error(HttpStatusCode::BadRequest);

  auto &rs = entry_->fields;
  auto &p = rs.parameters.fields;
  for (auto el : helper::json::member_iterator(doc)) {
    auto key = el.first;
    const database::entry::Field *param;
    if (!helper::container::get_ptr_if(
            p, [key](auto &v) { return v.name == key; }, &param)) {
      throw http::Error(HttpStatusCode::BadRequest,
                        "Not allowed parameter:"s + key);
    }
  }

  mrs::database::MysqlBind binds;
  std::string result;

  for (auto &el : p) {
    if (!result.empty()) result += ",";

    if (ownership_.user_ownership_enforced &&
        (ownership_.user_ownership_column == el.bind_name)) {
      result += to_sqlstring(ctxt->user.user_id).str();
    } else if (el.mode == mrs::database::entry::Field::Mode::modeIn) {
      auto it = doc.FindMember(el.name.c_str());
      if (it != doc.MemberEnd()) {
        mysqlrouter::sqlstring sql = get_sql_format(el.data_type);
        sql << it->value;
        result += sql.str();
      } else {
        result += "NULL";
      }
    } else if (el.mode == mrs::database::entry::Field::Mode::modeOut) {
      binds.fill_mysql_bind_for_out(el.data_type);
      result += "?";
    } else {
      auto it = doc.FindMember(el.name.c_str());
      result += "?";
      if (it != doc.MemberEnd()) {
        binds.fill_mysql_bind_for_inout(it->value, el.data_type);
      } else {
        binds.fill_null_as_inout(el.data_type);
      }
    }
  }

  // Stored procedures may change the state of the SQL session,
  // we need ensure that its going to be reset.
  // Set as dirty, directly before executing queries.
  session.set_dirty();

  database::QueryRestSP db;
  try {
    auto *gtid_manager = get_options().metadata.gtid ? gtid_manager_ : nullptr;

    slow_monitor_->execute(
        [&]() {
          db.query_entries(
              session.get(), schema_entry_->name, entry_->name, url,
              ownership_.user_ownership_column, result.c_str(),
              binds.parameters, rs,
              database::JsonTemplateType::kObjectNestedOutParameters,
              gtid_manager);
        },
        session.get(), get_options().query.timeout);

    Counter<kEntityCounterRestReturnedItems>::increment(db.items);
    Counter<kEntityCounterRestAffectedItems>::increment(
        session->affected_rows());
  } catch (const mysqlrouter::MySQLSession::Error &e) {
    return handler_mysqlerror(e, &db);
  }

  if (response_cache_) {
    auto entry = response_cache_->create_routine_entry(
        url,
        {reinterpret_cast<const char *>(request_body.data()),
         request_body.size()},
        db.response, db.items);
  }

  return {std::move(db.response)};
}

HttpResult HandlerDbObjectSP::handle_post(
    [[maybe_unused]] rest::RequestContext *ctxt,
    [[maybe_unused]] const std::vector<uint8_t> &document) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

HttpResult HandlerDbObjectSP::handle_get(rest::RequestContext *ctxt) {
  using namespace std::string_literals;

  Url::Keys keys;
  Url::Values values;

  auto url = get_endpoint_url(endpoint_);
  auto &requests_uri = ctxt->request->get_uri();
  const auto &query_kv = requests_uri.get_query_elements();

  auto &p = entry_->fields;
  auto &pf = p.parameters.fields;
  for (const auto &[key, _] : query_kv) {
    const database::entry::Field *param;
    CompareFieldName search_for(key);
    if (!helper::container::get_ptr_if(pf, search_for, &param)) {
      throw http::Error(HttpStatusCode::BadRequest,
                        "Not allowed parameter:"s + key);
    }
  }

  std::string result;
  mrs::database::MysqlBind binds;

  for (auto &el : pf) {
    if (!result.empty()) result += ",";

    if (ownership_.user_ownership_enforced &&
        (ownership_.user_ownership_column == el.bind_name)) {
      result += to_sqlstring(ctxt->user.user_id).str();
    } else if (el.mode == mrs::database::entry::Field::Mode::modeIn) {
      auto it = query_kv.find(el.name);
      if (query_kv.end() != it) {
        result += to_sqlstring(it->second, el.data_type).str();
      } else {
        result += "NULL";
      }
    } else if (el.mode == mrs::database::entry::Field::Mode::modeOut) {
      binds.fill_mysql_bind_for_out(el.data_type);
      result += "?";
    } else {
      auto it = query_kv.find(el.name);
      result += "?";
      if (query_kv.end() != it) {
        log_debug("Bind param el.data_type:%i %s", (int)el.data_type,
                  el.raw_data_type.c_str());
        binds.fill_mysql_bind_for_inout(it->second, el.data_type);
      } else {
        binds.fill_null_as_inout(el.data_type);
      }
    }
  }

  auto session = get_session(ctxt, collector::kMySQLConnectionUserdataRW);
  // Stored procedures may change the state of the SQL session,
  // we need ensure that its going to be reset.
  // Set as dirty, directly before executing queries.
  session.set_dirty();

  log_debug("HandlerDbObjectSP::handle_get start format=%i",
            (int)entry_->format);

  if (entry_->format == mrs::database::entry::DbObject::formatFeed) {
    log_debug(
        "HandlerDbObjectSP::handle_get - generating feed "
        "response");
    database::QueryRestSP db;
    try {
      auto *gtid_manager =
          get_options().metadata.gtid ? gtid_manager_ : nullptr;
      slow_monitor_->execute(
          [&]() {
            db.query_entries(
                session.get(), schema_entry_->name, entry_->name, url,
                ownership_.user_ownership_column, result.c_str(),
                binds.parameters, p,
                database::JsonTemplateType::kObjectNestedOutParameters,
                gtid_manager);
          },
          session.get(), get_options().query.timeout);

      Counter<kEntityCounterRestReturnedItems>::increment(db.items);
      Counter<kEntityCounterRestAffectedItems>::increment(
          session->affected_rows());
    } catch (const mysqlrouter::MySQLSession::Error &e) {
      return handler_mysqlerror(e, &db);
    }

    return {std::move(db.response)};
  }

  database::QueryRestSPMedia db;
  slow_monitor_->execute(
      [&]() {
        db.query_entries(session.get(), schema_entry_->name, entry_->name,
                         result.c_str());
      },
      session.get(), get_options().query.timeout);

  Counter<kEntityCounterRestReturnedItems>::increment(db.items);
  Counter<kEntityCounterRestAffectedItems>::increment(session->affected_rows());

  if (entry_->autodetect_media_type) {
    log_debug(
        "HandlerDbObjectSP::handle_get - autodetection "
        "response");
    helper::MediaDetector md;
    auto detected_type = md.detect(db.response);

    return {std::move(db.response), detected_type};
  }

  if (entry_->media_type.has_value()) {
    return {std::move(db.response), entry_->media_type.value()};
  }

  return {std::move(db.response), helper::MediaType::typeUnknownBinary};
}

uint32_t HandlerDbObjectSP::get_access_rights() const {
  return HandlerDbObjectTable::get_access_rights() &
         (mrs::database::entry::Operation::valueRead |
          mrs::database::entry::Operation::valueUpdate);
}

}  // namespace handler
}  // namespace endpoint
}  // namespace mrs
