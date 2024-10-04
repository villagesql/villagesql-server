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

#include "mrs/endpoint/handler/handler_db_object_function.h"

#include <string>

#include "helper/container/generic.h"
#include "helper/json/jvalue.h"
#include "helper/json/to_string.h"
#include "helper/media_detector.h"
#include "mrs/database/helper/sp_function_query.h"
#include "mrs/database/query_rest_function.h"
#include "mrs/http/error.h"
#include "mrs/monitored/gtid_functions.h"
#include "mrs/rest/request_context.h"
#include "mrs/router_observation_entities.h"

#include "mysql/harness/logging/logging.h"

IMPORT_LOG_FUNCTIONS()

namespace mrs {
namespace endpoint {
namespace handler {

using HttpResult = mrs::rest::Handler::HttpResult;
using CachedObject = collector::MysqlCacheManager::CachedObject;
using Url = helper::http::Url;
using Authorization = mrs::rest::Handler::Authorization;

// static std::string to_string(
//    const std::string &value,
//    mrs::database::entry::Parameter::ParameterDataType dt) {
//  switch (dt) {
//    case mrs::database::entry::Parameter::parameterString: {
//      rapidjson::Document d;
//      d.SetString(value.c_str(), value.length());
//      return helper::json::to_string(&d);
//    }
//    case mrs::database::entry::Parameter::parameterInt:
//      return value;
//    case mrs::database::entry::Parameter::parameterDouble:
//      return value;
//    case mrs::database::entry::Parameter::parameterBoolean:
//      return value;
//    case mrs::database::entry::Parameter::parameterLong:
//      return value;
//    case mrs::database::entry::Parameter::parameterTimestamp: {
//      rapidjson::Document d;
//      d.SetString(value.c_str(), value.length());
//      return helper::json::to_string(&d);
//    }
//    default:
//      return "";
//  }
//
//  return "";
//}
static CachedObject get_session(::mysqlrouter::MySQLSession *,
                                collector::MysqlCacheManager *cache_manager) {
  //  if (session) return CachedObject(nullptr, session);

  return cache_manager->get_instance(collector::kMySQLConnectionUserdataRW,
                                     false);
}

HandlerDbObjectFunction::HandlerDbObjectFunction(
    std::weak_ptr<DbObjectEndpoint> endpoint,
    mrs::interface::AuthorizeManager *auth_manager,
    mrs::GtidManager *gtid_manager, collector::MysqlCacheManager *cache,
    mrs::ResponseCache *response_cache, int64_t cache_ttl_ms)
    : HandlerDbObjectTable{endpoint, auth_manager,   gtid_manager,
                           cache,    response_cache, cache_ttl_ms} {}

HttpResult HandlerDbObjectFunction::handle_delete(
    [[maybe_unused]] rest::RequestContext *ctxt) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

static HttpResult handler_mysqlerror(const mysqlrouter::MySQLSession::Error &e,
                                     database::QueryRestFunction *db) {
  static const std::string k_state_with_user_defined_error = "45000";
  if (!db->get_sql_state()) throw e;

  auto sql_state = db->get_sql_state();
  log_debug("While handling a function, received a mysql-error with state: %s",
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
  log_debug("Function - generated custom HTTPstats + message:%s", json.c_str());
  return HttpResult(status, std::move(json), HttpResult::Type::typeJson);
}

HttpResult HandlerDbObjectFunction::handle_put(
    [[maybe_unused]] rest::RequestContext *ctxt) {
  using namespace std::string_literals;

  auto session = get_session(ctxt->sql_session_cache.get(), cache_);
  auto &input_buffer = ctxt->request->get_input_buffer();
  // TODO(lkotula): New api doesn't have inputbuffer, it has string (Shouldn't
  // be in review)
  auto data = input_buffer.pop_front(input_buffer.length());

  if (response_cache_) {
    auto entry = response_cache_->lookup_routine(
        ctxt->request->get_uri(),
        {reinterpret_cast<const char *>(data.data()), data.size()});
    if (entry) {
      Counter<kEntityCounterRestReturnedItems>::increment(entry->items);
      if (entry->media_type.has_value())
        return {std::string{entry->data}, entry->media_type.value()};
      else if (entry->media_type_str.has_value())
        return {std::string{entry->data}, entry->media_type_str.value()};
      else
        return {std::string{entry->data}};
    }
  }

  auto obj = entry_->object_description;
  auto values = database::create_function_argument_list(
      obj.get(), data, ownership_,
      ctxt->user.has_user_id ? &ctxt->user.user_id : nullptr);

  // Stored procedures may change the state of the SQL session,
  // we need ensure that its going to be reset.
  // Set as dirty, directly before executing queries.
  session.set_dirty();

  log_debug("HandlerDbObjectFunction::handle_put start format=%i",
            (int)entry_->format);

  database::QueryRestFunction db;
  try {
    if (entry_->format != mrs::database::entry::DbObject::formatMedia) {
      db.query_entries(session.get(), obj, values);

      Counter<kEntityCounterRestReturnedItems>::increment(db.items);
      Counter<kEntityCounterRestAffectedItems>::increment(
          session->affected_rows());

      database::QueryRestFunction::CustomMetadata custom_metadata;
      if (get_options().metadata.gtid && gtid_manager_) {
        auto gtid =
            mrs::monitored::get_session_tracked_gtids_for_metadata_response(
                session.get(), gtid_manager_);
        if (!gtid.empty()) {
          custom_metadata["gtid"] = gtid;
        }
      }
      db.serialize_response(custom_metadata);

      if (response_cache_) {
        auto entry = response_cache_->create_routine_entry(
            ctxt->request->get_uri(),
            {reinterpret_cast<const char *>(data.data()), data.size()},
            db.response, db.items);
      }

      return {std::move(db.response)};
    }

    db.query_raw(session.get(), obj, values);

    Counter<kEntityCounterRestReturnedItems>::increment(db.items);
    Counter<kEntityCounterRestAffectedItems>::increment(
        session->affected_rows());
  } catch (const mysqlrouter::MySQLSession::Error &e) {
    return handler_mysqlerror(e, &db);
  }

  if (entry_->autodetect_media_type) {
    log_debug("HandlerDbObjectFunction::handle_get - autodetection response");
    helper::MediaDetector md;
    auto detected_type = md.detect(db.response);

    if (response_cache_) {
      auto entry = response_cache_->create_routine_entry(
          ctxt->request->get_uri(),
          {reinterpret_cast<const char *>(data.data()), data.size()},
          db.response, db.items, detected_type);
    }

    return {std::move(db.response), detected_type};
  }

  if (entry_->media_type.has_value()) {
    if (response_cache_) {
      response_cache_->create_routine_entry(
          ctxt->request->get_uri(),
          {reinterpret_cast<const char *>(data.data()), data.size()},
          db.response, db.items, entry_->media_type.value());
    }

    return {std::move(db.response), entry_->media_type.value()};
  }

  if (response_cache_) {
    auto entry = response_cache_->create_routine_entry(
        ctxt->request->get_uri(),
        {reinterpret_cast<const char *>(data.data()), data.size()}, db.response,
        db.items, helper::MediaType::typeUnknownBinary);
  }

  return {std::move(db.response), helper::MediaType::typeUnknownBinary};
}

HttpResult HandlerDbObjectFunction::handle_post(
    [[maybe_unused]] rest::RequestContext *ctxt,
    [[maybe_unused]] const std::vector<uint8_t> &document) {
  throw http::Error(HttpStatusCode::NotImplemented);
}

HttpResult HandlerDbObjectFunction::handle_get(
    [[maybe_unused]] rest::RequestContext *ctxt) {
  using namespace std::string_literals;

  Url::Keys keys;
  Url::Values values;

  auto &requests_uri = ctxt->request->get_uri();
  auto obj = entry_->object_description;

  auto sql_values = database::create_function_argument_list(
      obj.get(), requests_uri.get_query_elements(), ownership_,
      ctxt->user.has_user_id ? &ctxt->user.user_id : nullptr);
  auto session = get_session(ctxt->sql_session_cache.get(), cache_);
  // Stored procedures may change the state of the SQL session,
  // we need ensure that its going to be reset.
  // Set as dirty, directly before executing queries.
  session.set_dirty();

  assert(entry_->format != mrs::database::entry::DbObject::formatFeed &&
         "Functions may generate only single value results, thus feed is not "
         "acceptable.");
  database::QueryRestFunction db;
  try {
    if (entry_->format != mrs::database::entry::DbObject::formatMedia) {
      log_debug(
          "HandlerDbObjectFunction::handle_get - generating 'Item' response");
      db.query_entries(session.get(), obj, sql_values);

      Counter<kEntityCounterRestReturnedItems>::increment(db.items);
      Counter<kEntityCounterRestAffectedItems>::increment(
          session->affected_rows());

      database::QueryRestFunction::CustomMetadata custom_metadata;
      if (get_options().metadata.gtid && gtid_manager_) {
        auto gtid =
            mrs::monitored::get_session_tracked_gtids_for_metadata_response(
                session.get(), gtid_manager_);
        if (!gtid.empty()) {
          custom_metadata["gtid"] = gtid;
        }
      }
      db.serialize_response(custom_metadata);

      return {std::move(db.response)};
    }

    db.query_raw(session.get(), obj, sql_values);

    log_debug("media has size:%i", (int)db.response.length());

    Counter<kEntityCounterRestReturnedItems>::increment(db.items);
    Counter<kEntityCounterRestAffectedItems>::increment(
        session->affected_rows());
  } catch (const mysqlrouter::MySQLSession::Error &e) {
    return handler_mysqlerror(e, &db);
  }

  if (entry_->autodetect_media_type) {
    log_debug("HandlerDbObjectFunction::handle_get - autodetection response");
    helper::MediaDetector md;
    auto detected_type = md.detect(db.response);

    return {std::move(db.response), detected_type};
  }

  if (entry_->media_type.has_value()) {
    return {std::move(db.response), entry_->media_type.value()};
  }

  return {std::move(db.response), helper::MediaType::typeUnknownBinary};
}

uint32_t HandlerDbObjectFunction::get_access_rights() const {
  return HandlerDbObjectTable::get_access_rights() &
         (mrs::database::entry::Operation::valueRead |
          mrs::database::entry::Operation::valueUpdate);
}

}  // namespace handler
}  // namespace endpoint
}  // namespace mrs
