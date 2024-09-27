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

#include "mrs/database/schema_monitor.h"

#include "helper/string/contains.h"
#include "helper/string/generic.h"

#include "mrs/database/monitor/db_access.h"
#include "mrs/database/query_statistics.h"
#include "mrs/database/query_version.h"

#include "mrs/observability/entity.h"
#include "mrs/router_observation_entities.h"

#include "mysqld_error.h"
#include "router_config.h"
#include "socket_operations.h"

#include "mysql/harness/logging/logging.h"

IMPORT_LOG_FUNCTIONS()

namespace mrs {
namespace database {

namespace {

std::vector<mrs::rest::entry::AppUrlHost> make_app_url_host(
    const std::vector<mrs::database::entry::UrlHost> &entries,
    const std::optional<std::string> &data) {
  std::vector<mrs::rest::entry::AppUrlHost> result;
  result.reserve(entries.size());

  for (const auto &entry : entries) {
    result.emplace_back(entry, data);
  }

  return result;
}

const std::string &to_string(
    mrs::interface::SupportedMrsMetadataVersion version) {
  const static std::string k_version2{"2"};
  const static std::string k_version3{"3"};
  return (version == mrs::interface::kSupportedMrsMetadataVersion_2
              ? k_version2
              : k_version3);
}

mrs::interface::SupportedMrsMetadataVersion query_supported_mrs_version(
    mysqlrouter::MySQLSession *session) {
  QueryVersion q;
  auto mrs_version = q.query_version(session);

  if (mrs_version.is_compatible({mrs::database::kCurrentMrsMetadataVersion}))
    return mrs::interface::kSupportedMrsMetadataVersion_3;

  if (mrs_version.is_compatible({{2, 2, 0}}))
    return mrs::interface::kSupportedMrsMetadataVersion_2;

  std::stringstream error_message;
  error_message << "Unsupported MRS version detected: " << mrs_version.major
                << "." << mrs_version.minor << "." << mrs_version.patch;
  throw std::runtime_error(error_message.str());
}

bool query_is_node_read_only(mysqlrouter::MySQLSession *session) {
  mysqlrouter::sqlstring q = "select @@super_read_only, @@read_only";

  auto result{session->query_one(q)};

  if (nullptr == result.get()) return false;
  if (!(*result)[0] || (!(*result)[1])) return false;

  return std::stoul(std::string((*result)[0])) == 1 ||
         std::stoul(std::string((*result)[1])) == 1;
}

}  // namespace

SchemaMonitor::SchemaMonitor(
    const mrs::Configuration &configuration,
    collector::MysqlCacheManager *cache, mrs::EndpointManager *dbobject_manager,
    authentication::AuthorizeManager *auth_manager,
    mrs::observability::EntitiesManager *entities_manager,
    mrs::GtidManager *gtid_manager,
    mrs::database::QueryFactoryProxy *query_factory)
    : configuration_{configuration},
      cache_{cache},
      dbobject_manager_{dbobject_manager},
      auth_manager_{auth_manager},
      entities_manager_{entities_manager},
      gtid_manager_{gtid_manager},
      proxy_query_factory_{query_factory} {}

SchemaMonitor::~SchemaMonitor() { stop(); }

void SchemaMonitor::start() {
  if (state_.exchange(k_initializing, k_running)) {
    log_debug("SchemaMonitor::start");
    run();
  }
}

void SchemaMonitor::stop() {
  waitable_.serialize_with_cv([this](void *, std::condition_variable &cv) {
    if (state_.exchange({k_initializing, k_running}, k_stopped)) {
      log_debug("SchemaMonitor::stop");
      cv.notify_all();
    }
  });
  // The thread might be already stopped or even it has never started
  if (monitor_thread_.joinable()) monitor_thread_.join();
}

class ServiceDisabled : public std::runtime_error {
 public:
  explicit ServiceDisabled() : std::runtime_error("service disabled") {}
};

void SchemaMonitor::run() {
  log_system("Starting MySQL REST Metadata monitor");

  bool force_clear{true};
  bool was_node_read_only{false},
      is_node_read_only{
          true};  // set to true to force the query on the first run
  const bool is_destination_dynamic = configuration_.provider_rw_->is_dynamic();
  bool state{false};
  do {
    try {
      auto session_check_version =
          cache_->get_instance(collector::kMySQLConnectionMetadataRW, true);
      if (!is_destination_dynamic && is_node_read_only) {
        is_node_read_only =
            query_is_node_read_only(session_check_version.get());
        if (was_node_read_only != is_node_read_only) {
          if (is_node_read_only) {
            log_warning("Node %s is read-only, stopping the REST service",
                        session_check_version.get()->get_address().c_str());
          } else {
            log_info("Node %s is not read-only",
                     session_check_version.get()->get_address().c_str());
          }
          was_node_read_only = is_node_read_only;
        }
        if (is_node_read_only) {
          continue;
        }
      }
      auto supported_schema_version =
          query_supported_mrs_version(session_check_version.get());

      auto factory{create_schema_monitor_factory(supported_schema_version)};

      proxy_query_factory_->change_subject(
          create_query_factory(supported_schema_version));

      DbAccess fetcher(proxy_query_factory_, factory.get(),
                       configuration_.router_id_);

      log_system("Monitoring MySQL REST metadata (version: %s)",
                 to_string(supported_schema_version).c_str());

      do {
        using namespace observability;
        auto session = session_check_version.empty()
                           ? cache_->get_instance(
                                 collector::kMySQLConnectionMetadataRW, true)
                           : std::move(session_check_version);

        fetcher.query(session.get());

        auto service_enabled = fetcher.get_state().service_enabled;
        if (service_enabled != state) {
          state = service_enabled;
          if (!service_enabled) {
            throw ServiceDisabled();
          }
        }

        if (fetcher.get_state_was_changed()) {
          auto global_json_config = fetcher.get_state().data.value_or("{}");
          dbobject_manager_->configure(global_json_config);
          auth_manager_->configure(global_json_config);
          gtid_manager_->configure(global_json_config);
          cache_->configure(global_json_config);

          log_debug("route turn=%s, changed=%s",
                    (fetcher.get_state().service_enabled ? "on" : "off"),
                    fetcher.get_state_was_changed() ? "yes" : "no");
        }

        if (!fetcher.get_auth_app_entries().empty()) {
          auth_manager_->update(fetcher.get_auth_app_entries());
          EntityCounter<kEntityCounterUpdatesAuthentications>::increment(
              fetcher.get_auth_app_entries().size());
        }

        if (!fetcher.get_host_entries().empty()) {
          dbobject_manager_->update(make_app_url_host(
              fetcher.get_host_entries(), fetcher.get_state().data));
        }

        if (!fetcher.get_service_entries().empty()) {
          dbobject_manager_->update(fetcher.get_service_entries());
        }

        if (!fetcher.get_schema_entries().empty()) {
          dbobject_manager_->update(fetcher.get_schema_entries());
        }

        if (!fetcher.get_content_set_entries().empty()) {
          dbobject_manager_->update(fetcher.get_content_set_entries());
        }

        auto db_object_entries = fetcher.get_db_object_entries();
        if (!db_object_entries.empty()) {
          dbobject_manager_->update(db_object_entries);
        }

        if (!fetcher.get_content_file_entries().empty()) {
          dbobject_manager_->update(fetcher.get_content_file_entries());
        }

        fetcher.update_access_factory_if_needed();

        if (fetcher.get_state().service_enabled) {
          auto socket_ops = mysql_harness::SocketOperations::instance();

          mysqlrouter::sqlstring update{
              "INSERT INTO mysql_rest_service_metadata.router"
              " (id, router_name, address, product_name, version, attributes, "
              "options)"
              " VALUES (?,?,?,?,?,'{}','{}') ON DUPLICATE KEY UPDATE "
              "version=?, last_check_in=NOW()"};

          update << configuration_.router_id_ << configuration_.router_name_
                 << socket_ops->get_local_hostname()
                 << MYSQL_ROUTER_PACKAGE_NAME << MYSQL_ROUTER_VERSION
                 << MYSQL_ROUTER_VERSION;
          session->execute(update.str());

          try {
            QueryStatistics store_stats;
            store_stats.update_statistics(
                session.get(), configuration_.router_id_,
                configuration_.metadata_refresh_interval_.count(),
                entities_manager_->fetch_counters());
          } catch (const std::exception &exc) {
            log_error(
                "Storing statistics failed, because of the following error:%s.",
                exc.what());
          }
        }
      } while (wait_until_next_refresh());
    } catch (const mrs::database::QueryState::NoRows &exc) {
      log_error("Can't refresh MRDS layout, because of the following error:%s.",
                exc.what());
      force_clear = true;
    } catch (const mysqlrouter::MySQLSession::Error &exc) {
      log_error("Can't refresh MRDS layout, because of the following error:%s.",
                exc.what());
      if (exc.code() == ER_BAD_DB_ERROR || exc.code() == ER_NO_SUCH_TABLE) {
        force_clear = true;
      }

      if (!is_destination_dynamic &&
          exc.code() == ER_OPTION_PREVENTS_STATEMENT) {
        is_node_read_only = true;
        force_clear = true;
      }
    } catch (const ServiceDisabled &) {
      force_clear = true;
    } catch (const std::exception &exc) {
      log_error("Can't refresh MRDS layout, because of the following error:%s.",
                exc.what());
    }

    if (force_clear) {
      dbobject_manager_->clear();
      auth_manager_->clear();
      force_clear = false;
    }
  } while (wait_until_next_refresh());

  log_system("Stopping MySQL REST Service monitor");
}

bool SchemaMonitor::wait_until_next_refresh() {
  waitable_.wait_for(
      std::chrono::seconds(configuration_.metadata_refresh_interval_),
      [this](void *) { return !state_.is(k_running); });
  return state_.is(k_running);
}

}  // namespace database
}  // namespace mrs
