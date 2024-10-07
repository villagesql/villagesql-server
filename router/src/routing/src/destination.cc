/*
  Copyright (c) 2015, 2024, Oracle and/or its affiliates.

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

#include "destination.h"

#include <algorithm>  // remove_if
#include <mutex>      // lock_guard
#include <stdexcept>  // out_of_range
#include "mysql/harness/destination.h"

// class DestinationNodesStateNotifier

AllowedNodesChangeCallbacksListIterator
DestinationNodesStateNotifier::register_allowed_nodes_change_callback(
    const AllowedNodesChangedCallback &clb) {
  std::lock_guard<std::mutex> lock(allowed_nodes_change_callbacks_mtx_);
  return allowed_nodes_change_callbacks_.insert(
      allowed_nodes_change_callbacks_.end(), clb);
}

void DestinationNodesStateNotifier::unregister_allowed_nodes_change_callback(
    const AllowedNodesChangeCallbacksListIterator &it) {
  std::lock_guard<std::mutex> lock(allowed_nodes_change_callbacks_mtx_);
  allowed_nodes_change_callbacks_.erase(it);
}

void DestinationNodesStateNotifier::register_start_router_socket_acceptor(
    const StartSocketAcceptorCallback &callback) {
  std::lock_guard<std::mutex> lock(socket_acceptor_handle_callbacks_mtx);
  start_router_socket_acceptor_callback_ = callback;
}

void DestinationNodesStateNotifier::unregister_start_router_socket_acceptor() {
  std::lock_guard<std::mutex> lock(socket_acceptor_handle_callbacks_mtx);
  start_router_socket_acceptor_callback_ = nullptr;
}

void DestinationNodesStateNotifier::register_stop_router_socket_acceptor(
    const StopSocketAcceptorCallback &callback) {
  std::lock_guard<std::mutex> lock(socket_acceptor_handle_callbacks_mtx);
  stop_router_socket_acceptor_callback_ = callback;
}

void DestinationNodesStateNotifier::unregister_stop_router_socket_acceptor() {
  std::lock_guard<std::mutex> lock(socket_acceptor_handle_callbacks_mtx);
  stop_router_socket_acceptor_callback_ = nullptr;
}

void DestinationNodesStateNotifier::register_md_refresh_callback(
    const MetadataRefreshCallback &callback) {
  std::lock_guard<std::mutex> lock(md_refresh_callback_mtx_);
  md_refresh_callback_ = callback;
}

void DestinationNodesStateNotifier::unregister_md_refresh_callback() {
  std::lock_guard<std::mutex> lock(md_refresh_callback_mtx_);
  md_refresh_callback_ = nullptr;
}

void DestinationNodesStateNotifier::register_query_quarantined_destinations(
    const QueryQuarantinedDestinationsCallback &callback) {
  std::lock_guard<std::mutex> lock(
      query_quarantined_destinations_callback_mtx_);
  query_quarantined_destinations_callback_ = callback;
}

void DestinationNodesStateNotifier::
    unregister_query_quarantined_destinations() {
  std::lock_guard<std::mutex> lock(
      query_quarantined_destinations_callback_mtx_);
  query_quarantined_destinations_callback_ = nullptr;
}

bool DestinationNodesStateNotifier::is_dynamic() { return false; }

std::string DestinationNodesStateNotifier::get_dynamic_plugin_name() {
  return {};
}

// class RouteDestination

void RouteDestination::add(const mysql_harness::Destination &dest) {
  auto dest_end = destinations_.end();

  if (std::find(destinations_.begin(), dest_end, dest) == dest_end) {
    std::lock_guard<std::mutex> lock(mutex_update_);
    destinations_.push_back(dest);
  }
}

void RouteDestination::remove(const mysql_harness::Destination &dest) {
  std::lock_guard<std::mutex> lock(mutex_update_);

  destinations_.erase(
      std::remove(destinations_.begin(), destinations_.end(), dest),
      destinations_.end());
}

mysql_harness::Destination RouteDestination::get(
    const mysql_harness::Destination &dest) {
  for (const auto &it : destinations_) {
    if (it == dest) return dest;
  }
  throw std::out_of_range("Destination " + dest.str() + " not found");
}

size_t RouteDestination::size() noexcept { return destinations_.size(); }

void RouteDestination::clear() {
  if (destinations_.empty()) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_update_);
  destinations_.clear();
}

std::vector<mysql_harness::Destination> RouteDestination::get_destinations()
    const {
  return destinations_;
}

void RouteDestination::start(const mysql_harness::PluginFuncEnv *) {}

std::optional<Destinations> RouteDestination::refresh_destinations(
    const Destinations &) {
  return std::nullopt;
}
