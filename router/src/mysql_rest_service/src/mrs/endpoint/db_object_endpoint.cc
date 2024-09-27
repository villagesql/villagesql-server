/*
  Copyright (c) 2024, Oracle and/or its affiliates.

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

#include "mrs/endpoint/db_object_endpoint.h"

#include "mrs/observability/entity.h"
#include "mrs/router_observation_entities.h"

namespace mrs {
namespace endpoint {

using DbObject = DbObjectEndpoint::DbObject;
using DbObjectPtr = DbObjectEndpoint::DbObjectPtr;
using UniversalId = DbObjectEndpoint::UniversalId;

DbObjectEndpoint::DbObjectEndpoint(const DbObject &entry,
                                   EndpointConfigurationPtr configuration,
                                   HandlerFactoryPtr factory)
    : OptionEndpoint({}, configuration, factory),
      entry_{std::make_shared<DbObject>(entry)} {}

UniversalId DbObjectEndpoint::get_id() const { return entry_->id; }

UniversalId DbObjectEndpoint::get_parent_id() const {
  return entry_->schema_id;
}

void DbObjectEndpoint::update() {
  Parent::update();
  observability::EntityCounter<kEntityCounterUpdatesObjects>::increment();
}

void DbObjectEndpoint::deactivate() { url_handlers_.clear(); }

void DbObjectEndpoint::activate() {
  url_handlers_.clear();
  url_handlers_.push_back(
      factory_->create_db_object_handler(shared_from_this()));
  url_handlers_.push_back(
      factory_->create_db_object_metadata_handler(shared_from_this()));
}

void DbObjectEndpoint::set(const DbObject &entry, EndpointBasePtr parent) {
  auto lock = std::unique_lock<std::shared_mutex>(endpoints_access_);
  entry_ = std::make_shared<DbObject>(entry);
  change_parent(parent);
  changed();
}

const DbObjectPtr DbObjectEndpoint::get() const { return entry_; }

bool DbObjectEndpoint::is_this_node_enabled() const { return entry_->enabled; }

bool DbObjectEndpoint::does_this_node_require_authentication() const {
  return entry_->requires_authentication;
}

std::string DbObjectEndpoint::get_my_url_path_part() const {
  return entry_->request_path;
}
std::string DbObjectEndpoint::get_my_url_part() const {
  return entry_->request_path;
}

std::optional<std::string> DbObjectEndpoint::get_options() const {
  return entry_->options;
}

}  // namespace endpoint
}  // namespace mrs
