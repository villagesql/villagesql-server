// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#include "villagesql/services/airlock_registry.h"

#include <unordered_map>
#include <utility>

#include "villagesql/services/mysql_service_registry.h"

namespace villagesql::services {

namespace {

std::unordered_map<std::string, AirlockHandler> &registry() {
  static std::unordered_map<std::string, AirlockHandler> r;
  return r;
}

}  // namespace

void register_airlock_handler(std::string name, AirlockHandler handler) {
  registry()[std::move(name)] = std::move(handler);
}

void unregister_airlock_handler(const std::string &name) {
  registry().erase(name);
}

void register_builtin_airlock_handlers() {
  register_mysql_service_airlock_handlers();
}

bool populate_airlock_requests(const vef_registration_t *reg,
                               std::string &error_message) {
  if (reg == nullptr || reg->protocol < VEF_PROTOCOL_2 ||
      reg->airlock_requests == nullptr || reg->airlock_request_count == 0) {
    return false;
  }

  for (unsigned int i = 0; i < reg->airlock_request_count; ++i) {
    const vef_airlock_request_t &req = reg->airlock_requests[i];
    if (req.name == nullptr) {
      error_message = "airlock request entry has null name";
      return true;
    }

    auto it = registry().find(req.name);
    if (it == registry().end()) {
      error_message =
          std::string("airlock channel not registered: ") + req.name;
      return true;
    }

    if (req.error_msg != nullptr) req.error_msg[0] = '\0';

    std::string handler_error;
    const bool err = it->second(req.in_bytes, req.in_size, handler_error);

    if (err) {
      error_message = std::string("airlock handler error for ") + req.name;
      if (!handler_error.empty()) {
        error_message += ": " + handler_error;
      }
      return true;
    }
  }

  return false;
}

}  // namespace villagesql::services
