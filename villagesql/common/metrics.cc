/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#include "villagesql/include/metrics.h"

#include <iterator>

#include "mysql/components/my_service.h"
#include "mysql/components/services/component_status_var_service.h"
#include "mysql/status_var.h"
#include "sql/mysqld.h"
#include "villagesql/include/error.h"

namespace villagesql {
namespace metrics {

std::atomic<uint64_t> g_veb_cache_repaired{0};

namespace {

struct StatusVarDef {
  const char *name;
  std::atomic<uint64_t> *counter;
};

// The table. A new counter is one entry here and one extern in metrics.h.
// Names are limited to [A-Za-z0-9_]; see the convention in metrics.h.
const StatusVarDef kStatusVars[] = {
    {"Villagesql_veb_cache_repaired", &g_veb_cache_repaired},
};

constexpr size_t kStatusVarCount = std::size(kStatusVars);

// What the service is handed, built from the table above by
// register_status_variables(). Must outlive registration: the service keeps
// this array, it does not copy it. One trailing terminator entry, as
// register_variable() expects.
//
// Each entry points straight at its counter. That is what lets one table drive
// every variable: the alternative, a SHOW_FUNC accessor, has to be written per
// variable.
SHOW_VAR g_show_vars[kStatusVarCount + 1];

// Pointing a SHOW_LONGLONG entry at a std::atomic<uint64_t> is reading it as a
// longlong, which holds while the two are layout compatible and the counter is
// lock free. This makes the server's read a single aligned 64-bit load, which
// cannot tear.
static_assert(sizeof(std::atomic<uint64_t>) == sizeof(longlong),
              "SHOW_LONGLONG reads the counter as a longlong");
static_assert(alignof(std::atomic<uint64_t>) == alignof(longlong),
              "SHOW_LONGLONG reads the counter as a longlong");
static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "a non-lock-free counter could tear under SHOW_LONGLONG");

bool g_registered = false;

}  // namespace

bool register_status_variables() {
  if (g_registered) return false;

  for (size_t i = 0; i < kStatusVarCount; ++i) {
    g_show_vars[i] = {kStatusVars[i].name,
                      reinterpret_cast<char *>(kStatusVars[i].counter),
                      SHOW_LONGLONG, SHOW_SCOPE_GLOBAL};
  }
  g_show_vars[kStatusVarCount] = {nullptr, nullptr, SHOW_UNDEF,
                                  SHOW_SCOPE_GLOBAL};

  my_service<SERVICE_TYPE(status_variable_registration)> reg_svc(
      "status_variable_registration", srv_registry);
  if (!reg_svc.is_valid()) {
    LogVSQL(ERROR_LEVEL,
            "Failed to acquire status_variable_registration service");
    return true;
  }

  if (reg_svc->register_variable(g_show_vars)) {
    LogVSQL(ERROR_LEVEL, "Failed to register VillageSQL status variables");
    return true;
  }

  g_registered = true;
  LogVSQL(INFORMATION_LEVEL, "Registered %zu VillageSQL status variables",
          kStatusVarCount);
  return false;
}

void unregister_status_variables() {
  if (!g_registered) return;

  my_service<SERVICE_TYPE(status_variable_registration)> reg_svc(
      "status_variable_registration", srv_registry);
  if (reg_svc.is_valid()) {
    reg_svc->unregister_variable(g_show_vars);
  }
  g_registered = false;
}

}  // namespace metrics
}  // namespace villagesql
