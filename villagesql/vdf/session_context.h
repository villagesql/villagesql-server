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

#ifndef VILLAGESQL_VDF_SESSION_CONTEXT_H
#define VILLAGESQL_VDF_SESSION_CONTEXT_H

#include "sql/sql_class.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql::vdf {

// THD::killed_state uses MySQL error-code values, so map instead of casting.
inline vef_kill_status_t vef_map_kill_status(THD::killed_state k) {
  switch (k) {
    case THD::NOT_KILLED:
      return VEF_KILL_NOT_KILLED;
    case THD::KILL_CONNECTION:
      return VEF_KILL_CONNECTION;
    case THD::KILL_QUERY:
      return VEF_KILL_QUERY;
    case THD::KILL_TIMEOUT:
      return VEF_KILL_TIMEOUT;
    case THD::KILLED_NO_VALUE:
      return VEF_KILL_UNKNOWN;
  }
  return VEF_KILL_UNKNOWN;
}

}  // namespace villagesql::vdf

#endif  // VILLAGESQL_VDF_SESSION_CONTEXT_H
