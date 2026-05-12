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

#ifndef VILLAGESQL_AIRLOCK_H
#define VILLAGESQL_AIRLOCK_H

// Airlock — narrow extension point for vsql-provided bridge layers.
//
// The airlock is deliberately specifically-shaped, not a general extension
// surface. It exists so that bridge layers (e.g. villagesql/mysql_services)
// can plug into the extension lifecycle without the SDK or its ABI gaining
// any awareness of what the bridge does.
//
// A "participant" is anything with a `void airlock(villagesql::airlock&)`
// method. Participants are passed to the extension builder via
// `.with_airlock(participant)`. During build the participant calls:
//
//   request(name, payload_bytes, payload_size)
//
// which queues a named request whose bytes the server's matching handler
// will interpret. The bytes typically include pointers into the participant's
// own storage (e.g. a destination pointer the server is asked to populate)
// so the server can do the work directly without callbacks.

#include <cstddef>

#include <villagesql/abi/types.h>
#include <villagesql/detail/airlock_state.h>

namespace villagesql {

class airlock {
 public:
  // Queue a request. The server's handler for `name` interprets the bytes
  // and does whatever the channel defines, including writing through any
  // pointers the bytes contain. If the handler fails, the server writes an
  // error message into a buffer supplied by the SDK and aborts extension
  // load.
  //
  // `in_bytes` must remain valid until vef_register() returns — typically
  // it points at a static member of the participant.
  void request(const char *name, const unsigned char *in_bytes,
               size_t in_size) {
    vef_airlock_request_t req{};
    req.name = name;
    req.in_bytes = in_bytes;
    req.in_size = in_size;
    req.error_msg = nullptr;  // assigned by add_request to a stable buffer
    detail::airlock::add_request(req);
  }
};

}  // namespace villagesql

#endif  // VILLAGESQL_AIRLOCK_H
