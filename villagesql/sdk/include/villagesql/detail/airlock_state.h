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

#ifndef VILLAGESQL_DETAIL_AIRLOCK_STATE_H
#define VILLAGESQL_DETAIL_AIRLOCK_STATE_H

// Per-extension state shared by villagesql::airlock and vef_register_impl.
// Stores airlock requests built during the builder's init() phase; the
// generated vef_register reads them via state().requests.
//
// Storage is dynamic. Error buffers are heap-allocated per request so that
// growth of the requests vector does not invalidate
// vef_airlock_request_t::error_msg pointers given to the server.

#include <array>
#include <cstddef>
#include <memory>
#include <vector>

#include <villagesql/abi/types.h>

namespace villagesql::detail::airlock {

struct State {
  std::vector<vef_airlock_request_t> requests;
  std::vector<std::unique_ptr<std::array<char, VEF_MAX_ERROR_LEN>>>
      error_buffers;
};

inline State &state() {
  static State s;
  return s;
}

inline void add_request(const vef_airlock_request_t &req) {
  State &s = state();
  auto buf = std::make_unique<std::array<char, VEF_MAX_ERROR_LEN>>();
  (*buf)[0] = '\0';
  vef_airlock_request_t copy = req;
  copy.error_msg = buf->data();
  s.error_buffers.push_back(std::move(buf));
  s.requests.push_back(copy);
}

}  // namespace villagesql::detail::airlock

#endif  // VILLAGESQL_DETAIL_AIRLOCK_STATE_H
