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
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_PREVIEW_PING_IMPL_H
#define VILLAGESQL_PREVIEW_PING_IMPL_H

#include <villagesql/abi/preview/ping.h>
#include <villagesql/preview/ping.h>

namespace vsql::preview_ping {

inline long long PingCapability::ping() const {
  return static_cast<long long>(abi_->ping());
}

inline bool PingCapability::available() const noexcept {
  return abi_ != nullptr;
}

}  // namespace vsql::preview_ping

#endif  // VILLAGESQL_PREVIEW_PING_IMPL_H
