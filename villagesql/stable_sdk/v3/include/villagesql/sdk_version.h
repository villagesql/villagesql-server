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

// Frozen SDK version for the stable v3 SDK snapshot.
// Generated at stabilization time; do not edit.

#ifndef VILLAGESQL_SDK_SDK_VERSION_H
#define VILLAGESQL_SDK_SDK_VERSION_H

#include <villagesql/abi/types.h>

namespace villagesql {
namespace detail {

// SDK version reported to the server during vef_register(). Frozen at
// the VillageSQL version when protocol v3 was stabilized.
constexpr vef_version_t kSdkVersion = {0, 0, 4, nullptr};

}  // namespace detail
}  // namespace villagesql

#endif  // VILLAGESQL_SDK_SDK_VERSION_H
