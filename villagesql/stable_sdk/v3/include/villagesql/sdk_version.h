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

// Frozen SDK version for the stable v3 ABI snapshot.

#ifndef VILLAGESQL_SDK_SDK_VERSION_H
#define VILLAGESQL_SDK_SDK_VERSION_H

#include <villagesql/abi/types.h>

namespace villagesql {
namespace detail {

// This is the version reported by an extension built directly against the
// frozen v3 headers (as the abi_v3 compatibility tests do), pinned to the
// VillageSQL version at which protocol v3 was stabilized. It intentionally
// stays fixed with the snapshot, the same way stable_sdk/v1 hard-codes its
// version in the VEF_GENERATE_ENTRY_POINTS macro.
//
// The shipped SDK does NOT use this value: when the SDK package is built, this
// header is overwritten with one generated from the current release version
// (see villagesql/sdk/include/villagesql/sdk_version.h.in and the packaging in
// villagesql/CMakeLists.txt), so extensions built against the SDK report the
// release they were built from.
constexpr vef_version_t kSdkVersion = {0, 0, 4, "only-for-tests"};

}  // namespace detail
}  // namespace villagesql

#endif  // VILLAGESQL_SDK_SDK_VERSION_H
