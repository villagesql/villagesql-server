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

// Layout of the build-info block that the post-link stamper rewrites. This is
// an implementation detail shared by build_info.cc, the stamper tool, and
// their unit tests; the stable public API is villagesql/include/build_info.h.

#ifndef VILLAGESQL_COMMON_BUILD_INFO_RAW_H_
#define VILLAGESQL_COMMON_BUILD_INFO_RAW_H_

#include <cstddef>

#include "villagesql/include/build_info.h"

namespace villagesql {

// Magic marker locating the build-info block inside a linked binary. The
// post-link stamper (vsql_build_info_stamper) searches the binary for these
// bytes and overwrites the block that follows. The marker must therefore be
// unique within mysqld (the stamper fails the build if it appears zero or
// multiple times) and must never change without updating the stamper.
inline constexpr char kBuildInfoMagic[] = "VSQL_BUILD_INFO_BLOCK_v1";

// Field sizes, each including the NUL terminator.
inline constexpr size_t kBuildInfoMagicSize = sizeof(kBuildInfoMagic);  // 25
inline constexpr size_t kBuildInfoShaSize = 41;
inline constexpr size_t kBuildInfoCountSize = 11;
inline constexpr size_t kBuildInfoTimestampSize = 21;
inline constexpr size_t kBuildInfoHostSize = 65;
inline constexpr size_t kBuildInfoOsSize = 65;
inline constexpr size_t kBuildInfoArchSize = 17;

// The build-info block as laid out in the binary. Every member is a char
// array, so the struct has alignment 1 and no padding: the stamper can
// overwrite it as one contiguous byte range. Fields are NUL-terminated
// C strings, NUL-padded to their fixed width.
struct BuildInfoRaw {
  char magic[kBuildInfoMagicSize];
  char git_sha[kBuildInfoShaSize];
  char files_added[kBuildInfoCountSize];
  char files_deleted[kBuildInfoCountSize];
  char files_modified[kBuildInfoCountSize];
  char build_timestamp[kBuildInfoTimestampSize];
  char build_host[kBuildInfoHostSize];
  char build_os[kBuildInfoOsSize];
  char build_arch[kBuildInfoArchSize];
};

static_assert(sizeof(BuildInfoRaw) == kBuildInfoMagicSize + kBuildInfoShaSize +
                                          3 * kBuildInfoCountSize +
                                          kBuildInfoTimestampSize +
                                          kBuildInfoHostSize +
                                          kBuildInfoOsSize + kBuildInfoArchSize,
              "BuildInfoRaw must have no padding");

// The single instance, defined in build_info.cc with placeholder values.
// volatile and non-const on purpose: the bytes are rewritten in the linked
// binary by the stamper, so the compiler must never fold the placeholder
// initializer values into readers. Do not weaken either qualifier.
extern volatile BuildInfoRaw vsql_build_info_raw;

// Parses a raw block into the public BuildInfo view. String members of the
// result point into `raw`, which must outlive the returned value. Empty
// git_sha parses as "unknown"; empty/garbage counts parse as 0. Exposed for
// unit testing; production code uses GetBuildInfo().
BuildInfo ParseBuildInfoRaw(const BuildInfoRaw &raw);

}  // namespace villagesql

#endif  // VILLAGESQL_COMMON_BUILD_INFO_RAW_H_
