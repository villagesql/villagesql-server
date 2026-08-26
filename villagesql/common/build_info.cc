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

// Committed, byte-stable definition of the build-info block. Real values are
// stamped into the linked binary by a POST_BUILD step (see
// villagesql/cmake/stamp_build_info.cmake); binaries that are not stamped
// (unit tests, tools) report these placeholder defaults. Keeping this file
// static means it never recompiles, never causes relink cascades, and is
// byte-identical in every ccache.

#include "villagesql/common/build_info_raw.h"

#include <cstdlib>

namespace villagesql {

// The initializer for magic must equal kBuildInfoMagic; the
// BuildInfoTest.GlobalBlockHasMagicAndDefaults unit test enforces the pair.
volatile BuildInfoRaw vsql_build_info_raw = {
    "VSQL_BUILD_INFO_BLOCK_v1", "unknown", "0", "0", "0", "", "", "", "",
};

namespace {

int ParseCount(const char *s) {
  return static_cast<int>(std::strtol(s, nullptr, 10));
}

}  // namespace

BuildInfo ParseBuildInfoRaw(const BuildInfoRaw &raw) {
  BuildInfo info;
  info.git_sha = raw.git_sha[0] != '\0' ? raw.git_sha : "unknown";
  info.files_added = ParseCount(raw.files_added);
  info.files_deleted = ParseCount(raw.files_deleted);
  info.files_modified = ParseCount(raw.files_modified);
  info.build_timestamp = raw.build_timestamp;
  info.build_host = raw.build_host;
  info.build_os = raw.build_os;
  info.build_arch = raw.build_arch;
  return info;
}

bool BuildInfo::is_dirty() const {
  return files_added != 0 || files_deleted != 0 || files_modified != 0;
}

const BuildInfo &GetBuildInfo() {
  // Copy out of the volatile block once. The byte-wise volatile reads force
  // the compiler to fetch the stamped bytes at runtime; the static copies
  // keep the BuildInfo's pointers valid for the process lifetime.
  static const BuildInfoRaw copy = [] {
    BuildInfoRaw c;
    const volatile char *src =
        reinterpret_cast<const volatile char *>(&vsql_build_info_raw);
    char *dst = reinterpret_cast<char *>(&c);
    for (size_t i = 0; i < sizeof(BuildInfoRaw); ++i) dst[i] = src[i];
    return c;
  }();
  static const BuildInfo info = ParseBuildInfoRaw(copy);
  return info;
}

}  // namespace villagesql
