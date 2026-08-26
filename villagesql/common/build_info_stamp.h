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

// In-memory core of the post-link build-info stamper: locate the magic-marked
// BuildInfoRaw block in a binary image and render replacement bytes for it.
// Header-only so the stamper tool and its unit test share one implementation
// without extra link targets. File I/O lives in build_info_stamper.cc.

#ifndef VILLAGESQL_COMMON_BUILD_INFO_STAMP_H_
#define VILLAGESQL_COMMON_BUILD_INFO_STAMP_H_

#include <cstring>
#include <string>
#include <string_view>

#include "villagesql/common/build_info_raw.h"

namespace villagesql {

// Values to stamp; field meanings match BuildInfoRaw (magic excluded).
struct BuildInfoStampValues {
  std::string git_sha;
  std::string files_added;
  std::string files_deleted;
  std::string files_modified;
  std::string build_timestamp;
  std::string build_host;
  std::string build_os;
  std::string build_arch;
};

// Returns the offset of the magic marker at or after `start`, or npos. The
// search includes the marker's NUL terminator, so a longer marker that starts
// with this one (a future "..._v10") does not match.
inline size_t FindBuildInfoBlock(std::string_view binary, size_t start = 0) {
  return binary.find(std::string_view(kBuildInfoMagic, kBuildInfoMagicSize),
                     start);
}

// Renders a complete block (magic + NUL-padded fields) into *out. Returns
// false and sets *err if a value does not fit its fixed-width field.
inline bool RenderBuildInfoBlock(const BuildInfoStampValues &v,
                                 BuildInfoRaw *out, std::string *err) {
  std::memset(out, 0, sizeof(*out));
  std::memcpy(out->magic, kBuildInfoMagic, kBuildInfoMagicSize);
  const struct {
    const std::string *value;
    char *dst;
    size_t size;
    const char *name;
  } fields[] = {
      {&v.git_sha, out->git_sha, sizeof(out->git_sha), "git_sha"},
      {&v.files_added, out->files_added, sizeof(out->files_added),
       "files_added"},
      {&v.files_deleted, out->files_deleted, sizeof(out->files_deleted),
       "files_deleted"},
      {&v.files_modified, out->files_modified, sizeof(out->files_modified),
       "files_modified"},
      {&v.build_timestamp, out->build_timestamp, sizeof(out->build_timestamp),
       "build_timestamp"},
      {&v.build_host, out->build_host, sizeof(out->build_host), "build_host"},
      {&v.build_os, out->build_os, sizeof(out->build_os), "build_os"},
      {&v.build_arch, out->build_arch, sizeof(out->build_arch), "build_arch"},
  };
  for (const auto &f : fields) {
    if (f.value->size() + 1 > f.size) {
      *err = std::string("value for ") + f.name + " too long (" +
             std::to_string(f.value->size()) + " chars, field holds " +
             std::to_string(f.size - 1) + ")";
      return false;
    }
    std::memcpy(f.dst, f.value->c_str(), f.value->size() + 1);
  }
  return true;
}

}  // namespace villagesql

#endif  // VILLAGESQL_COMMON_BUILD_INFO_STAMP_H_
