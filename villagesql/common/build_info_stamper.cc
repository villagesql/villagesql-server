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

// Post-link build-info stamper. Rewrites the magic-marked BuildInfoRaw block
// inside an already-linked binary with real values. Invoked by
// villagesql/cmake/stamp_build_info.cmake as a POST_BUILD step on mysqld.
//
// This is a build-host tool: it takes a complete binary path from argv (no
// path construction) and never ships, so plain <fstream> is used instead of
// the my_sys wrappers required in server code.

#include <fstream>
#include <iostream>
#include <string>

#include "villagesql/common/build_info_stamp.h"

namespace {

int Fail(const std::string &msg) {
  std::cerr << "vsql_build_info_stamper: " << msg << "\n";
  return 1;
}

}  // namespace

int main(int argc, char **argv) {
  if (argc != 10) {
    return Fail(
        "usage: vsql_build_info_stamper <binary> <git_sha> <files_added> "
        "<files_deleted> <files_modified> <build_timestamp> <build_host> "
        "<build_os> <build_arch>");
  }
  const std::string path = argv[1];
  villagesql::BuildInfoStampValues values;
  values.git_sha = argv[2];
  values.files_added = argv[3];
  values.files_deleted = argv[4];
  values.files_modified = argv[5];
  values.build_timestamp = argv[6];
  values.build_host = argv[7];
  values.build_os = argv[8];
  values.build_arch = argv[9];

  // Sized read rather than istreambuf_iterator: mysqld is hundreds of MB in a
  // debug build, and this runs on every relink.
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  if (!in) return Fail("cannot open " + path);
  const std::streamoff size = in.tellg();
  if (size < 0) return Fail("cannot size " + path);
  std::string contents(static_cast<size_t>(size), '\0');
  in.seekg(0);
  in.read(contents.data(), size);
  if (!in) return Fail("read failed for " + path);
  in.close();

  const size_t off = villagesql::FindBuildInfoBlock(contents);
  if (off == std::string_view::npos)
    return Fail("build-info magic not found in " + path +
                " (was the block optimized away?)");
  if (off + sizeof(villagesql::BuildInfoRaw) > contents.size())
    return Fail("build-info block truncated in " + path);
  if (villagesql::FindBuildInfoBlock(contents, off + 1) !=
      std::string_view::npos)
    return Fail("multiple build-info magics found in " + path);

  villagesql::BuildInfoRaw block;
  std::string err;
  if (!villagesql::RenderBuildInfoBlock(values, &block, &err)) return Fail(err);

  std::fstream out(path, std::ios::binary | std::ios::in | std::ios::out);
  if (!out) return Fail("cannot open " + path + " for writing");
  out.seekp(static_cast<std::streamoff>(off));
  out.write(reinterpret_cast<const char *>(&block), sizeof(block));
  out.close();
  if (!out) return Fail("write failed for " + path);
  return 0;
}
