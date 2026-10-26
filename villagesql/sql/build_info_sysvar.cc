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

// The @@villagesql_build_info sysvar. This lives here, compiled straight into
// the mysqld executable, rather than alongside the other VillageSQL sysvars in
// initialize.cc, because it is the only reader of GetBuildInfo(). The build
// metadata it reads is regenerated on every build, so keeping both out of
// sql_main means refreshing it relinks mysqld instead of the ~75 targets that
// link sql_main. See villagesql/cmake/build_info.cmake.
//
// Registration still works from an executable source: sysvars register through
// static initialization of a file-scope object, and an object compiled
// directly into the executable is always linked in (unlike an archive member,
// which is pulled in only when something references it).

#include "my_rapidjson_size_t.h"  // IWYU pragma: keep

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <string_view>

#include "sql/sql_class.h"
#include "sql/sys_vars.h"
#include "villagesql/include/build_info.h"
#include "villagesql/include/error.h"

namespace villagesql {

class Sys_var_villagesql_build_info : public Sys_var_charptr_func {
 public:
  Sys_var_villagesql_build_info(const char *name_arg, const char *comment_arg)
      : Sys_var_charptr_func(name_arg, comment_arg, GLOBAL) {}
  const uchar *global_value_ptr(THD *thd, std::string_view) override;
};

const uchar *Sys_var_villagesql_build_info::global_value_ptr(THD *thd,
                                                             std::string_view) {
  const BuildInfo &info = villagesql::GetBuildInfo();

  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> w(sb);
  w.StartObject();
  w.Key("git_sha");
  w.String(info.git_sha);
  w.Key("is_dirty");
  w.Bool(info.is_dirty());
  w.Key("files_added");
  w.Int(info.files_added);
  w.Key("files_deleted");
  w.Int(info.files_deleted);
  w.Key("files_modified");
  w.Int(info.files_modified);
  w.Key("build_timestamp");
  w.String(info.build_timestamp);
  w.Key("build_host");
  w.String(info.build_host);
  w.Key("build_os");
  w.String(info.build_os);
  w.Key("build_arch");
  w.String(info.build_arch);
  w.EndObject();

  size_t buf_size = sb.GetSize() + 1;
  char *buf = (char *)thd->alloc(buf_size);
  if (should_assert_if_null(buf))
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), buf_size);
  else
    std::copy(sb.GetString(), sb.GetString() + sb.GetSize() + 1, buf);
  return (uchar *)buf;
}

static Sys_var_villagesql_build_info Sys_villagesql_build_info(
    "villagesql_build_info",
    "VillageSQL build metadata (JSON: git SHA, work-tree file counts, build "
    "timestamp/host/OS/arch).");

}  // namespace villagesql
