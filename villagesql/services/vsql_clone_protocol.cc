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

#include "villagesql/services/vsql_clone_protocol_imp.h"

#include <mysql/components/service_implementation.h>
#include <mysql/service_mysql_alloc.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "my_sys.h"

#include "villagesql/veb/veb_file.h"

DEFINE_METHOD(int, mysql_vsql_clone_get_extensions,
              (THD * thd [[maybe_unused]], unsigned char **payload,
               size_t *length)) {
  *payload = nullptr;
  *length = 0;

  const std::string data = villagesql::veb::serialize_installed_extensions();
  if (data.empty()) {
    return 0;
  }

  auto *buf = static_cast<unsigned char *>(
      my_malloc(PSI_NOT_INSTRUMENTED, data.size(), MYF(MY_WME)));
  if (buf == nullptr) {
    return 1;
  }
  memcpy(buf, data.data(), data.size());
  *payload = buf;
  *length = data.size();
  return 0;
}

DEFINE_METHOD(void, mysql_vsql_clone_free_payload, (unsigned char *payload)) {
  my_free(payload);
}

DEFINE_METHOD(int, mysql_vsql_clone_validate_extensions,
              (THD * thd, const unsigned char *payload, size_t length,
               char *err_buf, size_t err_buf_len)) {
  const std::string data(reinterpret_cast<const char *>(payload), length);
  std::string error_message;
  if (villagesql::veb::validate_cloned_extensions(thd, data, &error_message)) {
    if (err_buf != nullptr && err_buf_len > 0) {
      snprintf(err_buf, err_buf_len, "%s", error_message.c_str());
    }
    return 1;
  }
  return 0;
}
