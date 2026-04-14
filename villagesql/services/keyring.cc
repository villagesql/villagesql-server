// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

#include "villagesql/services/keyring.h"

#include "mysql/components/my_service.h"
#include "mysql/components/services/keyring_metadata_query.h"
#include "mysql/components/services/keyring_reader_with_status.h"
#include "mysql/components/services/keyring_writer.h"
#include "mysql/service_plugin_registry.h"

namespace villagesql {
namespace services {

vef_keyring_result_t read_keyring(const char *data_id, const char *auth_id,
                                   unsigned char *buf, size_t buf_len,
                                   size_t *out_len) {
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return VEF_KEYRING_ERROR;

  my_service<SERVICE_TYPE(keyring_component_status)> status(
      "keyring_component_status", registry);
  if (!status.is_valid() || !status->is_initialized()) {
    mysql_plugin_registry_release(registry);
    return VEF_KEYRING_UNAVAILABLE;
  }

  my_service<SERVICE_TYPE(keyring_reader_with_status)> reader(
      "keyring_reader_with_status", registry);

  vef_keyring_result_t result = VEF_KEYRING_NOT_FOUND;
  my_h_keyring_reader_object obj = nullptr;
  if (!reader->init(data_id, auth_id, &obj) && obj != nullptr) {
    size_t key_len = 0, type_len = 0;
    if (!reader->fetch_length(obj, &key_len, &type_len) &&
        key_len <= buf_len) {
      char type_buf[64];
      size_t fetched_len = 0, fetched_type_len = 0;
      if (!reader->fetch(obj, buf, buf_len, &fetched_len, type_buf,
                         sizeof(type_buf), &fetched_type_len)) {
        *out_len = fetched_len;
        result = VEF_KEYRING_OK;
      }
    }
    reader->deinit(obj);
  }

  mysql_plugin_registry_release(registry);
  return result;
}

vef_keyring_result_t write_keyring(const char *data_id, const char *auth_id,
                                    const unsigned char *data,
                                    size_t data_len) {
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return VEF_KEYRING_ERROR;

  my_service<SERVICE_TYPE(keyring_component_status)> status(
      "keyring_component_status", registry);
  if (!status.is_valid() || !status->is_initialized()) {
    mysql_plugin_registry_release(registry);
    return VEF_KEYRING_UNAVAILABLE;
  }

  my_service<SERVICE_TYPE(keyring_writer)> writer("keyring_writer", registry);

  vef_keyring_result_t result =
      writer->store(data_id, auth_id, data, data_len, "SECRET")
          ? VEF_KEYRING_ERROR
          : VEF_KEYRING_OK;
  mysql_plugin_registry_release(registry);
  return result;
}

}  // namespace services
}  // namespace villagesql
