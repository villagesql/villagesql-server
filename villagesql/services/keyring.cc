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
#include "mysql/components/services/keyring_reader_with_status.h"
#include "mysql/components/services/keyring_writer.h"
#include "mysql/service_plugin_registry.h"

namespace villagesql {
namespace services {

namespace {

bool vef_read_keyring_impl(vef_context_t * /*ctx*/, const char *data_id,
                           const char *auth_id, unsigned char *buf,
                           size_t buf_len, size_t *out_len) {
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return true;

  my_service<SERVICE_TYPE(keyring_reader_with_status)> reader(
      "keyring_reader_with_status", registry);

  bool result = true;

  if (reader.is_valid()) {
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
          result = false;
        }
      }
      reader->deinit(obj);
    }
  }

  mysql_plugin_registry_release(registry);
  return result;
}

bool vef_write_keyring_impl(vef_context_t * /*ctx*/, const char *data_id,
                            const char *auth_id, const unsigned char *data,
                            size_t data_len) {
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return true;

  my_service<SERVICE_TYPE(keyring_writer)> writer("keyring_writer", registry);

  bool result = true;
  if (writer.is_valid()) {
    result = writer->store(data_id, auth_id, data, data_len, "SECRET");
  }

  mysql_plugin_registry_release(registry);
  return result;
}

}  // namespace

vef_context_t make_vef_context(vef_protocol_t protocol) {
  vef_context_t ctx{};
  ctx.protocol = protocol;
  if (protocol >= VEF_PROTOCOL_2) {
    ctx.read_keyring = vef_read_keyring_impl;
    ctx.write_keyring = vef_write_keyring_impl;
  }
  return ctx;
}

}  // namespace services
}  // namespace villagesql
