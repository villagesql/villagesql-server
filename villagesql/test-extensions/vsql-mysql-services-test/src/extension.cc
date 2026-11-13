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

// vsql_mysql_services_test extension: exercises the MySQL Services capability
// end-to-end.
//
// Declares three consumed services (component_status, reader, writer) on one
// MysqlServices capability. The server acquires each at load, before any VDF
// runs. If any fails to acquire, extension load fails.
//
// VDFs:
//   keyring_read(data_id, auth_id)              -> STRING
//       Reads a secret from the keyring via the SERVICE_TYPE(reader) vtable.
//   keyring_store(data_id, auth_id, value)      -> INT
//       Stores a secret via the SERVICE_TYPE(writer) vtable. Returns 0 on
//       success, 1 on error.

#include <cstddef>
#include <cstring>

#include <mysql/components/services/keyring_metadata_query.h>  // keyring_component_status
#include <mysql/components/services/keyring_reader_with_status.h>
#include <mysql/components/services/keyring_writer.h>
#include <villagesql/preview/mysql_services.h>
#include <villagesql/vsql.h>

using namespace vsql;

static preview_mysql_services::MysqlServices services;
VSQL_REQUIRE_SERVICE(services, keyring_component_status, status);
VSQL_REQUIRE_SERVICE(services, keyring_reader_with_status, reader);
VSQL_REQUIRE_SERVICE(services, keyring_writer, writer);

// keyring_read(data_id, auth_id)
//
// Returns the secret stored under (data_id, auth_id), or NULL if not found
// or the keyring component is uninitialised. Pass NULL auth_id for internal
// keys.
void keyring_read(StringArg data_id, StringArg auth_id, StringResult out) {
  if (data_id.is_null()) {
    out.set_null();
    return;
  }
  if (!status.valid() || !reader.valid()) {
    out.error("MySQL keyring services not wired up");
    return;
  }
  if (!status->is_initialized()) {
    out.error("Keyring component is not initialised");
    return;
  }

  const char *did = data_id.value().data();
  const char *aid = auth_id.is_null() ? nullptr : auth_id.value().data();

  my_h_keyring_reader_object obj = nullptr;
  if (reader->init(did, aid, &obj) || obj == nullptr) {
    out.set_null();
    return;
  }

  size_t key_len = 0, type_len = 0;
  if (reader->fetch_length(obj, &key_len, &type_len)) {
    reader->deinit(obj);
    out.set_null();
    return;
  }

  auto buf = out.buffer();
  if (key_len > buf.size()) {
    reader->deinit(obj);
    out.error("Keyring value too large for output buffer");
    return;
  }

  char type_buf[64];
  size_t fetched_len = 0, fetched_type_len = 0;
  if (reader->fetch(obj, reinterpret_cast<unsigned char *>(buf.data()),
                    buf.size(), &fetched_len, type_buf, sizeof(type_buf),
                    &fetched_type_len)) {
    reader->deinit(obj);
    out.set_null();
    return;
  }
  reader->deinit(obj);
  out.set_length(fetched_len);
}

// keyring_store(data_id, auth_id, value)
//
// Stores `value` in the keyring under (data_id, auth_id). Type is hard-coded
// to "SECRET". Returns 0 on success, 1 on error.
void keyring_store(StringArg data_id, StringArg auth_id, StringArg value,
                   IntResult out) {
  if (data_id.is_null() || value.is_null()) {
    out.set(1);
    return;
  }
  if (!status.valid() || !writer.valid()) {
    out.error("MySQL keyring services not wired up");
    return;
  }
  if (!status->is_initialized()) {
    out.error("Keyring component is not initialised");
    return;
  }

  const char *did = data_id.value().data();
  const char *aid = auth_id.is_null() ? nullptr : auth_id.value().data();
  auto val = value.value();

  bool err = writer->store(did, aid,
                           reinterpret_cast<const unsigned char *>(val.data()),
                           val.size(), "SECRET");
  out.set(err ? 1 : 0);
}

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .with(services)
                              .func(make_func<&keyring_read>("keyring_read")
                                        .returns(STRING)
                                        .param(STRING)
                                        .param(STRING)
                                        .build())
                              .func(make_func<&keyring_store>("keyring_store")
                                        .returns(INT)
                                        .param(STRING)
                                        .param(STRING)
                                        .param(STRING)
                                        .build()))
