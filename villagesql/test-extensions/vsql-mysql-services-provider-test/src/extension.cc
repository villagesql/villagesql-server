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

// vsql_mysql_services_provider_test extension: exercises ProvidedService.
//
// Defines a custom MySQL service "vsql_self_test_thing" with one method
// (say_hello), provides an implementation, and consumes the default
// implementation through a RequiredService<>. If the bridge wires both
// sides correctly, the consumer resolves to our own provider impl.
//
// VDF:
//   say_hello() -> STRING   Calls the provided impl through the consumer
//                           pointer and returns the impl's response.

#include <cstddef>
#include <cstring>

#include <mysql/components/service.h>
#include <villagesql/vsql.h>
#include <vsql/mysql_services.h>

using namespace vsql;

// -- Define the custom service -----------------------------------------------
//
// In real usage, both provider and consumer would include a shared header
// that declares the service. Here both sides live in the same TU.

BEGIN_SERVICE_DEFINITION(vsql_self_test_thing)
DECLARE_BOOL_METHOD(say_hello, (const char **out_str));
END_SERVICE_DEFINITION(vsql_self_test_thing)

// -- The implementation ------------------------------------------------------

static int impl_say_hello(const char **out_str) {
  *out_str = "hello from provided impl";
  return 0;
}

static SERVICE_TYPE_NO_CONST(vsql_self_test_thing) g_impl{
    /* .say_hello = */ &impl_say_hello,
};

// Register impl as the default implementation. Format is
// "<service_name>.<implementation_name>"; using the extension's manifest
// name as the implementation tag.
::vsql::mysql::ProvidedService<SERVICE_TYPE_NO_CONST(vsql_self_test_thing)>
    provider{"vsql_self_test_thing.vsql_mysql_services_provider_test", &g_impl};

// Consumer: acquire the default implementation by bare service name.
// Once provider has registered, this resolves to g_impl.
::vsql::mysql::RequiredService<SERVICE_TYPE(vsql_self_test_thing)> consumer{
    "vsql_self_test_thing"};

// -- VDF ---------------------------------------------------------------------

void say_hello(StringResult out) {
  if (!consumer.valid()) {
    out.error("consumer not wired up by airlock");
    return;
  }
  const char *msg = nullptr;
  if (consumer->say_hello(&msg) || msg == nullptr) {
    out.error("provider impl returned an error");
    return;
  }
  auto buf = out.buffer();
  size_t n = std::strlen(msg);
  if (n > buf.size()) n = buf.size();
  std::memcpy(buf.data(), msg, n);
  out.set_length(n);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension().with_airlock(provider).with_airlock(consumer).func(
        make_func<&say_hello>("say_hello").returns(STRING).no_params().build()))
