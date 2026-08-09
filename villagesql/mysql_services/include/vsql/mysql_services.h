// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VSQL_MYSQL_SERVICES_H
#define VSQL_MYSQL_SERVICES_H

// VillageSQL Extension SDK: MySQL Services bridge.
//
// Lets a VEF extension consume and provide MySQL registry services (provided
// by either a component or the server core). Plugs into the VEF SDK through
// the airlock — a deliberately narrow extension point — without the SDK
// having any awareness of MySQL.
//
// Each RequiredService / ProvidedService emits one airlock request whose
// payload includes the destination pointer (consumer) or the implementation
// pointer (provider). The server does the acquire / register inline during
// airlock dispatch — no callback into the extension. Release / unregister at
// unload are handled entirely server-side, tracked per extension.
//
// MANIFEST REQUIREMENT
// --------------------
//
// Every service requested or provided MUST be declared in the extension's
// manifest.json:
//
//   {
//     "name": "my_ext",
//     "version": "0.1.0",
//     "required_mysql_services": ["keyring_reader_with_status", ...],
//     "provided_mysql_services":  ["my_thing.my_ext", ...]
//   }
//
// The server refuses to acquire or register any service not declared.
//
// USAGE
// -----
//
//   #include <villagesql/extension.h>
//   #include <villagesql/vsql.h>
//   #include <vsql/mysql_services.h>
//   #include <mysql/components/services/keyring_reader_with_status.h>
//
//   VSQL_DECLARE_SERVICE(keyring_reader_with_status, reader);
//
//   void my_func_impl(IntArg /*a*/, IntResult out) {
//     reader->fetch(/* ... */);
//     out.set(0);
//   }
//
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension()
//       .with_airlock(reader)
//       .func(make_func<&my_func_impl>("f")
//                 .returns(INT).param(INT).build()))
//
// PROVIDING A SERVICE
// -------------------
//
//   SERVICE_TYPE_NO_CONST(my_thing) my_thing_impl{
//     .do_thing = &impl_do_thing,
//   };
//
//   ::vsql::mysql::ProvidedService<SERVICE_TYPE_NO_CONST(my_thing)>
//       provider{"my_thing.my_extension", &my_thing_impl};
//
//   make_extension().with_airlock(provider)...

#include <cstddef>

#include <villagesql/airlock.h>
#include <vsql/mysql_services/registry_abi.h>

namespace vsql::mysql {

// =============================================================================
// RequiredService — extension consumes a MySQL service.
// =============================================================================
//
// ST should be `SERVICE_TYPE(name)` (i.e. `const mysql_service_name_t`).
template <typename ST>
class RequiredService {
 public:
  explicit RequiredService(const char *service_name)
      : name_str_(service_name) {}

  // Becomes valid after the airlock has run; nullptr otherwise.
  ST *operator->() const { return ptr_; }
  ST &operator*() const { return *ptr_; }
  bool valid() const { return ptr_ != nullptr; }

  void airlock(::villagesql::airlock &a) {
    payload_.service_name = name_str_;
    payload_.destination = reinterpret_cast<const void **>(&ptr_);
    a.request(VSQL_MYSQL_SERVICE_REQUIRED_V1_CHANNEL,
              reinterpret_cast<const unsigned char *>(&payload_),
              sizeof(payload_));
  }

 private:
  const char *name_str_;
  ST *ptr_ = nullptr;
  vsql_mysql_service_required_v1_t payload_{};
};

// =============================================================================
// ProvidedService — extension provides a MySQL service implementation.
// =============================================================================
//
// ST_NO_CONST should be `SERVICE_TYPE_NO_CONST(name)`. full_name is
// "service_name.implementation_name".
template <typename ST_NO_CONST>
class ProvidedService {
 public:
  ProvidedService(const char *full_name, const ST_NO_CONST *impl)
      : name_str_(full_name), impl_(impl) {}

  void airlock(::villagesql::airlock &a) {
    payload_.full_name = name_str_;
    payload_.impl = reinterpret_cast<const void *>(impl_);
    a.request(VSQL_MYSQL_SERVICE_PROVIDED_V1_CHANNEL,
              reinterpret_cast<const unsigned char *>(&payload_),
              sizeof(payload_));
  }

 private:
  const char *name_str_;
  const ST_NO_CONST *impl_;
  vsql_mysql_service_provided_v1_t payload_{};
};

}  // namespace vsql::mysql

// VSQL_DECLARE_SERVICE(name, var) — convenience macro for declaring a
// RequiredService whose service-name string matches the SERVICE_TYPE name.
#define VSQL_DECLARE_SERVICE(name, var) \
  ::vsql::mysql::RequiredService<SERVICE_TYPE(name)> var { #name }

#endif  // VSQL_MYSQL_SERVICES_H
