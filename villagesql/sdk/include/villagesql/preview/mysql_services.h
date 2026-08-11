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
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// =============================================================================
// PREVIEW CAPABILITY — UNSTABLE API
// =============================================================================
// This header is part of the VEF preview surface. Its API and ABI may change
// or be removed without notice. See villagesql/preview/README.md for details.
// =============================================================================

#ifndef VILLAGESQL_PREVIEW_MYSQL_SERVICES_H
#define VILLAGESQL_PREVIEW_MYSQL_SERVICES_H

// Consume MySQL registry services from a VEF extension.
//
// Declare one MysqlServices capability at static scope, register the services
// it uses via require(), and pass it to .with(). VEF acquires them at load and
// releases them at unload.
//
// (Providing a service — registering the extension's own implementation into
// the registry — is a planned follow-up, not part of this capability yet.)
//
// HOW THIS MAPS TO MYSQL COMPONENT SERVICES
// -----------------------------------------
// This wrapper only handles ACQUIRING a service and handing you a pointer to
// it. What each service IS — its methods, their parameters, and return
// conventions — is defined and documented by MySQL, not here. For a service
// named NAME you read MySQL's own header:
//
//   include/mysql/components/services/NAME.h
//
// where a BEGIN_SERVICE_DEFINITION(NAME) block lists each method (as
// DECLARE_*_METHOD) with doxygen @param/@retval docs. To consume it:
//
//   1. Include that MySQL header (for SERVICE_TYPE(NAME) and its method decls).
//   2. VSQL_REQUIRE_SERVICE(services, NAME, var)  — acquires it into `var`.
//      (The service-name string is derived from NAME; if a service's registry
//      name differs from its SERVICE_TYPE name, use the explicit
//      services.require<SERVICE_TYPE(NAME)>("registry.name", ref) form.)
//   3. Check var.valid() — see below — then call methods via var->method(...)
//      EXACTLY as MySQL's header documents them (same parameters, same
//      convention that a bool return of `false` means success, `true` failure).
//
// So MySQL's header is your method reference; this SDK only replaces MySQL's
// `my_service<SERVICE_TYPE(NAME)>` acquire/release boilerplate with the
// capability. `var.valid()` is our equivalent of `my_service::is_valid()`.
//
// DOT vs ARROW: `var` is a ServiceRef wrapper. Use `.` for the wrapper's own
// API (var.valid()) and `->` to call through to the MySQL service
// (var->method()). ALWAYS check var.valid() before any var-> call — `->`
// dereferences the acquired pointer, which is null if acquisition failed.
//
// EXAMPLE
// -------
//
//   #include <villagesql/vsql.h>
//   #include <villagesql/preview/mysql_services.h>
//   #include <mysql/components/services/keyring_metadata_query.h>
//
//   using namespace vsql;
//
//   static vsql::preview_mysql_services::MysqlServices services;
//   VSQL_REQUIRE_SERVICE(services, keyring_component_status, status);
//
//   void f_impl(IntResult out) {
//     // is_initialized() is MySQL's method, documented in the service's
//     // header. Guard with valid() first, then call through with ->.
//     out.set((status.valid() && status->is_initialized()) ? 1 : 0);
//   }
//
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension().with(services).func(
//         make_func<&f_impl>("f").returns(INT).no_params().build()))

#include <cstddef>
#include <vector>

#include <villagesql/abi/preview/mysql_services.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

namespace vsql::preview_mysql_services {

// Typed accessor to a consumed service. Declare one (at static scope) and pass
// it to MysqlServices::require(); the server writes the acquired vtable into it
// at load, after which valid() is true. `ST` is `SERVICE_TYPE(name)`.
template <typename ST>
class ServiceRef {
 public:
  ST *operator->() const { return ptr_; }
  ST &operator*() const { return *ptr_; }
  bool valid() const { return ptr_ != nullptr; }

  // Where the server writes the acquired vtable. Handed to require() as the
  // config destination slot; not for extension code to set.
  const void **destination() { return reinterpret_cast<const void **>(&ptr_); }

 private:
  ST *ptr_ = nullptr;
};

class MysqlServices : public ::vsql::detail::CapabilityBase<MysqlServices> {
 public:
  // Consume `service_name`'s default implementation, resolving into `ref` once
  // the extension loads. `ref` must outlive the extension (declare it at static
  // scope, alongside the MysqlServices instance).
  template <typename ST>
  void require(const char *service_name, ServiceRef<ST> &ref) {
    required_.push_back({service_name, ref.destination()});
  }

 private:
  friend struct ::vsql::detail::CapabilityTraits<MysqlServices>;

  // Snapshot the vector into the config the server reads at load. Called once
  // during vef_register, after all require() static initializers have run, so
  // the vector is final and its data() pointer stable thereafter.
  const vef_preview_mysql_services_t *config() const {
    config_ = {required_.data(), required_.size()};
    return &config_;
  }

  // The capability loader skips any capability whose vtable_dest is null, so we
  // expose this field as the destination purely to signal "this capability is
  // present" and get on_populate called. MysqlServices carries no server
  // vtable — its real payload is capability_config, and the acquired service
  // pointers land in the ServiceRefs — so the (null) write here is unused.
  void *presence_gate_ = nullptr;

  std::vector<vef_mysql_service_required_t> required_;
  mutable vef_preview_mysql_services_t config_ = {};
};

}  // namespace vsql::preview_mysql_services

// Convenience: declare a ServiceRef and require a service whose registry name
// matches its SERVICE_TYPE name (the usual case). The name is written once;
// #name is the registry string and SERVICE_TYPE(name) the type. For an
// implementation name that differs from the type (e.g. "svc.impl"), declare a
// ServiceRef<SERVICE_TYPE(svc)> and call services.require("svc.impl", ref).
#define VSQL_REQUIRE_SERVICE(services, name, var)                            \
  static ::vsql::preview_mysql_services::ServiceRef<SERVICE_TYPE(name)> var; \
  static const int var##_vsql_req = ((services).require(#name, var), 0)

#include <villagesql/preview/detail/mysql_services_register.h>

#endif  // VILLAGESQL_PREVIEW_MYSQL_SERVICES_H
