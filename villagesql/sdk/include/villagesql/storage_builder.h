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

#ifndef VILLAGESQL_SDK_STORAGE_BUILDER_H
#define VILLAGESQL_SDK_STORAGE_BUILDER_H

// TODO(villagesql-beta): Column storage is not ready for external use.
// See storage_api.h for details.
//
// This file provides StorageBuilder for defining the column storage interface.
// For full documentation and examples, see extension.h.

#include <villagesql/abi/storage.h>

namespace villagesql {
namespace storage_builder {

// =============================================================================
// StorageBuilder
// =============================================================================
//
// Fluent API for assembling a vef_type_storage_intf_t. All seven storage
// function pointers must be provided; omitting any leaves the corresponding
// slot as nullptr, which the server will reject at load time.
//
// Usage:
//   make_storage()
//     .create(&MyStorage::create)
//     .drop(&MyStorage::drop)
//     .load(&MyStorage::load)
//     .insert(&MyStorage::insert)
//     .select(&MyStorage::select)
//     .mark_delete(&MyStorage::mark_delete)
//     .purge(&MyStorage::purge)
//     .build()

// Non-constexpr: its name appears verbatim in the compiler error when
// build() is evaluated at compile time with missing interface registrations.
void StorageBuilder_all_column_storage_interfaces_must_be_registered();

class StorageBuilder {
 public:
  constexpr StorageBuilder() : intf_{} {}

  constexpr StorageBuilder &create(vef_type_storage_create_func_t f) {
    intf_.create = f;
    return *this;
  }

  constexpr StorageBuilder &drop(vef_type_storage_drop_func_t f) {
    intf_.drop = f;
    return *this;
  }

  constexpr StorageBuilder &load(vef_type_storage_load_func_t f) {
    intf_.load = f;
    return *this;
  }

  constexpr StorageBuilder &insert(vef_type_storage_insert_func_t f) {
    intf_.insert = f;
    return *this;
  }

  constexpr StorageBuilder &select(vef_type_storage_select_func_t f) {
    intf_.select = f;
    return *this;
  }

  constexpr StorageBuilder &mark_delete(vef_type_storage_mark_delete_func_t f) {
    intf_.mark_delete = f;
    return *this;
  }

  constexpr StorageBuilder &purge(vef_type_storage_purge_func_t f) {
    intf_.purge = f;
    return *this;
  }

  constexpr vef_type_storage_intf_t build() const {
    if (!intf_.create || !intf_.drop || !intf_.load || !intf_.insert ||
        !intf_.select || !intf_.mark_delete || !intf_.purge) {
      // Calling a non-constexpr function here causes a compile-time error
      // when build() is evaluated in a constant expression. The function name
      // appears verbatim in the compiler diagnostic.
      StorageBuilder_all_column_storage_interfaces_must_be_registered();
    }
    vef_type_storage_intf_t result = intf_;
    result.version = VEF_STORAGE_TYPE_INTF_VERSION;
    return result;
  }

 private:
  vef_type_storage_intf_t intf_;
};

// Entry point: make_storage()
constexpr StorageBuilder make_storage() { return StorageBuilder{}; }

}  // namespace storage_builder
}  // namespace villagesql

#endif  // VILLAGESQL_SDK_STORAGE_BUILDER_H
