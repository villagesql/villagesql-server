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

#include <type_traits>

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
// Pass each function as a template argument. The SDK generates the required
// ABI wrapper automatically.
//
// Usage:
//   make_storage()
//     .create<&MyStorage::create>()
//     .drop<&MyStorage::drop>()
//     .load<&MyStorage::load>()
//     .insert<&MyStorage::insert>()
//     .select<&MyStorage::select>()
//     .mark_delete<&MyStorage::mark_delete>()
//     .purge<&MyStorage::purge>()
//     .build()
//
// Note: The Wrapper static methods use C++ linkage. The C++ standard does not
// allow `extern "C"` on template instantiations. Consequently, the ABI boundary
// relies on C and C++ having compatible calling conventions on all supported
// platforms.

namespace detail {

template <auto F>
struct CreateWrapper {
  static bool invoke(vef_storage_space_ref_t space_ref,
                     vef_storage_trx_ref_t trx_ref, uint32_t col_len,
                     vef_storage_arena_t *arena_ctx,
                     vef_storage_arena_func_t arena_alloc,
                     vef_storage_ctx_t **storage, char *error_msg,
                     uint32_t error_msg_len) {
    return F(space_ref, trx_ref, col_len, arena_ctx, arena_alloc, storage,
             error_msg, error_msg_len);
  }
};

template <auto F>
struct DropWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_trx_ref_t trx_ref,
                     char *error_msg, uint32_t error_msg_len) {
    return F(storage, trx_ref, error_msg, error_msg_len);
  }
};

template <auto F>
struct LoadWrapper {
  static bool invoke(vef_storage_ref_t storage_ref,
                     vef_storage_arena_t *arena_ctx,
                     vef_storage_arena_func_t arena_alloc,
                     vef_storage_ctx_t **storage, char *error_msg,
                     uint32_t error_msg_len) {
    return F(storage_ref, arena_ctx, arena_alloc, storage, error_msg,
             error_msg_len);
  }
};

template <auto F>
struct InsertWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_data_t col_data,
                     vef_storage_col_data_t rowid_prefix,
                     vef_storage_col_ref_t *col_ref, char *error_msg,
                     uint32_t error_msg_len) {
    return F(storage, mctx, trx_ref, col_data, rowid_prefix, col_ref, error_msg,
             error_msg_len);
  }
};

template <auto F>
struct SelectWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_col_ref_t col_ref,
                     vef_storage_col_data_t *col_data,
                     vef_storage_col_data_t *rowid_prefix,
                     vef_storage_trx_ref_t *trx_ref, bool *delete_marked,
                     char *error_msg, uint32_t error_msg_len) {
    return F(storage, mctx, col_ref, col_data, rowid_prefix, trx_ref,
             delete_marked, error_msg, error_msg_len);
  }
};

template <auto F>
struct MarkDeleteWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_ref_t col_ref, bool delete_mark,
                     char *error_msg, uint32_t error_msg_len) {
    return F(storage, mctx, trx_ref, col_ref, delete_mark, error_msg,
             error_msg_len);
  }
};

template <auto F>
struct PurgeWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_ref_t col_ref, char *error_msg,
                     uint32_t error_msg_len) {
    return F(storage, mctx, trx_ref, col_ref, error_msg, error_msg_len);
  }
};

}  // namespace detail

// Non-constexpr: its name appears verbatim in the compiler error when
// build() is evaluated at compile time with missing interface registrations.
void StorageBuilder_all_column_storage_interfaces_must_be_registered();

class StorageBuilder {
 public:
  constexpr StorageBuilder() : intf_{} {}

  // Each setter requires F to be a plain function pointer. This rejects
  // lambdas and functors at the call site with a clear message.
  template <auto F>
  constexpr StorageBuilder &create() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "create: F must be a plain function pointer");
    intf_.create = detail::CreateWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &drop() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "drop: F must be a plain function pointer");
    intf_.drop = detail::DropWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &load() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "load: F must be a plain function pointer");
    intf_.load = detail::LoadWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &insert() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "insert: F must be a plain function pointer");
    intf_.insert = detail::InsertWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &select() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "select: F must be a plain function pointer");
    intf_.select = detail::SelectWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &mark_delete() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "mark_delete: F must be a plain function pointer");
    intf_.mark_delete = detail::MarkDeleteWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &purge() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "purge: F must be a plain function pointer");
    intf_.purge = detail::PurgeWrapper<F>::invoke;
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
