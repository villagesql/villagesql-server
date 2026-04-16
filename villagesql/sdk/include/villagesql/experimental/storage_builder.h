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

#ifndef VILLAGESQL_EXPERIMENTAL_STORAGE_BUILDER_H
#define VILLAGESQL_EXPERIMENTAL_STORAGE_BUILDER_H

// TODO(villagesql-beta): Column storage is not ready for external use.
// See storage_api.h for details.
//
// This file provides StorageBuilder for defining the column storage interface.
// For full documentation and examples, see extension.h.

#include <type_traits>

#include <villagesql/abi/storage.h>
#include <villagesql/experimental/storage_api.h>

namespace villagesql {
namespace storage_builder {

// =============================================================================
// StorageBuilder<UserCtx>
// =============================================================================
//
// Fluent API for assembling a vef_type_storage_intf_t. All seven storage
// function pointers must be provided; omitting any leaves the corresponding
// slot as nullptr, which the server will reject at load time.
//
// UserCtx is the extension-defined per-column context type. The SDK allocates
// it from the InnoDB arena before calling create/load and destroys it
// automatically on drop by deleting the Arena (which calls ~UserCtx()).
// Extensions must not free it manually.
//
// Pass the user context type to make_storage<>() and each function as a
// template argument. The SDK generates the required ABI wrappers automatically.
//
// Usage:
//   make_storage<MyCtx>()
//     .create<&MyStorage::create>()
//     .drop<&MyStorage::drop>()
//     .load<&MyStorage::load>()
//     .insert<&MyStorage::insert>()
//     .select<&MyStorage::select>()
//     .mark_delete<&MyStorage::mark_delete>()
//     .purge<&MyStorage::purge>()
//     .build()
//
// The SDK constructs Column::StorageCtx<MyCtx> and passes it as the first
// argument to every extension function. Access the user context via
// storage->user(). For additional arena allocations use storage->arena().
//
// Expected extension function signatures (Ctx = Column::StorageCtx<MyCtx>):
//   bool create(Ctx*, Space::Ref, Segment::TrxRef, uint32_t col_len,
//               char* error_msg, uint32_t error_msg_len);
//   bool drop(Ctx*, Segment::TrxRef, char*, uint32_t);
//   bool load(Ctx*, Column::StorageRef, char*, uint32_t);
//   bool insert(Ctx*, MtrCtx::Ref, Segment::TrxRef, Column::Data col_data,
//               Column::Data rowid_prefix, Column::Ref*, char*, uint32_t);
//   bool select(Ctx*, MtrCtx::Ref, Column::Ref, Column::Data*, Column::Data*,
//               Segment::TrxRef*, bool* delete_marked, char*, uint32_t);
//   bool mark_delete(Ctx*, MtrCtx::Ref, Segment::TrxRef, Column::Ref,
//                    bool delete_mark, char*, uint32_t);
//   bool purge(Ctx*, MtrCtx::Ref, Segment::TrxRef, Column::Ref, char*,
//   uint32_t);
//

// All functions return false on success, true on error (writing to error_msg).
// The drop function only needs to handle InnoDB segment cleanup.
// The StorageCtx<UserCtx> destructor (~UserCtx) is called automatically
// by the SDK via the Arena after drop returns, regardless of success or
// failure. Drop is a one-shot operation: the Arena is always destroyed on
// return, so the server will not retry a failed drop.
//
// Note: The Wrapper static methods use C++ linkage. The C++ standard does not
// allow `extern "C"` on template instantiations. Consequently, the ABI boundary
// relies on C and C++ having compatible calling conventions on all supported
// platforms.

namespace detail {

template <typename UserCtx>
using StorageCtx = villagesql::storage::Column::StorageCtx<UserCtx>;

using Arena = villagesql::storage::Arena;

template <auto F, typename UserCtx>
struct CreateWrapper {
  static bool invoke(vef_storage_space_ref_t space_ref,
                     vef_storage_trx_ref_t trx_ref, uint32_t col_len,
                     vef_storage_arena_t *arena_ctx,
                     vef_storage_arena_func_t arena_alloc,
                     vef_storage_ctx_t **storage, char *error_msg,
                     uint32_t error_msg_len) {
    auto *arena = new (std::nothrow) Arena(arena_ctx, arena_alloc);
    if (arena == nullptr) {
      snprintf(error_msg, error_msg_len, "out of memory allocating arena");
      return true;
    }
    auto *ctx = arena->construct<StorageCtx<UserCtx>>(arena);
    if (ctx == nullptr || ctx->user() == nullptr) {
      delete arena;
      snprintf(error_msg, error_msg_len,
               "out of memory allocating storage context");
      return true;
    }
    *storage = reinterpret_cast<vef_storage_ctx_t *>(ctx);
    bool err = F(ctx, space_ref, trx_ref, col_len, error_msg, error_msg_len);
    if (err) {
      delete arena;
      *storage = nullptr;
    }
    return err;
  }
};

template <auto F, typename UserCtx>
struct DropWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_trx_ref_t trx_ref,
                     char *error_msg, uint32_t error_msg_len) {
    auto *ctx = reinterpret_cast<StorageCtx<UserCtx> *>(storage);
    Arena *arena = &ctx->arena();
    bool err = F(ctx, trx_ref, error_msg, error_msg_len);
    delete arena;
    return err;
  }
};

template <auto F, typename UserCtx>
struct LoadWrapper {
  static bool invoke(vef_storage_ref_t storage_ref,
                     vef_storage_arena_t *arena_ctx,
                     vef_storage_arena_func_t arena_alloc,
                     vef_storage_ctx_t **storage, char *error_msg,
                     uint32_t error_msg_len) {
    auto *arena = new (std::nothrow) Arena(arena_ctx, arena_alloc);
    if (arena == nullptr) {
      snprintf(error_msg, error_msg_len, "out of memory allocating arena");
      return true;
    }
    auto *ctx = arena->construct<StorageCtx<UserCtx>>(arena);
    if (ctx == nullptr || ctx->user() == nullptr) {
      delete arena;
      snprintf(error_msg, error_msg_len,
               "out of memory allocating storage context");
      return true;
    }
    *storage = reinterpret_cast<vef_storage_ctx_t *>(ctx);
    bool err = F(ctx, storage_ref, error_msg, error_msg_len);
    if (err) {
      delete arena;
      *storage = nullptr;
    }
    return err;
  }
};

template <auto F, typename UserCtx>
struct InsertWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_data_t col_data,
                     vef_storage_col_data_t rowid_prefix,
                     vef_storage_col_ref_t *col_ref, char *error_msg,
                     uint32_t error_msg_len) {
    return F(reinterpret_cast<StorageCtx<UserCtx> *>(storage), mctx, trx_ref,
             col_data, rowid_prefix, col_ref, error_msg, error_msg_len);
  }
};

template <auto F, typename UserCtx>
struct SelectWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_col_ref_t col_ref,
                     vef_storage_col_data_t *col_data,
                     vef_storage_col_data_t *rowid_prefix,
                     vef_storage_trx_ref_t *trx_ref, bool *delete_marked,
                     char *error_msg, uint32_t error_msg_len) {
    return F(reinterpret_cast<StorageCtx<UserCtx> *>(storage), mctx, col_ref,
             col_data, rowid_prefix, trx_ref, delete_marked, error_msg,
             error_msg_len);
  }
};

template <auto F, typename UserCtx>
struct MarkDeleteWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_ref_t col_ref, bool delete_mark,
                     char *error_msg, uint32_t error_msg_len) {
    return F(reinterpret_cast<StorageCtx<UserCtx> *>(storage), mctx, trx_ref,
             col_ref, delete_mark, error_msg, error_msg_len);
  }
};

template <auto F, typename UserCtx>
struct PurgeWrapper {
  static bool invoke(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_ref_t col_ref, char *error_msg,
                     uint32_t error_msg_len) {
    return F(reinterpret_cast<StorageCtx<UserCtx> *>(storage), mctx, trx_ref,
             col_ref, error_msg, error_msg_len);
  }
};

}  // namespace detail

// Non-constexpr: its name appears verbatim in the compiler error when
// build() is evaluated at compile time with missing interface registrations.
void StorageBuilder_all_column_storage_interfaces_must_be_registered();

template <typename UserCtx>
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
    intf_.create = detail::CreateWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &drop() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "drop: F must be a plain function pointer");
    intf_.drop = detail::DropWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &load() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "load: F must be a plain function pointer");
    intf_.load = detail::LoadWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &insert() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "insert: F must be a plain function pointer");
    intf_.insert = detail::InsertWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &select() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "select: F must be a plain function pointer");
    intf_.select = detail::SelectWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &mark_delete() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "mark_delete: F must be a plain function pointer");
    intf_.mark_delete = detail::MarkDeleteWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &purge() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "purge: F must be a plain function pointer");
    intf_.purge = detail::PurgeWrapper<F, UserCtx>::invoke;
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

// Returns a StorageBuilder for the given user context type. UserCtx is
// allocated from the InnoDB arena before create/load and destroyed
// automatically on drop. See the block comment above for full usage.
template <typename UserCtx>
constexpr StorageBuilder<UserCtx> make_storage() {
  return StorageBuilder<UserCtx>{};
}

}  // namespace storage_builder
}  // namespace villagesql

#endif  // VILLAGESQL_EXPERIMENTAL_STORAGE_BUILDER_H
