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

// =============================================================================
// PREVIEW CAPABILITY — UNSTABLE API
// =============================================================================
// This header is part of the VEF preview surface. Its API and ABI may change
// or be removed without notice. See villagesql/preview/README.md for details.
// =============================================================================

#ifndef VILLAGESQL_PREVIEW_STORAGE_BUILDER_H
#define VILLAGESQL_PREVIEW_STORAGE_BUILDER_H

// This file provides:
//   - StorageCapability: capability token passed to make_extension().with().
//     Declares the InnoDB storage API requirement.
//   - ColumnStoreCapability<N>: capability token for registering column storage
//     implementations. Use .column_store(desc) to attach storage descriptors.
//   - TypeStorageDescriptor: built by StorageBuilder<UserCtx>.build().
//   - make_column_store<UserCtx>(TypeObject): entry point for building a
//     type storage descriptor.
//
// For full documentation and examples, see extension.h.

#include <cstddef>
#include <type_traits>

#include <villagesql/abi/preview/storage.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>
#include <villagesql/preview/storage_api.h>

// vef_storage_arena is a forward-declared server-side handle that
// extensions only see by pointer.

namespace vsql::preview_storage_builder {

// =============================================================================
// TypeStorageDescriptor
// =============================================================================
//
// Thin wrapper around vef_type_storage_intf_t. Produced by
// StorageBuilder<UserCtx>::build() and passed to
// ColumnStoreCapability::column_store().

struct TypeStorageDescriptor {
  vef_type_storage_intf_t intf;
};

// =============================================================================
// StorageCapability
// =============================================================================
//
// Capability token that activates the InnoDB storage API for this extension.
//
// Usage:
//   static auto STORAGE = StorageCapability{};
//
//   VEF_GENERATE_ENTRY_POINTS(make_extension().with(STORAGE)...)
//
// The STORAGE variable must have static storage duration (declare as
// `static auto`) so that the server can write the vtable pointer into it
// at registration time.

class StorageCapability
    : public ::vsql::detail::CapabilityBase<StorageCapability> {
 public:
  StorageCapability() {}

  static constexpr const char *kName = VEF_PREVIEW_STORAGE_NAME;
};

// =============================================================================
// ColumnStoreCapability<N>
// =============================================================================
//
// Capability token that registers column storage implementations for custom
// types. Separate from StorageCapability: an extension may declare column
// storage without using the InnoDB API directly, or use the InnoDB API
// without registering column storage.
//
// Usage:
//   static constexpr auto kMyStorage =
//       make_column_store<MyCtx>(MY_TYPE)...build();
//
//   static auto COLUMN_STORE =
//   ColumnStoreCapability().column_store(kMyStorage);
//
//   VEF_GENERATE_ENTRY_POINTS(
//       make_extension().with(STORAGE).with(COLUMN_STORE).type(MYTYPE))
//
// Each .column_store(desc) call appends one descriptor and returns a new
// ColumnStoreCapability<N+1>. The COLUMN_STORE variable must have static
// storage duration (declare as `static auto`) so that the internal pointer
// array remains valid when the server reads the extension descriptor.

template <size_t N = 0>
class ColumnStoreCapability
    : public std::conditional_t<
          (N > 0), ::vsql::detail::CapabilityBase<ColumnStoreCapability<N>>,
          std::false_type> {
 public:
  static constexpr const char *kName = VEF_PREVIEW_COLUMN_STORE_NAME;

  ColumnStoreCapability() : ptrs_{} {}

  // Append a type storage descriptor. Returns ColumnStoreCapability<N+1>.
  ColumnStoreCapability<N + 1> column_store(
      const TypeStorageDescriptor &d) const {
    return ColumnStoreCapability<N + 1>(*this, &d.intf);
  }

  // Returns the extension descriptor passed to the server as capability_config.
  // Called once at registration time; COLUMN_STORE must be in static storage
  // by then so that ptrs_ has a stable address.
  const vef_preview_column_store_ext_desc_t *extension_desc() {
    ext_desc_.version = VEF_COLUMN_STORE_INTF_VERSION;
    ext_desc_.type_storage_count = static_cast<uint32_t>(N);
    ext_desc_.type_storages = N > 0 ? ptrs_ : nullptr;
    return &ext_desc_;
  }

  // Sink for the server-written vtable pointer. Not used by the extension.
  void *vtable_{nullptr};

  template <size_t M>
  friend class ColumnStoreCapability;

 private:
  // Constructor used by column_store() to build ColumnStoreCapability<N> from
  // ColumnStoreCapability<N-1> plus one new descriptor pointer.
  template <size_t M>
  ColumnStoreCapability(const ColumnStoreCapability<M> &base,
                        const vef_type_storage_intf_t *new_ptr)
      : ptrs_{} {
    static_assert(M + 1 == N, "internal construction size mismatch");
    for (size_t i = 0; i < M; i++) ptrs_[i] = base.ptrs_[i];
    ptrs_[M] = new_ptr;
  }

  const vef_type_storage_intf_t *ptrs_[N > 0 ? N : 1];
  // TODO(villagesql-beta): rename `ext_desc_` to `cc_` to match the
  // capability_config naming.
  vef_preview_column_store_ext_desc_t ext_desc_{};
};

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
// Pass the user context type and a TypeObject to make_column_store<>() and
// each function as a template argument. The SDK generates the required ABI
// wrappers automatically.
//
// Usage:
//   make_column_store<MyCtx>(MY_TYPE)
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
using StorageCtx = vsql::preview_storage::Column::StorageCtx<UserCtx>;
using Arena = vsql::preview_storage::Arena;
using vsql::preview_storage::detail::AllocateArenaStorage;

// Expected extension function pointer types. Each StorageBuilder setter
// static_asserts that F exactly matches the corresponding type alias.

template <typename UserCtx>
using CreateFn = bool (*)(StorageCtx<UserCtx> *,
                          vsql::preview_storage::Space::Ref,
                          vsql::preview_storage::Segment::TrxRef, uint32_t,
                          char *, uint32_t);

template <typename UserCtx>
using DropFn = bool (*)(StorageCtx<UserCtx> *,
                        vsql::preview_storage::Segment::TrxRef, char *,
                        uint32_t);

template <typename UserCtx>
using LoadFn = bool (*)(StorageCtx<UserCtx> *,
                        vsql::preview_storage::Column::StorageRef, char *,
                        uint32_t);

template <typename UserCtx>
using InsertFn = bool (*)(StorageCtx<UserCtx> *,
                          vsql::preview_storage::MtrCtx::Ref,
                          vsql::preview_storage::Segment::TrxRef,
                          vsql::preview_storage::Column::Data,
                          vsql::preview_storage::Column::Data,
                          vsql::preview_storage::Column::Ref *, char *,
                          uint32_t);

template <typename UserCtx>
using SelectFn = bool (*)(StorageCtx<UserCtx> *,
                          vsql::preview_storage::MtrCtx::Ref,
                          vsql::preview_storage::Column::Ref,
                          vsql::preview_storage::Column::Data *,
                          vsql::preview_storage::Column::Data *,
                          vsql::preview_storage::Segment::TrxRef *, bool *,
                          char *, uint32_t);

template <typename UserCtx>
using MarkDeleteFn = bool (*)(StorageCtx<UserCtx> *,
                              vsql::preview_storage::MtrCtx::Ref,
                              vsql::preview_storage::Segment::TrxRef,
                              vsql::preview_storage::Column::Ref, bool, char *,
                              uint32_t);

template <typename UserCtx>
using PurgeFn = bool (*)(StorageCtx<UserCtx> *,
                         vsql::preview_storage::MtrCtx::Ref,
                         vsql::preview_storage::Segment::TrxRef,
                         vsql::preview_storage::Column::Ref, char *, uint32_t);

template <auto F, typename UserCtx>
struct CreateWrapper {
  static bool invoke(vef_storage_space_ref_t space_ref,
                     vef_storage_trx_ref_t trx_ref, uint32_t col_len,
                     vef_storage_arena_t *arena_ctx,
                     vef_storage_arena_func_t arena_alloc,
                     vef_storage_ctx_t **storage, char *error_msg,
                     uint32_t error_msg_len) {
    void *arena_mem = AllocateArenaStorage(arena_ctx, arena_alloc);
    if (arena_mem == nullptr) {
      snprintf(error_msg, error_msg_len, "out of memory allocating arena");
      return true;
    }
    auto *arena = new (arena_mem) Arena(arena_ctx, arena_alloc);
    auto *ctx = arena->construct<StorageCtx<UserCtx>>(arena);
    if (ctx == nullptr || ctx->user() == nullptr) {
      arena->~Arena();
      snprintf(error_msg, error_msg_len,
               "out of memory allocating storage context");
      return true;
    }
    *storage = reinterpret_cast<vef_storage_ctx_t *>(ctx);
    bool err = F(ctx, space_ref, trx_ref, col_len, error_msg, error_msg_len);
    if (err) {
      arena->~Arena();
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
    arena->~Arena();
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
    void *arena_mem = AllocateArenaStorage(arena_ctx, arena_alloc);
    if (arena_mem == nullptr) {
      snprintf(error_msg, error_msg_len, "out of memory allocating arena");
      return true;
    }
    auto *arena = new (arena_mem) Arena(arena_ctx, arena_alloc);
    auto *ctx = arena->construct<StorageCtx<UserCtx>>(arena);
    if (ctx == nullptr || ctx->user() == nullptr) {
      arena->~Arena();
      snprintf(error_msg, error_msg_len,
               "out of memory allocating storage context");
      return true;
    }
    *storage = reinterpret_cast<vef_storage_ctx_t *>(ctx);
    bool err = F(ctx, storage_ref, error_msg, error_msg_len);
    if (err) {
      arena->~Arena();
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
  constexpr explicit StorageBuilder(const char *type_name)
      : type_name_(type_name), intf_{} {}

  // Each setter static_asserts that F exactly matches the expected function
  // pointer type (detail::CreateFn<UserCtx>, etc.), rejecting wrong signatures,
  // lambdas, and member function pointers at compile time.
  template <auto F>
  constexpr StorageBuilder &create() {
    static_assert(std::is_same_v<decltype(F), detail::CreateFn<UserCtx>>,
                  "create: expected bool(*)(StorageCtx<UserCtx>*, Space::Ref, "
                  "Segment::TrxRef, uint32_t col_len, char*, uint32_t)");
    intf_.create = detail::CreateWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &drop() {
    static_assert(
        std::is_same_v<decltype(F), detail::DropFn<UserCtx>>,
        "drop: expected bool(*)(StorageCtx<UserCtx>*, Segment::TrxRef, "
        "char*, uint32_t)");
    intf_.drop = detail::DropWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &load() {
    static_assert(
        std::is_same_v<decltype(F), detail::LoadFn<UserCtx>>,
        "load: expected bool(*)(StorageCtx<UserCtx>*, Column::StorageRef, "
        "char*, uint32_t)");
    intf_.load = detail::LoadWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &insert() {
    static_assert(std::is_same_v<decltype(F), detail::InsertFn<UserCtx>>,
                  "insert: expected bool(*)(StorageCtx<UserCtx>*, MtrCtx::Ref, "
                  "Segment::TrxRef, Column::Data, Column::Data, Column::Ref*, "
                  "char*, uint32_t)");
    intf_.insert = detail::InsertWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &select() {
    static_assert(
        std::is_same_v<decltype(F), detail::SelectFn<UserCtx>>,
        "select: expected bool(*)(StorageCtx<UserCtx>*, MtrCtx::Ref, "
        "Column::Ref, Column::Data*, Column::Data*, Segment::TrxRef*, "
        "bool*, char*, uint32_t)");
    intf_.select = detail::SelectWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &mark_delete() {
    static_assert(
        std::is_same_v<decltype(F), detail::MarkDeleteFn<UserCtx>>,
        "mark_delete: expected bool(*)(StorageCtx<UserCtx>*, MtrCtx::Ref, "
        "Segment::TrxRef, Column::Ref, bool delete_mark, char*, uint32_t)");
    intf_.mark_delete = detail::MarkDeleteWrapper<F, UserCtx>::invoke;
    return *this;
  }

  template <auto F>
  constexpr StorageBuilder &purge() {
    static_assert(std::is_same_v<decltype(F), detail::PurgeFn<UserCtx>>,
                  "purge: expected bool(*)(StorageCtx<UserCtx>*, MtrCtx::Ref, "
                  "Segment::TrxRef, Column::Ref, char*, uint32_t)");
    intf_.purge = detail::PurgeWrapper<F, UserCtx>::invoke;
    return *this;
  }

  constexpr TypeStorageDescriptor build() const {
    if (!intf_.create || !intf_.drop || !intf_.load || !intf_.insert ||
        !intf_.select || !intf_.mark_delete || !intf_.purge) {
      // Calling a non-constexpr function here causes a compile-time error
      // when build() is evaluated in a constant expression. The function name
      // appears verbatim in the compiler diagnostic.
      StorageBuilder_all_column_storage_interfaces_must_be_registered();
    }
    vef_type_storage_intf_t result = intf_;
    result.version = VEF_STORAGE_TYPE_INTF_VERSION;
    result.type_name = type_name_;
    return TypeStorageDescriptor{result};
  }

 private:
  const char *type_name_;
  vef_type_storage_intf_t intf_;
};

// Entry point for creating a StorageBuilder.
//
// Pass a TypeObject produced by vsql::make_type<Name>().build():
//   make_column_store<MyCtx>(MY_TYPE)...

template <
    typename UserCtx, typename TypeObj,
    typename = decltype(std::declval<const TypeObj &>().descriptor.vef_desc)>
constexpr StorageBuilder<UserCtx> make_column_store(const TypeObj &type) {
  return StorageBuilder<UserCtx>{type.name()};
}

}  // namespace vsql::preview_storage_builder

#include <villagesql/preview/detail/storage_builder_register.h>

#endif  // VILLAGESQL_PREVIEW_STORAGE_BUILDER_H
