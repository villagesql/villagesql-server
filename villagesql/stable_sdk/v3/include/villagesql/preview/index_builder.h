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

#ifndef VILLAGESQL_PREVIEW_INDEX_BUILDER_H
#define VILLAGESQL_PREVIEW_INDEX_BUILDER_H

// This file provides the C++ builder API for defining custom index types,
// index profiles, and index functions.
//
// QUICK START
// -----------
//
// 1. Define your per-index storage context and optional options struct:
//
//   struct HNSWOptions {
//     int M = 16;
//     int ef_construction = 200;
//     static bool parse(const vef_index_param_t *params, uint32_t count,
//                       HNSWOptions *out, char *error_msg,
//                       uint32_t error_msg_len);
//   };
//
//   struct HNSWContext {
//     // Per-index-instance runtime state
//   };
//
// 2. Implement the lifecycle, DML, and scan hooks (see signatures below).
//
// 3. Register with make_index_type:
//
//   static constexpr const char kHNSW[] = "hnsw";
//
//   static constexpr auto HNSW_INDEX =
//       make_index_type<kHNSW, HNSWContext>()
//           .lifecycle()
//               .create<&hnsw_create>()
//               .load<&hnsw_load>()
//               .drop<&hnsw_drop>()
//           .dml()
//               .insert<&hnsw_insert>()
//               .mark_delete<&hnsw_mark_delete>()
//               .purge<&hnsw_purge>()
//           .scan()
//               .begin<&hnsw_begin_scan>()
//               .position<&hnsw_position>()
//               .fetch<&hnsw_fetch>()
//               .save<&hnsw_save>()
//               .restore<&hnsw_restore>()
//               .end<&hnsw_end_scan>()
//           .global()
//               .capabilities(IndexSupport::KNN)
//               .storage_props(IndexStorage::HAS_COLUMN_REF |
//                              IndexStorage::REF_LOOKUP)
//               .options<HNSWOptions, &HNSWOptions::parse>()
//               .build();
//
// EXTENSION FUNCTION SIGNATURES
// ------------------------------
//
// Ctx = IndexStorageCtx<HNSWContext>. Extensions access per-index state via
// ctx->user() and the InnoDB arena via ctx->arena().
//
//   Lifecycle:
//     bool fn(Ctx*, const vef_index_ctx_t*, vef_storage_space_ref_t,
//             vef_storage_trx_ref_t, char*, uint32_t);           // create
//     bool fn(Ctx*, const vef_index_ctx_t*, vef_storage_trx_ref_t,
//             char*, uint32_t);                                   // drop
//     bool fn(Ctx*, const vef_index_ctx_t*, vef_storage_ref_t,
//             char*, uint32_t);                                   // load
//
//   DML:
//     bool fn(Ctx*, const vef_index_ctx_t*, vef_storage_trx_ref_t,
//             vef_storage_col_data_t* keys, vef_storage_col_data_t* pkeys,
//             vef_storage_col_ref_t* key_ref, char*, uint32_t);  // insert
//     bool fn(Ctx*, const vef_index_ctx_t*, vef_storage_trx_ref_t,
//             vef_storage_col_ref_t* key_ref,
//             vef_storage_col_data_t* keys, vef_storage_col_data_t* pkeys,
//             bool delete_mark, char*, uint32_t);                // mark_delete
//     bool fn(Ctx*, const vef_index_ctx_t*, vef_storage_trx_ref_t,
//             vef_storage_col_ref_t* key_ref,
//             vef_storage_col_data_t* keys, vef_storage_col_data_t* pkeys,
//             char*, uint32_t);                                   // purge
//
//   Scan:
//     bool fn(Ctx*, const vef_index_ctx_t*, vef_storage_mtr_ref_t,
//             const vef_index_scan_desc_t*, vef_index_cursor_ref_t*, bool*,
//             char*, uint32_t);                                   // begin
//     bool fn(vef_index_cursor_ref_t, vef_index_cursor_op_t, bool*,
//             char*, uint32_t);                                   // position
//     bool fn(vef_index_cursor_ref_t, vef_storage_col_ref_t*,
//             vef_storage_col_data_t* keys, vef_storage_col_data_t* pkeys,
//             char*, uint32_t);                                   // fetch
//     bool fn(vef_index_cursor_ref_t, char*, uint32_t);          // save
//     bool fn(vef_index_cursor_ref_t, vef_storage_mtr_ref_t, bool*,
//             char*, uint32_t);                                   // restore
//     void fn(vef_index_cursor_ref_t*);                          // end
//
// CURSOR OWNERSHIP
// ----------------
//
// Cursor state is managed entirely by the extension. In begin(), allocate a
// cursor object and assign it to *cursor. In end(), free the cursor and set
// *cursor to nullptr. The SDK does not wrap cursor state.
//
// INDEX PROFILE
// -------------
//
//   make_index_profile("hnsw_l2")
//       .for_type(SVECTOR)
//       .using_index("hnsw")
//       .with_function(1, "l2_distance")   // fn_id 1 -> l2_distance
//       .ordering(IndexOrdering::ASC)
//       .build();
//
// INDEX FUNCTION
// --------------
//
//   make_index_function<&svector_distance_l2>("l2_distance")
//       .returns(REAL)
//       .param(SVECTOR)
//       .param(SVECTOR)
//       .deterministic()
//       .build();
//

#include <cstdio>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

#include <villagesql/abi/preview/index.h>
#include <villagesql/preview/storage_api.h>

namespace vsql::preview_index_builder {

using Arena = vsql::preview_storage::Arena;

// Index capability flags for use with .capabilities(). These mirror the
// VEF_INDEX_CAP_* constants in index.h and can be combined with |.
enum class IndexSupport : vef_index_cap_t {
  POINT_LOOKUP = VEF_INDEX_CAP_POINT_LOOKUP,
  RANGE_SCAN = VEF_INDEX_CAP_RANGE_SCAN,
  REVERSE_SCAN = VEF_INDEX_CAP_REVERSE_SCAN,
  ORDER_BY = VEF_INDEX_CAP_ORDER_BY,
  KNN = VEF_INDEX_CAP_KNN,
};

constexpr IndexSupport operator|(IndexSupport a, IndexSupport b) {
  return static_cast<IndexSupport>(static_cast<vef_index_cap_t>(a) |
                                   static_cast<vef_index_cap_t>(b));
}

// Index storage property flags for use with .storage_props(). These mirror
// the VEF_INDEX_STORAGE_* constants in index.h and can be combined with |.
enum class IndexStorage : vef_index_storage_t {
  HAS_COLUMN_REF = VEF_INDEX_STORAGE_HAS_COLUMN_REF,
  HAS_ROW_REF = VEF_INDEX_STORAGE_HAS_ROW_REF,
  REF_LOOKUP = VEF_INDEX_STORAGE_REF_LOOKUP,
};

constexpr IndexStorage operator|(IndexStorage a, IndexStorage b) {
  return static_cast<IndexStorage>(static_cast<vef_index_storage_t>(a) |
                                   static_cast<vef_index_storage_t>(b));
}

// Ordering of index scan results for use with make_index_profile().
enum class IndexOrdering : uint8_t {
  ASC = 0,
  DESC = 1,
};

// The SDK allocates IndexStorageCtx<T> and T from the InnoDB arena before
// calling create/load, and destroys the arena (which calls ~T) after drop
// returns. Extensions must not delete ctx, ctx->user(), or the arena directly.
template <typename T>
class IndexStorageCtx {
 public:
  explicit IndexStorageCtx(Arena *arena) : m_arena(arena) {
    verify_layout();
    m_user = m_arena->construct<T>();
  }

  T *user() { return m_user; }
  const T *user() const { return m_user; }
  Arena &arena() { return *m_arena; }

  void set_ref(vef_storage_ref_t ref) { m_ctx.ref = ref; }
  vef_storage_ref_t get_ref() const { return m_ctx.ref; }

 private:
  vef_storage_ctx_t m_ctx{};
  Arena *m_arena = nullptr;
  T *m_user = nullptr;

  // IndexStorageCtx<T> wraps vef_storage_ctx_t with a typed per-index context
  // T. Must be standard layout so the SDK can cast it to/from
  // vef_storage_ctx_t*.
  static void verify_layout() {
    static_assert(std::is_standard_layout_v<IndexStorageCtx>,
                  "IndexStorageCtx<T> must be standard layout for ABI cast — "
                  "check that no non-standard-layout member was added");
    static_assert(offsetof(IndexStorageCtx, m_ctx) == 0,
                  "IndexStorageCtx<T> must begin with m_ctx for ABI cast");
  }
};

namespace detail {

// Note: Wrapper static methods use C++ linkage. The C++ standard does not
// allow extern "C" on template instantiations. The ABI boundary relies on C
// and C++ having compatible calling conventions on all supported platforms.

template <auto F, typename Context>
struct CreateWrapper {
  static bool invoke(const vef_index_ctx_t *index_ctx,
                     vef_storage_space_ref_t space_ref,
                     vef_storage_trx_ref_t trx_ref,
                     vef_storage_arena_t *arena_ctx,
                     vef_storage_arena_func_t arena_alloc,
                     vef_storage_ctx_t **storage, char *error_msg,
                     uint32_t error_msg_len) {
    auto *arena = new (std::nothrow) Arena(arena_ctx, arena_alloc);
    if (arena == nullptr) {
      snprintf(error_msg, error_msg_len, "out of memory allocating arena");
      return true;
    }
    auto *ctx = arena->construct<IndexStorageCtx<Context>>(arena);
    if (ctx == nullptr || ctx->user() == nullptr) {
      delete arena;
      snprintf(error_msg, error_msg_len,
               "out of memory allocating index storage context");
      return true;
    }
    *storage = reinterpret_cast<vef_storage_ctx_t *>(ctx);
    bool err = F(ctx, index_ctx, space_ref, trx_ref, error_msg, error_msg_len);
    if (err) {
      delete arena;
      *storage = nullptr;
    }
    return err;
  }
};

template <auto F, typename Context>
struct DropWrapper {
  static bool invoke(const vef_index_ctx_t *index_ctx,
                     vef_storage_ctx_t *storage, vef_storage_trx_ref_t trx_ref,
                     char *error_msg, uint32_t error_msg_len) {
    auto *ctx = reinterpret_cast<IndexStorageCtx<Context> *>(storage);
    Arena *arena = &ctx->arena();
    bool err = F(ctx, index_ctx, trx_ref, error_msg, error_msg_len);
    delete arena;
    return err;
  }
};

template <auto F, typename Context>
struct LoadWrapper {
  static bool invoke(const vef_index_ctx_t *index_ctx,
                     vef_storage_ref_t storage_ref,
                     vef_storage_arena_t *arena_ctx,
                     vef_storage_arena_func_t arena_alloc,
                     vef_storage_ctx_t **storage, char *error_msg,
                     uint32_t error_msg_len) {
    auto *arena = new (std::nothrow) Arena(arena_ctx, arena_alloc);
    if (arena == nullptr) {
      snprintf(error_msg, error_msg_len, "out of memory allocating arena");
      return true;
    }
    auto *ctx = arena->construct<IndexStorageCtx<Context>>(arena);
    if (ctx == nullptr || ctx->user() == nullptr) {
      delete arena;
      snprintf(error_msg, error_msg_len,
               "out of memory allocating index storage context");
      return true;
    }
    *storage = reinterpret_cast<vef_storage_ctx_t *>(ctx);
    bool err = F(ctx, index_ctx, storage_ref, error_msg, error_msg_len);
    if (err) {
      delete arena;
      *storage = nullptr;
    }
    return err;
  }
};

template <auto F, typename Context>
struct InsertWrapper {
  static bool invoke(const vef_index_ctx_t *index_ctx,
                     vef_storage_ctx_t *storage, vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_data_t *key_columns,
                     vef_storage_col_data_t *pkey_columns,
                     vef_storage_col_ref_t *key_ref, char *error_msg,
                     uint32_t error_msg_len) {
    return F(reinterpret_cast<IndexStorageCtx<Context> *>(storage), index_ctx,
             trx_ref, key_columns, pkey_columns, key_ref, error_msg,
             error_msg_len);
  }
};

template <auto F, typename Context>
struct MarkDeleteWrapper {
  static bool invoke(const vef_index_ctx_t *index_ctx,
                     vef_storage_ctx_t *storage, vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_ref_t *key_ref,
                     vef_storage_col_data_t *key_columns,
                     vef_storage_col_data_t *pkey_columns, bool delete_mark,
                     char *error_msg, uint32_t error_msg_len) {
    return F(reinterpret_cast<IndexStorageCtx<Context> *>(storage), index_ctx,
             trx_ref, key_ref, key_columns, pkey_columns, delete_mark,
             error_msg, error_msg_len);
  }
};

template <auto F, typename Context>
struct PurgeWrapper {
  static bool invoke(const vef_index_ctx_t *index_ctx,
                     vef_storage_ctx_t *storage, vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_ref_t *key_ref,
                     vef_storage_col_data_t *key_columns,
                     vef_storage_col_data_t *pkey_columns, char *error_msg,
                     uint32_t error_msg_len) {
    return F(reinterpret_cast<IndexStorageCtx<Context> *>(storage), index_ctx,
             trx_ref, key_ref, key_columns, pkey_columns, error_msg,
             error_msg_len);
  }
};

template <auto F, typename Context>
struct ScanBeginWrapper {
  static bool invoke(const vef_index_ctx_t *index_ctx,
                     vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     const vef_index_scan_desc_t *scan_desc,
                     vef_index_cursor_ref_t *cursor, bool *eof, char *error_msg,
                     uint32_t error_msg_len) {
    return F(reinterpret_cast<IndexStorageCtx<Context> *>(storage), index_ctx,
             mctx, scan_desc, cursor, eof, error_msg, error_msg_len);
  }
};

// For the remaining scan functions (position, fetch, save, restore, end) the
// user function signature matches the ABI signature directly — no storage
// context cast is needed. These wrappers are thin pass-throughs for calling
// convention consistency.

template <auto F>
struct ScanPositionWrapper {
  static bool invoke(vef_index_cursor_ref_t cursor, vef_index_cursor_op_t op,
                     bool *eof, char *error_msg, uint32_t error_msg_len) {
    return F(cursor, op, eof, error_msg, error_msg_len);
  }
};

template <auto F>
struct ScanFetchWrapper {
  static bool invoke(vef_index_cursor_ref_t cursor,
                     vef_storage_col_ref_t *key_ref,
                     vef_storage_col_data_t *key_columns,
                     vef_storage_col_data_t *pkey_columns, char *error_msg,
                     uint32_t error_msg_len) {
    return F(cursor, key_ref, key_columns, pkey_columns, error_msg,
             error_msg_len);
  }
};

template <auto F>
struct ScanSaveWrapper {
  static bool invoke(vef_index_cursor_ref_t cursor, char *error_msg,
                     uint32_t error_msg_len) {
    return F(cursor, error_msg, error_msg_len);
  }
};

template <auto F>
struct ScanRestoreWrapper {
  static bool invoke(vef_index_cursor_ref_t cursor, vef_storage_mtr_ref_t mctx,
                     bool *eof, char *error_msg, uint32_t error_msg_len) {
    return F(cursor, mctx, eof, error_msg, error_msg_len);
  }
};

template <auto F>
struct ScanEndWrapper {
  static void invoke(vef_index_cursor_ref_t *cursor) { F(cursor); }
};

// Adapts a typed parse function to the C ABI's void* options_out.
// ParseFn must have signature:
//   bool fn(const vef_index_param_t*, uint32_t, Options*, char*, uint32_t)
template <typename Options, auto ParseFn>
struct ParseWrapper {
  static bool invoke(const vef_index_param_t *params, uint32_t count,
                     void *options_out, char *error_msg,
                     uint32_t error_msg_len) {
    return ParseFn(params, count, static_cast<Options *>(options_out),
                   error_msg, error_msg_len);
  }
};

// always_false<T> is always false but depends on T, so static_assert(
// always_false<T>, ...) in a function template is only evaluated on
// instantiation (i.e. when the function is actually called), not eagerly.
template <typename...>
inline constexpr bool always_false = false;

}  // namespace detail

// IndexTypeBuilder_storage_props_must_set_row_or_column_ref is a non-constexpr
// function whose name appears in the compiler diagnostic when
// GlobalBuilder::build() is evaluated as a constant expression with a
// storage_props value that has neither HAS_ROW_REF nor HAS_COLUMN_REF.
void IndexTypeBuilder_storage_props_must_set_row_or_column_ref();

// Descriptor returned by GlobalBuilder::build(). Consumed by the extension
// builder (e.g. make_extension().index_type(HNSW_INDEX)).
struct IndexTypeDesc {
  const char *name;
  vef_type_index_intf_t intf;
};

// make_index_type<Name, Context> builds a vef_type_index_intf_t through four
// mandatory sections chained in strict order:
//
//   .lifecycle()  ->  .dml()  ->  .scan()  ->  .global()  ->  .build()
//
// Each section has mandatory methods that must all be called before the chain
// can advance.  Calling the next section's gate method with any mandatory
// method missing produces a static_assert naming the missing registration.
// Methods within a section may appear in any order.
//
// Context is the extension-defined per-index storage state. The SDK allocates
// IndexStorageCtx<Context> from the InnoDB arena before calling create/load,
// and destroys the arena (which calls ~Context) after drop returns.

// Forward declarations.
template <typename Context, uint32_t Bits>
class LifecycleBuilder;
template <typename Context, uint32_t Bits>
class DMLBuilder;
template <typename Context, uint32_t Bits>
class ScanBuilder;
template <typename Context, uint32_t Bits>
class GlobalBuilder;

// LifecycleBuilder — entered via IndexTypeRootBuilder::lifecycle().
// Mandatory before .dml(): create(), load(), drop().
template <typename Context, uint32_t Bits>
class LifecycleBuilder {
  static constexpr uint32_t kCreate = 1u << 0;
  static constexpr uint32_t kLoad = 1u << 1;
  static constexpr uint32_t kDrop = 1u << 2;

 public:
  constexpr LifecycleBuilder(const char *name, vef_type_index_intf_t intf)
      : name_(name), intf_(intf) {}

  template <auto F>
  constexpr LifecycleBuilder<Context, Bits | kCreate> create() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "create: F must be a plain function pointer");
    intf_.create = detail::CreateWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr LifecycleBuilder<Context, Bits | kLoad> load() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "load: F must be a plain function pointer");
    intf_.load = detail::LoadWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr LifecycleBuilder<Context, Bits | kDrop> drop() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "drop: F must be a plain function pointer");
    intf_.drop = detail::DropWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  constexpr DMLBuilder<Context, 0> dml() && {
    static_assert(
        (Bits & kCreate) != 0,
        "lifecycle: create() must be registered before calling dml()");
    static_assert((Bits & kLoad) != 0,
                  "lifecycle: load() must be registered before calling dml()");
    static_assert((Bits & kDrop) != 0,
                  "lifecycle: drop() must be registered before calling dml()");
    return {name_, intf_};
  }

  template <typename T = void>
  constexpr ScanBuilder<Context, 0> scan() && {
    static_assert(detail::always_false<T>,
                  "dml() section must be entered before scan()");
    return {name_, intf_};
  }

  template <typename T = void>
  constexpr GlobalBuilder<Context, 0> global() && {
    static_assert(detail::always_false<T>,
                  "dml() and scan() sections must be entered before global()");
    return {name_, intf_};
  }

 private:
  const char *name_;
  vef_type_index_intf_t intf_;
};

// DMLBuilder — entered via LifecycleBuilder::dml().
// Mandatory before .scan(): insert(), mark_delete(), purge().
template <typename Context, uint32_t Bits>
class DMLBuilder {
  static constexpr uint32_t kInsert = 1u << 0;
  static constexpr uint32_t kMarkDelete = 1u << 1;
  static constexpr uint32_t kPurge = 1u << 2;

 public:
  constexpr DMLBuilder(const char *name, vef_type_index_intf_t intf)
      : name_(name), intf_(intf) {}

  template <auto F>
  constexpr DMLBuilder<Context, Bits | kInsert> insert() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "insert: F must be a plain function pointer");
    intf_.insert = detail::InsertWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr DMLBuilder<Context, Bits | kMarkDelete> mark_delete() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "mark_delete: F must be a plain function pointer");
    intf_.mark_delete = detail::MarkDeleteWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr DMLBuilder<Context, Bits | kPurge> purge() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "purge: F must be a plain function pointer");
    intf_.purge = detail::PurgeWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  constexpr ScanBuilder<Context, 0> scan() && {
    static_assert((Bits & kInsert) != 0,
                  "dml: insert() must be registered before calling scan()");
    static_assert(
        (Bits & kMarkDelete) != 0,
        "dml: mark_delete() must be registered before calling scan()");
    static_assert((Bits & kPurge) != 0,
                  "dml: purge() must be registered before calling scan()");
    return {name_, intf_};
  }

  template <typename T = void>
  constexpr GlobalBuilder<Context, 0> global() && {
    static_assert(detail::always_false<T>,
                  "scan() section must be entered before global()");
    return {name_, intf_};
  }

 private:
  const char *name_;
  vef_type_index_intf_t intf_;
};

// ScanBuilder — entered via DMLBuilder::scan().
// Mandatory before .global(): begin(), position(), fetch(), save(), restore(),
// end().
template <typename Context, uint32_t Bits>
class ScanBuilder {
  static constexpr uint32_t kBegin = 1u << 0;
  static constexpr uint32_t kPosition = 1u << 1;
  static constexpr uint32_t kFetch = 1u << 2;
  static constexpr uint32_t kSave = 1u << 3;
  static constexpr uint32_t kRestore = 1u << 4;
  static constexpr uint32_t kEnd = 1u << 5;

 public:
  constexpr ScanBuilder(const char *name, vef_type_index_intf_t intf)
      : name_(name), intf_(intf) {}

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kBegin> begin() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "begin: F must be a plain function pointer");
    intf_.scan_begin = detail::ScanBeginWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kPosition> position() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "position: F must be a plain function pointer");
    intf_.scan_position = detail::ScanPositionWrapper<F>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kFetch> fetch() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "fetch: F must be a plain function pointer");
    intf_.scan_fetch = detail::ScanFetchWrapper<F>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kSave> save() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "save: F must be a plain function pointer");
    intf_.scan_save = detail::ScanSaveWrapper<F>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kRestore> restore() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "restore: F must be a plain function pointer");
    intf_.scan_restore = detail::ScanRestoreWrapper<F>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kEnd> end() && {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "end: F must be a plain function pointer");
    intf_.scan_end = detail::ScanEndWrapper<F>::invoke;
    return {name_, intf_};
  }

  constexpr GlobalBuilder<Context, 0> global() && {
    static_assert((Bits & kBegin) != 0,
                  "scan: begin() must be registered before calling global()");
    static_assert(
        (Bits & kPosition) != 0,
        "scan: position() must be registered before calling global()");
    static_assert((Bits & kFetch) != 0,
                  "scan: fetch() must be registered before calling global()");
    static_assert((Bits & kSave) != 0,
                  "scan: save() must be registered before calling global()");
    static_assert((Bits & kRestore) != 0,
                  "scan: restore() must be registered before calling global()");
    static_assert((Bits & kEnd) != 0,
                  "scan: end() must be registered before calling global()");
    return {name_, intf_};
  }

  template <typename T = void>
  constexpr IndexTypeDesc build() && {
    static_assert(detail::always_false<T>,
                  "global() section must be entered before build()");
    return {name_, intf_};
  }

 private:
  const char *name_;
  vef_type_index_intf_t intf_;
};

// GlobalBuilder — entered via ScanBuilder::global().
// Mandatory before .build(): capabilities(), storage_props().
// Optional: options().
template <typename Context, uint32_t Bits>
class GlobalBuilder {
  static constexpr uint32_t kCapabilities = 1u << 0;
  static constexpr uint32_t kStorageProps = 1u << 1;

 public:
  constexpr GlobalBuilder(const char *name, vef_type_index_intf_t intf)
      : name_(name), intf_(intf) {}

  constexpr GlobalBuilder<Context, Bits | kCapabilities> capabilities(
      IndexSupport caps) && {
    intf_.capabilities = static_cast<vef_index_cap_t>(caps);
    return {name_, intf_};
  }

  constexpr GlobalBuilder<Context, Bits | kStorageProps> storage_props(
      IndexStorage props) && {
    intf_.storage_props = static_cast<vef_index_storage_t>(props);
    return {name_, intf_};
  }

  // Bind a parameterized options struct and its parse function.
  // ParseFn must have signature:
  //   bool fn(const vef_index_param_t*, uint32_t, OptionsStruct*,
  //           char*, uint32_t)
  // The server allocates sizeof(OptionsStruct) bytes, calls parse() to fill
  // them, then passes the result as index_ctx->options to create().
  template <typename OptionsStruct, auto ParseFn>
  constexpr GlobalBuilder<Context, Bits> options() && {
    intf_.options_size = static_cast<uint32_t>(sizeof(OptionsStruct));
    intf_.parse = detail::ParseWrapper<OptionsStruct, ParseFn>::invoke;
    return {name_, intf_};
  }

  constexpr IndexTypeDesc build() && {
    static_assert((Bits & kCapabilities) != 0,
                  "global: capabilities() must be set before calling build()");
    static_assert((Bits & kStorageProps) != 0,
                  "global: storage_props() must be set before calling build()");
    if constexpr ((Bits & kStorageProps) != 0) {
      if (!(intf_.storage_props & (VEF_INDEX_STORAGE_HAS_ROW_REF |
                                   VEF_INDEX_STORAGE_HAS_COLUMN_REF))) {
        IndexTypeBuilder_storage_props_must_set_row_or_column_ref();
      }
    }
    vef_type_index_intf_t intf = intf_;
    intf.version = VEF_INDEX_TYPE_INTF_VERSION;
    return {name_, intf};
  }

 private:
  const char *name_;
  vef_type_index_intf_t intf_;
};

// IndexTypeRootBuilder — returned by make_index_type<Name, Context>().
// The only available method is .lifecycle(), which begins the section chain.
template <const char *Name, typename Context>
class IndexTypeRootBuilder {
 public:
  constexpr IndexTypeRootBuilder() : intf_{} {}

  constexpr LifecycleBuilder<Context, 0> lifecycle() && {
    return {Name, intf_};
  }

 private:
  vef_type_index_intf_t intf_;
};

template <const char *Name, typename Context>
constexpr IndexTypeRootBuilder<Name, Context> make_index_type() {
  return IndexTypeRootBuilder<Name, Context>{};
}

// ===========================================================================
// make_index_profile
// ===========================================================================
//
// Defines how a specific custom type behaves with a given index type. Each
// .with_function(fn_id, name) call binds a profile function to the fn_id used
// by the index storage implementation when calling vef_index_profile_fn.
//
// Descriptor returned by make_index_function().build(). Passing one of these
// to IndexProfileBuilder::with_function() instead of a raw string enforces
// that the bound function was declared by the same extension.
struct IndexFunctionDesc {
  const char *name;
};

struct IndexProfileFunctionBinding {
  uint32_t fn_id;
  const char *function_name;
};

struct IndexProfileDesc {
  const char *name;
  const char *type_name;
  const char *index_type_name;
  std::vector<IndexProfileFunctionBinding> functions;
  bool ordering_asc;
  bool default_for_type;
};

class IndexProfileBuilder {
 public:
  explicit IndexProfileBuilder(const char *name) {
    desc_.name = name;
    desc_.type_name = nullptr;
    desc_.index_type_name = nullptr;
    desc_.ordering_asc = true;
    desc_.default_for_type = false;
  }

  IndexProfileBuilder &for_type(const char *type_name) {
    desc_.type_name = type_name;
    return *this;
  }

  IndexProfileBuilder &using_index(const char *index_type_name) {
    desc_.index_type_name = index_type_name;
    return *this;
  }

  // Bind fn_id to a function registered by make_index_function or make_func.
  // Multiple calls register multiple bindings; fn_ids must be unique within
  // the profile.
  IndexProfileBuilder &with_function(uint32_t fn_id,
                                     const char *function_name) {
    desc_.functions.push_back({fn_id, function_name});
    return *this;
  }

  // Typed overload: accepts an IndexFunctionDesc directly, enforcing that the
  // bound function was declared by the same extension via make_index_function.
  IndexProfileBuilder &with_function(uint32_t fn_id,
                                     const IndexFunctionDesc &fn) {
    desc_.functions.push_back({fn_id, fn.name});
    return *this;
  }

  IndexProfileBuilder &ordering(IndexOrdering ord) {
    desc_.ordering_asc = (ord == IndexOrdering::ASC);
    return *this;
  }

  // When true, this profile is used if no profile is named at CREATE INDEX.
  IndexProfileBuilder &default_for_type(bool is_default) {
    desc_.default_for_type = is_default;
    return *this;
  }

  IndexProfileDesc build() { return std::move(desc_); }

 private:
  IndexProfileDesc desc_;
};

inline IndexProfileBuilder make_index_profile(const char *name) {
  return IndexProfileBuilder(name);
}

// ===========================================================================
// make_index_function
// ===========================================================================
//
// Registers a function for internal use by index operations (distance,
// comparison). Index functions are distinct from SQL functions registered via
// make_func: the optimizer needs explicit metadata about which functions define
// index behavior, and index functions may be internal-only.
//
class IndexFunctionBuilder {
 public:
  explicit IndexFunctionBuilder(const char *name) { desc_.name = name; }

  IndexFunctionBuilder &returns(const char * /*type*/) { return *this; }
  IndexFunctionBuilder &param(const char * /*type*/) { return *this; }
  IndexFunctionBuilder &deterministic() { return *this; }

  IndexFunctionDesc build() { return desc_; }

 private:
  IndexFunctionDesc desc_;
};

// F is accepted as a template parameter so the call site matches the final API
// shape (make_index_function<&fn>("name")), but is intentionally not stored or
// called in this stub. The function pointer is discarded; only the name is
// retained.
template <auto F>
inline IndexFunctionBuilder make_index_function(const char *name) {
  return IndexFunctionBuilder(name);
}

}  // namespace vsql::preview_index_builder

#endif  // VILLAGESQL_PREVIEW_INDEX_BUILDER_H
