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
// INDEX FUNCTION
// --------------
//
// Index functions must be deterministic. Build one and store it as static
// const - it is passed by reference to IndexProfileBuilder::with_function().
//
//   static const auto MY_INDEX_FN =
//       make_index_function<&my_index_fn>("my_index_fn")
//           .returns(vsql::REAL)
//           .param(MY_TYPE)     // TypeDesc built by vsql::make_type<kMyType>
//           .param(MY_TYPE)
//           .deterministic()
//           .build();
//
// INDEX PROFILE
// -------------
// TODO(villagesql-indexing): Evaluate using string-based function identifiers
// instead of numeric fn_ids, and assess the impact on function invocation and
// builder-generated execution context.
//
// Binds a custom type to an index type and assigns per-profile function IDs.
// for_type() takes the type name string (the same kMyType constant passed to
// make_type<kMyType>), not the TypeDesc. with_function() takes an
// IndexFunctionDesc from make_index_function().build(), not a raw string.
//
//   static const auto MY_PROFILE =
//       make_index_profile("my_profile")
//           .for_type(kMyType)                // name string, not the TypeDesc
//           .using_index(kMyIndex)
//           .with_function(1, MY_INDEX_FN)    // fn_id 1 -> MY_INDEX_FN
//           .with_function(2, MY_HELPER_FN)   // fn_id 2 -> MY_HELPER_FN
//           .ordering(IndexOrdering::ASC)
//           .build();
//
// EXTENSION REGISTRATION
// ----------------------
//
// Putting the pieces together: index functions, index type, profile, capability
// tokens, and the extension entry point.
//
//   static constexpr const char kMyType[]  = "my_type";
//   static constexpr const char kMyIndex[] = "my_index";
//
//   // Type descriptor (see vsql.h). kMyType is the name string used below.
//   constexpr auto MY_TYPE = vsql::make_type<kMyType>()...build();
//
//   // Step 1 - index functions (static: addresses must be stable).
//   static const auto MY_INDEX_FN =
//       make_index_function<&my_index_fn>("my_index_fn")
//           .returns(vsql::REAL)
//           .param(MY_TYPE)
//           .param(MY_TYPE)
//           .deterministic()
//           .build();
//
//   static const auto MY_HELPER_FN =
//       make_index_function<&my_helper_fn>("my_helper_fn")
//           .returns(vsql::REAL)
//           .param(MY_TYPE)
//           .deterministic()
//           .build();
//
//   // Step 2 - index type (all 12 hooks required; constexpr).
//   static constexpr auto MY_INDEX =
//       make_index_type<kMyIndex, MyContext>()
//           .lifecycle()
//               .create<&my_create>()
//               .load<&my_load>()
//               .drop<&my_drop>()
//           .dml()
//               .insert<&my_insert>()
//               .mark_delete<&my_mark_delete>()
//               .purge<&my_purge>()
//           .scan()
//               .begin<&my_begin_scan>()
//               .position<&my_position>()
//               .fetch<&my_fetch>()
//               .save<&my_save>()
//               .restore<&my_restore>()
//               .end<&my_end_scan>()
//           .global()
//               .capabilities(IndexSupport::POINT_LOOKUP |
//                             IndexSupport::RANGE_SCAN | ...)
//               .storage_props(IndexStorage::HAS_ROW_REF | ...)
//               .options<MyOptions, &MyOptions::parse>()
//               .build();
//
//   // Step 3 - profile: for_type takes the name string, not the TypeDesc.
//   static const auto MY_PROFILE =
//       make_index_profile("my_profile")
//           .for_type(kMyType)
//           .using_index(kMyIndex)
//           .with_function(1, MY_INDEX_FN)
//           .with_function(2, MY_HELPER_FN)
//           .ordering(IndexOrdering::ASC)
//           .build();
//
//   // Step 4 - capability tokens (static: SDK holds pointers into them).
//   static auto INDEX_TYPE    = IndexTypeCapability().index_type(MY_INDEX);
//   static auto INDEX_PROFILE =
//   IndexProfileCapability().index_profile(MY_PROFILE);
//
//   // Step 5 - extension entry point.
//   VEF_GENERATE_ENTRY_POINTS(
//       vsql::make_extension()
//           .with(INDEX_TYPE)
//           .with(INDEX_PROFILE)
//           .type(MY_TYPE))

#include <algorithm>
#include <array>
#include <cstdio>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

#include <villagesql/abi/preview/index.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>
#include <villagesql/preview/storage_api.h>
#include <villagesql/vsql/func_builder.h>

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

// Ordering flags for use with make_index_profile().ordering(). Values can be
// combined with | to indicate a profile supports both scan directions.
enum class IndexOrdering : uint8_t {
  NONE = VEF_INDEX_ORDERING_NONE,
  ASC = VEF_INDEX_ORDERING_ASC,
  DESC = VEF_INDEX_ORDERING_DESC,
};

constexpr IndexOrdering operator|(IndexOrdering a, IndexOrdering b) {
  return static_cast<IndexOrdering>(static_cast<uint8_t>(a) |
                                    static_cast<uint8_t>(b));
}

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

// Descriptor returned by GlobalBuilder::build(). Passed to
// IndexTypeCapability::index_type() and registered via make_extension().with().
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

// Descriptor returned by make_index_function().build(). Passing one of these
// to IndexProfileBuilder::with_function() instead of a raw string enforces
// that the bound function was declared by the same extension.
struct IndexFunctionDesc {
  const char *name;
  vef_vdf_func_t vdf;
  vef_type_t return_type;
  std::array<vef_type_t, vsql::func_builder::kMaxParams> param_types;
  size_t num_params;
  bool is_deterministic;
};

struct IndexProfileFunctionBinding {
  uint32_t fn_id;
  IndexFunctionDesc function;
};

struct IndexProfileDesc {
  const char *name;
  const char *type_name;
  const char *index_type_name;
  // User-visible SQL functions associated with this profile.
  // The optimizer may generate index scan plans for calls to these functions.
  std::vector<IndexProfileFunctionBinding> functions;
  // Helper functions invoked only by the index implementation via profile_fn.
  // fn_ids are independent of the functions fn_id sequence.
  std::vector<IndexProfileFunctionBinding> helpers;
  uint8_t ordering;
  bool default_for_type;
};

class IndexProfileBuilder {
 public:
  explicit IndexProfileBuilder(const char *name) {
    desc_.name = name;
    desc_.type_name = nullptr;
    desc_.index_type_name = nullptr;
    desc_.ordering = static_cast<uint8_t>(IndexOrdering::NONE);
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

  // Bind fn_id to a user-visible SQL function. The optimizer may generate an
  // index scan plan for calls to this function. fn_ids must be unique within
  // the functions list.
  IndexProfileBuilder &with_function(uint32_t fn_id,
                                     const IndexFunctionDesc &fn) {
    desc_.functions.push_back({fn_id, fn});
    return *this;
  }

  // Bind fn_id to a helper function invoked only by the index implementation
  // via vef_index_ctx_t.profile_fn. fn_ids are independent of the functions
  // sequence and must be unique within the helpers list.
  IndexProfileBuilder &with_helper(uint32_t fn_id,
                                   const IndexFunctionDesc &fn) {
    desc_.helpers.push_back({fn_id, fn});
    return *this;
  }

  IndexProfileBuilder &ordering(IndexOrdering ord) {
    desc_.ordering = static_cast<uint8_t>(ord);
    return *this;
  }

  // When true, this profile is used if no profile is named at CREATE INDEX.
  IndexProfileBuilder &default_for_type(bool is_default) {
    desc_.default_for_type = is_default;
    return *this;
  }

  IndexProfileDesc build() {
    auto by_fn_id = [](const IndexProfileFunctionBinding &a,
                       const IndexProfileFunctionBinding &b) {
      return a.fn_id < b.fn_id;
    };
    std::sort(desc_.functions.begin(), desc_.functions.end(), by_fn_id);
    std::sort(desc_.helpers.begin(), desc_.helpers.end(), by_fn_id);
    return std::move(desc_);
  }

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
// Registers a function for use by index operations (distance, comparison).
// Index functions extend the VDF builder: make_index_function<&fn>("name")
// uses the same .returns()/.param()/.deterministic() chain as make_func, and
// .build() produces an IndexFunctionDesc carrying the full VDF metadata.
//
// IndexFuncBuilder<F, N> wraps vsql::func_builder::FuncBuilder<F, N> via
// containment, shadowing the methods that must preserve the derived type
// through the chain. No changes to func_builder.h are required.

template <auto F, size_t NumParams, uint32_t Bits = 0,
          vsql::func_builder::ParamMode Mode =
              vsql::func_builder::ParamMode::kUnset>
class IndexFuncBuilder {
  using Inner = vsql::func_builder::FuncBuilder<F, NumParams, Mode>;
  static constexpr uint32_t kDeterministic = 1u << 0;

 public:
  explicit IndexFuncBuilder(Inner inner) : inner_(std::move(inner)) {}

  IndexFuncBuilder &returns(const char *t) {
    inner_.returns(t);
    return *this;
  }

  IndexFuncBuilder<F, NumParams + 1, Bits,
                   vsql::func_builder::ParamMode::kFixed>
  param(const char *t) const {
    return IndexFuncBuilder<F, NumParams + 1, Bits,
                            vsql::func_builder::ParamMode::kFixed>{
        inner_.param(t)};
  }

  IndexFuncBuilder<F, NumParams, Bits | kDeterministic, Mode> deterministic()
      && {
    inner_.deterministic(true);
    return IndexFuncBuilder<F, NumParams, Bits | kDeterministic, Mode>{
        std::move(inner_)};
  }

  IndexFunctionDesc build() const {
    static_assert(
        (Bits & kDeterministic) != 0,
        "index function: deterministic() must be called before build()");
    auto d = inner_.build();
    IndexFunctionDesc result{};
    result.name = d.name();
    result.vdf = d.vdf();
    result.return_type = d.return_type();
    result.num_params = d.num_params();
    for (size_t i = 0; i < d.num_params(); ++i) {
      result.param_types[i] = d.params()[i];
    }
    result.is_deterministic = d.deterministic();
    return result;
  }

 private:
  Inner inner_;
};

template <auto F>
inline IndexFuncBuilder<F, 0> make_index_function(const char *name) {
  return IndexFuncBuilder<F, 0>{vsql::func_builder::make_func<F>(name)};
}

// ===========================================================================
// IndexTypeCapability<N>
// ===========================================================================
//
// Capability token that registers custom index type implementations.
//
// Usage:
//   static constexpr auto HNSW_INDEX = make_index_type<...>()...build();
//
//   static auto INDEX_TYPE = IndexTypeCapability().index_type(HNSW_INDEX);
//
//   VEF_GENERATE_ENTRY_POINTS(
//       make_extension().with(INDEX_TYPE).type(MY_TYPE))
//
// Each .index_type(desc) call appends one descriptor and returns a new
// IndexTypeCapability<N+1>. INDEX_TYPE must have static storage duration.
// The IndexTypeDesc passed to each .index_type() call must also have static
// storage duration so that the intf pointer stored internally remains valid.

template <size_t N = 0>
class IndexTypeCapability
    : public std::conditional_t<
          (N > 0), ::vsql::detail::CapabilityBase<IndexTypeCapability<N>>,
          std::false_type> {
 public:
  static constexpr const char *kName = VEF_PREVIEW_INDEX_TYPE_NAME;
  static constexpr uint32_t kAbiVersion = VEF_PREVIEW_INDEX_TYPE_ABI_VERSION;

  IndexTypeCapability() : regs_{} {}

  IndexTypeCapability<N + 1> index_type(const IndexTypeDesc &d) const {
    return IndexTypeCapability<N + 1>(*this, {d.name, &d.intf});
  }

  const vef_preview_index_type_ext_desc_t *extension_desc() {
    ext_desc_.version = VEF_PREVIEW_INDEX_TYPE_ABI_VERSION;
    ext_desc_.count = static_cast<uint32_t>(N);
    ext_desc_.types = N > 0 ? regs_ : nullptr;
    return &ext_desc_;
  }

  void *vtable_{nullptr};

  template <size_t M>
  friend class IndexTypeCapability;

 private:
  template <size_t M>
  IndexTypeCapability(const IndexTypeCapability<M> &base,
                      vef_index_type_reg_t reg)
      : regs_{} {
    static_assert(M + 1 == N, "internal construction size mismatch");
    for (size_t i = 0; i < M; ++i) regs_[i] = base.regs_[i];
    regs_[M] = reg;
  }

  vef_index_type_reg_t regs_[N > 0 ? N : 1];
  vef_preview_index_type_ext_desc_t ext_desc_{};
};

// ===========================================================================
// IndexProfileCapability<N>
// ===========================================================================
//
// Capability token that registers index profiles. Each profile binds a custom
// type to an index type and declares its helper functions. The server registers
// those functions as SQL VDFs automatically when the profile is loaded.
//
// Usage:
//   static const auto MY_PROFILE = make_index_profile("hnsw_l2")...build();
//
//   static auto INDEX_PROFILE =
//       IndexProfileCapability().index_profile(MY_PROFILE);
//
//   VEF_GENERATE_ENTRY_POINTS(
//       make_extension().with(INDEX_PROFILE).type(MY_TYPE))
//
// Each .index_profile(desc) call appends one descriptor and returns a new
// IndexProfileCapability<N+1>. INDEX_PROFILE must have static storage
// duration. The IndexProfileDesc passed to each .index_profile() call must
// also have static storage duration.

template <size_t N = 0>
class IndexProfileCapability
    : public std::conditional_t<
          (N > 0), ::vsql::detail::CapabilityBase<IndexProfileCapability<N>>,
          std::false_type> {
 public:
  static constexpr const char *kName = VEF_PREVIEW_INDEX_PROFILE_NAME;
  static constexpr uint32_t kAbiVersion = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION;

  IndexProfileCapability() : descs_{}, c_regs_{}, ext_desc_{} {}

  IndexProfileCapability<N + 1> index_profile(const IndexProfileDesc &d) const {
    return IndexProfileCapability<N + 1>(*this, &d);
  }

  // Builds flat C ABI structs from the stored descriptors and returns a
  // pointer to the extension descriptor. Called once at registration time;
  // INDEX_PROFILE must be in static storage by then so that the internal
  // arrays have stable addresses.
  const vef_preview_index_profile_ext_desc_t *extension_desc() {
    for (size_t i = 0; i < N; ++i) {
      const IndexProfileDesc &d = *descs_[i];

      auto fill_bindings =
          [](const std::vector<IndexProfileFunctionBinding> &src,
             std::vector<vef_index_profile_fn_binding_t> &dst) {
            dst.clear();
            for (const auto &fb : src) {
              vef_index_profile_fn_binding_t b{};
              b.fn_id = fb.fn_id;
              b.name = fb.function.name;
              b.vdf = fb.function.vdf;
              b.signature.return_type = fb.function.return_type;
              b.signature.param_count =
                  static_cast<unsigned int>(fb.function.num_params);
              b.signature.params = fb.function.num_params > 0
                                       ? fb.function.param_types.data()
                                       : nullptr;
              b.is_deterministic = fb.function.is_deterministic ? 1 : 0;
              dst.push_back(std::move(b));
            }
          };

      fill_bindings(d.functions, fn_bindings_[i]);
      fill_bindings(d.helpers, helper_bindings_[i]);

      vef_index_profile_reg_t &r = c_regs_[i];
      r.name = d.name;
      r.type_name = d.type_name;
      r.index_type_name = d.index_type_name;
      r.function_count = static_cast<uint32_t>(fn_bindings_[i].size());
      r.functions = fn_bindings_[i].empty() ? nullptr : fn_bindings_[i].data();
      r.helper_count = static_cast<uint32_t>(helper_bindings_[i].size());
      r.helpers =
          helper_bindings_[i].empty() ? nullptr : helper_bindings_[i].data();
      r.ordering = d.ordering;
      r.default_for_type = d.default_for_type ? 1 : 0;
    }
    ext_desc_.version = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION;
    ext_desc_.count = static_cast<uint32_t>(N);
    ext_desc_.profiles = N > 0 ? c_regs_ : nullptr;
    return &ext_desc_;
  }

  void *vtable_{nullptr};

  template <size_t M>
  friend class IndexProfileCapability;

 private:
  template <size_t M>
  IndexProfileCapability(const IndexProfileCapability<M> &base,
                         const IndexProfileDesc *d)
      : descs_{}, c_regs_{}, ext_desc_{} {
    static_assert(M + 1 == N, "internal construction size mismatch");
    for (size_t i = 0; i < M; ++i) descs_[i] = base.descs_[i];
    descs_[M] = d;
  }

  const IndexProfileDesc *descs_[N > 0 ? N : 1];
  vef_index_profile_reg_t c_regs_[N > 0 ? N : 1];
  // Flat binding arrays built lazily in extension_desc(). One vector per
  // profile; .data() is pointed to by c_regs_[i].functions / .helpers.
  std::vector<vef_index_profile_fn_binding_t> fn_bindings_[N > 0 ? N : 1];
  std::vector<vef_index_profile_fn_binding_t> helper_bindings_[N > 0 ? N : 1];
  vef_preview_index_profile_ext_desc_t ext_desc_;
};

}  // namespace vsql::preview_index_builder

namespace vsql::detail {

template <size_t N>
struct CapabilityTraits<::vsql::preview_index_builder::IndexTypeCapability<N>> {
  static constexpr const char *kName = VEF_PREVIEW_INDEX_TYPE_NAME;
  static constexpr const char *kCppTypeName =
      "vsql::preview_index_builder::IndexTypeCapability";
  using CapabilityConfigType = vef_preview_index_type_ext_desc_t;
  static constexpr const char *kVtableHash = "ver-1";
  static constexpr const char *kCapabilityConfigHash = "ver-1";

  static void *vtable_destination(
      ::vsql::preview_index_builder::IndexTypeCapability<N> *p) noexcept {
    return static_cast<void *>(&p->vtable_);
  }

  static const void *capability_config(
      ::vsql::preview_index_builder::IndexTypeCapability<N> *p) noexcept {
    return p->extension_desc();
  }
};

template <size_t N>
struct CapabilityTraits<
    ::vsql::preview_index_builder::IndexProfileCapability<N>> {
  static constexpr const char *kName = VEF_PREVIEW_INDEX_PROFILE_NAME;
  static constexpr const char *kCppTypeName =
      "vsql::preview_index_builder::IndexProfileCapability";
  using CapabilityConfigType = vef_preview_index_profile_ext_desc_t;
  static constexpr const char *kVtableHash = "ver-1";
  static constexpr const char *kCapabilityConfigHash = "ver-1";

  static void *vtable_destination(
      ::vsql::preview_index_builder::IndexProfileCapability<N> *p) noexcept {
    return static_cast<void *>(&p->vtable_);
  }

  static const void *capability_config(
      ::vsql::preview_index_builder::IndexProfileCapability<N> *p) noexcept {
    return p->extension_desc();
  }
};

}  // namespace vsql::detail

#endif  // VILLAGESQL_PREVIEW_INDEX_BUILDER_H
