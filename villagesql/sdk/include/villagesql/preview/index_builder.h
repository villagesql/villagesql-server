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

#ifndef VILLAGESQL_PREVIEW_INDEX_BUILDER_H
#define VILLAGESQL_PREVIEW_INDEX_BUILDER_H

// TODO(villagesql-beta): Custom index storage is not ready for external use.
// The ABI and API are under active development and will change without notice.
// Do not use this in production extensions.
//
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
//     static HNSWOptions parse(const std::map<std::string, std::string>& p);
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
//           .capabilities(IndexSupport::KNN)
//           .storage_props(IndexStorage::HAS_COLUMN_REF |
//                          IndexStorage::REF_LOOKUP)
//           .options<HNSWOptions, &HNSWOptions::parse>()
//           .build();
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
// TODO(villagesql-indexing): make_index_function is a stub; full server-side
// integration is pending.

#include <cstdio>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

#include <villagesql/abi/index.h>
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

// IndexStorageCtx<T> wraps vef_storage_ctx_t with a typed per-index context T.
// Must be standard layout so the SDK can cast it to/from vef_storage_ctx_t*.
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

}  // namespace detail

// Non-constexpr functions whose names appear in the compiler diagnostic when
// build() is evaluated in a constant expression with a missing registration.
void IndexTypeBuilder_all_index_interfaces_must_be_registered();
void IndexTypeBuilder_capabilities_must_be_set();
void IndexTypeBuilder_storage_props_must_set_row_or_column_ref();

// Descriptor returned by IndexTypeBuilder::build(). Consumed by the extension
// builder (e.g. make_extension().index_type(HNSW_INDEX)).
// TODO(villagesql-indexing): add index_type() to the extension builder.
struct IndexTypeDesc {
  const char *name;
  vef_type_index_intf_t intf;
};

// IndexTypeBuilder<Name, Context>: fluent builder for vef_type_index_intf_t.
//
// Context is the extension-defined per-index storage state. The SDK allocates
// IndexStorageCtx<Context> from the InnoDB arena before calling create/load,
// and destroys the arena (which calls ~Context) after drop returns.
//
// .lifecycle(), .dml(), .scan() are grouping markers that return *this for
// readability; all registration methods are on the same builder class.
template <const char *Name, typename Context>
class IndexTypeBuilder {
 public:
  constexpr IndexTypeBuilder() : intf_{} {}

  // Section markers — return *this to allow chaining into sub-groups.
  constexpr IndexTypeBuilder &lifecycle() { return *this; }
  constexpr IndexTypeBuilder &dml() { return *this; }
  constexpr IndexTypeBuilder &scan() { return *this; }

  template <auto F>
  constexpr IndexTypeBuilder &create() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "create: F must be a plain function pointer");
    intf_.create = detail::CreateWrapper<F, Context>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &drop() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "drop: F must be a plain function pointer");
    intf_.drop = detail::DropWrapper<F, Context>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &load() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "load: F must be a plain function pointer");
    intf_.load = detail::LoadWrapper<F, Context>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &insert() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "insert: F must be a plain function pointer");
    intf_.insert = detail::InsertWrapper<F, Context>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &mark_delete() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "mark_delete: F must be a plain function pointer");
    intf_.mark_delete = detail::MarkDeleteWrapper<F, Context>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &purge() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "purge: F must be a plain function pointer");
    intf_.purge = detail::PurgeWrapper<F, Context>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &begin() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "begin: F must be a plain function pointer");
    intf_.scan_begin = detail::ScanBeginWrapper<F, Context>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &position() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "position: F must be a plain function pointer");
    intf_.scan_position = detail::ScanPositionWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &fetch() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "fetch: F must be a plain function pointer");
    intf_.scan_fetch = detail::ScanFetchWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &save() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "save: F must be a plain function pointer");
    intf_.scan_save = detail::ScanSaveWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &restore() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "restore: F must be a plain function pointer");
    intf_.scan_restore = detail::ScanRestoreWrapper<F>::invoke;
    return *this;
  }

  template <auto F>
  constexpr IndexTypeBuilder &end() {
    static_assert(std::is_pointer_v<decltype(F)> &&
                      std::is_function_v<std::remove_pointer_t<decltype(F)>>,
                  "end: F must be a plain function pointer");
    intf_.scan_end = detail::ScanEndWrapper<F>::invoke;
    return *this;
  }

  // Set optimizer capability flags (VEF_INDEX_CAP_*).
  constexpr IndexTypeBuilder &capabilities(IndexSupport caps) {
    intf_.capabilities = static_cast<vef_index_cap_t>(caps);
    return *this;
  }

  // Set physical storage property flags (VEF_INDEX_STORAGE_*).
  constexpr IndexTypeBuilder &storage_props(IndexStorage props) {
    intf_.storage_props = static_cast<vef_index_storage_t>(props);
    return *this;
  }

  // Bind a parameterized options struct and its parse function.
  // TODO(villagesql-indexing): options parsing is not yet supported by the
  // server ABI. This call is accepted and ignored.
  template <typename OptionsStruct, auto ParseFn>
  constexpr IndexTypeBuilder &options() {
    return *this;
  }

  constexpr IndexTypeDesc build() const {
    if (!intf_.create || !intf_.drop || !intf_.load || !intf_.insert ||
        !intf_.mark_delete || !intf_.purge || !intf_.scan_begin ||
        !intf_.scan_position || !intf_.scan_fetch || !intf_.scan_save ||
        !intf_.scan_restore || !intf_.scan_end) {
      IndexTypeBuilder_all_index_interfaces_must_be_registered();
    }
    if (intf_.capabilities == VEF_INDEX_CAP_NONE) {
      IndexTypeBuilder_capabilities_must_be_set();
    }
    if (!(intf_.storage_props &
          (VEF_INDEX_STORAGE_HAS_ROW_REF | VEF_INDEX_STORAGE_HAS_COLUMN_REF))) {
      IndexTypeBuilder_storage_props_must_set_row_or_column_ref();
    }
    vef_type_index_intf_t intf = intf_;
    intf.version = VEF_INDEX_TYPE_INTF_VERSION;
    return {Name, intf};
  }

 private:
  vef_type_index_intf_t intf_;
};

template <const char *Name, typename Context>
constexpr IndexTypeBuilder<Name, Context> make_index_type() {
  return IndexTypeBuilder<Name, Context>{};
}

// ===========================================================================
// make_index_profile
// ===========================================================================
//
// Defines how a specific custom type behaves with a given index type. Each
// .with_function(fn_id, name) call binds a profile function to the fn_id used
// by the index storage implementation when calling vef_index_profile_fn.
//
// TODO(villagesql-indexing): IndexProfileDesc is not yet consumed by the
// server; registration API pending.

// Descriptor returned by make_index_function().build(). Passing one of these
// to IndexProfileBuilder::with_function() instead of a raw string enforces
// that the bound function was declared by the same extension.
struct IndexFunctionDesc {
  const char *name;
  // TODO(villagesql-indexing): add return type, parameter types, and
  // determinism flag once server-side consumption is defined.
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
// TODO(villagesql-indexing): IndexFunctionDesc is a stub. Full integration
// with the function registry (return type, parameter types, determinism flag)
// is pending server-side support.

class IndexFunctionBuilder {
 public:
  explicit IndexFunctionBuilder(const char *name) { desc_.name = name; }

  // TODO(villagesql-indexing): store and validate these once server-side
  // consumption is defined.
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
// retained. TODO(villagesql-indexing): store F once server integration is done.
template <auto F>
inline IndexFunctionBuilder make_index_function(const char *name) {
  return IndexFunctionBuilder(name);
}

}  // namespace vsql::preview_index_builder

#endif  // VILLAGESQL_PREVIEW_INDEX_BUILDER_H
