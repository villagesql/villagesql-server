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
//     static bool parse(const Index::Parameter *params, uint32_t count,
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
//               .capabilities(Index::Support::KNN |
//                             Index::Support::APPROXIMATE)
//               .storage_props(Index::Storage::HAS_COLUMN_REF |
//                              Index::Storage::REF_LOOKUP)
//               .options<HNSWOptions, &HNSWOptions::parse>()
//               .build();
//
// EXTENSION FUNCTION SIGNATURES
// ------------------------------
// using namespace vsql::preview_index_builder;
// using vsql::preview_storage::MtrCtx;
// using vsql::preview_storage::Segment;
// using vsql::preview_storage::Space;
//
// Ctx = Index::StorageCtx<HNSWContext>. Extensions access per-index state via
// ctx->user() and the InnoDB arena via ctx->arena().
//
//   Lifecycle:
//     bool create(Ctx*, const Index&, Space::Ref,
//                 Segment::TrxRef, char*, uint32_t);
//     bool drop(Ctx*, const Index&, Segment::TrxRef,
//               char*, uint32_t);
//     bool load(Ctx*, const Index&, Index::StorageRef,
//               char*, uint32_t);
//
//   DML:
//     bool insert(Ctx*, const Index&, Segment::TrxRef,
//                 IndexScanKey::KeyPartData* keys,
//                 IndexScanKey::KeyPartData* pkeys,
//                 IndexScanKey::KeyPartRef* key_ref, char*, uint32_t);
//     bool mark_delete(Ctx*, const Index&, Segment::TrxRef,
//                      IndexScanKey::KeyPartRef* key_ref,
//                      IndexScanKey::KeyPartData* keys,
//                      IndexScanKey::KeyPartData* pkeys,
//                      bool delete_mark, char*, uint32_t);
//     bool purge(Ctx*, const Index&, Segment::TrxRef,
//                IndexScanKey::KeyPartRef* key_ref,
//                IndexScanKey::KeyPartData* keys,
//                IndexScanKey::KeyPartData* pkeys,
//                char*, uint32_t);
//
//   Scan:
//     bool begin(Ctx*, const Index&, MtrCtx::Ref,
//                const IndexScanDesc&, Index::Cursor*, bool*,
//                char*, uint32_t);
//     bool position(Index::Cursor, Index::CursorOp, bool*,
//                   char*, uint32_t);
//     bool fetch(Index::Cursor, IndexScanKey::KeyPartRef*,
//                IndexScanKey::KeyPartData* keys,
//                IndexScanKey::KeyPartData* pkeys,
//                Segment::TrxRef* trx_ref, char*, uint32_t);
//     bool save(Index::Cursor, char*, uint32_t);
//     bool restore(Index::Cursor, MtrCtx::Ref, bool*,
//                  char*, uint32_t);
//     void end(Index::Cursor*);
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
// with_helper() functions are internal to the index implementation (not
// SQL-visible) and are declared with make_index_function():
//
//   static constexpr auto MY_HELPER_FN =
//       make_index_function<&my_helper_fn>("my_helper_fn")
//           .returns(vsql::REAL)
//           .param(MY_TYPE)     // TypeDesc built by vsql::make_type<kMyType>
//           .deterministic()
//           .build();
//
// with_function() functions ARE SQL-visible: declare them with the normal
// vsql::make_func() and register that same object with .func() on the
// extension builder AND with_function() on the profile.
//
// INDEX PROFILE
// -------------
// TODO(villagesql-indexing): Evaluate using string-based function identifiers
// instead of numeric fn_ids, and assess the impact on function invocation and
// builder-generated execution context.
//
// Binds a custom type to an index type and assigns per-profile function IDs.
// for_type() takes the type name string. with_function<Desc>(fn_id) and
// with_helper<Desc>(fn_id) both take their bound VDF as a template argument.
// with_function() also takes an optional FunctionKind: Restrict (default) for
// a filter condition, or Order for a function that defines the row order.
//
//   static constexpr auto MY_INDEX_FN =
//       make_func<&my_index_fn>("my_index_fn")
//           .returns(vsql::REAL)
//           .param(MY_TYPE)
//           .param(MY_TYPE)
//           .deterministic()
//           .build();
//
//   static const auto MY_PROFILE =
//       make_index_profile("my_profile")
//           .for_type(kMyType)                // name string, not the TypeDesc
//           .using_index(kMyIndex)
//           .with_function<MY_INDEX_FN>(1, FunctionKind::Order)
//                                             // fn_id 1 -> MY_INDEX_FN
//           .with_helper<MY_HELPER_FN>(1)     // fn_id 1 -> MY_HELPER_FN
//           .ordering(Index::Ordering::ASC)
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
//   // Step 1 - functions (static: addresses must be stable). MY_INDEX_FN is
//   // SQL-visible: declare it with make_func(), register as SQL function
//   // by .func() and register as index profile function by .with_function().
//   // MY_HELPER_FN is internal-only: register by make_index_function().
//   static constexpr auto MY_INDEX_FN =
//       make_func<&my_index_fn>("my_index_fn")
//           .returns(vsql::REAL)
//           .param(MY_TYPE)
//           .param(MY_TYPE)
//           .deterministic()
//           .build();
//
//   static constexpr auto MY_HELPER_FN =
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
//               .capabilities(Index::Support::POINT_LOOKUP |
//                             Index::Support::RANGE_SCAN | ...)
//               .storage_props(Index::Storage::HAS_ROW_REF | ...)
//               .options<MyOptions, &MyOptions::parse>()
//               .build();
//
//   // Step 3 - profile: for_type takes the name string, not the TypeDesc.
//   static const auto MY_PROFILE =
//       make_index_profile("my_profile")
//           .for_type(kMyType)
//           .using_index(kMyIndex)
//           .with_function<MY_INDEX_FN>(1, FunctionKind::Restrict)
//           .with_helper<MY_HELPER_FN>(1)
//           .ordering(Index::Ordering::ASC)
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
//           .func(MY_INDEX_FN)
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

using vsql::preview_storage::Segment;

using vsql::preview_storage::detail::AllocateArenaStorage;
using vsql::preview_storage::detail::ERROR_MSG_SIZE;
using vsql::preview_storage::detail::tl_error_msg;

// Describes a key used to define an index scan. The profile function ID
// determines the semantics of the key columns.
// This is a temporary view and must not be copied or saved by an extension.
class IndexScanKey {
 public:
  using KeyPartData = vef_storage_col_data_t;
  using KeyPartRef = vef_storage_col_ref_t;

  static constexpr KeyPartRef EMPTY_REF = VEF_STORAGE_EMPTY_COLUMN_REF;

  explicit IndexScanKey(const vef_index_scan_key_t &key) : key_(key) {}
  IndexScanKey(const IndexScanKey &) = delete;
  IndexScanKey &operator=(const IndexScanKey &) = delete;

  uint32_t profile_function_id() const { return key_.profile_function_id; }
  uint32_t num_columns() const { return key_.num_key_columns; }
  bool is_bounded() const { return key_.key_columns != nullptr; }

  const KeyPartData &operator[](uint32_t i) const {
    return key_.key_columns[i];
  }

 private:
  const vef_index_scan_key_t &key_;
};

// Describes an index scan, including its scan direction and scan keys.
// This is a temporary view and must not be copied or saved by an extension.
class IndexScanDesc {
 public:
  using ReadViewRef = vef_readview_ref_t;

  explicit IndexScanDesc(const vef_index_scan_desc_t &desc) : desc_(desc) {}
  IndexScanDesc(const IndexScanDesc &) = delete;
  IndexScanDesc &operator=(const IndexScanDesc &) = delete;

  bool reverse() const { return static_cast<bool>(desc_.reverse); }
  bool is_limit_exact() const {
    return static_cast<bool>(desc_.limit_is_exact);
  }
  uint32_t limit() const { return desc_.limit; }
  uint32_t num_keys() const { return desc_.num_keys; }
  ReadViewRef read_view() const { return desc_.readview; }
  bool needs_trx_ref() const { return read_view() == nullptr; }

  IndexScanKey operator[](uint32_t i) const {
    return IndexScanKey{desc_.keys[i]};
  }

 private:
  const vef_index_scan_desc_t &desc_;
};

// Provides access to the index definition and services required by an
// extension. This is a copyable handle to an index context that remains
// valid during the scan. An extension can save it in its cursor handle.
class Index {
 public:
  using Ref = vef_index_ref_t;
  using StorageRef = vef_storage_ref_t;
  using Cursor = vef_index_cursor_ref_t;
  using Parameter = vef_index_param_t;

  // The SDK allocates Index::StorageCtx<T> and T from the InnoDB arena before
  // calling create/load, and destroys the arena (which calls ~T) after drop
  // returns. Extensions must not delete ctx, ctx->user(), or the arena
  // directly.
  template <typename T>
  using StorageCtx = vsql::preview_storage::StorageCtx<T>;

  // Capability flags for use with .capabilities(). Can be combined with |.
  enum class Support : vef_index_cap_t {
    POINT_LOOKUP = VEF_INDEX_CAP_POINT_LOOKUP,
    RANGE_SCAN = VEF_INDEX_CAP_RANGE_SCAN,
    REVERSE_SCAN = VEF_INDEX_CAP_REVERSE_SCAN,
    ORDER_BY = VEF_INDEX_CAP_ORDER_BY,
    KNN = VEF_INDEX_CAP_KNN,
    APPROXIMATE = VEF_INDEX_CAP_APPROXIMATE,
    MVCC_VISIBILITY = VEF_INDEX_CAP_MVCC_VISIBILITY,
  };

  // Storage property flags for use with .storage_props(). Can be combined with
  // |.
  enum class Storage : vef_index_storage_t {
    HAS_COLUMN_REF = VEF_INDEX_STORAGE_HAS_COLUMN_REF,
    HAS_ROW_REF = VEF_INDEX_STORAGE_HAS_ROW_REF,
    REF_LOOKUP = VEF_INDEX_STORAGE_REF_LOOKUP,
  };

  // Ordering flags for use with make_index_profile().ordering(). Can be
  // combined with | to indicate a profile supports both scan directions.
  enum class Ordering : uint8_t {
    NONE = VEF_INDEX_ORDERING_NONE,
    ASC = VEF_INDEX_ORDERING_ASC,
    DESC = VEF_INDEX_ORDERING_DESC,
  };

  enum class CursorOp : vef_index_cursor_op_t {
    Next = VEF_INDEX_CURSOR_OP_NEXT,
    Prev = VEF_INDEX_CURSOR_OP_PREV,
  };

  explicit Index(const vef_index_ctx_t *ctx) : ctx_(ctx) {}

  bool is_unique() const { return ctx_->is_unique != 0; }

  uint32_t get_num_key_cols() const { return ctx_->num_key_columns; }

  uint32_t get_max_col_len(uint32_t key_pos) const {
    return ctx_->key_len_fn(ctx_->index_ref, key_pos, false);
  }

  uint32_t get_primary_num_key_cols() const {
    return ctx_->num_primary_key_columns;
  }

  uint32_t get_primary_max_col_len(uint32_t key_pos) const {
    return ctx_->key_len_fn(ctx_->index_ref, key_pos, true);
  }

  template <typename T>
  const T *options() const {
    return static_cast<const T *>(ctx_->options);
  }

  // args are the raw column values (persisted format) the profile/helper
  // function operates on, e.g. the indexed column and a query value for a
  // distance function. key_pos identifies which key column's bound profile to
  // dispatch fn_id through.
  template <typename Result, typename... ColData>
  void profile(uint32_t key_pos, uint32_t fn_id, Result *result,
               const ColData &...args) const {
    static_assert((std::is_same_v<ColData, vef_storage_col_data_t> && ...),
                  "profile() args must be vef_storage_col_data_t");
    const void *argv[] = {static_cast<const void *>(&args)...};
    ctx_->profile_fn(ctx_->index_ref, key_pos, fn_id, argv, sizeof...(ColData),
                     result);
  }

  template <typename Result, typename... ColData>
  void helper(uint32_t key_pos, uint32_t fn_id, Result *result,
              const ColData &...args) const {
    static_assert((std::is_same_v<ColData, vef_storage_col_data_t> && ...),
                  "helper() args must be vef_storage_col_data_t");
    const void *argv[] = {static_cast<const void *>(&args)...};
    ctx_->helper_fn(ctx_->index_ref, key_pos, fn_id, argv, sizeof...(ColData),
                    result);
  }

  // Reads the value key_ref points to for the key column at key_pos. key_data
  // must point at a buffer of key_data->length bytes, at least
  // get_max_col_len(key_pos); on success key_data->length is the value length.
  // If trx_ref is not nullptr, it is set to the transaction reference of the
  // value read, e.g. for a column-only scan that needs visibility information
  // without a row lookup. If delete_mark is not nullptr, it is set to whether
  // the value is marked as deleted.
  // Must not be called while holding page latches.
  bool get_key_data(uint32_t key_pos, IndexScanKey::KeyPartRef key_ref,
                    IndexScanKey::KeyPartData *key_data,
                    Segment::TrxRef *trx_ref = nullptr,
                    bool *delete_mark = nullptr) const {
    return ctx_->col_ref_to_data_fn(ctx_->index_ref, key_pos, key_ref, key_data,
                                    trx_ref, delete_mark, tl_error_msg,
                                    ERROR_MSG_SIZE);
  }

  // Checks whether the key column at key_pos is sorted in descending order.
  bool is_key_desc(uint32_t key_pos) const {
    return ctx_->key_is_desc_fn(ctx_->index_ref, key_pos);
  }

  // Derives the column reference for a value of the key column at key_pos that
  // is already in the column store, e.g. a key column handed to insert().
  bool get_key_ref(uint32_t key_pos, IndexScanKey::KeyPartData key_data,
                   IndexScanKey::KeyPartRef *key_ref) const {
    return ctx_->col_data_to_ref_fn(ctx_->index_ref, key_pos, key_data, key_ref,
                                    tl_error_msg, ERROR_MSG_SIZE);
  }

  const char *get_error() const { return tl_error_msg; }

  // Checks whether trx_ref's changes are visible under read_view.
  bool is_trx_visible(IndexScanDesc::ReadViewRef read_view,
                      Segment::TrxRef trx_ref) const {
    return ctx_->trx_is_visible_fn(ctx_->index_ref, read_view, trx_ref);
  }

 private:
  const vef_index_ctx_t *ctx_;
};

constexpr Index::Support operator|(Index::Support a, Index::Support b) {
  return static_cast<Index::Support>(static_cast<vef_index_cap_t>(a) |
                                     static_cast<vef_index_cap_t>(b));
}

constexpr Index::Storage operator|(Index::Storage a, Index::Storage b) {
  return static_cast<Index::Storage>(static_cast<vef_index_storage_t>(a) |
                                     static_cast<vef_index_storage_t>(b));
}

constexpr Index::Ordering operator|(Index::Ordering a, Index::Ordering b) {
  return static_cast<Index::Ordering>(static_cast<uint8_t>(a) |
                                      static_cast<uint8_t>(b));
}

namespace detail {

// Note: Wrapper static methods use C++ linkage. The C++ standard does not
// allow extern "C" on template instantiations. The ABI boundary relies on C
// and C++ having compatible calling conventions on all supported platforms.

using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Space;

// Expected extension function pointer types. Each builder setter
// static_asserts that F exactly matches the corresponding type alias.

// Lifecycle
template <typename Context>
using CreateFn = bool (*)(Index::StorageCtx<Context> *, const Index &,
                          Space::Ref, Segment::TrxRef, char *, uint32_t);

template <typename Context>
using DropFn = bool (*)(Index::StorageCtx<Context> *, const Index &,
                        Segment::TrxRef, char *, uint32_t);

template <typename Context>
using LoadFn = bool (*)(Index::StorageCtx<Context> *, const Index &,
                        Index::StorageRef, char *, uint32_t);

// DML
template <typename Context>
using InsertFn = bool (*)(Index::StorageCtx<Context> *, const Index &,
                          Segment::TrxRef, IndexScanKey::KeyPartData *,
                          IndexScanKey::KeyPartData *,
                          IndexScanKey::KeyPartRef *, char *, uint32_t);

template <typename Context>
using MarkDeleteFn = bool (*)(Index::StorageCtx<Context> *, const Index &,
                              Segment::TrxRef, IndexScanKey::KeyPartRef *,
                              IndexScanKey::KeyPartData *,
                              IndexScanKey::KeyPartData *, bool, char *,
                              uint32_t);

template <typename Context>
using PurgeFn = bool (*)(Index::StorageCtx<Context> *, const Index &,
                         Segment::TrxRef, IndexScanKey::KeyPartRef *,
                         IndexScanKey::KeyPartData *,
                         IndexScanKey::KeyPartData *, char *, uint32_t);

// Scan
template <typename Context>
using ScanBeginFn = bool (*)(Index::StorageCtx<Context> *, const Index &,
                             MtrCtx::Ref, const IndexScanDesc &,
                             Index::Cursor *, bool *, char *, uint32_t);

using ScanPositionFn = bool (*)(Index::Cursor, Index::CursorOp, bool *, char *,
                                uint32_t);

using ScanFetchFn = bool (*)(Index::Cursor, IndexScanKey::KeyPartRef *,
                             IndexScanKey::KeyPartRef *,
                             IndexScanKey::KeyPartData *,
                             IndexScanKey::KeyPartData *, Segment::TrxRef *,
                             bool *, char *, uint32_t);

using ScanSaveFn = bool (*)(Index::Cursor, char *, uint32_t);

using ScanRestoreFn = bool (*)(Index::Cursor, MtrCtx::Ref, bool *, char *,
                               uint32_t);

using ScanEndFn = void (*)(Index::Cursor *);

template <auto F, typename Context>
struct CreateWrapper {
  static bool invoke(const vef_index_ctx_t *index_ctx,
                     vef_storage_space_ref_t space_ref,
                     vef_storage_trx_ref_t trx_ref,
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
    auto *ctx = arena->construct<Index::StorageCtx<Context>>(arena);
    if (ctx == nullptr || ctx->user() == nullptr) {
      arena->~Arena();
      snprintf(error_msg, error_msg_len,
               "out of memory allocating index storage context");
      return true;
    }
    *storage = reinterpret_cast<vef_storage_ctx_t *>(ctx);
    const Index index{index_ctx};
    bool err = F(ctx, index, space_ref, trx_ref, error_msg, error_msg_len);
    if (err) {
      arena->~Arena();
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
    auto *ctx = reinterpret_cast<Index::StorageCtx<Context> *>(storage);
    Arena *arena = &ctx->arena();
    const Index index{index_ctx};
    bool err = F(ctx, index, trx_ref, error_msg, error_msg_len);
    arena->~Arena();
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
    void *arena_mem = AllocateArenaStorage(arena_ctx, arena_alloc);
    if (arena_mem == nullptr) {
      snprintf(error_msg, error_msg_len, "out of memory allocating arena");
      return true;
    }
    auto *arena = new (arena_mem) Arena(arena_ctx, arena_alloc);
    auto *ctx = arena->construct<Index::StorageCtx<Context>>(arena);
    if (ctx == nullptr || ctx->user() == nullptr) {
      arena->~Arena();
      snprintf(error_msg, error_msg_len,
               "out of memory allocating index storage context");
      return true;
    }
    *storage = reinterpret_cast<vef_storage_ctx_t *>(ctx);
    const Index index{index_ctx};
    bool err = F(ctx, index, storage_ref, error_msg, error_msg_len);
    if (err) {
      arena->~Arena();
      *storage = nullptr;
    }
    return err;
  }
};

// TODO(villagesql-indexing): Expose unload as a builder-configurable hook
// (e.g. .unload<F>()) so extensions can run their own cleanup.
template <typename Context>
struct UnloadWrapper {
  static bool invoke(const vef_index_ctx_t *index_ctx,
                     vef_storage_ctx_t *storage, char *error_msg,
                     uint32_t error_msg_len) {
    (void)index_ctx;
    (void)error_msg;
    (void)error_msg_len;
    auto *ctx = reinterpret_cast<Index::StorageCtx<Context> *>(storage);
    Arena *arena = &ctx->arena();
    arena->~Arena();
    return false;
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
    const Index index{index_ctx};
    return F(reinterpret_cast<Index::StorageCtx<Context> *>(storage), index,
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
    const Index index{index_ctx};
    return F(reinterpret_cast<Index::StorageCtx<Context> *>(storage), index,
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
    const Index index{index_ctx};
    return F(reinterpret_cast<Index::StorageCtx<Context> *>(storage), index,
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
    const Index index{index_ctx};
    const IndexScanDesc desc{*scan_desc};
    return F(reinterpret_cast<Index::StorageCtx<Context> *>(storage), index,
             mctx, desc, cursor, eof, error_msg, error_msg_len);
  }
};

template <auto F>
struct ScanPositionWrapper {
  static bool invoke(vef_index_cursor_ref_t cursor, vef_index_cursor_op_t op,
                     bool *eof, char *error_msg, uint32_t error_msg_len) {
    return F(cursor, static_cast<Index::CursorOp>(op), eof, error_msg,
             error_msg_len);
  }
};

template <auto F>
struct ScanFetchWrapper {
  static bool invoke(vef_index_cursor_ref_t cursor,
                     vef_storage_col_ref_t *key_ref,
                     vef_storage_col_ref_t *key_column_refs,
                     vef_storage_col_data_t *key_columns,
                     vef_storage_col_data_t *pkey_columns,
                     vef_storage_trx_ref_t *trx_ref, bool *delete_mark,
                     char *error_msg, uint32_t error_msg_len) {
    return F(cursor, key_ref, key_column_refs, key_columns, pkey_columns,
             trx_ref, delete_mark, error_msg, error_msg_len);
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
// Index::StorageCtx<Context> from the InnoDB arena before calling create/load,
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
    static_assert(
        std::is_same_v<decltype(F), detail::CreateFn<Context>>,
        "create: expected bool(*)(Index::StorageCtx<Context>*, const Index&, "
        "Space::Ref, Segment::TrxRef, char*, uint32_t)");
    intf_.create = detail::CreateWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr LifecycleBuilder<Context, Bits | kLoad> load() && {
    static_assert(
        std::is_same_v<decltype(F), detail::LoadFn<Context>>,
        "load: expected bool(*)(Index::StorageCtx<Context>*, const Index&, "
        "Index::StorageRef, char*, uint32_t)");
    intf_.load = detail::LoadWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr LifecycleBuilder<Context, Bits | kDrop> drop() && {
    static_assert(
        std::is_same_v<decltype(F), detail::DropFn<Context>>,
        "drop: expected bool(*)(Index::StorageCtx<Context>*, const Index&, "
        "Segment::TrxRef, char*, uint32_t)");
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
    static_assert(
        std::is_same_v<decltype(F), detail::InsertFn<Context>>,
        "insert: expected bool(*)(Index::StorageCtx<Context>*, const Index&, "
        "Segment::TrxRef, KeyPartData* keys, KeyPartData* pkeys, "
        "KeyPartRef* key_ref, char*, uint32_t)");
    intf_.insert = detail::InsertWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr DMLBuilder<Context, Bits | kMarkDelete> mark_delete() && {
    static_assert(std::is_same_v<decltype(F), detail::MarkDeleteFn<Context>>,
                  "mark_delete: expected bool(*)(Index::StorageCtx<Context>*, "
                  "const Index&, Segment::TrxRef, KeyPartRef* key_ref, "
                  "KeyPartData* keys, KeyPartData* pkeys, bool delete_mark, "
                  "char*, uint32_t)");
    intf_.mark_delete = detail::MarkDeleteWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr DMLBuilder<Context, Bits | kPurge> purge() && {
    static_assert(
        std::is_same_v<decltype(F), detail::PurgeFn<Context>>,
        "purge: expected bool(*)(Index::StorageCtx<Context>*, const Index&, "
        "Segment::TrxRef, KeyPartRef* key_ref, KeyPartData* keys, "
        "KeyPartData* pkeys, char*, uint32_t)");
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
    static_assert(
        std::is_same_v<decltype(F), detail::ScanBeginFn<Context>>,
        "begin: expected bool(*)(Index::StorageCtx<Context>*, const Index&, "
        "MtrCtx::Ref, const IndexScanDesc&, Index::Cursor*, bool*, "
        "char*, uint32_t)");
    intf_.scan_begin = detail::ScanBeginWrapper<F, Context>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kPosition> position() && {
    static_assert(
        std::is_same_v<decltype(F), detail::ScanPositionFn>,
        "position: expected bool(*)(Index::Cursor, Index::CursorOp, bool*, "
        "char*, uint32_t)");
    intf_.scan_position = detail::ScanPositionWrapper<F>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kFetch> fetch() && {
    static_assert(
        std::is_same_v<decltype(F), detail::ScanFetchFn>,
        "fetch: expected bool(*)(Index::Cursor, KeyPartRef*, "
        "KeyPartRef* key_refs, KeyPartData* keys, KeyPartData* pkeys, "
        "TrxRef*, bool* delete_mark, char*, uint32_t)");
    intf_.scan_fetch = detail::ScanFetchWrapper<F>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kSave> save() && {
    static_assert(std::is_same_v<decltype(F), detail::ScanSaveFn>,
                  "save: expected bool(*)(Index::Cursor, char*, uint32_t)");
    intf_.scan_save = detail::ScanSaveWrapper<F>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kRestore> restore() && {
    static_assert(
        std::is_same_v<decltype(F), detail::ScanRestoreFn>,
        "restore: expected bool(*)(Index::Cursor, MtrCtx::Ref, bool*, "
        "char*, uint32_t)");
    intf_.scan_restore = detail::ScanRestoreWrapper<F>::invoke;
    return {name_, intf_};
  }

  template <auto F>
  constexpr ScanBuilder<Context, Bits | kEnd> end() && {
    static_assert(std::is_same_v<decltype(F), detail::ScanEndFn>,
                  "end: expected void(*)(Index::Cursor*)");
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
      Index::Support caps) && {
    intf_.capabilities = static_cast<vef_index_cap_t>(caps);
    return {name_, intf_};
  }

  constexpr GlobalBuilder<Context, Bits | kStorageProps> storage_props(
      Index::Storage props) && {
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
    intf.unload = detail::UnloadWrapper<Context>::invoke;
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
// .with_function()/.with_helper() call binds a profile function to the fn_id
// used by the index storage implementation when calling vef_index_profile_fn.

// Internal descriptor for one profile function binding, built by
// with_function()/with_helper() from whatever func_builder descriptor their
// template argument names. Extension authors never name this type.
struct IndexFunctionDesc {
  const char *name;
  vef_protocol_t protocol;
  vef_vdf_func_t vdf;
  vef_type_t return_type;
  std::array<vef_type_t, vsql::func_builder::kMaxParams> param_types;
  size_t num_params;
  bool is_deterministic;
};

// Descriptor returned by make_index_function().build(). It wraps the
// func_builder descriptor in a distinct type so that with_helper() can accept
// only index helpers and with_function() can accept only make_func()
// descriptors. It deliberately has no required_protocol(), so it cannot be
// passed to make_extension().func().
template <size_t NumParams>
struct IndexHelperDesc {
  vsql::func_builder::detail::StaticFuncDesc<NumParams> desc;
};

namespace detail {

template <typename T>
inline constexpr bool is_static_func_desc_v = false;
template <size_t N>
inline constexpr bool
    is_static_func_desc_v<vsql::func_builder::detail::StaticFuncDesc<N>> = true;

template <typename T>
inline constexpr bool is_index_helper_desc_v = false;
template <size_t N>
inline constexpr bool is_index_helper_desc_v<IndexHelperDesc<N>> = true;

template <const auto &FuncDesc>
using DescType = std::remove_cv_t<std::remove_reference_t<decltype(FuncDesc)>>;

template <size_t N>
constexpr const vsql::func_builder::detail::StaticFuncDesc<N> &UnwrapFuncDesc(
    const vsql::func_builder::detail::StaticFuncDesc<N> &d) {
  return d;
}

template <size_t N>
constexpr const vsql::func_builder::detail::StaticFuncDesc<N> &UnwrapFuncDesc(
    const IndexHelperDesc<N> &d) {
  return d.desc;
}

// Enforces the norm shared by with_function() and with_helper(): the bound
// function must be deterministic, stateless, not an aggregate, fixed-arity,
// have no parameterized custom-type argument, and return REAL. This holds
// for both kinds of binding -- index scan implementations might invoke them
// through the same raw vef_vdf_func_t call, with no prerun()/user_data round
// trip, and require a REAL result.
template <const auto &FuncDesc>
void CheckIndexProfileFuncNorm() {
  constexpr const auto &d = UnwrapFuncDesc(FuncDesc);
  static_assert(d.deterministic(),
                "index profile function/helper must call .deterministic() "
                "-- the optimizer may cache or reorder index-scan-eligible "
                "calls");
  static_assert(d.prerun() == nullptr && d.postrun() == nullptr,
                "index profile function/helper must not use prerun()/postrun() "
                "state -- it is invoked with no user_data set up");
  static_assert(d.clear() == nullptr && d.accumulate() == nullptr,
                "index profile function/helper must not be an aggregate -- "
                "use make_func() or make_index_function(), not "
                "make_aggregate_func()");
  static_assert(d.num_params() != VEF_PARAM_VARARGS,
                "varargs VDFs are not supported as index profile functions "
                "or helpers");
  static_assert(d.return_type().id == VEF_TYPE_REAL,
                "index profile functions and helpers must return REAL");
}

// Converts a func_builder-produced descriptor (a make_func() StaticFuncDesc or
// an IndexHelperDesc) into an IndexFunctionDesc. Callers are responsible for
// running CheckIndexProfileFuncNorm() on the descriptor first.
template <typename FuncDescT>
constexpr IndexFunctionDesc ToIndexFunctionDesc(const FuncDescT &desc) {
  const auto &d = UnwrapFuncDesc(desc);
  IndexFunctionDesc result{};
  result.name = d.name();
  result.protocol = d.required_protocol();
  result.vdf = d.vdf();
  result.return_type = d.return_type();
  result.num_params = d.num_params();
  for (size_t i = 0; i < d.num_params(); ++i) {
    result.param_types[i] = d.params()[i];
  }
  result.is_deterministic = d.deterministic();
  return result;
}

}  // namespace detail

// Kind of an index profile function: a filter condition on the indexed rows
// (Restrict) or an ordering of them (Order).
enum class FunctionKind : vef_index_profile_fn_kind_t {
  Restrict = VEF_INDEX_PROFILE_FN_KIND_RESTRICT,
  Order = VEF_INDEX_PROFILE_FN_KIND_ORDER,
};

struct IndexProfileFunctionBinding {
  uint32_t fn_id;
  vef_index_profile_fn_kind_t kind;
  IndexFunctionDesc function;
};

struct IndexProfileDesc {
  const char *name;
  const char *type_name;
  const char *index_type_name;
  // User-visible SQL functions associated with this profile.
  // The optimizer may generate index scan plans for calls to these functions.
  std::vector<IndexProfileFunctionBinding> functions;
  // Helper functions invoked only by the index implementation via helper_fn.
  // fn_ids are independent of the functions fn_id sequence.
  std::vector<IndexProfileFunctionBinding> helpers;
  uint8_t ordering;
  bool default_for_type;
};

namespace detail {

// Which of the once-only setters a profile chain has already used. The set is
// carried in IndexProfileBuilder's type, so calling one of them twice is a
// compile error instead of a silent overwrite of the earlier value.
// with_function() and with_helper() are additive and are not tracked here.
enum : uint32_t {
  kProfileHasType = 1u << 0,
  kProfileHasIndexType = 1u << 1,
  kProfileHasOrdering = 1u << 2,
};

}  // namespace detail

// Builds one index profile. Chain the setters and call build():
//
//   make_index_profile("my_hnsw_l2")
//       .for_type(kMyType)
//       .using_index(kMyIndex)
//       .with_function(1, MY_L2_FN)
//       .ordering(Index::Ordering::ASC)
//       .build();
//
// for_type(), using_index() and ordering() may each be called at most once;
// for_type() and using_index() are required. The setters are rvalue-qualified
// because each returns a builder of a different type, so the chain has to be
// a single expression. To build from a named builder, move it:
// std::move(b).build().
template <uint32_t Set = 0>
class IndexProfileBuilder {
  template <uint32_t>
  friend class IndexProfileBuilder;

  using Self = IndexProfileBuilder<Set>;

 public:
  explicit IndexProfileBuilder(const char *name) {
    desc_.name = name;
    desc_.type_name = nullptr;
    desc_.index_type_name = nullptr;
    desc_.ordering = static_cast<uint8_t>(Index::Ordering::NONE);
    desc_.default_for_type = false;
  }

  IndexProfileBuilder<Set | detail::kProfileHasType> for_type(
      const char *type_name) && {
    static_assert(!(Set & detail::kProfileHasType),
                  "index profile: for_type() called more than once");
    desc_.type_name = type_name;
    return IndexProfileBuilder<Set | detail::kProfileHasType>(std::move(desc_));
  }

  IndexProfileBuilder<Set | detail::kProfileHasIndexType> using_index(
      const char *index_type_name) && {
    static_assert(!(Set & detail::kProfileHasIndexType),
                  "index profile: using_index() called more than once");
    desc_.index_type_name = index_type_name;
    return IndexProfileBuilder<Set | detail::kProfileHasIndexType>(
        std::move(desc_));
  }

  // Bind fn_id to a user-visible SQL function. kind says whether the function
  // restricts the indexed rows (default) or defines their order. FuncDesc is a
  // VDF built with vsql::make_func(...).build(), e.g. `static constexpr auto
  // MY_FN = make_func<&my_fn>(...).build();`. A descriptor from
  // make_index_function() is rejected at compile time. See
  // detail::CheckIndexProfileFuncNorm() for the shape FuncDesc must have;
  // violating it is a compile error. fn_ids must be unique within the
  // functions list (checked by the server at registration time, not here).
  template <const auto &FuncDesc>
  Self &&with_function(uint32_t fn_id,
                       FunctionKind kind = FunctionKind::Restrict) && {
    static_assert(
        detail::is_static_func_desc_v<detail::DescType<FuncDesc>>,
        "with_function() requires a function built with vsql::make_func(); "
        "functions built with make_index_function() go in with_helper()");
    detail::CheckIndexProfileFuncNorm<FuncDesc>();
    desc_.functions.push_back({fn_id,
                               static_cast<vef_index_profile_fn_kind_t>(kind),
                               detail::ToIndexFunctionDesc(FuncDesc)});
    return std::move(*this);
  }

  // Bind fn_id to a helper function invoked only by the index implementation
  // via vef_index_ctx_t.helper_fn. FuncDesc is declared with
  // make_index_function(...).build() -- unlike with_function(), helpers are
  // not SQL-visible. A descriptor from vsql::make_func() is rejected at
  // compile time. Declare FuncDesc with static storage duration usable in
  // a constant expression, e.g. `static constexpr auto MY_HELPER_FN =
  // make_index_function<&my_helper_fn>(...).build();`.
  //
  // See detail::CheckIndexProfileFuncNorm() for the shape FuncDesc must have;
  // violating it is a compile error. fn_ids are independent of the functions
  // fn_id sequence and must be unique within the helpers list.
  template <const auto &FuncDesc>
  Self &&with_helper(uint32_t fn_id) && {
    static_assert(
        detail::is_index_helper_desc_v<detail::DescType<FuncDesc>>,
        "with_helper() requires a function built with make_index_function(); "
        "SQL-visible functions built with vsql::make_func() go in "
        "with_function()");
    detail::CheckIndexProfileFuncNorm<FuncDesc>();
    desc_.helpers.push_back({fn_id, VEF_INDEX_PROFILE_FN_KIND_INVALID,
                             detail::ToIndexFunctionDesc(FuncDesc)});
    return std::move(*this);
  }

  IndexProfileBuilder<Set | detail::kProfileHasOrdering> ordering(
      Index::Ordering ord) && {
    static_assert(!(Set & detail::kProfileHasOrdering),
                  "index profile: ordering() called more than once");
    desc_.ordering = static_cast<uint8_t>(ord);
    return IndexProfileBuilder<Set | detail::kProfileHasOrdering>(
        std::move(desc_));
  }

  // When true, this profile is used if no profile is named at CREATE INDEX.
  // At most one profile may be the default for a given (data type, index type)
  // pair; an extension that declares two is rejected at registration.
  // TODO(villagesql-indexing): Once the names an extension supplies are
  // validated (see the TODO(villagesql-production) in veb/validate.cc),
  // revisit checking this at compile time too. Restricting names to ASCII
  // would allow a constexpr matcher to work.
  Self &&default_for_type(bool is_default) && {
    desc_.default_for_type = is_default;
    return std::move(*this);
  }

  IndexProfileDesc build() && {
    static_assert(Set & detail::kProfileHasType,
                  "index profile: for_type() must be called before build()");
    static_assert(Set & detail::kProfileHasIndexType,
                  "index profile: using_index() must be called before build()");
    auto by_fn_id = [](const IndexProfileFunctionBinding &a,
                       const IndexProfileFunctionBinding &b) {
      return a.fn_id < b.fn_id;
    };
    std::sort(desc_.functions.begin(), desc_.functions.end(), by_fn_id);
    std::sort(desc_.helpers.begin(), desc_.helpers.end(), by_fn_id);
    return std::move(desc_);
  }

 private:
  explicit IndexProfileBuilder(IndexProfileDesc &&desc)
      : desc_(std::move(desc)) {}

  IndexProfileDesc desc_;
};

inline IndexProfileBuilder<> make_index_profile(const char *name) {
  return IndexProfileBuilder<>(name);
}

// ===========================================================================
// make_index_function
// ===========================================================================
// Registers an internal helper function for use by index operations
// (distance, comparison) via IndexProfileBuilder::with_helper(). SQL-visible
// functions bound via with_function() do not use this -- they are declared
// with the normal vsql::make_func() instead (see INDEX PROFILE above). The
// builder mirrors vsql::make_func() for the options that are valid on an index
// function; prerun(), postrun(), varargs(), buffer_size() and
// max_result_length() are not offered because the profile norm forbids or
// ignores them (see detail::CheckIndexProfileFuncNorm()).

template <auto F, size_t NumParams,
          vsql::func_builder::ParamMode Mode =
              vsql::func_builder::ParamMode::kUnset>
class IndexFuncBuilder {
  using Inner = vsql::func_builder::FuncBuilder<F, NumParams, Mode>;

 public:
  explicit constexpr IndexFuncBuilder(Inner inner) : inner_(std::move(inner)) {}

  constexpr IndexFuncBuilder &returns(const char *t) {
    inner_.returns(t);
    return *this;
  }

  constexpr IndexFuncBuilder<F, NumParams + 1,
                             vsql::func_builder::ParamMode::kFixed>
  param(const char *t) const {
    return IndexFuncBuilder<F, NumParams + 1,
                            vsql::func_builder::ParamMode::kFixed>{
        inner_.param(t)};
  }

  constexpr IndexFuncBuilder<F, 0, vsql::func_builder::ParamMode::kFixed>
  no_params() const {
    return IndexFuncBuilder<F, 0, vsql::func_builder::ParamMode::kFixed>{
        inner_.no_params()};
  }

  constexpr IndexFuncBuilder &deterministic(bool d = true) {
    inner_.deterministic(d);
    return *this;
  }

  constexpr IndexHelperDesc<NumParams> build() const {
    return IndexHelperDesc<NumParams>{inner_.build()};
  }

 private:
  Inner inner_;
};

template <auto F>
constexpr IndexFuncBuilder<F, 0> make_index_function(const char *name) {
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
// type to an index type and declares its function and helper bindings. This
// only wires fn_id -> function mappings for the index implementation and the
// optimizer; a with_function() binding needs its VDF registered via .func().
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
              b.kind = fb.kind;
              b.name = fb.function.name;
              b.protocol = fb.function.protocol;
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
