// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// vsql_index_test extension: exercises the preview custom index builder API
// (vsql::preview_index_builder) through a no-op DUMMY_HNSW index type.
//
// NOTE: This is an internal testing tool, not an example of how to write a
// VillageSQL extension. For guidance on writing extensions, see the examples
// under villagesql/examples/ and the SDK documentation.
//
// This extension defines:
//
//   DUMMY_VECTOR  - A fixed 4-element float32 custom type. Codec stubs always
//                   produce/return "[0,0,0,0]". Exercises make_type.
//
//   DUMMY_HNSW    - A no-op custom index type that registers all 12 required
//                   lifecycle, DML, and scan hooks as stubs. Every operation
//                   succeeds immediately without doing any real work. Exercises
//                   the full make_index_type builder and validates that
//                   IndexTypeDesc::build() accepts a fully-wired descriptor.
//
//   dummy_hnsw_l2 - An index profile binding DUMMY_HNSW to the DUMMY_VECTOR
//                   type with two function bindings: dummy_l2_distance
//                   (fn_id=1) and dummy_helper_fn (fn_id=2, a generic
//                   placeholder showing that profiles can bind arbitrary
//                   per-profile helpers). Exercises IndexProfileBuilder and the
//                   typed with_function() overload that accepts
//                   IndexFunctionDesc directly.
//
//   dummy_l2_distance - A stub index function (DUMMY_VECTOR, DUMMY_VECTOR ->
//                       REAL) registered via make_index_function. Exercises the
//                       make_index_function builder.
//
//   dummy_helper_fn   - A stub index function (DUMMY_VECTOR -> REAL) registered
//                       via make_index_function.
//
// TODO(villagesql-indexing): Once make_extension().index_type() and
// index_profile() are implemented, wire DUMMY_HNSW_INDEX and
// DUMMY_HNSW_L2_PROFILE into VEF_GENERATE_ENTRY_POINTS. Index functions are
// registered automatically as part of profile registration.

#include <villagesql/preview/index_builder.h>
#include <villagesql/vsql.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string_view>

using namespace vsql::preview_index_builder;

// Options parsed from WITH (...) at CREATE INDEX time.
// Passed as index_ctx->options during create(); available via index_ctx in
// every call.
struct DummyHNSWOptions {
  uint32_t M = 16;
  uint32_t ef_construction = 200;
};

// Per-index dummy state. No real data is stored.
struct DummyHNSWCtx {};

using Ctx = IndexStorageCtx<DummyHNSWCtx>;

// A minimal stub cursor. begin() sets *cursor to point at this; end() frees it.
struct DummyCursor {};

// ============================================================================
// Lifecycle stubs
// ============================================================================

static bool dummy_create(Ctx * /*ctx*/, const vef_index_ctx_t * /*index_ctx*/,
                         vef_storage_space_ref_t /*space_ref*/,
                         vef_storage_trx_ref_t /*trx_ref*/, char * /*err*/,
                         uint32_t /*err_len*/) {
  return false;
}

static bool dummy_drop(Ctx * /*ctx*/, const vef_index_ctx_t * /*index_ctx*/,
                       vef_storage_trx_ref_t /*trx_ref*/, char * /*err*/,
                       uint32_t /*err_len*/) {
  return false;
}

static bool dummy_load(Ctx * /*ctx*/, const vef_index_ctx_t * /*index_ctx*/,
                       vef_storage_ref_t /*storage_ref*/, char * /*err*/,
                       uint32_t /*err_len*/) {
  return false;
}

// ============================================================================
// DML stubs
// ============================================================================

static bool dummy_insert(Ctx * /*ctx*/, const vef_index_ctx_t * /*index_ctx*/,
                         vef_storage_trx_ref_t /*trx_ref*/,
                         vef_storage_col_data_t * /*key_columns*/,
                         vef_storage_col_data_t * /*pkey_columns*/,
                         vef_storage_col_ref_t * /*key_ref*/, char * /*err*/,
                         uint32_t /*err_len*/) {
  return false;
}

static bool dummy_mark_delete(Ctx * /*ctx*/,
                              const vef_index_ctx_t * /*index_ctx*/,
                              vef_storage_trx_ref_t /*trx_ref*/,
                              vef_storage_col_ref_t * /*key_ref*/,
                              vef_storage_col_data_t * /*key_columns*/,
                              vef_storage_col_data_t * /*pkey_columns*/,
                              bool /*delete_mark*/, char * /*err*/,
                              uint32_t /*err_len*/) {
  return false;
}

static bool dummy_purge(Ctx * /*ctx*/, const vef_index_ctx_t * /*index_ctx*/,
                        vef_storage_trx_ref_t /*trx_ref*/,
                        vef_storage_col_ref_t * /*key_ref*/,
                        vef_storage_col_data_t * /*key_columns*/,
                        vef_storage_col_data_t * /*pkey_columns*/,
                        char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

// ============================================================================
// Scan stubs
// ============================================================================

static bool dummy_begin(Ctx * /*ctx*/, const vef_index_ctx_t * /*index_ctx*/,
                        vef_storage_mtr_ref_t /*mctx*/,
                        const vef_index_scan_desc_t * /*scan_desc*/,
                        vef_index_cursor_ref_t *cursor, bool *eof,
                        char * /*err*/, uint32_t /*err_len*/) {
  *cursor = new DummyCursor{};
  *eof = true;
  return false;
}

static bool dummy_position(vef_index_cursor_ref_t /*cursor*/,
                           vef_index_cursor_op_t /*op*/, bool *eof,
                           char * /*err*/, uint32_t /*err_len*/) {
  *eof = true;
  return false;
}

static bool dummy_fetch(vef_index_cursor_ref_t /*cursor*/,
                        vef_storage_col_ref_t * /*key_ref*/,
                        vef_storage_col_data_t * /*key_columns*/,
                        vef_storage_col_data_t * /*pkey_columns*/,
                        char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

static bool dummy_save(vef_index_cursor_ref_t /*cursor*/, char * /*err*/,
                       uint32_t /*err_len*/) {
  return false;
}

static bool dummy_restore(vef_index_cursor_ref_t /*cursor*/,
                          vef_storage_mtr_ref_t /*mctx*/, bool *eof,
                          char * /*err*/, uint32_t /*err_len*/) {
  *eof = true;
  return false;
}

static void dummy_end(vef_index_cursor_ref_t *cursor) {
  delete static_cast<DummyCursor *>(*cursor);
  *cursor = nullptr;
}

// ============================================================================
// Index type registration
// ============================================================================

static constexpr const char kDummyVECTOR[] = "dummy_type_vector";

static constexpr const char kDummyHNSW[] = "dummy_index_hnsw";
static constexpr const char kDummyProfileL2[] = "dummy_profile_hnsw_l2";

static constexpr const char kDummyHNSWFunc1[] = "dummy_l2_distance";
static constexpr const char kDummyHNSWFunc2[] = "dummy_helper_fn";

// ============================================================================
// Vector type codec stubs (fixed 4-element float32, always returns "[0,0,0,0]")
// ============================================================================

template <size_t N>
inline constexpr int64_t DECODE_BUFFER_SIZE = static_cast<int64_t>(N);

static void vector_from_string(std::string_view /*from*/,
                               vsql::CustomResult out) {
  vsql::Span<unsigned char> buf = out.buffer();
  if (buf.size() < 16) {
    out.error("buffer too small");
    return;
  }
  memset(buf.data(), 0, 16);
  out.set_length(16);
}

static void vector_to_string(vsql::CustomArg /*in*/, vsql::StringResult out) {
  out.set(std::string_view("[0,0,0,0]"));
}

static int vector_compare(vsql::CustomArg /*a*/, vsql::CustomArg /*b*/) {
  return 0;
}

constexpr auto DUMMY_VECTOR =
    vsql::make_type<kDummyVECTOR>()
        // Data length related functions
        .persisted_length(16)
        .max_decode_buffer_length(DECODE_BUFFER_SIZE<256>)

        // Data conversion and compare
        .from_string<&vector_from_string>()
        .to_string<&vector_to_string>()
        .compare<&vector_compare>()
        .intrinsic_default_str("[0,0,0,0]")
        .build();

// Parse WITH (...) options for DUMMY_HNSW. Accepts "M" and "ef_construction".
static bool dummy_parse_options(const vef_index_param_t *params, uint32_t count,
                                DummyHNSWOptions *out, char *error_msg,
                                uint32_t error_msg_len) {
  *out = DummyHNSWOptions{};
  for (uint32_t i = 0; i < count; ++i) {
    const char *key = params[i].key;
    const char *val = params[i].value;
    char *end;
    if (strcmp(key, "M") == 0) {
      unsigned long v = strtoul(val, &end, 10);
      if (*end != '\0' || v == 0 || v > 65536) {
        snprintf(error_msg, error_msg_len,
                 "M must be a positive integer <= 65536, got '%s'", val);
        return true;
      }
      out->M = static_cast<uint32_t>(v);
    } else if (strcmp(key, "ef_construction") == 0) {
      unsigned long v = strtoul(val, &end, 10);
      if (*end != '\0' || v == 0) {
        snprintf(error_msg, error_msg_len,
                 "ef_construction must be a positive integer, got '%s'", val);
        return true;
      }
      out->ef_construction = static_cast<uint32_t>(v);
    } else {
      snprintf(error_msg, error_msg_len, "unknown option '%s'", key);
      return true;
    }
  }
  return false;
}

// Build the index type descriptor. This validates at compile time that all 12
// hooks are wired, capabilities is non-zero, and storage_props declares at
// least one reference type.
//
// TODO(villagesql-indexing): pass DUMMY_HNSW_INDEX to
// make_extension().index_type() once that method is implemented.
// clang-format off
static constexpr auto DUMMY_HNSW_INDEX =
    make_index_type<kDummyHNSW, DummyHNSWCtx>()
        .lifecycle()
            .create<&dummy_create>()
            .load<&dummy_load>()
            .drop<&dummy_drop>()

        .dml()
            .insert<&dummy_insert>()
            .mark_delete<&dummy_mark_delete>()
            .purge<&dummy_purge>()

        .scan()
            .begin<&dummy_begin>()
            .position<&dummy_position>()
            .fetch<&dummy_fetch>()
            .save<&dummy_save>()
            .restore<&dummy_restore>()
            .end<&dummy_end>()

        .global()
            .capabilities(IndexSupport::KNN)
            .storage_props(IndexStorage::HAS_COLUMN_REF | IndexStorage::REF_LOOKUP)
            .options<DummyHNSWOptions, &dummy_parse_options>()

        .build();
// clang-format on

// ============================================================================
// Index function (stub)
// ============================================================================
static void dummy_l2_distance_impl(vsql::CustomArg /*a*/, vsql::CustomArg /*b*/,
                                   vsql::RealResult out) {
  out.set(0.0);
}

static void dummy_helper_fn_impl(vsql::CustomArg /*a*/, vsql::RealResult out) {
  out.set(0.0);
}

// DUMMY_L2_FN and DUMMY_HELPER_FN are registered automatically when the
// profile is registered via make_extension().index_profile().
static const auto DUMMY_L2_FN =
    make_index_function<&dummy_l2_distance_impl>(kDummyHNSWFunc1)
        .returns(vsql::REAL)
        .param(DUMMY_VECTOR)
        .param(DUMMY_VECTOR)
        .deterministic()
        .build();

static const auto DUMMY_HELPER_FN =
    make_index_function<&dummy_helper_fn_impl>(kDummyHNSWFunc2)
        .returns(vsql::REAL)
        .param(DUMMY_VECTOR)
        .deterministic()
        .build();

// ============================================================================
// Index profile
// ============================================================================

// TODO(villagesql-indexing): pass DUMMY_HNSW_L2_PROFILE to
// make_extension().index_profile() once that method is implemented.
static const auto DUMMY_HNSW_L2_PROFILE = make_index_profile(kDummyProfileL2)
                                              .for_type(kDummyVECTOR)
                                              .using_index(kDummyHNSW)
                                              .with_function(1, DUMMY_L2_FN)
                                              .with_function(2, DUMMY_HELPER_FN)
                                              .ordering(IndexOrdering::ASC)
                                              .default_for_type(true)
                                              .build();

// ============================================================================
// Extension entry point
// ============================================================================

// TODO(villagesql-indexing): Register index type and profile once the
// corresponding make_extension() methods are implemented. Index functions
// bound to the profile are registered automatically as part of profile
// registration.
VEF_GENERATE_ENTRY_POINTS(vsql::make_extension().type(DUMMY_VECTOR)
                          //      .index_type(DUMMY_HNSW_INDEX)
                          //      .index_profile(DUMMY_HNSW_L2_PROFILE)
)
