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

#include <villagesql/preview/index_builder.h>
#include <villagesql/vsql.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

using namespace vsql::preview_index_builder;

using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Segment;
using vsql::preview_storage::Space;

// Defaults for DUMMY_HNSW's WITH (...) options. dummy_parse_options writes
// these into the parameter map at CREATE INDEX time and the server persists
// them, so changing a value here cannot alter an index that already exists.
// DummyHNSWOptions deliberately has no member initializers: a default lives in
// exactly one place, and that place is dummy_parse_options.
constexpr uint32_t kDefaultM = 16;
constexpr uint32_t kDefaultEfConstruction = 200;

// Largest M a DUMMY_HNSW index may declare.
constexpr unsigned long kMaxM = 65536;

// Options parsed from WITH (...) at CREATE INDEX time.
// Passed as index_ctx->options during create(); available via index_ctx in
// every call.
struct DummyHNSWOptions {
  uint32_t M;
  uint32_t ef_construction;
};

// Per-index dummy state. No real data is stored.
struct DummyHNSWCtx {};

using Ctx = Index::StorageCtx<DummyHNSWCtx>;

// A minimal stub cursor. begin() sets *cursor to point at this; end() frees it.
struct DummyCursor {};

// ============================================================================
// Lifecycle stubs
// ============================================================================

static bool dummy_create(Ctx * /*ctx*/, const Index & /*index*/,
                         Space::Ref /*space_ref*/, Segment::TrxRef /*trx_ref*/,
                         char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

static bool dummy_drop(Ctx * /*ctx*/, const Index & /*index*/,
                       Segment::TrxRef /*trx_ref*/, char * /*err*/,
                       uint32_t /*err_len*/) {
  return false;
}

static bool dummy_load(Ctx * /*ctx*/, const Index & /*index*/,
                       Index::StorageRef /*storage_ref*/, char * /*err*/,
                       uint32_t /*err_len*/) {
  return false;
}

// ============================================================================
// DML stubs
// ============================================================================

static bool dummy_insert(Ctx * /*ctx*/, const Index & /*index*/,
                         Segment::TrxRef /*trx_ref*/,
                         IndexScanKey::KeyPartData * /*key_columns*/,
                         IndexScanKey::KeyPartData * /*pkey_columns*/,
                         IndexScanKey::KeyPartRef * /*key_ref*/, char * /*err*/,
                         uint32_t /*err_len*/) {
  return false;
}

static bool dummy_mark_delete(Ctx * /*ctx*/, const Index & /*index*/,
                              Segment::TrxRef /*trx_ref*/,
                              IndexScanKey::KeyPartRef * /*key_ref*/,
                              IndexScanKey::KeyPartData * /*key_columns*/,
                              IndexScanKey::KeyPartData * /*pkey_columns*/,
                              bool /*delete_mark*/, char * /*err*/,
                              uint32_t /*err_len*/) {
  return false;
}

static bool dummy_purge(Ctx * /*ctx*/, const Index & /*index*/,
                        Segment::TrxRef /*trx_ref*/,
                        IndexScanKey::KeyPartRef * /*key_ref*/,
                        IndexScanKey::KeyPartData * /*key_columns*/,
                        IndexScanKey::KeyPartData * /*pkey_columns*/,
                        char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

// ============================================================================
// Scan stubs
// ============================================================================

static bool dummy_begin(Ctx * /*ctx*/, const Index & /*index*/,
                        MtrCtx::Ref /*mctx*/,
                        const IndexScanDesc & /*scan_desc*/,
                        Index::Cursor *cursor, bool *eof, char * /*err*/,
                        uint32_t /*err_len*/) {
  *cursor = new DummyCursor{};
  *eof = true;
  return false;
}

static bool dummy_position(Index::Cursor /*cursor*/, Index::CursorOp /*op*/,
                           bool *eof, char * /*err*/, uint32_t /*err_len*/) {
  *eof = true;
  return false;
}

static bool dummy_fetch(Index::Cursor /*cursor*/,
                        IndexScanKey::KeyPartRef * /*key_ref*/,
                        IndexScanKey::KeyPartData * /*key_columns*/,
                        IndexScanKey::KeyPartData * /*pkey_columns*/,
                        char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

static bool dummy_save(Index::Cursor /*cursor*/, char * /*err*/,
                       uint32_t /*err_len*/) {
  return false;
}

static bool dummy_restore(Index::Cursor /*cursor*/, MtrCtx::Ref /*mctx*/,
                          bool *eof, char * /*err*/, uint32_t /*err_len*/) {
  *eof = true;
  return false;
}

static void dummy_end(Index::Cursor *cursor) {
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

// Validate one unsigned WITH (...) value. A max of 0 means no upper bound.
// Returns false and stores the value in *out on success.
static bool dummy_parse_uint(const char *name, const std::string &val,
                             unsigned long max, uint32_t *out, char *error_msg,
                             uint32_t error_msg_len) {
  char *end = nullptr;
  unsigned long v = strtoul(val.c_str(), &end, 10);
  if (*end != '\0' || v == 0 || (max != 0 && v > max)) {
    if (max != 0) {
      snprintf(error_msg, error_msg_len,
               "%s must be a positive integer <= %lu, got '%s'", name, max,
               val.c_str());
    } else {
      snprintf(error_msg, error_msg_len,
               "%s must be a positive integer, got '%s'", name, val.c_str());
    }
    return true;
  }
  *out = static_cast<uint32_t>(v);
  return false;
}

// Complete, validate and parse WITH (...) options for DUMMY_HNSW.
//
// Called once at CREATE INDEX, where the server keeps the completed parameters
// and persists them, and again on every index open, where it keeps the options
// struct and checks that completing the stored parameters changes nothing.
// Both calls look the same from here.
//
// try_emplace never overwrites a parameter that is present, and validation
// does not care whether a value came from the user or from this function's own
// earlier output, so completing its own output returns that same output.
static bool dummy_parse_options(std::map<std::string, std::string> &params,
                                DummyHNSWOptions *out, char *error_msg,
                                uint32_t error_msg_len) {
  uint32_t discard = 0;
  for (const auto &[key, value] : params) {
    if (key == "m") {
      if (dummy_parse_uint("M", value, kMaxM, &discard, error_msg,
                           error_msg_len))
        return true;
    } else if (key == "ef_construction") {
      if (dummy_parse_uint("ef_construction", value, 0, &discard, error_msg,
                           error_msg_len))
        return true;
    } else if (key != "label" && key != "mode") {
      snprintf(error_msg, error_msg_len, "unknown option '%s'", key.c_str());
      return true;
    }
  }

  // std::to_string on an unsigned produces digits only, so these round-trip
  // through the server's lowercasing unchanged.
  params.try_emplace("m", std::to_string(kDefaultM));
  params.try_emplace("ef_construction", std::to_string(kDefaultEfConstruction));

  *out = DummyHNSWOptions{};
  return dummy_parse_uint("M", params["m"], kMaxM, &out->M, error_msg,
                          error_msg_len) ||
         dummy_parse_uint("ef_construction", params["ef_construction"], 0,
                          &out->ef_construction, error_msg, error_msg_len);
}

// Build the index type descriptor. This validates at compile time that all 12
// hooks are wired, capabilities is non-zero, and storage_props declares at
// least one reference type.
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
            .capabilities(Index::Support::KNN)
            .storage_props(Index::Storage::HAS_COLUMN_REF | Index::Storage::REF_LOOKUP)
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

static const auto DUMMY_HNSW_L2_PROFILE = make_index_profile(kDummyProfileL2)
                                              .for_type(kDummyVECTOR)
                                              .using_index(kDummyHNSW)
                                              .with_function(1, DUMMY_L2_FN)
                                              .with_helper(1, DUMMY_HELPER_FN)
                                              .ordering(Index::Ordering::ASC)
                                              .default_for_type(true)
                                              .build();

// ============================================================================
// Extension entry point
// ============================================================================

static auto INDEX_TYPE =
    vsql::preview_index_builder::IndexTypeCapability().index_type(
        DUMMY_HNSW_INDEX);
static auto INDEX_PROFILE =
    vsql::preview_index_builder::IndexProfileCapability().index_profile(
        DUMMY_HNSW_L2_PROFILE);

VEF_GENERATE_ENTRY_POINTS(vsql::make_extension()
                              .with(INDEX_TYPE)
                              .with(INDEX_PROFILE)
                              .type(DUMMY_VECTOR))
