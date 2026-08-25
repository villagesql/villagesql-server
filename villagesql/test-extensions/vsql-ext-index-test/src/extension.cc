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

// vsql_ext_index_test extension: a custom index type over an externally-stored
// custom type whose declared field width exceeds the server's key-length limit.
// The fixture for https://github.com/villagesql/villagesql-server/issues/1034.
//
// Not an example of how to write an extension; the hooks are no-op stubs. It
// defines:
//   ext_index_wide_blob   - externally-stored type, 4096-byte persisted field
//   ext_index             - custom index type over it
//   ext_index_default     - default index profile
//   ext_index_fn,
//   ext_index_helper_fn   - stub index functions the profile binds

#include <villagesql/preview/index_builder.h>
#include <villagesql/preview/storage_api.h>
#include <villagesql/preview/storage_builder.h>
#include <villagesql/vsql.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string_view>

using namespace vsql::preview_index_builder;

namespace storage = vsql::preview_storage;
using storage::MtrCtx;
using storage::Segment;
using storage::Space;
using vsql::preview_storage_builder::ColumnStoreCapability;
using vsql::preview_storage_builder::make_column_store;
using vsql::preview_storage_builder::StorageCapability;

// ============================================================================
// Externally-stored WIDE_BLOB type
// ============================================================================
//
// The persisted (inline) field layout is kFieldSize bytes:
//   [0..7]              — Column::Ref placeholder (zero; filled in by the
//                          server after insert returns *col_ref)
//   [kRefSize..end]     — opaque payload bytes (passed as col_data to
//                          insert/select)
//
// from_string must produce exactly persisted_length (kFieldSize) bytes.
// compare and to_string receive the full persisted field; to_string reports the
// payload byte length so the value round-trips visibly in the test.
//
// The 12K width is the whole point of this fixture: it is wider than the
// 3072-byte B-tree key-length limit that trips ER_INDEX_COLUMN_TOO_LONG, and
// wider than the 8K half-page boundary that trips the InnoDB record-size error,
// but well under the custom-column backing cap so INSTALL EXTENSION succeeds.

static constexpr size_t kRefSize = 8;          // sizeof(Column::Ref)
static constexpr size_t kPayloadSize = 12280;  // opaque payload bytes
static constexpr size_t kFieldSize = kRefSize + kPayloadSize;  // = 12288 (12K)

// The payload width the storage layer round-trips as col_data.
static constexpr uint32_t kColDataSize = static_cast<uint32_t>(kPayloadSize);

// Per-column user context. Populated during create/load and used by every
// subsequent storage call for that column.
struct WideBlobCtx {
  storage::Space::Ref space = 0;
  storage::Segment::PageRef root_page = storage::Page::INVALID_REF;
};

using StoreCtx = storage::Column::StorageCtx<WideBlobCtx>;

// Absolute page offsets for each field within the DATA area of a data page.
constexpr storage::Page::Offset kValueOff = storage::Page::HEADER_SIZE;
constexpr storage::Page::Offset kRowidOff =
    storage::Page::HEADER_SIZE + kColDataSize;
constexpr storage::Page::Offset kTrxOff =
    storage::Page::HEADER_SIZE + kColDataSize + 8;
constexpr storage::Page::Offset kFlagOff =
    storage::Page::HEADER_SIZE + kColDataSize + 16;

// StorageRef packs (space_ref, root_page_num) into a uint64:
//   high 32 bits — space_ref
//   low  32 bits — root_page_num
static storage::Column::StorageRef encode_storage_ref(
    storage::Space::Ref space, storage::Segment::PageRef root) {
  return (static_cast<storage::Column::StorageRef>(space) << 32) |
         static_cast<storage::Column::StorageRef>(root);
}

// ============================================================================
// Type codec (from_string / to_string / compare)
// ============================================================================

static void wide_blob_from_string(std::string_view s, vsql::CustomResult out) {
  auto buf = out.buffer();
  if (buf.size() < kFieldSize) return;  // wrapper default warning
  // [0..kRefSize): zero placeholder for Column::Ref (server fills after
  // insert).
  memset(buf.data(), 0, kRefSize);
  // [kRefSize..end): opaque payload, zero-padded to kPayloadSize.
  memset(buf.data() + kRefSize, 0, kPayloadSize);
  size_t n = s.size() < kPayloadSize ? s.size() : kPayloadSize;
  if (n > 0) memcpy(buf.data() + kRefSize, s.data(), n);
  out.set_length(kFieldSize);
}

static void wide_blob_to_string(vsql::CustomArg in, vsql::StringResult out) {
  // Receives the full persisted field (kFieldSize bytes: ref + payload).
  // Report the number of non-zero payload bytes as "len=<n>" so a stored value
  // round-trips visibly without dumping 4KB of bytes into the .result.
  auto data = in.value();
  if (data.size() < kFieldSize) return;  // wrapper default ERROR
  const unsigned char *payload = data.data() + kRefSize;
  size_t n = kPayloadSize;
  while (n > 0 && payload[n - 1] == 0) --n;
  auto buf = out.buffer();
  int written = snprintf(buf.data(), buf.size(), "len=%zu", n);
  if (written < 0) return;
  out.set_length(static_cast<size_t>(written));
}

static int wide_blob_compare(vsql::CustomArg a, vsql::CustomArg b) {
  // Receives the full persisted field. Compare the payload portion only; the
  // Column::Ref prefix is server-owned bookkeeping, not part of the value.
  auto va = a.value();
  auto vb = b.value();
  if (va.size() < kFieldSize || vb.size() < kFieldSize) return 0;
  return memcmp(va.data() + kRefSize, vb.data() + kRefSize, kPayloadSize);
}

// ============================================================================
// Storage interface functions
// ============================================================================

bool wide_blob_create(StoreCtx *ctx, storage::Space::Ref space,
                      storage::Segment::TrxRef trx, uint32_t /*col_len*/,
                      char *err, uint32_t err_len) {
  storage::Segment::PageRef root_page;
  if (storage::Segment::create(space, 1, trx, root_page) !=
      storage::Error::SUCCESS) {
    snprintf(err, err_len, "wide_blob create: %s",
             storage::last_error().data());
    return true;
  }
  ctx->user()->space = space;
  ctx->user()->root_page = root_page;
  ctx->set_ref(encode_storage_ref(space, root_page));
  return false;
}

bool wide_blob_drop(StoreCtx *ctx, storage::Segment::TrxRef trx, char *err,
                    uint32_t err_len) {
  if (storage::Segment::drop(ctx->user()->space, trx, ctx->user()->root_page) !=
      storage::Error::SUCCESS) {
    snprintf(err, err_len, "wide_blob drop: %s", storage::last_error().data());
    return true;
  }
  return false;
}

bool wide_blob_load(StoreCtx *ctx, storage::Column::StorageRef storage_ref,
                    char * /*err*/, uint32_t /*err_len*/) {
  ctx->user()->space = static_cast<storage::Space::Ref>(storage_ref >> 32);
  ctx->user()->root_page =
      static_cast<storage::Segment::PageRef>(storage_ref & 0xFFFFFFFF);
  ctx->set_ref(storage_ref);
  return false;
}

bool wide_blob_insert(StoreCtx *ctx, storage::MtrCtx::Ref mctx,
                      storage::Segment::TrxRef trx,
                      storage::Column::Data col_data,
                      storage::Column::Data rowid_prefix,
                      storage::Column::Ref *col_ref, char *err,
                      uint32_t err_len) {
  if (col_data.length != kColDataSize) {
    snprintf(err, err_len, "wide_blob insert: expected %u-byte value, got %u",
             kColDataSize, col_data.length);
    return true;
  }

  storage::Page root;
  if (root.load(ctx->user()->space, ctx->user()->root_page,
                storage::Page::Latch::EXCLUSIVE,
                mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "wide_blob insert: root page load failed: %s",
             storage::last_error().data());
    return true;
  }

  storage::Segment::Ref seg = storage::Segment::get_header(root, 0);
  if (seg == nullptr) {
    snprintf(err, err_len, "wide_blob insert: segment header not found");
    return true;
  }

  storage::Page data_page;
  if (data_page.load_new(seg, mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "wide_blob insert: page allocation failed: %s",
             storage::last_error().data());
    return true;
  }

  data_page.write_string(kValueOff, col_data.data, col_data.length, mctx);

  // Store up to 8 bytes of rowid_prefix, zero-padded.
  unsigned char rowid_buf[8] = {};
  uint32_t rowid_copy = rowid_prefix.length < 8 ? rowid_prefix.length : 8;
  if (rowid_copy > 0) memcpy(rowid_buf, rowid_prefix.data, rowid_copy);
  data_page.write_string(kRowidOff, rowid_buf, 8, mctx);

  data_page.write_integer_8(kTrxOff, static_cast<uint64_t>(trx), mctx);
  data_page.write_integer_1(kFlagOff, 0, mctx);

  *col_ref = static_cast<storage::Column::Ref>(data_page.get_ref());
  return false;
}

bool wide_blob_select(StoreCtx *ctx, storage::MtrCtx::Ref mctx,
                      storage::Column::Ref col_ref,
                      storage::Column::Data *col_data,
                      storage::Column::Data *rowid_prefix,
                      storage::Segment::TrxRef *trx_ref, bool *delete_marked,
                      char *err, uint32_t err_len) {
  auto page_num = static_cast<storage::Segment::PageRef>(col_ref);
  storage::Page page;
  if (page.load(ctx->user()->space, page_num, storage::Page::Latch::SHARED,
                mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "wide_blob select: page load failed: %s",
             storage::last_error().data());
    return true;
  }

  // Return pointers directly into the latched page frame. These remain valid
  // until the server commits the mini-transaction.
  const unsigned char *base = page.get_data();
  col_data->data = base + kValueOff;
  col_data->length = kColDataSize;
  rowid_prefix->data = base + kRowidOff;
  rowid_prefix->length = 8;
  *trx_ref =
      static_cast<storage::Segment::TrxRef>(page.read_integer_8(kTrxOff));
  *delete_marked = page.read_integer_1(kFlagOff) != 0;
  return false;
}

bool wide_blob_mark_delete(StoreCtx *ctx, storage::MtrCtx::Ref mctx,
                           storage::Segment::TrxRef trx,
                           storage::Column::Ref col_ref, bool delete_mark,
                           char *err, uint32_t err_len) {
  auto page_num = static_cast<storage::Segment::PageRef>(col_ref);
  storage::Page page;
  if (page.load(ctx->user()->space, page_num, storage::Page::Latch::EXCLUSIVE,
                mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "wide_blob mark_delete: page load failed: %s",
             storage::last_error().data());
    return true;
  }
  page.write_integer_1(kFlagOff, delete_mark ? 1 : 0, mctx);
  page.write_integer_8(kTrxOff, static_cast<uint64_t>(trx), mctx);
  return false;
}

bool wide_blob_purge(StoreCtx * /*ctx*/, storage::MtrCtx::Ref /*mctx*/,
                     storage::Segment::TrxRef /*trx*/,
                     storage::Column::Ref /*col_ref*/, char * /*err*/,
                     uint32_t /*err_len*/) {
  // TODO(villagesql-beta): Free the individual data page once a per-page
  // release API is available. Until then, pages are reclaimed only when the
  // segment is dropped (on column drop).
  return false;
}

// ============================================================================
// Index type: no-op stubs (bool hooks return false == success)
// ============================================================================

// Per-index dummy state. No real data is stored.
struct ExtIndexCtx {};

using IdxCtx = Index::StorageCtx<ExtIndexCtx>;

// A minimal stub cursor. begin() sets *cursor to point at this; end() frees it.
struct ExtIndexCursor {};

static bool ext_index_create(IdxCtx * /*ctx*/, const Index & /*index*/,
                             Space::Ref /*space_ref*/,
                             Segment::TrxRef /*trx_ref*/, char * /*err*/,
                             uint32_t /*err_len*/) {
  return false;
}

static bool ext_index_drop(IdxCtx * /*ctx*/, const Index & /*index*/,
                           Segment::TrxRef /*trx_ref*/, char * /*err*/,
                           uint32_t /*err_len*/) {
  return false;
}

static bool ext_index_load(IdxCtx * /*ctx*/, const Index & /*index*/,
                           Index::StorageRef /*storage_ref*/, char * /*err*/,
                           uint32_t /*err_len*/) {
  return false;
}

static bool ext_index_insert(IdxCtx * /*ctx*/, const Index & /*index*/,
                             Segment::TrxRef /*trx_ref*/,
                             IndexScanKey::KeyPartData * /*key_columns*/,
                             IndexScanKey::KeyPartData * /*pkey_columns*/,
                             IndexScanKey::KeyPartRef * /*key_ref*/,
                             char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

static bool ext_index_mark_delete(IdxCtx * /*ctx*/, const Index & /*index*/,
                                  Segment::TrxRef /*trx_ref*/,
                                  IndexScanKey::KeyPartRef * /*key_ref*/,
                                  IndexScanKey::KeyPartData * /*key_columns*/,
                                  IndexScanKey::KeyPartData * /*pkey_columns*/,
                                  bool /*delete_mark*/, char * /*err*/,
                                  uint32_t /*err_len*/) {
  return false;
}

static bool ext_index_purge(IdxCtx * /*ctx*/, const Index & /*index*/,
                            Segment::TrxRef /*trx_ref*/,
                            IndexScanKey::KeyPartRef * /*key_ref*/,
                            IndexScanKey::KeyPartData * /*key_columns*/,
                            IndexScanKey::KeyPartData * /*pkey_columns*/,
                            char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

static bool ext_index_begin(IdxCtx * /*ctx*/, const Index & /*index*/,
                            MtrCtx::Ref /*mctx*/,
                            const IndexScanDesc & /*scan_desc*/,
                            Index::Cursor *cursor, bool *eof, char * /*err*/,
                            uint32_t /*err_len*/) {
  *cursor = new ExtIndexCursor{};
  *eof = true;
  return false;
}

static bool ext_index_position(Index::Cursor /*cursor*/, Index::CursorOp /*op*/,
                               bool *eof, char * /*err*/,
                               uint32_t /*err_len*/) {
  *eof = true;
  return false;
}

static bool ext_index_fetch(Index::Cursor /*cursor*/,
                            IndexScanKey::KeyPartRef * /*key_ref*/,
                            IndexScanKey::KeyPartData * /*key_columns*/,
                            IndexScanKey::KeyPartData * /*pkey_columns*/,
                            char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

static bool ext_index_save(Index::Cursor /*cursor*/, char * /*err*/,
                           uint32_t /*err_len*/) {
  return false;
}

static bool ext_index_restore(Index::Cursor /*cursor*/, MtrCtx::Ref /*mctx*/,
                              bool *eof, char * /*err*/, uint32_t /*err_len*/) {
  *eof = true;
  return false;
}

static void ext_index_end(Index::Cursor *cursor) {
  delete static_cast<ExtIndexCursor *>(*cursor);
  *cursor = nullptr;
}

// ============================================================================
// Index functions (stubs)
// ============================================================================

static void ext_index_fn_impl(vsql::CustomArg /*a*/, vsql::CustomArg /*b*/,
                              vsql::RealResult out) {
  out.set(0.0);
}

static void ext_index_helper_fn_impl(vsql::CustomArg /*a*/,
                                     vsql::RealResult out) {
  out.set(0.0);
}

// ============================================================================
// Names
// ============================================================================

static constexpr const char kWideBlobTypeName[] = "ext_index_wide_blob";
static constexpr const char kExtIndex[] = "ext_index";
static constexpr const char kExtIndexProfile[] = "ext_index_default";
static constexpr const char kExtIndexFunc1[] = "ext_index_fn";
static constexpr const char kExtIndexFunc2[] = "ext_index_helper_fn";

// ============================================================================
// Type, storage, index type, functions, and profile descriptors
// ============================================================================

constexpr auto WIDE_BLOB =
    vsql::make_type<kWideBlobTypeName>()
        .persisted_length(
            kFieldSize)  // 8 bytes Column::Ref + 4088 bytes payload = 4096
        .max_decode_buffer_length(32)  // "len=<n>" fits comfortably
        .from_string<&wide_blob_from_string>()
        .to_string<&wide_blob_to_string>()
        .compare<&wide_blob_compare>()
        .intrinsic_default_str("")
        .build();

static constexpr auto kWideBlobStorage =
    make_column_store<WideBlobCtx>(WIDE_BLOB)
        .create<&wide_blob_create>()
        .drop<&wide_blob_drop>()
        .load<&wide_blob_load>()
        .insert<&wide_blob_insert>()
        .select<&wide_blob_select>()
        .mark_delete<&wide_blob_mark_delete>()
        .purge<&wide_blob_purge>()
        .build();

// Build the index type descriptor. This validates at compile time that all 12
// hooks are wired, capabilities is non-zero, and storage_props declares at
// least one reference type.
// clang-format off
static constexpr auto EXT_INDEX =
    make_index_type<kExtIndex, ExtIndexCtx>()
        .lifecycle()
            .create<&ext_index_create>()
            .load<&ext_index_load>()
            .drop<&ext_index_drop>()

        .dml()
            .insert<&ext_index_insert>()
            .mark_delete<&ext_index_mark_delete>()
            .purge<&ext_index_purge>()

        .scan()
            .begin<&ext_index_begin>()
            .position<&ext_index_position>()
            .fetch<&ext_index_fetch>()
            .save<&ext_index_save>()
            .restore<&ext_index_restore>()
            .end<&ext_index_end>()

        .global()
            .capabilities(Index::Support::KNN)
            .storage_props(Index::Storage::HAS_COLUMN_REF | Index::Storage::REF_LOOKUP)

        .build();
// clang-format on

static const auto EXT_INDEX_FN =
    make_index_function<&ext_index_fn_impl>(kExtIndexFunc1)
        .returns(vsql::REAL)
        .param(WIDE_BLOB)
        .param(WIDE_BLOB)
        .deterministic()
        .build();

static const auto EXT_INDEX_HELPER_FN =
    make_index_function<&ext_index_helper_fn_impl>(kExtIndexFunc2)
        .returns(vsql::REAL)
        .param(WIDE_BLOB)
        .deterministic()
        .build();

static const auto EXT_INDEX_PROFILE = make_index_profile(kExtIndexProfile)
                                          .for_type(kWideBlobTypeName)
                                          .using_index(kExtIndex)
                                          .with_function(1, EXT_INDEX_FN)
                                          .with_helper(1, EXT_INDEX_HELPER_FN)
                                          .ordering(Index::Ordering::ASC)
                                          .default_for_type(true)
                                          .build();

// ============================================================================
// Extension entry point
// ============================================================================

static auto STORAGE = StorageCapability{};
static auto COLUMN_STORE =
    ColumnStoreCapability().column_store(kWideBlobStorage);
static auto INDEX_TYPE =
    vsql::preview_index_builder::IndexTypeCapability().index_type(EXT_INDEX);
static auto INDEX_PROFILE =
    vsql::preview_index_builder::IndexProfileCapability().index_profile(
        EXT_INDEX_PROFILE);

VEF_GENERATE_ENTRY_POINTS(vsql::make_extension()
                              .with(STORAGE)
                              .with(COLUMN_STORE)
                              .with(INDEX_TYPE)
                              .with(INDEX_PROFILE)
                              .type(WIDE_BLOB))
