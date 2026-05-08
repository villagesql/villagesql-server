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

// vsql_storage_test extension: exercises the preview column storage API
// (vsql::preview_storage) through a simple STORED_INT custom type.
//
// NOTE: This is an internal testing tool, not an example of how to write a
// VillageSQL extension. For guidance on writing extensions, see the examples
// under villagesql/examples/ and the SDK documentation.
//
// Types provided:
//
//   STORED_INT  - A signed 64-bit integer whose value is stored in a dedicated
//                 InnoDB segment (one page per row) rather than inline in the
//                 clustered index. Exercises the full create/drop/load/insert/
//                 select/mark_delete/purge storage lifecycle.
//
//                 String representation is the decimal integer, e.g. '42',
//                 '-7', '0'.
//
// Page data layout (absolute page offsets, within the DATA area):
//   HEADER_SIZE + 0  [8 bytes] — encoded integer value (as written by
//                                 from_string)
//   HEADER_SIZE + 8  [8 bytes] — rowid_prefix (zero-padded to 8 bytes)
//   HEADER_SIZE + 16 [8 bytes] — last-writer trx_ref
//   HEADER_SIZE + 24 [1 byte]  — delete-mark flag (0 = live, 1 = deleted)

#include <villagesql/preview/storage_api.h>
#include <villagesql/preview/storage_builder.h>
#include <villagesql/vsql.h>

#include <cassert>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace storage = vsql::preview_storage;
using vsql::preview_storage_builder::ColumnStoreCapability;
using vsql::preview_storage_builder::make_column_store;
using vsql::preview_storage_builder::StorageCapability;

// Per-column user context. Populated during create/load and used by every
// subsequent storage call for that column.
struct StoredIntCtx {
  storage::Space::Ref space = 0;
  storage::Segment::PageRef root_page = storage::Page::INVALID_REF;
};

using Ctx = storage::Column::StorageCtx<StoredIntCtx>;

// Absolute page offsets for each field within the DATA area of a data page.
constexpr storage::Page::Offset kValueOff = storage::Page::HEADER_SIZE;
constexpr storage::Page::Offset kRowidOff = storage::Page::HEADER_SIZE + 8;
constexpr storage::Page::Offset kTrxOff = storage::Page::HEADER_SIZE + 16;
constexpr storage::Page::Offset kFlagOff = storage::Page::HEADER_SIZE + 24;

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

// The persisted field layout is 16 bytes:
//   [0..7]  — Column::Ref placeholder (zero; filled in by the server after
//              insert returns *col_ref)
//   [8..15] — big-endian int64 value (passed as col_data to insert/select)
//
// from_string must produce exactly persisted_length (16) bytes. compare
// receives the full 16-byte field and must look at the value portion [8..15].
// to_string receives only the 8-byte col_data returned by select, so it reads
// from offset 0.

static constexpr size_t kRefSize = 8;  // sizeof(Column::Ref)
static constexpr size_t kValSize = 8;  // sizeof(big-endian int64)
static constexpr size_t kFieldSize = kRefSize + kValSize;  // = persisted_length

static void write_be64(unsigned char *dst, int64_t val) {
  uint64_t u = static_cast<uint64_t>(val);
  dst[0] = static_cast<unsigned char>(u >> 56);
  dst[1] = static_cast<unsigned char>(u >> 48);
  dst[2] = static_cast<unsigned char>(u >> 40);
  dst[3] = static_cast<unsigned char>(u >> 32);
  dst[4] = static_cast<unsigned char>(u >> 24);
  dst[5] = static_cast<unsigned char>(u >> 16);
  dst[6] = static_cast<unsigned char>(u >> 8);
  dst[7] = static_cast<unsigned char>(u);
}

static int64_t read_be64(const unsigned char *src) {
  return static_cast<int64_t>((static_cast<uint64_t>(src[0]) << 56) |
                              (static_cast<uint64_t>(src[1]) << 48) |
                              (static_cast<uint64_t>(src[2]) << 40) |
                              (static_cast<uint64_t>(src[3]) << 32) |
                              (static_cast<uint64_t>(src[4]) << 24) |
                              (static_cast<uint64_t>(src[5]) << 16) |
                              (static_cast<uint64_t>(src[6]) << 8) |
                              static_cast<uint64_t>(src[7]));
}

void stored_int_from_string(std::string_view s, vsql::CustomResult out) {
  auto buf = out.buffer();
  if (buf.size() < kFieldSize) return;  // wrapper default warning
  int64_t val = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), val);
  if (ec != std::errc{} || ptr != s.data() + s.size()) return;
  // [0..7]: zero placeholder for Column::Ref (server fills this after insert).
  memset(buf.data(), 0, kRefSize);
  // [8..15]: big-endian encoded integer value (becomes col_data for insert).
  write_be64(buf.data() + kRefSize, val);
  out.set_length(kFieldSize);
}

void stored_int_to_string(vsql::CustomArg in, vsql::StringResult out) {
  // Receives the full persisted field (16 bytes: ref + value). Read the value
  // from the second half, skipping the Column::Ref prefix.
  auto data = in.value();
  if (data.size() < kFieldSize) return;  // wrapper default ERROR
  int64_t val = read_be64(data.data() + kRefSize);
  auto buf = out.buffer();
  auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), val);
  if (ec != std::errc{}) return;
  out.set_length(static_cast<size_t>(ptr - buf.data()));
}

int stored_int_compare(vsql::CustomArg a, vsql::CustomArg b) {
  // Receives the full persisted field (16 bytes: ref + value). The value is
  // in the last 8 bytes regardless of whether the ref has been written yet.
  auto va = a.value();
  auto vb = b.value();
  if (va.size() < kFieldSize || vb.size() < kFieldSize) return 0;
  int64_t v_a = read_be64(va.data() + kRefSize);
  int64_t v_b = read_be64(vb.data() + kRefSize);
  if (v_a < v_b) return -1;
  if (v_a > v_b) return 1;
  return 0;
}

// ============================================================================
// Storage interface functions
// ============================================================================

bool stored_int_create(Ctx *ctx, storage::Space::Ref space,
                       storage::Segment::TrxRef trx, uint32_t /*col_len*/,
                       char *err, uint32_t err_len) {
  storage::Segment::PageRef root_page;
  if (storage::Segment::create(space, 1, trx, root_page) !=
      storage::Error::SUCCESS) {
    snprintf(err, err_len, "stored_int create: %s",
             storage::last_error().data());
    return true;
  }
  ctx->user()->space = space;
  ctx->user()->root_page = root_page;
  ctx->set_ref(encode_storage_ref(space, root_page));
  return false;
}

bool stored_int_drop(Ctx *ctx, storage::Segment::TrxRef trx, char *err,
                     uint32_t err_len) {
  if (storage::Segment::drop(ctx->user()->space, trx, ctx->user()->root_page) !=
      storage::Error::SUCCESS) {
    snprintf(err, err_len, "stored_int drop: %s", storage::last_error().data());
    return true;
  }
  return false;
}

bool stored_int_load(Ctx *ctx, storage::Column::StorageRef storage_ref,
                     char * /*err*/, uint32_t /*err_len*/) {
  ctx->user()->space = static_cast<storage::Space::Ref>(storage_ref >> 32);
  ctx->user()->root_page =
      static_cast<storage::Segment::PageRef>(storage_ref & 0xFFFFFFFF);
  ctx->set_ref(storage_ref);
  return false;
}

bool stored_int_insert(Ctx *ctx, const storage::MtrCtx &mctx,
                       storage::Segment::TrxRef trx,
                       storage::Column::Data col_data,
                       storage::Column::Data rowid_prefix,
                       storage::Column::Ref *col_ref, char *err,
                       uint32_t err_len) {
  if (col_data.length != 8) {
    snprintf(err, err_len, "stored_int insert: expected 8-byte value, got %u",
             col_data.length);
    return true;
  }

  storage::Page root;
  if (root.load(ctx->user()->space, ctx->user()->root_page,
                storage::Page::Latch::EXCLUSIVE,
                mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "stored_int insert: root page load failed: %s",
             storage::last_error().data());
    return true;
  }

  storage::Segment::Ref seg = storage::Segment::get_header(root, 0);
  if (seg == nullptr) {
    snprintf(err, err_len, "stored_int insert: segment header not found");
    return true;
  }

  storage::Page data_page;
  if (data_page.load_new(seg, mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "stored_int insert: page allocation failed: %s",
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

bool stored_int_select(Ctx *ctx, const storage::MtrCtx &mctx,
                       storage::Column::Ref col_ref,
                       storage::Column::Data *col_data,
                       storage::Column::Data *rowid_prefix,
                       storage::Segment::TrxRef *trx_ref, bool *delete_marked,
                       char *err, uint32_t err_len) {
  auto page_num = static_cast<storage::Segment::PageRef>(col_ref);
  storage::Page page;
  if (page.load(ctx->user()->space, page_num, storage::Page::Latch::SHARED,
                mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "stored_int select: page load failed: %s",
             storage::last_error().data());
    return true;
  }

  // Return pointers directly into the latched page frame. These remain valid
  // until the server commits the mini-transaction. Note: to_string reads from
  // the persisted field in the main row, not from this col_data.
  const unsigned char *base = page.get_data();
  col_data->data = base + kValueOff;
  col_data->length = 8;
  rowid_prefix->data = base + kRowidOff;
  rowid_prefix->length = 8;
  *trx_ref =
      static_cast<storage::Segment::TrxRef>(page.read_integer_8(kTrxOff));
  *delete_marked = page.read_integer_1(kFlagOff) != 0;
  return false;
}

bool stored_int_mark_delete(Ctx *ctx, const storage::MtrCtx &mctx,
                            storage::Segment::TrxRef trx,
                            storage::Column::Ref col_ref, bool delete_mark,
                            char *err, uint32_t err_len) {
  auto page_num = static_cast<storage::Segment::PageRef>(col_ref);
  storage::Page page;
  if (page.load(ctx->user()->space, page_num, storage::Page::Latch::EXCLUSIVE,
                mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "stored_int mark_delete: page load failed: %s",
             storage::last_error().data());
    return true;
  }
  page.write_integer_1(kFlagOff, delete_mark ? 1 : 0, mctx);
  page.write_integer_8(kTrxOff, static_cast<uint64_t>(trx), mctx);
  return false;
}

bool stored_int_purge(Ctx * /*ctx*/, const storage::MtrCtx & /*mctx*/,
                      storage::Segment::TrxRef /*trx*/,
                      storage::Column::Ref /*col_ref*/, char * /*err*/,
                      uint32_t /*err_len*/) {
  // TODO(villagesql-beta): Free the individual data page once a per-page
  // release API is available. Until then, pages are reclaimed only when the
  // segment is dropped (on column drop).
  return false;
}

// ============================================================================
// Type and extension registration
// ============================================================================

static constexpr const char kStoredIntTypeName[] = "STORED_INT";

constexpr auto STORED_INT =
    vsql::make_type<kStoredIntTypeName>()
        .persisted_length(
            kFieldSize)  // 8 bytes Column::Ref + 8 bytes encoded value
        .max_decode_buffer_length(22)  // INT64_MIN is 20 digits + sign + NUL
        .from_string<&stored_int_from_string>()
        .to_string<&stored_int_to_string>()
        .compare<&stored_int_compare>()
        .intrinsic_default_str("0")
        .build();

static constexpr auto kStoredIntStorage =
    make_column_store<StoredIntCtx>(STORED_INT)
        .create<&stored_int_create>()
        .drop<&stored_int_drop>()
        .load<&stored_int_load>()
        .insert<&stored_int_insert>()
        .select<&stored_int_select>()
        .mark_delete<&stored_int_mark_delete>()
        .purge<&stored_int_purge>()
        .build();

static auto STORAGE = StorageCapability{};
static auto COLUMN_STORE =
    ColumnStoreCapability().column_store(kStoredIntStorage);

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension().with(STORAGE).with(COLUMN_STORE).type(STORED_INT))
