/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

// =============================================================================
// PREVIEW CAPABILITY — UNSTABLE API
// =============================================================================
// This header is part of the VEF preview surface. Its API and ABI may change
// or be removed without notice. See villagesql/preview/README.md for details.
// =============================================================================

// Interface to InnoDB for extensions.

#ifndef VILLAGESQL_PREVIEW_STORAGE_API_H_
#define VILLAGESQL_PREVIEW_STORAGE_API_H_

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <new>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <villagesql/abi/preview/storage.h>

static_assert(VEF_STORAGE_SE_INTF_VERSION == 1,
              "This C++ wrapper supports ABI v1 only");

namespace vsql::preview_storage {

// Error codes returned by storage ABI functions.
enum class Error {
  SUCCESS = VEF_STORAGE_SUCCESS,
  GENERAL = VEF_STORAGE_ERROR_GENERAL,
  OUT_OF_MEMORY = VEF_STORAGE_ERROR_OUT_OF_MEMORY,
  OUT_OF_SPACE = VEF_STORAGE_ERROR_OUT_OF_SPACE,
  INVALID_ARGUMENT = VEF_STORAGE_ERROR_INVALID_ARGUMENT,
  PAGE_LOAD = VEF_STORAGE_ERROR_PAGE_LOAD,
};

// Implementation details — not part of the public API; do not use directly.
namespace detail {
inline constexpr size_t ERROR_MSG_SIZE = 512;
inline thread_local char tl_error_msg[ERROR_MSG_SIZE] = {};

// TODO(villagesql-indexing): remove this global pointer, instead passing the
// capability in the API.
// Module-level vtable pointer. The server writes it via vtable_dest during
// extension registration
// (CapabilityTraits<StorageCapability>::vtable_destination returns &g_abi). The
// inline implementations below call through this pointer so that extensions
// that don't use storage never emit references to vef_storage_* symbols at link
// time.
inline const vef_preview_storage_t *g_abi = nullptr;
}  // namespace detail

// Returns a string_view over the error message from the last failed call on
// this thread, whether from the storage ABI or a local pre-condition check.
// Empty if the last call succeeded or provided no message. Valid until the
// next storage call on this thread.
inline std::string_view last_error() { return detail::tl_error_msg; }

// Typical usage sequence (error checks omitted for brevity):
//
//   Creating storage (DDL, called once per column):
//     Segment::PageRef root_page_num;
//     Segment::create(space, num_segments, trx, root_page_num);
//     // Persist root_page_num in column metadata for later DML use.
//
//   Reading data (DML read path):
//     MtrCtx mtr;
//     MtrCtx::Ref mtr_ref = mtr.start();
//     Page page;
//     page.load(space, data_page_num, Page::Latch::SHARED, mtr_ref);
//     uint32_t value = page.read_integer_4(Page::HEADER_SIZE + data_offset);
//     mtr.commit();  // releases all page latches held by mtr
//
//   Writing data (DML write path):
//     MtrCtx mtr;
//     MtrCtx::Ref mtr_ref = mtr.start();
//     Page root;
//     root.load(space, root_page_num, Page::Latch::EXCLUSIVE, mtr_ref);
//     Segment::Ref seg = Segment::get_header(root, 0);
//     Page data_page;
//     data_page.load_new(seg, mtr_ref);  // allocates a fresh page
//     data_page.write_integer_4(Page::HEADER_SIZE + data_offset, value,
//                               mtr_ref);
//     mtr.commit();  // persists redo log and releases latches
//
//   Dropping storage (DDL, called once on column drop):
//     Segment::drop(space, trx, root_page_num);

// Mini-transaction that controls concurrent access, latching, and WAL
// persistence for a set of pages. Start one with ::start(), perform reads and
// writes under its scope, then call ::commit() to persist the redo log and
// release all page latches acquired under this mtr. MtrCtx objects are not
// copyable or movable; allocate them on the stack for each operation.
class MtrCtx {
 public:
  using Ref = vef_storage_mtr_ref_t;

  [[nodiscard]] Ref start();
  void commit();
  ~MtrCtx();

  MtrCtx() = default;

  MtrCtx(const MtrCtx &) = delete;
  MtrCtx &operator=(const MtrCtx &) = delete;

  MtrCtx(MtrCtx &&) = delete;
  MtrCtx &operator=(MtrCtx &&) = delete;

 private:
  static constexpr size_t MAX_STACK_SIZE = 1536;

  alignas(std::max_align_t) unsigned char m_buf[MAX_STACK_SIZE];
  unsigned char *m_heap_buf = nullptr;
  size_t m_heap_size = 0;

  size_t m_align = alignof(std::max_align_t);
  Ref m_ref = nullptr;
};

// InnoDB tablespace that stores physical data on disk as fixed-size pages.
// A Space::Ref is supplied by VEF when it calls the extension's storage
// interface methods (e.g. column storage operations). The Space object
// itself is opaque; only its Ref type is used directly.
class Space {
 public:
  using Ref = vef_storage_space_ref_t;

  Space() = delete;
};

class Page;

// Storage allocation unit within a tablespace. A segment acquires individual
// pages and groups of pages (extents) from the tablespace as it grows; these
// are returned to the tablespace when the segment is dropped. Segments are
// created once via ::create(), which returns a root page number that must be
// persisted in column metadata. Segment headers live at the start of the root
// page and are accessed via ::get_header() to allocate new pages.
class Segment {
 public:
  // Opaque segment header reference. Obtained from ::get_header() and passed
  // to Page::load_new(); do not dereference or perform arithmetic on it.
  using Ref = void *;

  // SE Transaction ID
  using TrxRef = vef_storage_trx_ref_t;

  // Page number
  using PageRef = vef_storage_page_num_t;

  // One byte for number of segments created in the root page. See the root
  // page data format documented below in Page.
  static constexpr uint16_t NUM_SEGMENTS_SIZE =
      VEF_STORAGE_SEGMENT_NUM_SEGMENTS_SIZE;

  // Size in bytes of each segment header.
  static constexpr uint16_t HEADER_SIZE = VEF_STORAGE_SEGMENT_HEADER_SIZE;

  // Create storage segments within the tablespace, returning the root page
  // reference. The root page contains the header for all segments created.
  // @param space - Tablespace reference
  // @param num_segments - Number of segments to create
  // @param trx_ref - Transaction reference for DDL logging
  // @param root_page_ref - Output parameter to receive the root page reference
  // @return Error::SUCCESS on success, other Error code on failure
  [[nodiscard]] static Error create(Space::Ref space, uint8_t num_segments,
                                    TrxRef trx_ref, PageRef &root_page_ref);

  // Get a segment reference from number (0 to N-1) where N is the number of
  // segments created earlier by create. Requires the root page to be latched
  // exclusively — asserts in debug builds if not.
  static Ref get_header(Page &root_page, size_t seg_no);

  // Drop the storage segments associated with the root page.
  // @param space - Tablespace reference
  // @param trx_ref - Transaction reference for DDL logging
  // @param root_page_ref - Root page reference containing segment headers
  // @return Error::SUCCESS on success, other Error code on failure
  [[nodiscard]] static Error drop(Space::Ref space, TrxRef trx_ref,
                                  PageRef root_page_ref);

  Segment() = delete;
};

// Smallest unit of storage within a tablespace, accessed as an in-memory
// buffer from the InnoDB buffer pool. A page must be loaded (::load() or
// ::load_new()) under a MtrCtx before it can be read or written.
//
// A Page object has no destructor — it does not release its latch on
// destruction. Latches are released either explicitly via ::release() or
// automatically when the associated MtrCtx is committed. A Page object must
// not be used after the MtrCtx it was loaded with has been committed. However,
// a Page object may be reused by loading it again via ::load() or
// ::load_new() with the same MtrCtx or a new one.
class Page {
 public:
  Page() = default;

  // Offset within a page.
  using Offset = vef_storage_page_offset_t;

  // Page number within a tablespace.
  using Ref = Segment::PageRef;

  // Invalid reference.
  static constexpr uint32_t INVALID_REF = VEF_STORAGE_PAGE_NUM_INVALID;

  // Page latch types.
  enum class Latch {
    NO_LATCH = VEF_STORAGE_PAGE_LATCH_NONE,
    SHARED = VEF_STORAGE_PAGE_LATCH_SHARED,
    SHARED_EXCLUSIVE = VEF_STORAGE_PAGE_LATCH_SHARED_EXCLUSIVE,
    EXCLUSIVE = VEF_STORAGE_PAGE_LATCH_EXCLUSIVE
  };

  // Page header and trailer size that should not be overwritten.
  static constexpr uint16_t HEADER_SIZE = VEF_STORAGE_PAGE_HEADER_SIZE;
  static constexpr uint16_t TRAILER_SIZE = VEF_STORAGE_PAGE_TRAILER_SIZE;

  // General Page format:
  // Name:       |-----Header-----|-----------DATA-------------|--Trailer--|
  // Size(bytes):|-------38-------|-------16338(16k-38-8)------|-----8-----|

  // Total page size in bytes can be obtained by ::get_size(). InnoDB
  // currently supports 4k, 8k, 16k, 32k, 64k page size which is decided
  // at the time of DB creation (--initialize) and cannot be changed later.
  // The default page size is 16k. Extension authors are free to use all the
  // "DATA" portion of the page except for the root page explained next.

  // Root Page data format: [DATA] part from above page format (16338 bytes)
  // Extension authors could use multiple segments to store data tracked in the
  // root page. The Segment headers are always placed in the beginning of the
  // root page and should not be overwritten.
  // Name:        |N|-seg-1-|-seg-2-| . . . |-seg-N-|-------- DATA----------|
  // Size(bytes): |1|--10---|--10---| . . . |--10---|--16338 - (1 + 10*N)---|

  // Get page reference ID.
  Ref get_ref() const;

  // Get pointer to the start of the page frame (includes the page header).
  // User DATA starts at offset Page::HEADER_SIZE within this pointer.
  const unsigned char *get_data() const;

  // @return true, if the page is loaded
  // If latch type is provided, also verifies the page is latched with that type
  bool is_loaded() const;
  bool is_loaded(Latch latch) const;

  // Get logical page size for the tablespace.
  static uint32_t get_size(Space::Ref space);

  // Get the user-accessible DATA area size for this page (excludes the page
  // header and trailer). Extension authors should use this to validate that
  // their data fits within the page before reading or writing.
  // Requires the page to be loaded — asserts and returns 0 if not.
  uint32_t data_size() const;

  // Read number of segments from root page.
  uint8_t read_num_segments() const;

  // Read previous page link.
  Ref read_prev_link() const;

  // Read next page link.
  Ref read_next_link() const;

  // Read previous and next page links.
  void read_links(Ref &prev_page, Ref &next_page) const;

  // Write previous page link.
  void write_prev_link(Ref prev_page, MtrCtx::Ref mtr);

  // Write next page link.
  void write_next_link(Ref next_page, MtrCtx::Ref mtr);

  // Write previous and next page links.
  void write_links(Ref prev_page, Ref next_page, MtrCtx::Ref mtr);

  // Load a page from InnoDB buffer pool latched by mtr.
  [[nodiscard]] Error load(Space::Ref space, Ref page, Latch latch,
                           MtrCtx::Ref mtr);

  // Allocate a new page from the segment and latch it before returning.
  [[nodiscard]] Error load_new(Segment::Ref segment_header, MtrCtx::Ref mtr);

  // Upgrade a page to EXCLUSIVE or SHARED_EXCLUSIVE latch. Only valid when the
  // page was loaded with NO_LATCH; upgrading an already-latched page is not
  // supported. SHARED is rejected because InnoDB does not allow upgrading a
  // NO_LATCH page to SHARED — only stronger write-oriented latches are
  // permitted via this interface.
  // @param latch - In:  requested latch mode; must be EXCLUSIVE or
  //                     SHARED_EXCLUSIVE.
  //                Out: on Error::INVALID_ARGUMENT, overwritten with the
  //                     current latch state so the caller can inspect why
  //                     the request was rejected.
  // @return Error::SUCCESS if latched, Error::INVALID_ARGUMENT if a
  //         pre-condition was not met, or another Error code on ABI failure.
  [[nodiscard]] Error latch(Latch &latch, MtrCtx::Ref mtr);

  // Explicitly release the current latch if it was acquired by the extension
  // via the ::load or ::latch interface. Page latches are normally released
  // automatically when the mini-transaction commits.
  // @return Error::SUCCESS if released, Error::INVALID_ARGUMENT if:
  //   - the page is not loaded (programming error — asserts in debug builds),
  //   - the page position was not stored (e.g. pages allocated via ::load_new
  //     are released on mtr commit, not via this interface).
  // Other Error codes indicate an ABI failure (e.g. page was not latched by
  // the extension).
  [[nodiscard]] Error release(MtrCtx::Ref mtr);

  // Write integer and string values at a DATA area offset with redo logging
  // (WAL). Asserts and returns without writing if the page is not loaded, if
  // offset is within the page header (< HEADER_SIZE), or if
  // [offset, offset+size) exceeds the DATA area. Use write_prev_link /
  // write_next_link to update header link fields.
  void write_integer_1(Offset offset, uint8_t value, MtrCtx::Ref mtr);
  void write_integer_2(Offset offset, uint16_t value, MtrCtx::Ref mtr);
  void write_integer_4(Offset offset, uint32_t value, MtrCtx::Ref mtr);
  void write_integer_8(Offset offset, uint64_t value, MtrCtx::Ref mtr);
  void write_string(Offset offset, const unsigned char *str, size_t len,
                    MtrCtx::Ref mtr);

  // Read integer values at a DATA area offset. Asserts and returns 0 if the
  // page is not loaded, if offset is within the page header (< HEADER_SIZE),
  // or if [offset, offset+size) exceeds the DATA area. Use read_prev_link /
  // read_next_link to read header link fields.
  uint8_t read_integer_1(Offset offset) const;
  uint16_t read_integer_2(Offset offset) const;
  uint32_t read_integer_4(Offset offset) const;
  uint64_t read_integer_8(Offset offset) const;

  // There is no read_string counterpart to write_string. To read back a byte
  // string written at a known offset, use get_data() + offset directly.

  Page(const Page &) = delete;
  Page &operator=(const Page &) = delete;

  Page(Page &&) = delete;
  Page &operator=(Page &&) = delete;

 private:
  friend class Segment;

  // Mutable access for internal use by Segment::get_header() only.
  // Named distinctly from the public const get_data() so that calling
  // get_data() on a non-const Page always resolves to the public overload.
  unsigned char *get_mutable_data();

  using BlockRef = vef_storage_block_ref_t;

  static constexpr uint64_t INVALID_POS = VEF_STORAGE_PAGE_POS_INVALID;

  // Reset all members to their default/invalid state.
  void reset();

  // Verify page is loaded and [offset, offset+size) lies within the DATA area
  // (HEADER_SIZE <= offset and offset+size <= m_data_size - TRAILER_SIZE).
  // Used by public read_integer_* / write_integer_* / write_string to prevent
  // extension code from directly accessing header or trailer fields.
  // Asserts on failure so misuse is caught in debug builds.
  bool data_bounds_check(Offset offset, size_t size) const;

  // Read integer value at an offset.
  template <typename T>
  T read_integer(Offset offset) const;

  // Helpers for reading big-endian integers from a raw byte pointer.
  static uint8_t mach_read_from_1(const unsigned char *b);
  static uint16_t mach_read_from_2(const unsigned char *b);
  static uint32_t mach_read_from_4(const unsigned char *b);
  static uint64_t mach_read_from_8(const unsigned char *b);

  // Buffer pool block information. Initialized while loading a page.
  Ref m_ref = INVALID_REF;
  BlockRef m_block = nullptr;
  unsigned char *m_data = nullptr;
  uint32_t m_data_size = 0;

  // Position of page within mtr for releasing and latching at a later point.
  uint64_t m_position = INVALID_POS;

  // Current latch, if loaded.
  Latch m_latch = Latch::NO_LATCH;
};

// MtrCtx inline implementations
inline MtrCtx::Ref MtrCtx::start() {
  assert(m_ref == nullptr);
  if (m_ref != nullptr) {
    snprintf(detail::tl_error_msg, detail::ERROR_MSG_SIZE,
             "start: mtr is already started");
    return nullptr;
  }
  uint32_t required_size = 0;
  uint32_t required_alignment = 0;

  m_ref = detail::g_abi->mtr_start(m_buf, MAX_STACK_SIZE, &required_size,
                                   &required_alignment, detail::tl_error_msg,
                                   detail::ERROR_MSG_SIZE);
  if (m_ref != nullptr) {
    return m_ref;
  }

  size_t align = static_cast<size_t>(required_alignment);
  if (align < alignof(std::max_align_t)) {
    align = alignof(std::max_align_t);
  }

  if (!m_heap_buf || required_size > m_heap_size || align != m_align) {
    if (m_heap_buf) {
      ::operator delete(m_heap_buf, std::align_val_t(m_align));
    }
    m_align = align;
    m_heap_size = required_size;
    m_heap_buf = static_cast<unsigned char *>(
        ::operator new(m_heap_size, std::align_val_t(m_align), std::nothrow));
    if (m_heap_buf == nullptr) {
      m_heap_size = 0;
      snprintf(detail::tl_error_msg, detail::ERROR_MSG_SIZE,
               "start: out of memory allocating mtr buffer (%zu bytes)",
               static_cast<size_t>(required_size));
      return nullptr;
    }
  }
  m_ref = detail::g_abi->mtr_start(m_heap_buf, m_heap_size, &required_size,
                                   &required_alignment, detail::tl_error_msg,
                                   detail::ERROR_MSG_SIZE);
  return m_ref;
}

inline void MtrCtx::commit() {
  if (m_ref != nullptr) {
    detail::g_abi->mtr_commit(m_ref);
    m_ref = nullptr;
  }
}

inline MtrCtx::~MtrCtx() {
  commit();
  if (m_heap_buf) {
    ::operator delete(m_heap_buf, std::align_val_t(m_align));
  }
}

// Segment inline implementations
inline Error Segment::create(Space::Ref space, uint8_t num_segments,
                             TrxRef trx_ref, PageRef &root_page_ref) {
  return static_cast<Error>(detail::g_abi->segment_create(
      space, num_segments, trx_ref, &root_page_ref, detail::tl_error_msg,
      detail::ERROR_MSG_SIZE));
}

inline Segment::Ref Segment::get_header(Page &root_page, size_t seg_no) {
  assert(root_page.is_loaded(Page::Latch::EXCLUSIVE));
  if (!root_page.is_loaded()) return nullptr;

  uint8_t num_segments = root_page.read_num_segments();
  assert(seg_no < num_segments);
  if (seg_no >= num_segments) return nullptr;

  Page::Offset seg_offset = static_cast<Page::Offset>(
      Page::HEADER_SIZE + NUM_SEGMENTS_SIZE + seg_no * Segment::HEADER_SIZE);
  assert(static_cast<uint64_t>(seg_offset - Page::HEADER_SIZE) + HEADER_SIZE <=
         root_page.data_size());
  if (static_cast<uint64_t>(seg_offset - Page::HEADER_SIZE) + HEADER_SIZE >
      root_page.data_size())
    return nullptr;
  return static_cast<Ref>(root_page.get_mutable_data() + seg_offset);
}

inline Error Segment::drop(Space::Ref space, TrxRef trx_ref,
                           PageRef root_page_ref) {
  return static_cast<Error>(detail::g_abi->segment_drop(
      space, trx_ref, root_page_ref, detail::tl_error_msg,
      detail::ERROR_MSG_SIZE));
}

// Page inline implementations
inline Page::Ref Page::get_ref() const { return m_ref; }

inline unsigned char *Page::get_mutable_data() { return m_data; }

inline const unsigned char *Page::get_data() const { return m_data; }

inline bool Page::is_loaded() const { return m_block != nullptr; }

inline bool Page::is_loaded(Latch latch) const {
  if (!is_loaded()) return false;
  return m_latch == latch;
}

inline uint32_t Page::get_size(Space::Ref space) {
  return detail::g_abi->page_get_size(space);
}

inline uint32_t Page::data_size() const {
  assert(is_loaded());
  if (!is_loaded()) return 0;
  return m_data_size - HEADER_SIZE - TRAILER_SIZE;
}

inline uint8_t Page::read_num_segments() const {
  return read_integer_1(Page::HEADER_SIZE);
}

inline Page::Ref Page::read_prev_link() const {
  if (!is_loaded()) return INVALID_REF;
  return mach_read_from_4(m_data + VEF_STORAGE_FIL_PAGE_PREV);
}

inline Page::Ref Page::read_next_link() const {
  if (!is_loaded()) return INVALID_REF;
  return mach_read_from_4(m_data + VEF_STORAGE_FIL_PAGE_NEXT);
}

inline void Page::read_links(Ref &prev_page, Ref &next_page) const {
  prev_page = read_prev_link();
  next_page = read_next_link();
}

inline void Page::write_prev_link(Ref prev_page, MtrCtx::Ref mtr) {
  assert(is_loaded());
  if (!is_loaded()) return;
  detail::g_abi->page_write_integer(m_block, VEF_STORAGE_FIL_PAGE_PREV,
                                    prev_page, VEF_STORAGE_PAGE_INT_4BYTES,
                                    mtr);
}

inline void Page::write_next_link(Ref next_page, MtrCtx::Ref mtr) {
  assert(is_loaded());
  if (!is_loaded()) return;
  detail::g_abi->page_write_integer(m_block, VEF_STORAGE_FIL_PAGE_NEXT,
                                    next_page, VEF_STORAGE_PAGE_INT_4BYTES,
                                    mtr);
}

inline void Page::write_links(Ref prev_page, Ref next_page, MtrCtx::Ref mtr) {
  write_prev_link(prev_page, mtr);
  write_next_link(next_page, mtr);
}

inline void Page::reset() {
  m_block = nullptr;
  m_position = INVALID_POS;
  m_ref = INVALID_REF;
  m_data = nullptr;
  m_data_size = 0;
  m_latch = Latch::NO_LATCH;
}

inline bool Page::data_bounds_check(Offset offset, size_t size) const {
  if (!is_loaded() || offset < HEADER_SIZE ||
      static_cast<uint64_t>(offset) + size > m_data_size - TRAILER_SIZE) {
    assert(false);
    return false;
  }
  return true;
}

inline Error Page::load(Space::Ref space, Ref page, Latch latch,
                        MtrCtx::Ref mtr) {
  reset();
  auto err = static_cast<Error>(detail::g_abi->page_load(
      &m_block, &m_position, &m_data, &m_data_size, space, page,
      static_cast<vef_storage_latch_t>(latch), mtr, detail::tl_error_msg,
      detail::ERROR_MSG_SIZE));
  if (err != Error::SUCCESS) {
    reset();
    return err;
  }
  m_latch = latch;
  m_ref = page;
  return err;
}

inline Error Page::load_new(Segment::Ref segment_header, MtrCtx::Ref mtr) {
  reset();
  auto err = static_cast<Error>(detail::g_abi->page_allocate_and_load(
      &m_block, &m_ref, &m_data, &m_data_size,
      static_cast<unsigned char *>(segment_header), mtr, detail::tl_error_msg,
      detail::ERROR_MSG_SIZE));
  if (err != Error::SUCCESS) {
    reset();
    return err;
  }
  m_latch = Latch::EXCLUSIVE;
  // m_position remains INVALID_POS (set by reset()): newly allocated pages
  // have no stored position and are released automatically on mtr commit.
  return err;
}

inline Error Page::latch(Latch &latch, MtrCtx::Ref mtr) {
  assert(is_loaded());
  if (!is_loaded()) {
    snprintf(detail::tl_error_msg, detail::ERROR_MSG_SIZE,
             "latch: page is not loaded");
    latch = m_latch;
    return Error::INVALID_ARGUMENT;
  }
  if (m_latch != Latch::NO_LATCH) {
    snprintf(detail::tl_error_msg, detail::ERROR_MSG_SIZE,
             "latch: page already holds a latch");
    latch = m_latch;
    return Error::INVALID_ARGUMENT;
  }
  if (m_position == INVALID_POS) {
    snprintf(detail::tl_error_msg, detail::ERROR_MSG_SIZE,
             "latch: page has no stored position; loaded via load_new");
    latch = m_latch;
    return Error::INVALID_ARGUMENT;
  }
  if (latch == Latch::NO_LATCH || latch == Latch::SHARED) {
    snprintf(detail::tl_error_msg, detail::ERROR_MSG_SIZE,
             "latch: requested mode must be EXCLUSIVE or SHARED_EXCLUSIVE");
    latch = m_latch;
    return Error::INVALID_ARGUMENT;
  }

  auto abi_latch = static_cast<vef_storage_latch_t>(latch);
  auto err = static_cast<Error>(
      detail::g_abi->page_latch(m_block, m_position, abi_latch, mtr,
                                detail::tl_error_msg, detail::ERROR_MSG_SIZE));
  if (err == Error::SUCCESS) {
    m_latch = latch;
  }
  return err;
}

inline Error Page::release(MtrCtx::Ref mtr) {
  assert(is_loaded());
  if (!is_loaded()) {
    snprintf(detail::tl_error_msg, detail::ERROR_MSG_SIZE,
             "release: page is not loaded");
    return Error::INVALID_ARGUMENT;
  }
  // Cannot release a page whose position was not stored (e.g. pages allocated
  // via load_new). Such pages are held by the mtr and released on mtr commit.
  if (m_position == INVALID_POS) {
    snprintf(detail::tl_error_msg, detail::ERROR_MSG_SIZE,
             "release: page has no stored position; loaded via load_new, "
             "released automatically on mtr commit");
    return Error::INVALID_ARGUMENT;
  }

  auto err = static_cast<Error>(detail::g_abi->page_release(
      m_block, m_position, mtr, detail::tl_error_msg, detail::ERROR_MSG_SIZE));
  if (err != Error::SUCCESS) return err;

  reset();

  return Error::SUCCESS;
}

inline void Page::write_integer_1(Offset offset, uint8_t value,
                                  MtrCtx::Ref mtr) {
  if (!data_bounds_check(offset, 1)) return;
  detail::g_abi->page_write_integer(m_block, offset, value,
                                    VEF_STORAGE_PAGE_INT_1BYTE, mtr);
}

inline void Page::write_integer_2(Offset offset, uint16_t value,
                                  MtrCtx::Ref mtr) {
  if (!data_bounds_check(offset, 2)) return;
  detail::g_abi->page_write_integer(m_block, offset, value,
                                    VEF_STORAGE_PAGE_INT_2BYTES, mtr);
}

inline void Page::write_integer_4(Offset offset, uint32_t value,
                                  MtrCtx::Ref mtr) {
  if (!data_bounds_check(offset, 4)) return;
  detail::g_abi->page_write_integer(m_block, offset, value,
                                    VEF_STORAGE_PAGE_INT_4BYTES, mtr);
}

inline void Page::write_integer_8(Offset offset, uint64_t value,
                                  MtrCtx::Ref mtr) {
  if (!data_bounds_check(offset, 8)) return;
  detail::g_abi->page_write_integer(m_block, offset, value,
                                    VEF_STORAGE_PAGE_INT_8BYTES, mtr);
}

inline void Page::write_string(Offset offset, const unsigned char *str,
                               size_t len, MtrCtx::Ref mtr) {
  if (!data_bounds_check(offset, len)) return;
  detail::g_abi->page_write_string(m_block, offset, str, len, mtr);
}

template <typename T>
inline T Page::read_integer(Offset offset) const {
  static_assert(
      sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
      "read_integer only supports 1, 2, 4, or 8 byte integer types");

  if (!data_bounds_check(offset, sizeof(T))) return 0;

  const unsigned char *loc = get_data() + offset;

  if constexpr (sizeof(T) == 1) return mach_read_from_1(loc);
  if constexpr (sizeof(T) == 2) return mach_read_from_2(loc);
  if constexpr (sizeof(T) == 4) return mach_read_from_4(loc);
  if constexpr (sizeof(T) == 8) return mach_read_from_8(loc);

  // Unreachable due to static_assert, but needed for compiler
  return 0;
}

inline uint8_t Page::mach_read_from_1(const unsigned char *b) {
  return static_cast<uint8_t>(b[0]);
}

inline uint16_t Page::mach_read_from_2(const unsigned char *b) {
  return (static_cast<uint16_t>(b[0]) << 8) | static_cast<uint16_t>(b[1]);
}

inline uint32_t Page::mach_read_from_4(const unsigned char *b) {
  return (static_cast<uint32_t>(b[0]) << 24) |
         (static_cast<uint32_t>(b[1]) << 16) |
         (static_cast<uint32_t>(b[2]) << 8) | static_cast<uint32_t>(b[3]);
}

inline uint64_t Page::mach_read_from_8(const unsigned char *b) {
  uint64_t high = mach_read_from_4(b);
  uint64_t low = mach_read_from_4(b + 4);
  return (high << 32) | low;
}

inline uint8_t Page::read_integer_1(Offset o) const {
  return read_integer<uint8_t>(o);
}
inline uint16_t Page::read_integer_2(Offset o) const {
  return read_integer<uint16_t>(o);
}
inline uint32_t Page::read_integer_4(Offset o) const {
  return read_integer<uint32_t>(o);
}
inline uint64_t Page::read_integer_8(Offset o) const {
  return read_integer<uint64_t>(o);
}

// General-purpose allocator backed by the InnoDB extension memory arena.
// All allocations are freed at once when InnoDB reclaims the arena
// (e.g., when a column is dropped). Callers must NOT free individual
// allocations manually. Objects with non-trivial destructors constructed via
// construct<T>() are registered and called automatically in reverse
// construction order when the Arena is destroyed.
class Arena {
 public:
  Arena() = default;
  Arena(vef_storage_arena_t *handle, vef_storage_arena_func_t func)
      : m_handle(handle), m_func(func) {}

  ~Arena() {
    for (auto it = m_destructors.rbegin(); it != m_destructors.rend(); ++it)
      it->fn(it->obj);
  }

  Arena(const Arena &) = delete;
  Arena &operator=(const Arena &) = delete;

  // Allocate memory for T, construct it with args, and return a pointer.
  // Handles alignment: if alignof(T) exceeds the arena's minimum guarantee,
  // over-allocates and adjusts the pointer. If T has a non-trivial destructor,
  // it is registered and called automatically when the Arena is destroyed.
  // Returns nullptr on allocation failure or if T's constructor throws.
  template <typename T, typename... Args>
  T *construct(Args &&...args) {
    void *mem = allocate_aligned(sizeof(T), alignof(T));
    if (mem == nullptr) return nullptr;
    T *obj;
    try {
      obj = new (mem) T(std::forward<Args>(args)...);
    } catch (...) {
      return nullptr;
    }
    if constexpr (!std::is_trivially_destructible_v<T>) {
      m_destructors.push_back(
          {[](void *p) { std::destroy_at(static_cast<T *>(p)); }, obj});
    }
    return obj;
  }

 private:
  vef_storage_arena_t *m_handle = nullptr;
  vef_storage_arena_func_t m_func = nullptr;
  struct Dtor {
    void (*fn)(void *);
    void *obj;
  };
  std::vector<Dtor> m_destructors;

  void *allocate_aligned(uint32_t size, uint32_t align) {
    if (align <= VEF_STORAGE_MIN_ALLOCATOR_ALIGNMENT) {
      return m_func(m_handle, size);
    }
    // Over-allocate to guarantee alignment: worst-case waste is (align-1)
    // bytes.
    void *raw = m_func(m_handle, size + align - 1);
    if (raw == nullptr) return nullptr;
    uintptr_t addr = reinterpret_cast<uintptr_t>(raw);
    return reinterpret_cast<void *>((addr + align - 1) &
                                    ~(uintptr_t{align - 1}));
  }
};

namespace detail {

// Allocates storage for the Arena object itself out of the extension memory
// arena, rather than the process heap. The object's storage is reclaimed
// together with the rest of the extension arena.
//
// TODO(villagesql-indexing): Add an unload hook so ~Arena() is invoked even
// when the extension arena is reclaimed without explicitly destroying the
// Arena object (e.g. dict cache eviction, table close, or server shutdown).
inline void *AllocateArenaStorage(vef_storage_arena_t *arena_ctx,
                                  vef_storage_arena_func_t arena_alloc) {
  // Arena's alignment is already covered by the allocator's minimum guarantee.
  static_assert(alignof(Arena) <= VEF_STORAGE_MIN_ALLOCATOR_ALIGNMENT);
  return arena_alloc(arena_ctx, sizeof(Arena));
}

}  // namespace detail

// StorageCtx<T> wraps the persistent storage reference and a user context of
// type T allocated from the InnoDB arena. The storage builder wrappers
// construct it before calling the extension and delete the Arena on column
// drop, which automatically calls ~T(). Extensions receive a ready-to-use
// StorageCtx<T>* with user() already populated and must not free any of this
// memory manually.
template <typename T>
class StorageCtx {
 public:
  explicit StorageCtx(Arena *arena) : m_arena(arena) {
    verify_layout();
    m_user = m_arena->construct<T>();
  }

  T *user() { return m_user; }
  const T *user() const { return m_user; }

  // Returns the arena for additional allocations beyond the user context.
  Arena &arena() { return *m_arena; }

  void set_ref(vef_storage_ref_t ref) { m_ctx.ref = ref; }
  vef_storage_ref_t get_ref() const { return m_ctx.ref; }

 private:
  vef_storage_ctx_t m_ctx{};
  // Raw pointer to the owning Arena.
  // - Cannot use std::unique_ptr: StorageCtx<T> must remain standard-layout
  //   for ABI compatibility (it is cast to vef_storage_ctx_t).
  // - The Arena itself is heap-allocated and owned by the storage_builder
  //   wrappers (Create/Load/Drop).
  Arena *m_arena = nullptr;
  T *m_user = nullptr;

  // Verify ABI layout requirements. Called from the constructor so these fire
  // when StorageCtx<T> is first instantiated with a concrete type.
  static void verify_layout() {
    static_assert(std::is_standard_layout_v<StorageCtx>,
                  "StorageCtx<T> must be standard layout for ABI "
                  "cast — check that no non-standard-layout member was added");
    static_assert(offsetof(StorageCtx, m_ctx) == 0,
                  "StorageCtx<T> must begin with m_ctx for ABI cast");
  }
};

// Type aliases and types for the Custom Type Storage Interface. These appear
// in vef_type_storage_intf_t function signatures and are used by extensions
// to implement column storage.
struct Column {
  // Column-level references and data buffers.
  using Ref = vef_storage_col_ref_t;
  using Data = vef_storage_col_data_t;
  using StorageRef = vef_storage_ref_t;

  static constexpr Ref EMPTY_REF = VEF_STORAGE_EMPTY_COLUMN_REF;

  template <typename T>
  using StorageCtx = vsql::preview_storage::StorageCtx<T>;
};

}  // namespace vsql::preview_storage

#endif  // VILLAGESQL_PREVIEW_STORAGE_API_H_
