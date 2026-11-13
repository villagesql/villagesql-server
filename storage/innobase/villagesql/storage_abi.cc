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

// C ABI implementation for InnoDB storage interface

#include <cstdio>
#include <iterator>
#include <new>

#include "buf0buf.h"
#include "fil0types.h"
#include "fsp0fsp.h"
#include "log0chkp.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "page0page.h"
#include "ut0dbg.h"

#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"

// Lookup table mapping ABI latch types (VEF_STORAGE_PAGE_LATCH_*)
// to corresponding InnoDB RW latch modes. ABI latch values are
// contiguous integers starting at 0.
static constexpr std::array<ulint, 4> kInnodbLatch{
    RW_NO_LATCH,  // VEF_STORAGE_PAGE_LATCH_NONE
    RW_S_LATCH,   // VEF_STORAGE_PAGE_LATCH_SHARED
    RW_SX_LATCH,  // VEF_STORAGE_PAGE_LATCH_SHARED_EXCLUSIVE
    RW_X_LATCH    // VEF_STORAGE_PAGE_LATCH_EXCLUSIVE
};

static ulint abi_to_innodb_latch(vef_storage_latch_t latch) {
  const auto idx = static_cast<size_t>(latch);
  if (latch < 0 || idx >= kInnodbLatch.size()) {
    return RW_NO_LATCH;
  }
  return kInnodbLatch[idx];
}

// Mini-Transaction Interface Implementation

extern "C" vef_storage_mtr_ref_t vef_storage_mtr_start(
    void *buffer, uint32_t buffer_size, uint32_t *required_size,
    uint32_t *required_alignment, char *error_msg, uint32_t error_msg_len) {
  constexpr uint32_t mtr_size = sizeof(mtr_t);
  constexpr uint32_t mtr_alignment = alignof(mtr_t);

  if (required_size != nullptr) {
    *required_size = mtr_size;
  }

  if (required_alignment != nullptr) {
    *required_alignment = mtr_alignment;
  }

  if (buffer == nullptr) {
    return nullptr;
  }

  if (buffer_size < mtr_size) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len,
               "Buffer too small: need %u bytes, got %u", mtr_size,
               buffer_size);
    }
    return nullptr;
  }

  // Check alignment
  static_assert((mtr_alignment & (mtr_alignment - 1)) == 0,
                "mtr_alignment must be power of two");

  auto addr = reinterpret_cast<std::uintptr_t>(buffer);
  if ((addr & (mtr_alignment - 1)) != 0) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len,
               "Buffer misaligned: need %u-byte alignment", mtr_alignment);
    }
    return nullptr;
  }

  // Placement new to construct mtr_t in the provided buffer
  mtr_t *mtr = new (buffer) mtr_t();
  mtr->start();

  return static_cast<vef_storage_mtr_ref_t>(mtr);
}

extern "C" void vef_storage_mtr_commit(vef_storage_mtr_ref_t ref) {
  if (ref == nullptr) {
    return;
  }

  mtr_t *mtr = static_cast<mtr_t *>(ref);
  mtr->commit();
  mtr->~mtr_t();
}

// Storage Segment Interface Implementation

extern "C" int vef_storage_segment_create(
    vef_storage_space_ref_t space_ref, uint8_t num_segments,
    vef_storage_trx_ref_t trx_ref, vef_storage_page_num_t *root_page_num_p,
    char *error_msg, uint32_t error_msg_len) {
  if (root_page_num_p == nullptr || num_segments == 0) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len, "Invalid argument: %s",
               root_page_num_p == nullptr ? "root_page_num_p is null"
                                          : "num_segments is 0");
    }
    return VEF_STORAGE_ERROR_INVALID_ARGUMENT;
  }
  *root_page_num_p = VEF_STORAGE_PAGE_NUM_INVALID;
  uint8_t num_allocated = 0;

  // Create first segment
  log_free_check();

  mtr_t mtr;
  mtr.start();

  // First segment: allocate root page
  uint16_t seg_offset =
      VEF_STORAGE_PAGE_HEADER_SIZE + VEF_STORAGE_SEGMENT_NUM_SEGMENTS_SIZE;
  buf_block_t *block = fseg_create(space_ref, 0, seg_offset, &mtr);

  if (block == nullptr) {
    ib::warn(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: First Segment allocation failed.";
    mtr.commit();
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len, "First segment allocation failed");
    }
    return VEF_STORAGE_ERROR_OUT_OF_SPACE;
  }

  vef_storage_page_num_t root_page_num = block->get_page_no();
  *root_page_num_p = root_page_num;

  unsigned char *page_data = buf_block_get_frame(block);
  // Write number of segments
  num_allocated = 1;
  mlog_write_ulint(page_data + VEF_STORAGE_PAGE_HEADER_SIZE, num_allocated,
                   MLOG_1BYTE, &mtr);
  mtr.commit();

  // Create remaining segments
  while (num_allocated < num_segments) {
    log_free_check();
    mtr.start();

    seg_offset = VEF_STORAGE_PAGE_HEADER_SIZE +
                 VEF_STORAGE_SEGMENT_NUM_SEGMENTS_SIZE +
                 num_allocated * VEF_STORAGE_SEGMENT_HEADER_SIZE;

    block = fseg_create(space_ref, root_page_num, seg_offset, &mtr);

    if (block == nullptr) {
      ib::warn(ER_VILLAGESQL_GENERIC_MESSAGE)
          << "InnoDB: Segment allocation failed after allocating "
          << num_allocated << " of " << num_segments << " segments.";
      mtr.commit();
      if (error_msg != nullptr && error_msg_len > 0) {
        snprintf(error_msg, error_msg_len,
                 "Segment allocation failed after %u of %u segments",
                 num_allocated, num_segments);
      }
      // TODO(villagesql-indexing): Remove after DDL logging is implemented.
      // This is best effort to clean the segments and won't work if server
      // crashes before or while performing this step. The segments would leak
      // in worst case.
      (void)vef_storage_segment_drop(space_ref, trx_ref, root_page_num, nullptr,
                                     0);
      return VEF_STORAGE_ERROR_OUT_OF_SPACE;
    }
    num_allocated++;
    page_data = buf_block_get_frame(block);
    mlog_write_ulint(page_data + VEF_STORAGE_PAGE_HEADER_SIZE, num_allocated,
                     MLOG_1BYTE, &mtr);
    mtr.commit();
  }
  return VEF_STORAGE_SUCCESS;
}

// Free all pages of one segment, reloading the root page between mtr restarts,
// then free the segment header. Leaves mtr active on return.
static int vef_drop_one_segment(vef_storage_space_ref_t space_ref,
                                vef_storage_page_num_t root_page_num,
                                uint8_t seg_no, buf_block_t **root_block_p,
                                mtr_t *mtr) {
  uint16_t seg_offset = VEF_STORAGE_PAGE_HEADER_SIZE +
                        VEF_STORAGE_SEGMENT_NUM_SEGMENTS_SIZE +
                        seg_no * VEF_STORAGE_SEGMENT_HEADER_SIZE;

  unsigned char *page_data = buf_block_get_frame(*root_block_p);
  unsigned char *seg_header = page_data + seg_offset;

  auto free_step = [&]() {
    // Free all pages except the segment header for first segment holding the
    // root page.
    return (seg_no == 0) ? fseg_free_step_not_header(seg_header, false, mtr)
                         : fseg_free_step(seg_header, false, mtr);
  };

  while (!free_step()) {
    // Commit and restart mtr between steps, reloading the root page each time.
    mtr->commit();
    log_free_check();
    mtr->start();

    page_size_t page_size(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE, false);
    page_id_t page_id(space_ref, root_page_num);
    *root_block_p =
        buf_page_get(page_id, page_size, RW_X_LATCH, UT_LOCATION_HERE, mtr);

    if (*root_block_p == nullptr) {
      return VEF_STORAGE_ERROR_PAGE_LOAD;
    }

    // Re-get header pointer after reloading the page
    page_data = buf_block_get_frame(*root_block_p);
    seg_header = page_data + seg_offset;
  }

  // Free the segment header
  if (seg_no == 0)
    while (!fseg_free_step(seg_header, false, mtr));

  return VEF_STORAGE_SUCCESS;
}

extern "C" int vef_storage_segment_drop(vef_storage_space_ref_t space_ref,
                                        vef_storage_trx_ref_t trx_ref,
                                        vef_storage_page_num_t root_page_num,
                                        char *error_msg,
                                        uint32_t error_msg_len) {
  (void)trx_ref;
  log_free_check();

  mtr_t mtr;
  mtr.start();

  page_size_t page_size(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE, false);
  page_id_t page_id(space_ref, root_page_num);

  buf_block_t *block =
      buf_page_get(page_id, page_size, RW_X_LATCH, UT_LOCATION_HERE, &mtr);

  if (block == nullptr) {
    ib::warn(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Segment drop failed while loading root page with page ID: "
        << page_id;
    mtr.commit();
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len,
               "Segment drop failed to load root page %u in space %u",
               root_page_num, space_ref);
    }
    return VEF_STORAGE_ERROR_PAGE_LOAD;
  }

  unsigned char *page_data = buf_block_get_frame(block);
  uint8_t num_segments =
      mach_read_from_1(page_data + VEF_STORAGE_PAGE_HEADER_SIZE);

  if (num_segments == 0) {
    mtr.commit();
    return VEF_STORAGE_SUCCESS;
  }

  auto num_total = num_segments;

  do {
    --num_segments;

    int err = vef_drop_one_segment(space_ref, root_page_num, num_segments,
                                   &block, &mtr);

    if (err != 0) {
      auto num_dropped = num_total - (num_segments + 1);
      ib::warn(ER_VILLAGESQL_GENERIC_MESSAGE)
          << "InnoDB: Segment drop failed after dropping " << num_dropped
          << " of " << static_cast<uint32_t>(num_total) << " segments.";
      mtr.commit();
      if (error_msg != nullptr && error_msg_len > 0) {
        snprintf(error_msg, error_msg_len,
                 "Segment drop failed after dropping %u of %u segments",
                 static_cast<uint32_t>(num_dropped),
                 static_cast<uint32_t>(num_total));
      }
      return VEF_STORAGE_ERROR_PAGE_LOAD;
    }

    page_data = buf_block_get_frame(block);
    mlog_write_ulint(page_data + VEF_STORAGE_PAGE_HEADER_SIZE, num_segments,
                     MLOG_1BYTE, &mtr);
  } while (num_segments > 0);

  mtr.commit();
  return VEF_STORAGE_SUCCESS;
}

// Page Interface Implementation

extern "C" int vef_storage_page_load(
    vef_storage_block_ref_t *block_p, uint64_t *position_p,
    unsigned char **data_p, uint32_t *data_size_p,
    vef_storage_space_ref_t space_ref, vef_storage_page_num_t page_num,
    vef_storage_latch_t latch_mode, vef_storage_mtr_ref_t mtr_ref,
    char *error_msg, uint32_t error_msg_len) {
  if (block_p == nullptr || position_p == nullptr || data_p == nullptr ||
      data_size_p == nullptr || mtr_ref == nullptr) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len, "Invalid argument: null pointer");
    }
    return VEF_STORAGE_ERROR_INVALID_ARGUMENT;
  }

  mtr_t *mtr = static_cast<mtr_t *>(mtr_ref);

  page_size_t page_size(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE, false);
  page_id_t page_id(space_ref, page_num);

  ulint innodb_latch = abi_to_innodb_latch(latch_mode);
  ulint savepoint = mtr->get_savepoint();

  buf_block_t *buf_block =
      buf_page_get(page_id, page_size, innodb_latch, UT_LOCATION_HERE, mtr);

  if (buf_block == nullptr) {
    ib::warn(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Failed to load page for page ID: " << page_id;
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len, "Failed to load page %u in space %u",
               page_num, space_ref);
    }
    return VEF_STORAGE_ERROR_PAGE_LOAD;
  }

  *block_p = static_cast<vef_storage_block_ref_t>(buf_block);
  *position_p = static_cast<uint64_t>(savepoint);
  *data_p = buf_block_get_frame(buf_block);
  *data_size_p = UNIV_PAGE_SIZE;

  return VEF_STORAGE_SUCCESS;
}

extern "C" int vef_storage_page_allocate_and_load(
    vef_storage_block_ref_t *block_p, vef_storage_page_num_t *page_num_p,
    unsigned char **data_p, uint32_t *data_size_p,
    unsigned char *segment_header, vef_storage_mtr_ref_t mtr_ref,
    char *error_msg, uint32_t error_msg_len) {
  if (block_p == nullptr || page_num_p == nullptr || data_p == nullptr ||
      data_size_p == nullptr || segment_header == nullptr ||
      mtr_ref == nullptr) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len, "Invalid argument: null pointer");
    }
    return VEF_STORAGE_ERROR_INVALID_ARGUMENT;
  }

  mtr_t *mtr = static_cast<mtr_t *>(mtr_ref);

  // Allocate in a separate redo-logged mtr, latching/initializing the page in
  // the caller's content mtr, so allocation is crash-recoverable even when the
  // contents are written no-redo. Mirrors Page_load::init in btr0load.cc.
  mtr_t alloc_mtr;
  alloc_mtr.start();
  buf_block_t *buf_block = fseg_alloc_free_page_general(
      segment_header, 0, FSP_NO_DIR, false, &alloc_mtr, mtr);
  alloc_mtr.commit();

  if (buf_block == nullptr) {
    ib::warn(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Failed to allocate page.";
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len, "Failed to allocate page");
    }
    return VEF_STORAGE_ERROR_OUT_OF_SPACE;
  }

  // Set page type
  unsigned char *page_data = buf_block_get_frame(buf_block);
  mlog_write_ulint(page_data + FIL_PAGE_TYPE, FIL_PAGE_TYPE_BLOB, MLOG_2BYTES,
                   mtr);

  *block_p = static_cast<vef_storage_block_ref_t>(buf_block);
  *page_num_p = buf_block->get_page_no();
  *data_p = page_data;
  *data_size_p = UNIV_PAGE_SIZE;

  return VEF_STORAGE_SUCCESS;
}

extern "C" int vef_storage_page_latch(vef_storage_block_ref_t block,
                                      uint64_t position,
                                      vef_storage_latch_t latch,
                                      vef_storage_mtr_ref_t mtr_ref,
                                      char *error_msg, uint32_t error_msg_len) {
  if (block == nullptr || mtr_ref == nullptr) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len, "Invalid argument: %s is null",
               block == nullptr ? "block" : "mtr_ref");
    }
    return VEF_STORAGE_ERROR_INVALID_ARGUMENT;
  }

  mtr_t *mtr = static_cast<mtr_t *>(mtr_ref);
  buf_block_t *buf_block = static_cast<buf_block_t *>(block);

  ulint savepoint = static_cast<ulint>(position);

  if (latch == VEF_STORAGE_PAGE_LATCH_SHARED_EXCLUSIVE) {
    mtr->sx_latch_at_savepoint(savepoint, buf_block);
  } else if (latch == VEF_STORAGE_PAGE_LATCH_EXCLUSIVE) {
    mtr->x_latch_at_savepoint(savepoint, buf_block);
  }
  return VEF_STORAGE_SUCCESS;
}

extern "C" int vef_storage_page_release(vef_storage_block_ref_t block,
                                        uint64_t position,
                                        vef_storage_mtr_ref_t mtr_ref,
                                        char *error_msg,
                                        uint32_t error_msg_len) {
  if (block == nullptr || mtr_ref == nullptr) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len, "Invalid argument: %s is null",
               block == nullptr ? "block" : "mtr_ref");
    }
    return VEF_STORAGE_ERROR_INVALID_ARGUMENT;
  }

  mtr_t *mtr = static_cast<mtr_t *>(mtr_ref);
  buf_block_t *buf_block = static_cast<buf_block_t *>(block);

  ulint savepoint = static_cast<ulint>(position);
  mtr->release_block_at_savepoint(savepoint, buf_block);

  return VEF_STORAGE_SUCCESS;
}

extern "C" uint32_t vef_storage_page_get_size(
    vef_storage_space_ref_t space_ref) {
  (void)space_ref;  // Currently all pages are same size
  return UNIV_PAGE_SIZE;
}

extern "C" void vef_storage_page_write_integer(
    vef_storage_block_ref_t block, vef_storage_page_offset_t offset,
    uint64_t value, vef_storage_integer_bytes_t bytes,
    vef_storage_mtr_ref_t mtr_ref) {
  if (block == nullptr || mtr_ref == nullptr) {
    return;
  }

  buf_block_t *buf_block = static_cast<buf_block_t *>(block);
  unsigned char *page_data = buf_block_get_frame(buf_block);
  mtr_t *mtr = static_cast<mtr_t *>(mtr_ref);

  unsigned char *loc = page_data + offset;
  auto val = static_cast<ulint>(value);

  switch (bytes) {
    case VEF_STORAGE_PAGE_INT_1BYTE:
      mlog_write_ulint(loc, val, MLOG_1BYTE, mtr);
      break;
    case VEF_STORAGE_PAGE_INT_2BYTES:
      mlog_write_ulint(loc, val, MLOG_2BYTES, mtr);
      break;
    case VEF_STORAGE_PAGE_INT_4BYTES:
      mlog_write_ulint(loc, val, MLOG_4BYTES, mtr);
      break;
    case VEF_STORAGE_PAGE_INT_8BYTES:
      mlog_write_ull(loc, val, mtr);
      break;
    default:
      ib::warn(ER_VILLAGESQL_GENERIC_MESSAGE)
          << "InnoDB: Invalid integer type to write: " << val;
  }
}

extern "C" void vef_storage_page_write_string(vef_storage_block_ref_t block,
                                              vef_storage_page_offset_t offset,
                                              const unsigned char *str,
                                              uint32_t len,
                                              vef_storage_mtr_ref_t mtr_ref) {
  if (block == nullptr || str == nullptr || mtr_ref == nullptr) {
    return;
  }

  buf_block_t *buf_block = static_cast<buf_block_t *>(block);
  unsigned char *page_data = buf_block_get_frame(buf_block);
  mtr_t *mtr = static_cast<mtr_t *>(mtr_ref);

  unsigned char *loc = page_data + offset;
  mlog_write_string(loc, str, len, mtr);
}

// Force the linker to retain this translation unit so all vef_storage_*
// symbols remain visible in mysqld for extensions loaded at runtime.
using fn_ptr = void (*)();

static fn_ptr vef_storage_symbols[] = {
    (fn_ptr)&vef_storage_mtr_start,
    (fn_ptr)&vef_storage_mtr_commit,
    (fn_ptr)&vef_storage_segment_create,
    (fn_ptr)&vef_storage_segment_drop,
    (fn_ptr)&vef_storage_page_load,
    (fn_ptr)&vef_storage_page_allocate_and_load,
    (fn_ptr)&vef_storage_page_latch,
    (fn_ptr)&vef_storage_page_release,
    (fn_ptr)&vef_storage_page_get_size,
    (fn_ptr)&vef_storage_page_write_integer,
    (fn_ptr)&vef_storage_page_write_string,
};

void vef_storage_api_wrapper_init() { (void)vef_storage_symbols; }
