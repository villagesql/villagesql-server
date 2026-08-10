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

// Argument validation in vef_storage_page_latch(), the preview storage ABI
// entry point.
//

#include <gtest/gtest.h>

#include <cstdint>

#include <villagesql/abi/preview/storage.h>

namespace villagesql_unittest {

class StoragePageLatchTest : public ::testing::Test {
 protected:
  // Non-null addresses that must never be dereferenced on the paths below.
  vef_storage_block_ref_t opaque_block() {
    return static_cast<vef_storage_block_ref_t>(&m_block_placeholder);
  }
  vef_storage_mtr_ref_t opaque_mtr() {
    return static_cast<vef_storage_mtr_ref_t>(&m_mtr_placeholder);
  }

  unsigned char m_block_placeholder = 0;
  unsigned char m_mtr_placeholder = 0;
};

// A null block is rejected before anything is cast or dereferenced
TEST_F(StoragePageLatchTest, RejectsNullBlock) {
  char err[512] = {};
  EXPECT_EQ(VEF_STORAGE_ERROR_INVALID_ARGUMENT,
            vef_storage_page_latch(nullptr, 0, VEF_STORAGE_PAGE_LATCH_EXCLUSIVE,
                                   opaque_mtr(), err, sizeof(err)));
  EXPECT_STREQ("Invalid argument: block is null", err);
}

// A null mtr is rejected for the same reason: there is nothing to latch in.
TEST_F(StoragePageLatchTest, RejectsNullMtr) {
  char err[512] = {};
  EXPECT_EQ(VEF_STORAGE_ERROR_INVALID_ARGUMENT,
            vef_storage_page_latch(opaque_block(), 0,
                                   VEF_STORAGE_PAGE_LATCH_EXCLUSIVE, nullptr,
                                   err, sizeof(err)));
  EXPECT_STREQ("Invalid argument: mtr_ref is null", err);
}

// When both are null the message names the block
TEST_F(StoragePageLatchTest, BothNullReportsBlock) {
  char err[512] = {};
  EXPECT_EQ(VEF_STORAGE_ERROR_INVALID_ARGUMENT,
            vef_storage_page_latch(nullptr, 0, VEF_STORAGE_PAGE_LATCH_EXCLUSIVE,
                                   nullptr, err, sizeof(err)));
  EXPECT_STREQ("Invalid argument: block is null", err);
}

// The error buffer is optional: a caller that does not want the message can
// pass nullptr, or a zero length, and still gets the status code.
TEST_F(StoragePageLatchTest, ToleratesNullErrorBuffer) {
  EXPECT_EQ(VEF_STORAGE_ERROR_INVALID_ARGUMENT,
            vef_storage_page_latch(nullptr, 0, VEF_STORAGE_PAGE_LATCH_EXCLUSIVE,
                                   nullptr, nullptr, 0));

  // A non-null buffer with zero length must not be written to either.
  char err[8] = "untouch";
  EXPECT_EQ(VEF_STORAGE_ERROR_INVALID_ARGUMENT,
            vef_storage_page_latch(nullptr, 0, VEF_STORAGE_PAGE_LATCH_EXCLUSIVE,
                                   nullptr, err, 0));
  EXPECT_STREQ("untouch", err);
}

}  // namespace villagesql_unittest
