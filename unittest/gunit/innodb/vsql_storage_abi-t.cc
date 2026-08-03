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

// Tests for the mini-transaction entry points of the VEF storage ABI
// implemented in storage/innobase/villagesql/storage_abi.cc.
//
// vef_storage_mtr_start() uses a caller-allocates protocol: the extension
// supplies raw storage and the server placement-constructs an mtr_t into it.
// The function reports the size and alignment it needs, and the caller
// retries with a big enough buffer. These tests pin that negotiation contract
// and the start/commit lifecycle.
//

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>

// univ.i must precede any other InnoDB header.
#include "univ.i"

#include "ut0new.h"

#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"

namespace vsql_storage_abi_unittest {

// Heap block with an explicit alignment, so a test can hand
// vef_storage_mtr_start() exactly the storage it negotiated for.
class AlignedBuffer {
 public:
  AlignedBuffer(size_t size, size_t align)
      : m_ptr(static_cast<unsigned char *>(
            ::operator new(size, std::align_val_t(align)))),
        m_size(size),
        m_align(align) {}

  ~AlignedBuffer() { ::operator delete(m_ptr, std::align_val_t(m_align)); }

  AlignedBuffer(const AlignedBuffer &) = delete;
  AlignedBuffer &operator=(const AlignedBuffer &) = delete;

  unsigned char *get() const { return m_ptr; }
  uint32_t size() const { return static_cast<uint32_t>(m_size); }

 private:
  unsigned char *m_ptr;
  size_t m_size;
  size_t m_align;
};

class StorageAbiMtrTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // In debug builds mtr_t::start() registers the mtr in a thread-local
    // ut::unordered_set, which allocates through ut::allocator. Safe to call
    // repeatedly; only the first call initializes.
    ut_new_boot_safe();
  }

  void SetUp() override {
    // A null buffer is the "how much do you need?" probe: it must report the
    // requirements and start nothing.
    ASSERT_EQ(nullptr, vef_storage_mtr_start(
                           nullptr, 0, &m_required_size, &m_required_alignment,
                           m_error, static_cast<uint32_t>(sizeof(m_error))));
    ASSERT_GT(m_required_size, 0u);
    ASSERT_GT(m_required_alignment, 0u);
  }

  // Negotiated requirements, filled in by SetUp().
  uint32_t m_required_size = 0;
  uint32_t m_required_alignment = 0;
  char m_error[512] = {};
};

// Probing with a null buffer reports the requirements and is not an error:
// callers use this to size their allocation on the first call.
TEST_F(StorageAbiMtrTest, ReportsRequirementsForNullBuffer) {
  // SetUp() already performed the probe and checked the return value.
  EXPECT_STREQ("", m_error) << "requirement probe must not report an error";
}

// Every out parameter is optional. An extension that only wants to know
// whether a fixed buffer works may pass nullptr for all of them.
TEST_F(StorageAbiMtrTest, ToleratesNullOutParams) {
  EXPECT_EQ(nullptr,
            vef_storage_mtr_start(nullptr, 0, nullptr, nullptr, nullptr, 0));

  // Same for a real buffer: the size/alignment reports may be skipped once the
  // caller already knows the requirements.
  AlignedBuffer buf(m_required_size, m_required_alignment);
  vef_storage_mtr_ref_t ref = vef_storage_mtr_start(
      buf.get(), buf.size(), nullptr, nullptr, nullptr, 0);
  ASSERT_NE(nullptr, ref);
  vef_storage_mtr_commit(ref);
}

// One byte short must fail rather than construct an mtr_t over storage that
// cannot hold it, and must still report the requirements so the caller can
// retry.
TEST_F(StorageAbiMtrTest, RejectsUndersizedBuffer) {
  AlignedBuffer buf(m_required_size, m_required_alignment);

  uint32_t reported_size = 0;
  uint32_t reported_alignment = 0;
  char err[512] = {};

  EXPECT_EQ(nullptr, vef_storage_mtr_start(buf.get(), m_required_size - 1,
                                           &reported_size, &reported_alignment,
                                           err, sizeof(err)));

  EXPECT_EQ(m_required_size, reported_size)
      << "a failed start must still report the size needed to retry";
  EXPECT_EQ(m_required_alignment, reported_alignment);
  EXPECT_NE(nullptr, strstr(err, "too small")) << "error_msg was: " << err;
}

// A big enough but misaligned buffer must be rejected: placement-new over it
// would produce an mtr_t whose members violate their alignment requirements.
TEST_F(StorageAbiMtrTest, RejectsMisalignedBuffer) {
  if (m_required_alignment == 1) {
    GTEST_SKIP() << "mtr_t has no alignment requirement to violate";
  }

  // Over-allocate so that get() + 1 still has required_size bytes behind it,
  // and the size check passes before the alignment check is reached.
  AlignedBuffer buf(m_required_size + m_required_alignment,
                    m_required_alignment);

  char err[512] = {};
  EXPECT_EQ(nullptr, vef_storage_mtr_start(buf.get() + 1, m_required_size,
                                           nullptr, nullptr, err, sizeof(err)));
  EXPECT_NE(nullptr, strstr(err, "misaligned")) << "error_msg was: " << err;
}

// Success path: a buffer of exactly the negotiated size and alignment
// yields a usable mtr reference that commits cleanly.
TEST_F(StorageAbiMtrTest, StartsAndCommitsWithExactBuffer) {
  AlignedBuffer buf(m_required_size, m_required_alignment);

  vef_storage_mtr_ref_t ref =
      vef_storage_mtr_start(buf.get(), buf.size(), nullptr, nullptr, m_error,
                            static_cast<uint32_t>(sizeof(m_error)));

  ASSERT_NE(nullptr, ref) << "error_msg was: " << m_error;
  // The mtr is constructed in the caller's storage, not somewhere else.
  EXPECT_EQ(static_cast<void *>(buf.get()), ref);

  vef_storage_mtr_commit(ref);
}

// Callers are allowed to pass more storage than required.
TEST_F(StorageAbiMtrTest, AcceptsOversizedBuffer) {
  AlignedBuffer buf(m_required_size * 2, m_required_alignment);

  vef_storage_mtr_ref_t ref = vef_storage_mtr_start(
      buf.get(), buf.size(), nullptr, nullptr, m_error, sizeof(m_error));

  ASSERT_NE(nullptr, ref) << "error_msg was: " << m_error;
  vef_storage_mtr_commit(ref);
}

// commit() runs the mtr_t destructor, leaving the caller's storage reusable.
// The SDK wrapper relies on this: it keeps one buffer for the lifetime of the
// MtrCtx and starts several mtrs in it.
TEST_F(StorageAbiMtrTest, BufferIsReusableAcrossMtrs) {
  AlignedBuffer buf(m_required_size, m_required_alignment);

  for (int i = 0; i < 3; ++i) {
    vef_storage_mtr_ref_t ref = vef_storage_mtr_start(
        buf.get(), buf.size(), nullptr, nullptr, m_error, sizeof(m_error));
    ASSERT_NE(nullptr, ref)
        << "iteration " << i << ", error_msg was: " << m_error;
    vef_storage_mtr_commit(ref);
  }
}

// Two mtrs can be live at once in separate buffers.
TEST_F(StorageAbiMtrTest, SupportsTwoConcurrentMtrs) {
  AlignedBuffer outer(m_required_size, m_required_alignment);
  AlignedBuffer inner(m_required_size, m_required_alignment);

  vef_storage_mtr_ref_t outer_ref = vef_storage_mtr_start(
      outer.get(), outer.size(), nullptr, nullptr, m_error, sizeof(m_error));
  ASSERT_NE(nullptr, outer_ref) << "error_msg was: " << m_error;

  vef_storage_mtr_ref_t inner_ref = vef_storage_mtr_start(
      inner.get(), inner.size(), nullptr, nullptr, m_error, sizeof(m_error));
  ASSERT_NE(nullptr, inner_ref) << "error_msg was: " << m_error;
  EXPECT_NE(outer_ref, inner_ref);

  vef_storage_mtr_commit(inner_ref);
  vef_storage_mtr_commit(outer_ref);
}

// Committing a null reference is a no-op, so an extension whose start() failed
// can unconditionally commit on the way out.
TEST_F(StorageAbiMtrTest, CommitOfNullRefIsNoOp) {
  vef_storage_mtr_commit(nullptr);
}

}  // namespace vsql_storage_abi_unittest
