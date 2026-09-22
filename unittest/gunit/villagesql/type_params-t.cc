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

#include <gtest/gtest.h>

#include <cstddef>
#include <map>
#include <string>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/type_params.h>

namespace villagesql_unittest {

// ===========================================================================
// write_params_to
// ===========================================================================
//
// Serializes a params map into a caller-supplied buffer under the
// vef_inferred_type_params_t contract. Every bind_and_check_types hook reaches
// it through BindResult::set_return / set_arg, and the constant-string
// from_string inference path writes its inferred params back the same way.
//
// Two properties carry weight beyond this function:
//
//   1. The snprintf convention. actual_len is the length the whole string
//      WOULD have taken, not the number of bytes written. Callers must test
//      overflow before reading actual_len bytes out of the buffer, which is
//      why ValidateAndConvertVDFArguments checks it first.
//
//   2. Canonical (key-sorted) order. TypeParameters::operator== is a raw
//      string compare, and the constructor the server feeds a hook's output
//      into only splits -- it does not sort, unlike the DDL path's from_raw.
//      Unsorted output here would make a hook-produced parameterization
//      compare unequal to the identical one declared on a column.

class WriteParamsToTest : public ::testing::Test {
 protected:
  // Runs write_params_to with a declared capacity of `cap`, carved out of a
  // larger sentinel-filled block so a write past max_buf_len is detectable.
  void Run(const std::map<std::string, std::string> &m, size_t cap) {
    // fill with 0xAA bytes (sentinel)
    buf_.assign(kBlock, kSentinel);
    out_ = {};
    out_.buf = buf_.data();
    out_.max_buf_len = cap;
    vsql::write_params_to(&out_, m);
  }

  // The bytes written. Only meaningful when !out_.overflow -- on overflow the
  // contract leaves the buffer undefined.
  std::string Written() const {
    return std::string(buf_.data(), out_.actual_len);
  }

  // True when nothing was written at or past the declared capacity.
  bool GuardIntact(size_t cap) const {
    return buf_.find_first_not_of(kSentinel, cap) == std::string::npos;
  }

  static constexpr size_t kBlock = 256;
  static constexpr char kSentinel = '\xAA';

  std::string buf_;
  vef_inferred_type_params_t out_{};
};

TEST_F(WriteParamsToTest, EmptyMapWritesNothing) {
  Run({}, 64);
  EXPECT_EQ(out_.actual_len, 0u);
  EXPECT_FALSE(out_.overflow);
  EXPECT_TRUE(GuardIntact(0));
}

TEST_F(WriteParamsToTest, SinglePairExactBytes) {
  Run({{"dimension", "3"}}, 64);
  EXPECT_FALSE(out_.overflow);
  std::string written = Written();
  EXPECT_EQ(written, "dimension=3");
  EXPECT_EQ(out_.actual_len, written.length());
  EXPECT_TRUE(GuardIntact(11));
}

TEST_F(WriteParamsToTest, OutputIsInCanonicalSortedOrder) {
  std::map<std::string, std::string> m;
  m["type"] = "float";
  m["dimension"] = "3";
  Run(m, 64);
  EXPECT_FALSE(out_.overflow);
  EXPECT_EQ(Written(), "dimension=3,type=float");
}

TEST_F(WriteParamsToTest, ExactFitDoesNotOverflow) {
  // "dimension=3,type=float" is 22 bytes; a 22-byte buffer must be accepted.
  Run({{"dimension", "3"}, {"type", "float"}}, 22);
  EXPECT_FALSE(out_.overflow);
  std::string written = Written();
  EXPECT_EQ(written, "dimension=3,type=float");
  EXPECT_EQ(out_.actual_len, written.length());
  EXPECT_TRUE(GuardIntact(22));
}

TEST_F(WriteParamsToTest, OneByteShortSetsOverflow) {
  Run({{"dimension", "3"}, {"type", "float"}}, 21);
  EXPECT_TRUE(out_.overflow);
  EXPECT_EQ(out_.actual_len, strlen("dimension=3,type=float"));
  EXPECT_TRUE(GuardIntact(21));
}

// The snprintf convention: actual_len is the length of the WHOLE string, not
// of the part that fitted. Here the first pair (11 bytes) is written and the
// second is not, yet actual_len reports all 22.
TEST_F(WriteParamsToTest, ActualLenCountsWholeStringNotWhatFit) {
  Run({{"dimension", "3"}, {"type", "float"}}, 11);
  EXPECT_TRUE(out_.overflow);
  EXPECT_EQ(out_.actual_len, strlen("dimension=3,type=float"));
  EXPECT_TRUE(GuardIntact(11));
}

TEST_F(WriteParamsToTest, ZeroCapacityNonEmptyMap) {
  Run({{"dimension", "3"}}, 0);
  EXPECT_TRUE(out_.overflow);
  EXPECT_EQ(out_.actual_len, strlen("dimension=3"));
  EXPECT_TRUE(GuardIntact(0));
}

// No NUL is appended: actual_len is the whole story, and the byte just past
// the output is left as the caller had it.
TEST_F(WriteParamsToTest, BufferIsNotNulTerminated) {
  Run({{"dimension", "3"}}, 64);
  ASSERT_FALSE(out_.overflow);
  std::string written = Written();
  EXPECT_EQ(written, "dimension=3");
  ASSERT_EQ(out_.actual_len, written.length());
  EXPECT_EQ(buf_[out_.actual_len], kSentinel);
}

// Both outputs are assigned unconditionally, so a struct that overflowed once
// is safe to reuse.
TEST_F(WriteParamsToTest, ReuseClearsOverflow) {
  Run({{"dimension", "3"}, {"type", "float"}}, 5);
  ASSERT_TRUE(out_.overflow);

  Run({{"dimension", "3"}}, 64);
  ASSERT_FALSE(out_.overflow);
  std::string written = Written();
  EXPECT_EQ(written, "dimension=3");
  EXPECT_EQ(out_.actual_len, written.length());
}

}  // namespace villagesql_unittest
