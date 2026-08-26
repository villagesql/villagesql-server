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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "villagesql/common/build_info_stamp.h"

namespace villagesql_unittest {

using villagesql::BuildInfo;
using villagesql::BuildInfoRaw;
using villagesql::BuildInfoStampValues;
using villagesql::FindBuildInfoBlock;
using villagesql::kBuildInfoMagic;
using villagesql::ParseBuildInfoRaw;
using villagesql::RenderBuildInfoBlock;

namespace {

BuildInfoStampValues TestValues() {
  BuildInfoStampValues v;
  v.git_sha = "6dc6ca1c6c9e4c5898d8fbe6872174fe76631d6e";
  v.files_added = "1";
  v.files_deleted = "0";
  v.files_modified = "2";
  v.build_timestamp = "2026-08-26T18:42:47Z";
  v.build_host = "MacBook-Pro-46.local";
  v.build_os = "Darwin-25.5.0";
  v.build_arch = "arm64";
  return v;
}

// A fake "binary": junk, then a placeholder block, then junk.
std::string FakeBinary() {
  std::string binary(1000, '\xAB');
  BuildInfoRaw placeholder = {};
  std::memcpy(placeholder.magic, kBuildInfoMagic, sizeof(kBuildInfoMagic));
  std::strcpy(placeholder.git_sha, "unknown");
  binary.append(reinterpret_cast<const char *>(&placeholder),
                sizeof(placeholder));
  binary.append(1000, '\xCD');
  return binary;
}

}  // namespace

TEST(BuildInfoStampTest, FindsBlock) {
  std::string binary = FakeBinary();
  EXPECT_EQ(FindBuildInfoBlock(binary), 1000u);
  EXPECT_EQ(FindBuildInfoBlock(binary, 1001), std::string_view::npos);
  EXPECT_EQ(FindBuildInfoBlock("no magic here"), std::string_view::npos);
}

TEST(BuildInfoStampTest, RenderRoundTripsThroughParser) {
  BuildInfoRaw block;
  std::string err;
  ASSERT_TRUE(RenderBuildInfoBlock(TestValues(), &block, &err)) << err;
  EXPECT_EQ(0,
            std::memcmp(block.magic, kBuildInfoMagic, sizeof(kBuildInfoMagic)));
  BuildInfo info = ParseBuildInfoRaw(block);
  EXPECT_STREQ(info.git_sha, "6dc6ca1c6c9e4c5898d8fbe6872174fe76631d6e");
  EXPECT_EQ(info.files_added, 1);
  EXPECT_EQ(info.files_deleted, 0);
  EXPECT_EQ(info.files_modified, 2);
  EXPECT_TRUE(info.is_dirty());
  EXPECT_STREQ(info.build_timestamp, "2026-08-26T18:42:47Z");
  EXPECT_STREQ(info.build_host, "MacBook-Pro-46.local");
}

TEST(BuildInfoStampTest, RendersEmptyValues) {
  BuildInfoStampValues v = TestValues();
  v.build_timestamp = "";
  v.build_host = "";
  BuildInfoRaw block;
  std::string err;
  ASSERT_TRUE(RenderBuildInfoBlock(v, &block, &err)) << err;
  BuildInfo info = ParseBuildInfoRaw(block);
  EXPECT_STREQ(info.build_timestamp, "");
  EXPECT_STREQ(info.build_host, "");
}

TEST(BuildInfoStampTest, RejectsOversizeValue) {
  BuildInfoStampValues v = TestValues();
  v.git_sha = std::string(41, 'a');  // 41 chars + NUL > 41-byte field
  BuildInfoRaw block;
  std::string err;
  EXPECT_FALSE(RenderBuildInfoBlock(v, &block, &err));
  EXPECT_THAT(err, ::testing::HasSubstr("git_sha"));
}

TEST(BuildInfoStampTest, PatchedBufferParsesBack) {
  std::string binary = FakeBinary();
  size_t off = FindBuildInfoBlock(binary);
  ASSERT_NE(off, std::string_view::npos);
  BuildInfoRaw block;
  std::string err;
  ASSERT_TRUE(RenderBuildInfoBlock(TestValues(), &block, &err)) << err;
  std::memcpy(&binary[off], &block, sizeof(block));

  BuildInfoRaw readback;
  std::memcpy(&readback, binary.data() + off, sizeof(readback));
  BuildInfo info = ParseBuildInfoRaw(readback);
  EXPECT_STREQ(info.git_sha, "6dc6ca1c6c9e4c5898d8fbe6872174fe76631d6e");
  EXPECT_EQ(binary.size(), 2000u + sizeof(BuildInfoRaw));
}

}  // namespace villagesql_unittest
