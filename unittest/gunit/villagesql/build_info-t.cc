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

#include "villagesql/common/build_info_raw.h"

namespace villagesql_unittest {

using villagesql::BuildInfo;
using villagesql::BuildInfoRaw;
using villagesql::GetBuildInfo;
using villagesql::kBuildInfoMagic;
using villagesql::ParseBuildInfoRaw;

TEST(BuildInfoTest, ParsesAllZeroBlockToDefaults) {
  BuildInfoRaw raw = {};
  BuildInfo info = ParseBuildInfoRaw(raw);
  EXPECT_STREQ(info.git_sha, "unknown");
  EXPECT_EQ(info.files_added, 0);
  EXPECT_EQ(info.files_deleted, 0);
  EXPECT_EQ(info.files_modified, 0);
  EXPECT_FALSE(info.is_dirty());
  EXPECT_STREQ(info.build_timestamp, "");
  EXPECT_STREQ(info.build_host, "");
  EXPECT_STREQ(info.build_os, "");
  EXPECT_STREQ(info.build_arch, "");
}

TEST(BuildInfoTest, ParsesStampedValues) {
  BuildInfoRaw raw = {};
  std::strcpy(raw.git_sha, "6dc6ca1c6c9e4c5898d8fbe6872174fe76631d6e");
  std::strcpy(raw.files_added, "1");
  std::strcpy(raw.files_deleted, "0");
  std::strcpy(raw.files_modified, "2");
  std::strcpy(raw.build_timestamp, "2026-08-26T18:42:47Z");
  std::strcpy(raw.build_host, "MacBook-Pro-46.local");
  std::strcpy(raw.build_os, "Darwin-25.5.0");
  std::strcpy(raw.build_arch, "arm64");
  BuildInfo info = ParseBuildInfoRaw(raw);
  EXPECT_STREQ(info.git_sha, "6dc6ca1c6c9e4c5898d8fbe6872174fe76631d6e");
  EXPECT_EQ(info.files_added, 1);
  EXPECT_EQ(info.files_deleted, 0);
  EXPECT_EQ(info.files_modified, 2);
  EXPECT_TRUE(info.is_dirty());
  EXPECT_STREQ(info.build_timestamp, "2026-08-26T18:42:47Z");
  EXPECT_STREQ(info.build_host, "MacBook-Pro-46.local");
  EXPECT_STREQ(info.build_os, "Darwin-25.5.0");
  EXPECT_STREQ(info.build_arch, "arm64");
}

// This test binary is never stamped, so the global block must carry the
// placeholder defaults, and its magic must match the constant the stamper
// searches for (the initializer repeats the literal; this guards the pair).
TEST(BuildInfoTest, GlobalBlockHasMagicAndDefaults) {
  char magic[sizeof(villagesql::vsql_build_info_raw.magic)];
  for (size_t i = 0; i < sizeof(magic); ++i)
    magic[i] = villagesql::vsql_build_info_raw.magic[i];
  EXPECT_STREQ(magic, kBuildInfoMagic);

  const BuildInfo &info = GetBuildInfo();
  EXPECT_STREQ(info.git_sha, "unknown");
  EXPECT_FALSE(info.is_dirty());
}

}  // namespace villagesql_unittest
