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

#include "villagesql/services/preview/sql_query.h"

namespace villagesql::services {

TEST(SqlQueryTextTest, RejectsMalformedTextWithoutReturningPartialData) {
  // Invalid leading byte, truncated sequence, surrogate, and out-of-range
  // codepoint.
  for (const std::string bytes :
       {"\xFF", "\xC3", "\xED\xA0\x80", "\xF4\x90\x80\x80"}) {
    std::string result = "unchanged";
    EXPECT_EQ(copy_query_result_text(bytes.data(), bytes.size(),
                                     &my_charset_utf8mb4_bin, &result),
              QueryTextStatus::kInvalid);
    EXPECT_EQ(result, "unchanged");
  }
}

TEST(SqlQueryTextTest, TextNulAndBinaryBytesArePreserved) {
  const std::string text("a\0\xC3\xA9", 4);  // a, NUL, é
  std::string result;
  EXPECT_EQ(copy_query_result_text(text.data(), text.size(),
                                   &my_charset_utf8mb4_bin, &result),
            QueryTextStatus::kOk);
  EXPECT_EQ(result, text);
  const std::string binary("\xFF\0", 2);
  EXPECT_EQ(copy_query_result_text(binary.data(), binary.size(),
                                   &my_charset_bin, &result),
            QueryTextStatus::kOk);
  EXPECT_EQ(result, binary);
}

}  // namespace villagesql::services
