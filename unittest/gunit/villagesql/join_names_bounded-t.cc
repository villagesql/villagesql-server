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

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "villagesql/sql/parse_tree_items.h"

namespace villagesql_unittest {

TEST(JoinNamesBoundedTest, EmptyList) {
  EXPECT_EQ("", join_names_bounded({}, 100));
}

TEST(JoinNamesBoundedTest, AllFitWithRoomToSpare) {
  EXPECT_EQ("compare, from_string, to_string",
            join_names_bounded({"compare", "from_string", "to_string"}, 100));
}

TEST(JoinNamesBoundedTest, AllFitExactly) {
  std::vector<std::string> names{"aaaa", "bbbb", "cccc"};
  EXPECT_EQ("aaaa, bbbb, cccc", join_names_bounded(names, 16));
}

TEST(JoinNamesBoundedTest, BudgetOneByteShort) {
  std::vector<std::string> names{"aaaa", "bbbb", "cccc"};
  EXPECT_EQ("aaaa, and 2 more methods", join_names_bounded(names, 15));
}

TEST(JoinNamesBoundedTest, SmallBudgetDoesNotFitNotListedSummary) {
  std::vector<std::string> names{"aaaa", "bbbb", "cccc"};
  EXPECT_EQ("", join_names_bounded(names, 9));
}

TEST(JoinNamesBoundedTest, FirstNameDoesNotFitButNotListedSummaryDoes) {
  std::vector<std::string> names{"aaaa", "bbbb", "cccc"};
  EXPECT_EQ("3 not listed", join_names_bounded(names, 12));
}

TEST(JoinNamesBoundedTest, FirstNameDoesNotFit) {
  std::vector<std::string> names{"aaaa", "bbbb", "cccc"};
  EXPECT_EQ("", join_names_bounded(names, 3));
}

TEST(JoinNamesBoundedTest, ZeroBudget) {
  std::vector<std::string> names{"aaaa", "bbbb"};
  EXPECT_EQ("", join_names_bounded(names, 0));
}

}  // namespace villagesql_unittest
