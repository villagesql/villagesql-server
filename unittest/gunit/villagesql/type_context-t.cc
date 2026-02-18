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

#include "unittest/gunit/test_utils.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/systable/helpers.h"

namespace villagesql_unittest {

class TypeParametersTest : public ::testing::Test {
 protected:
  void SetUp() override {
    villagesql::test_set_lower_case_table_names(0);
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }
};

TEST_F(TypeParametersTest, ToJsonEmpty) {
  villagesql::TypeParameters params;
  EXPECT_EQ(params.to_json(), "{}");
}

TEST_F(TypeParametersTest, FromJsonEmptyObject) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_json("{}");
  EXPECT_TRUE(params.empty());
}

TEST_F(TypeParametersTest, ToJsonSingleParam) {
  villagesql::TypeParameters params({{"dimension", "1536"}});
  std::string json = params.to_json();
  EXPECT_EQ(json, R"({"dimension":"1536"})");
}

TEST_F(TypeParametersTest, ToJsonMultipleParams) {
  villagesql::TypeParameters params(
      {{"dimension", "1536"}, {"metric", "cosine"}});
  std::string json = params.to_json();
  // std::map is sorted by key, so "dimension" comes before "metric"
  EXPECT_EQ(json, R"({"dimension":"1536","metric":"cosine"})");
}

TEST_F(TypeParametersTest, FromJsonEmpty) {
  villagesql::TypeParameters params = villagesql::TypeParameters::from_json("");
  EXPECT_TRUE(params.empty());
}

TEST_F(TypeParametersTest, FromJsonValid) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_json(R"({"dimension":"1536"})");
  EXPECT_FALSE(params.empty());
  EXPECT_EQ(params.get("dimension"), "1536");
}

TEST_F(TypeParametersTest, FromJsonMultiple) {
  villagesql::TypeParameters params = villagesql::TypeParameters::from_json(
      R"({"dimension":"1536","metric":"cosine"})");
  EXPECT_FALSE(params.empty());
  EXPECT_EQ(params.get("dimension"), "1536");
  EXPECT_EQ(params.get("metric"), "cosine");
}

TEST_F(TypeParametersTest, FromJsonInvalid) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_json("not json");
  EXPECT_TRUE(params.empty());
}

TEST_F(TypeParametersTest, RoundTrip) {
  villagesql::TypeParameters original(
      {{"dimension", "1536"}, {"metric", "cosine"}});
  std::string json = original.to_json();
  villagesql::TypeParameters restored =
      villagesql::TypeParameters::from_json(json);

  EXPECT_EQ(original, restored);
  EXPECT_EQ(original.str(), restored.str());
  EXPECT_EQ(original.get("dimension"), restored.get("dimension"));
  EXPECT_EQ(original.get("metric"), restored.get("metric"));
}

TEST_F(TypeParametersTest, RoundTripSingleParam) {
  villagesql::TypeParameters original({{"dimension", "3"}});
  std::string json = original.to_json();
  villagesql::TypeParameters restored =
      villagesql::TypeParameters::from_json(json);

  EXPECT_EQ(original, restored);
  EXPECT_EQ(original.get("dimension"), "3");
}

}  // namespace villagesql_unittest
