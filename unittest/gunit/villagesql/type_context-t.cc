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

#include <cinttypes>
#include <cstring>

#include "unittest/gunit/test_utils.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/systable/helpers.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql_unittest {

// Dummy function pointers for TypeDescriptor construction
static bool dummy_encode(unsigned char *, size_t, const char *, size_t,
                         size_t *) {
  return false;
}
static bool dummy_decode(const unsigned char *, size_t, char *, size_t,
                         size_t *) {
  return false;
}
static int dummy_compare(const unsigned char *, size_t, const unsigned char *,
                         size_t) {
  return 0;
}

// resolve_params that succeeds and returns computed sizes
static bool resolve_params_ok(const vef_type_param_t *params,
                              size_t param_count,
                              vef_type_resolved_params_t *result,
                              char * /*error_msg*/) {
  // Compute persisted_length from "dimension" param: dimension * 4 bytes
  for (size_t i = 0; i < param_count; i++) {
    if (strcmp(params[i].key, "dimension") == 0) {
      int64_t dim = strtoll(params[i].value, nullptr, 10);
      result->persisted_length = dim * 4;
      result->max_decode_buffer_length = dim * 32;
      return false;
    }
  }
  return true;
}

// resolve_params that always fails
static bool resolve_params_fail(const vef_type_param_t * /*params*/,
                                size_t /*param_count*/,
                                vef_type_resolved_params_t * /*result*/,
                                char *error_msg) {
  snprintf(error_msg, VEF_MAX_ERROR_LEN, "unsupported parameter combination");
  return true;
}

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

class TypeContextTest : public ::testing::Test {
 protected:
  void SetUp() override {
    villagesql::test_set_lower_case_table_names(0);
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }
};

TEST_F(TypeContextTest, FixedLengthTypeUsesDescriptorValues) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("COMPLEX", "test_ext", "1.0.0"), 1, 16, 256,
      villagesql::EncodeOp(dummy_encode), villagesql::DecodeOp(dummy_decode),
      villagesql::CompareOp(dummy_compare));
  villagesql::TypeContextKey key("COMPLEX", "test_ext", "1.0.0");
  villagesql::TypeContext ctx(key, &desc);

  EXPECT_EQ(ctx.persisted_length(), 16);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 256);
}

TEST_F(TypeContextTest, ParameterizedTypeUsesResolvedValues) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"), 1, -1, 0,
      villagesql::EncodeOp(dummy_encode), villagesql::DecodeOp(dummy_decode),
      villagesql::CompareOp(dummy_compare), std::nullopt, nullptr,
      resolve_params_ok);
  villagesql::TypeParameters params({{"dimension", "1536"}});
  villagesql::TypeContextKey key(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"), params);
  villagesql::TypeContext ctx(key, &desc);

  // resolve_params_ok computes: dimension * 4, dimension * 32
  EXPECT_EQ(ctx.persisted_length(), 1536 * 4);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 1536 * 32);
}

TEST_F(TypeContextTest, ResolveParamsFailureFallsBackToDescriptor) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"), 1, -1, 0,
      villagesql::EncodeOp(dummy_encode), villagesql::DecodeOp(dummy_decode),
      villagesql::CompareOp(dummy_compare), std::nullopt, nullptr,
      resolve_params_fail);
  villagesql::TypeParameters params({{"dimension", "1536"}});
  villagesql::TypeContextKey key(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"), params);
  villagesql::TypeContext ctx(key, &desc);

  // resolve_params_fail returns error, so we fall back to descriptor values
  EXPECT_EQ(ctx.persisted_length(), -1);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 0);
}

TEST_F(TypeContextTest, EmptyParamsSkipsResolveCallback) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"), 1, -1, 0,
      villagesql::EncodeOp(dummy_encode), villagesql::DecodeOp(dummy_decode),
      villagesql::CompareOp(dummy_compare), std::nullopt, nullptr,
      resolve_params_fail);
  // No parameters — should use descriptor values directly, not call
  // resolve_params (which would fail)
  villagesql::TypeContextKey key("VVECTOR", "test_ext", "1.0.0");
  villagesql::TypeContext ctx(key, &desc);

  EXPECT_EQ(ctx.persisted_length(), -1);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 0);
}

}  // namespace villagesql_unittest
