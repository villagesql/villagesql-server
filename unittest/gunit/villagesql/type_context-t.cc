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
#include <map>
#include <string>

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

// resolve_params that succeeds and returns computed sizes.
// Parses "dimension" from the canonical string, computes sizes.
static void resolve_params_ok_vdf(vef_context_t * /*ctx*/, vef_vdf_args_t *args,
                                  vef_vdf_result_t *result) {
  std::string input(args->values[0]->str_value, args->values[0]->str_len);
  // Parse "dimension=N" from canonical string
  int64_t dim = 0;
  size_t start = 0;
  while (start < input.size()) {
    size_t comma = input.find(',', start);
    if (comma == std::string::npos) comma = input.size();
    size_t eq = input.find('=', start);
    if (eq != std::string::npos && eq < comma) {
      std::string key = input.substr(start, eq - start);
      std::string value = input.substr(eq + 1, comma - eq - 1);
      if (key == "dimension") {
        dim = strtoll(value.c_str(), nullptr, 10);
      }
    }
    start = comma + 1;
  }
  if (dim <= 0) {
    result->type = VEF_RESULT_ERROR;
    snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "invalid dimension");
    return;
  }
  int64_t persisted = dim * 4;
  int64_t decode_buf = dim * 32;
  int written = snprintf(result->str_buf, result->max_str_len,
                         "%" PRId64 ",%" PRId64, persisted, decode_buf);
  result->type = VEF_RESULT_VALUE;
  result->actual_len = static_cast<size_t>(written);
}

// resolve_params that always fails
static void resolve_params_fail_vdf(vef_context_t * /*ctx*/,
                                    vef_vdf_args_t * /*args*/,
                                    vef_vdf_result_t *result) {
  result->type = VEF_RESULT_ERROR;
  snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
           "unsupported parameter combination");
}

// Build a mock vef_func_desc_t for a resolve_params VDF wrapper.
static vef_func_desc_t make_resolve_params_fd(const char *name,
                                              vef_vdf_func_t vdf) {
  static vef_type_t rp_param = {VEF_TYPE_STRING, nullptr};
  static vef_signature_t rp_sig = {1, &rp_param, {VEF_TYPE_STRING, nullptr}};
  return {VEF_PROTOCOL_3,
          name,
          &rp_sig,
          vdf,
          nullptr,
          nullptr,
          VEF_MAX_TYPE_PARAMS_STRING_LEN,
          false,
          nullptr,
          nullptr};
}

class TypeParametersTest : public ::testing::Test {
 protected:
  void SetUp() override {
    villagesql::test_set_lower_case_table_names(0);
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }
};

TEST_F(TypeParametersTest, EmptyByDefault) {
  villagesql::TypeParameters params;
  EXPECT_TRUE(params.empty());
  EXPECT_EQ(params.str(), "");
}

TEST_F(TypeParametersTest, ConstructFromCanonicalString) {
  villagesql::TypeParameters params("dimension=1536");
  EXPECT_FALSE(params.empty());
  EXPECT_EQ(params.str(), "dimension=1536");
}

TEST_F(TypeParametersTest, ConstructFromCanonicalMultiple) {
  villagesql::TypeParameters params("dimension=1536,metric=cosine");
  EXPECT_FALSE(params.empty());
  EXPECT_EQ(params.str(), "dimension=1536,metric=cosine");
}

TEST_F(TypeParametersTest, FromRawEmpty) {
  villagesql::TypeParameters params = villagesql::TypeParameters::from_raw("");
  EXPECT_TRUE(params.empty());
}

TEST_F(TypeParametersTest, FromRawSingle) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_raw("dimension=1536");
  EXPECT_FALSE(params.empty());
  EXPECT_EQ(params.str(), "dimension=1536");
}

TEST_F(TypeParametersTest, FromRawMultipleSorted) {
  // Keys get sorted alphabetically
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_raw("metric=cosine,dimension=1536");
  EXPECT_EQ(params.str(), "dimension=1536,metric=cosine");
}

TEST_F(TypeParametersTest, FromRawLowercases) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_raw("Dimension=1536");
  EXPECT_EQ(params.str(), "dimension=1536");
}

TEST_F(TypeParametersTest, FromRawTrimsWhitespace) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_raw(" dimension = 1536 ");
  EXPECT_EQ(params.str(), "dimension=1536");
}

TEST_F(TypeParametersTest, Equality) {
  villagesql::TypeParameters a("dimension=1536");
  villagesql::TypeParameters b("dimension=1536");
  villagesql::TypeParameters c("dimension=3");
  EXPECT_EQ(a, b);
  EXPECT_FALSE(a == c);
}

TEST_F(TypeParametersTest, Ordering) {
  villagesql::TypeParameters a("dimension=1536");
  villagesql::TypeParameters b("dimension=3");
  // "dimension=1536" < "dimension=3" (string comparison)
  EXPECT_TRUE(a < b);
}

TEST_F(TypeParametersTest, EmptyEntries) {
  villagesql::TypeParameters params;
  EXPECT_EQ(params.count(), 0u);
  EXPECT_EQ(params.key_data(), nullptr);
  EXPECT_EQ(params.value_data(), nullptr);
}

TEST_F(TypeParametersTest, SingleEntry) {
  villagesql::TypeParameters params("dimension=1536");
  EXPECT_EQ(params.count(), 1u);
  ASSERT_NE(params.key_data(), nullptr);
  ASSERT_NE(params.value_data(), nullptr);
  EXPECT_STREQ(params.key_data()[0], "dimension");
  EXPECT_STREQ(params.value_data()[0], "1536");
}

TEST_F(TypeParametersTest, MultipleEntries) {
  villagesql::TypeParameters params("dimension=1536,metric=cosine");
  EXPECT_EQ(params.count(), 2u);
  ASSERT_NE(params.key_data(), nullptr);
  ASSERT_NE(params.value_data(), nullptr);
  EXPECT_STREQ(params.key_data()[0], "dimension");
  EXPECT_STREQ(params.value_data()[0], "1536");
  EXPECT_STREQ(params.key_data()[1], "metric");
  EXPECT_STREQ(params.value_data()[1], "cosine");
}

class TypeContextTest : public ::testing::Test {
 protected:
  void SetUp() override {
    villagesql::test_set_lower_case_table_names(0);
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }

  static villagesql::TypeContext make_context(
      const villagesql::TypeContextKey &key,
      const villagesql::TypeDescriptor *descriptor) {
    return villagesql::TypeContext(key, descriptor);
  }
};

TEST_F(TypeContextTest, FixedLengthTypeUsesDescriptorValues) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("COMPLEX", "test_ext", "1.0.0"),
      VEF_PROTOCOL_1, 1, 16, 256, /*max_persisted_length=*/0,
      /*is_variable_length=*/false, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeContextKey key("COMPLEX", "test_ext", "1.0.0");
  villagesql::TypeContext ctx = make_context(key, &desc);

  EXPECT_EQ(ctx.persisted_length(), 16);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 256);
}

TEST_F(TypeContextTest, ParameterizedTypeUsesResolvedValues) {
  static auto rp_ok_fd =
      make_resolve_params_fd("rp_ok", &resolve_params_ok_vdf);

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, -1, 0, /*max_persisted_length=*/0,
      /*is_variable_length=*/true, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      villagesql::ResolveParamsFunction(&rp_ok_fd));
  villagesql::TypeParameters params("dimension=1536");
  villagesql::TypeContextKey key(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"), params);
  villagesql::TypeContext ctx = make_context(key, &desc);

  // resolve_params_ok computes: dimension * 4, dimension * 32
  EXPECT_EQ(ctx.persisted_length(), 1536 * 4);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 1536 * 32);
}

TEST_F(TypeContextTest, ResolveParamsFailureFallsBackToDescriptor) {
  static auto rp_fail_fd =
      make_resolve_params_fd("rp_fail", &resolve_params_fail_vdf);

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, -1, 0, /*max_persisted_length=*/0,
      /*is_variable_length=*/true, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      villagesql::ResolveParamsFunction(&rp_fail_fd));
  villagesql::TypeParameters params("dimension=1536");
  villagesql::TypeContextKey key(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"), params);
  villagesql::TypeContext ctx = make_context(key, &desc);

  // resolve_params_fail returns error, so we fall back to descriptor values
  EXPECT_EQ(ctx.persisted_length(), -1);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 0);
}

TEST_F(TypeContextTest, EmptyParamsSkipsResolveCallback) {
  static auto rp_fail_fd2 =
      make_resolve_params_fd("rp_fail2", &resolve_params_fail_vdf);

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, -1, 0, /*max_persisted_length=*/0,
      /*is_variable_length=*/true, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      villagesql::ResolveParamsFunction(&rp_fail_fd2));
  // No parameters — should use descriptor values directly, not call
  // resolve_params (which would fail)
  villagesql::TypeContextKey key("VVECTOR", "test_ext", "1.0.0");
  villagesql::TypeContext ctx = make_context(key, &desc);

  EXPECT_EQ(ctx.persisted_length(), -1);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 0);
}

TEST_F(TypeContextTest, SameKeysAreCompatible) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("COMPLEX", "test_ext", "1.0.0"),
      VEF_PROTOCOL_1, 1, 16, 256, /*max_persisted_length=*/0,
      /*is_variable_length=*/false, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeContextKey key("COMPLEX", "test_ext", "1.0.0");
  villagesql::TypeContext a = make_context(key, &desc);
  villagesql::TypeContext b = make_context(key, &desc);
  EXPECT_TRUE(a.is_compatible_with(b));
  EXPECT_TRUE(b.is_compatible_with(a));
  EXPECT_TRUE(a.is_assignable_with(b));
  EXPECT_TRUE(b.is_assignable_with(a));
}

TEST_F(TypeContextTest, DifferentTypeNamesAreNotCompatible) {
  villagesql::TypeDescriptor desc_a(
      villagesql::TypeDescriptorKey("COMPLEX", "test_ext", "1.0.0"),
      VEF_PROTOCOL_1, 1, 16, 256, /*max_persisted_length=*/0,
      /*is_variable_length=*/false, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeDescriptor desc_b(
      villagesql::TypeDescriptorKey("OTHER", "test_ext", "1.0.0"),
      VEF_PROTOCOL_1, 1, 16, 256, /*max_persisted_length=*/0,
      /*is_variable_length=*/false, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeContext a = make_context(
      villagesql::TypeContextKey("COMPLEX", "test_ext", "1.0.0"), &desc_a);
  villagesql::TypeContext b = make_context(
      villagesql::TypeContextKey("OTHER", "test_ext", "1.0.0"), &desc_b);
  EXPECT_FALSE(a.is_compatible_with(b));
  EXPECT_FALSE(b.is_compatible_with(a));
  EXPECT_FALSE(a.is_assignable_with(b));
  EXPECT_FALSE(b.is_assignable_with(a));
}

TEST_F(TypeContextTest, DifferentParametersAreNotCompatible) {
  static auto rp_ok_fd =
      make_resolve_params_fd("rp_ok_compat", &resolve_params_ok_vdf);

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, -1, 0, /*max_persisted_length=*/0,
      /*is_variable_length=*/true, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      villagesql::ResolveParamsFunction(&rp_ok_fd));
  villagesql::TypeContextKey key_3(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      villagesql::TypeParameters("dimension=3"));
  villagesql::TypeContextKey key_4(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      villagesql::TypeParameters("dimension=4"));
  villagesql::TypeContext v3 = make_context(key_3, &desc);
  villagesql::TypeContext v4 = make_context(key_4, &desc);
  EXPECT_FALSE(v3.is_compatible_with(v4));
  EXPECT_FALSE(v4.is_compatible_with(v3));
  EXPECT_FALSE(v3.is_assignable_with(v4));
  EXPECT_FALSE(v4.is_assignable_with(v3));
}

TEST_F(TypeContextTest, UnknownParametersAreAssignableWithKnown) {
  static auto rp_ok_fd =
      make_resolve_params_fd("rp_ok_compat", &resolve_params_ok_vdf);

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, -1, 0, /*max_persisted_length=*/0,
      /*is_variable_length=*/true, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      villagesql::ResolveParamsFunction(&rp_ok_fd));
  villagesql::TypeContextKey key_unknown(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      villagesql::TypeParameters(""));
  villagesql::TypeContextKey key_4(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      villagesql::TypeParameters("dimension=4"));
  villagesql::TypeContext v_unknown = make_context(key_unknown, &desc);
  villagesql::TypeContext v4 = make_context(key_4, &desc);
  EXPECT_FALSE(v_unknown.is_compatible_with(v4));
  EXPECT_FALSE(v4.is_compatible_with(v_unknown));
  EXPECT_TRUE(v_unknown.is_assignable_with(v4));
  EXPECT_FALSE(v4.is_assignable_with(v_unknown));
}

TEST_F(TypeContextTest, UnknownParametersAreAssignableWithUnknown) {
  static auto rp_ok_fd =
      make_resolve_params_fd("rp_ok_compat", &resolve_params_ok_vdf);

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, -1, 0, /*max_persisted_length=*/0,
      /*is_variable_length=*/true, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      villagesql::ResolveParamsFunction(&rp_ok_fd));
  villagesql::TypeContextKey key_unknown(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      villagesql::TypeParameters(""));
  villagesql::TypeContextKey key_unknown2(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      villagesql::TypeParameters(""));
  villagesql::TypeContext v_unknown = make_context(key_unknown, &desc);
  villagesql::TypeContext v_unknown2 = make_context(key_unknown2, &desc);
  EXPECT_TRUE(v_unknown.is_compatible_with(v_unknown2));
  EXPECT_TRUE(v_unknown2.is_compatible_with(v_unknown));
  EXPECT_TRUE(v_unknown.is_assignable_with(v_unknown2));
  EXPECT_TRUE(v_unknown2.is_assignable_with(v_unknown));
}

}  // namespace villagesql_unittest
