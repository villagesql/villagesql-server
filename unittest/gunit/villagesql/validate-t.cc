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

// Unit tests for validate_extension_registration().
//
// These tests exercise the pure validation path — no THD, no VictionaryClient,
// no .so loading — by constructing vef_registration_t structs directly.

#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "unittest/gunit/test_utils.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/veb/validate.h"
#include "villagesql/veb/veb_file.h"

namespace villagesql_unittest {

// Minimal stub function pointers — they are never called during validation,
// only stored in the resulting descriptors.
static bool stub_encode(unsigned char *, size_t, const char *, size_t,
                        size_t *) {
  return false;
}
static bool stub_decode(const unsigned char *, size_t, char *, size_t,
                        size_t *) {
  return false;
}
static int stub_compare(const unsigned char *, size_t, const unsigned char *,
                        size_t) {
  return 0;
}
static void stub_vdf(vef_context_t *, vef_vdf_args_t *, vef_vdf_result_t *) {}
static void stub_clear(vef_context_t *, vef_vdf_args_t *) {}
static void stub_accumulate(vef_context_t *, vef_vdf_args_t *,
                            vef_vdf_result_t *) {}

class ValidateExtensionRegistrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    villagesql::test_set_lower_case_table_names(0);
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }

  // Build an ExtensionRegistration wrapping a raw vef_registration_t.
  // dlhandle and unregister_func are irrelevant for validation.
  static villagesql::veb::ExtensionRegistration make_ext_reg(
      vef_registration_t *reg, vef_protocol_t protocol) {
    villagesql::veb::ExtensionRegistration ext_reg;
    ext_reg.registration = reg;
    ext_reg.negotiated_protocol = protocol;
    ext_reg.dlhandle = nullptr;
    ext_reg.unregister_func = nullptr;
    return ext_reg;
  }

  // Minimal valid v1 type descriptor.
  static vef_type_desc_t make_v1_type(const char *name,
                                      int64_t max_decode_buffer_length = 256) {
    vef_type_desc_t td = {};
    td.protocol = VEF_PROTOCOL_1;
    td.name = name;
    td.persisted_length = 16;
    td.max_decode_buffer_length = max_decode_buffer_length;
    td.encode_func = stub_encode;
    td.decode_func = stub_decode;
    td.compare_func = stub_compare;
    return td;
  }

  // Minimal valid scalar func descriptor.
  static vef_func_desc_t make_scalar_func(const char *name,
                                          vef_signature_t *sig) {
    vef_func_desc_t fd = {};
    fd.protocol = VEF_PROTOCOL_1;
    fd.name = name;
    fd.signature = sig;
    fd.vdf = stub_vdf;
    return fd;
  }
};

// A valid registration with one type and one func produces the right
// descriptor count and names.
TEST_F(ValidateExtensionRegistrationTest, ValidV1TypeAndFunc) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  vef_type_desc_t *types[] = {&td};

  vef_type_t ret = {VEF_TYPE_STRING, nullptr};
  vef_signature_t sig = {0, nullptr, ret};
  vef_func_desc_t fd = make_scalar_func("my_func", &sig);
  vef_func_desc_t *funcs[] = {&fd};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(result->types.size(), 1u);
  ASSERT_EQ(result->funcs.size(), 1u);
  EXPECT_EQ(result->types[0].type_name(), "MYTYPE");
  EXPECT_EQ(result->types[0].extension_name(), "my_ext");
  EXPECT_EQ(result->types[0].extension_version(), "1.0.0");
  EXPECT_EQ(result->funcs[0].function_name(), "my_func");
  EXPECT_EQ(result->funcs[0].extension_name(), "my_ext");
}

// A registration with no types or funcs produces an empty
// ValidatedRegistration.
TEST_F(ValidateExtensionRegistrationTest, EmptyRegistration) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.extension_name = "empty_ext";

  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "empty_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->types.empty());
  EXPECT_TRUE(result->funcs.empty());
}

// A null registration pointer (extension registered nothing) is allowed.
TEST_F(ValidateExtensionRegistrationTest, NullRegistrationPointer) {
  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(nullptr, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->types.empty());
  EXPECT_TRUE(result->funcs.empty());
}

// A null pointer in the types array fails with a descriptive error.
TEST_F(ValidateExtensionRegistrationTest, NullTypeDescriptor) {
  vef_type_desc_t *types[] = {nullptr};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;

  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("NULL"), std::string::npos);
}

// A type with max_decode_buffer_length <= 0 fails with a descriptive error.
TEST_F(ValidateExtensionRegistrationTest, ZeroMaxDecodeBufferLength) {
  vef_type_desc_t td = make_v1_type("MYTYPE", 0);
  vef_type_desc_t *types[] = {&td};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;

  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("max_decode_buffer_length"), std::string::npos);
  EXPECT_NE(error.find("MYTYPE"), std::string::npos);
}

// A null pointer in the funcs array fails with a descriptive error.
TEST_F(ValidateExtensionRegistrationTest, NullFuncDescriptor) {
  vef_func_desc_t *funcs[] = {nullptr};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.extension_name = "my_ext";
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("NULL"), std::string::npos);
}

// Setting clear without accumulate (protocol 2) fails.
TEST_F(ValidateExtensionRegistrationTest, ClearWithoutAccumulate) {
  vef_type_t ret = {VEF_TYPE_INT, nullptr};
  vef_signature_t sig = {0, nullptr, ret};
  vef_func_desc_t fd = make_scalar_func("bad_agg", &sig);
  fd.protocol = VEF_PROTOCOL_2;
  fd.clear = stub_clear;
  fd.accumulate = nullptr;
  vef_func_desc_t *funcs[] = {&fd};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_2;
  reg.extension_name = "my_ext";
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_2), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("bad_agg"), std::string::npos);
  EXPECT_NE(error.find("clear"), std::string::npos);
  EXPECT_NE(error.find("accumulate"), std::string::npos);
}

// Setting accumulate without clear (protocol 2) fails.
TEST_F(ValidateExtensionRegistrationTest, AccumulateWithoutClear) {
  vef_type_t ret = {VEF_TYPE_INT, nullptr};
  vef_signature_t sig = {0, nullptr, ret};
  vef_func_desc_t fd = make_scalar_func("bad_agg", &sig);
  fd.protocol = VEF_PROTOCOL_2;
  fd.clear = nullptr;
  fd.accumulate = stub_accumulate;
  vef_func_desc_t *funcs[] = {&fd};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_2;
  reg.extension_name = "my_ext";
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_2), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("bad_agg"), std::string::npos);
}

// clear/accumulate asymmetry is not checked for protocol-1 extensions
// (those fields weren't defined yet).
TEST_F(ValidateExtensionRegistrationTest,
       ClearWithoutAccumulateProtocol1Ignored) {
  vef_type_t ret = {VEF_TYPE_INT, nullptr};
  vef_signature_t sig = {0, nullptr, ret};
  vef_func_desc_t fd = make_scalar_func("my_func", &sig);
  fd.clear = stub_clear;
  fd.accumulate = nullptr;
  vef_func_desc_t *funcs[] = {&fd};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.extension_name = "my_ext";
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->funcs.size(), 1u);
}

// Multiple types and funcs are all validated and returned.
TEST_F(ValidateExtensionRegistrationTest, MultipleTypesAndFuncs) {
  vef_type_desc_t td1 = make_v1_type("TYPE_A");
  vef_type_desc_t td2 = make_v1_type("TYPE_B");
  vef_type_desc_t *types[] = {&td1, &td2};

  vef_type_t ret = {VEF_TYPE_STRING, nullptr};
  vef_signature_t sig = {0, nullptr, ret};
  vef_func_desc_t fd1 = make_scalar_func("func_one", &sig);
  vef_func_desc_t fd2 = make_scalar_func("func_two", &sig);
  vef_func_desc_t fd3 = make_scalar_func("func_three", &sig);
  vef_func_desc_t *funcs[] = {&fd1, &fd2, &fd3};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.extension_name = "my_ext";
  reg.type_count = 2;
  reg.types = types;
  reg.func_count = 3;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::validate_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "2.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->types.size(), 2u);
  EXPECT_EQ(result->funcs.size(), 3u);
}

}  // namespace villagesql_unittest
