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
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/systable/helpers.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql_unittest {

// Dummy function pointers for testing
static bool dummy_encode(unsigned char *, size_t, const char *, size_t,
                         size_t *) {
  return false;  // Success
}

static bool dummy_decode(const unsigned char *, size_t, char *, size_t,
                         size_t *) {
  return false;  // Success
}

static int dummy_compare(const unsigned char *, size_t, const unsigned char *,
                         size_t) {
  return 0;  // Equal
}

static size_t dummy_hash(const unsigned char *, size_t) { return 42; }

class TypeDescriptorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Reset lower_case_table_names to default for consistent testing
    villagesql::test_set_lower_case_table_names(0);
    // Set system character set
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }
};

// Test TypeDescriptorKey construction and normalization
TEST_F(TypeDescriptorTest, KeyConstruction) {
  villagesql::TypeDescriptorKey key("COMPLEX", "my_extension", "1.0.0");

  // Key should be normalized (lowercase)
  EXPECT_EQ(key.str(), "complex.my_extension.1.0.0");
}

// Test TypeDescriptorKey comparison
TEST_F(TypeDescriptorTest, KeyComparison) {
  villagesql::TypeDescriptorKey key1("COMPLEX", "ext", "1.0");
  villagesql::TypeDescriptorKey key2("complex", "EXT", "1.0");
  villagesql::TypeDescriptorKey key3("VECTOR", "ext", "1.0");

  // Same normalized key should be equal
  EXPECT_EQ(key1, key2);
  EXPECT_EQ(key1.str(), key2.str());

  // Different keys should not be equal
  EXPECT_NE(key1, key3);
  EXPECT_LT(key1, key3);  // "complex" < "vector"
}

// Test TypeDescriptor construction
TEST_F(TypeDescriptorTest, Construction) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("MYTYPE", "test_ext", "2.0.0"),
      VEF_PROTOCOL_1,
      1,                              // implementation_type
      16,                             // persisted_length
      256,                            // max_decode_buffer_length
      0,                              // max_persisted_length
      villagesql::LengthKind::Fixed,  // length_kind
      villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare),
      villagesql::HashFunction(dummy_hash));

  // Check identity fields
  EXPECT_EQ(desc.type_name(), "MYTYPE");
  EXPECT_EQ(desc.extension_name(), "test_ext");
  EXPECT_EQ(desc.extension_version(), "2.0.0");

  // Check key is correctly constructed
  EXPECT_EQ(desc.key().str(), "mytype.test_ext.2.0.0");

  // Check implementation details
  EXPECT_EQ(desc.implementation_type(), 1);
  EXPECT_EQ(desc.persisted_length(), 16);
  EXPECT_EQ(desc.max_decode_buffer_length(), 256);

  // Verify functions store the correct function pointers
  EXPECT_EQ(desc.compare_fn().fn(), &dummy_compare);
  EXPECT_EQ(desc.hash_fn()->fn(), &dummy_hash);
}

// Test TypeDescriptor with nullptr hash (optional)
TEST_F(TypeDescriptorTest, ConstructionWithNullHash) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("NOHASH", "ext", "1.0"), VEF_PROTOCOL_1, 0,
      8, 64,
      0,                              // max_persisted_length
      villagesql::LengthKind::Fixed,  // length_kind
      villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));

  EXPECT_FALSE(desc.hash_fn().has_value());
  EXPECT_EQ(desc.compare_fn().fn(), &dummy_compare);
  EXPECT_FALSE(desc.int_to_params_fn().has_value());
  EXPECT_FALSE(desc.resolve_params_fn().has_value());
}

// Test that TypeDescriptor can be used with SystemTableMap (compile check)
// This verifies the key_type typedef and key() method work correctly
TEST_F(TypeDescriptorTest, KeyTypeCompatibility) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("TEST", "ext", "1.0"), VEF_PROTOCOL_1, 0, 8,
      64,
      0,                              // max_persisted_length
      villagesql::LengthKind::Fixed,  // length_kind
      villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));

  // Verify key_type is TypeDescriptorKey
  static_assert(std::is_same_v<villagesql::TypeDescriptor::key_type,
                               villagesql::TypeDescriptorKey>,
                "key_type should be TypeDescriptorKey");

  // Verify key() returns the right type
  const villagesql::TypeDescriptorKey &key = desc.key();
  EXPECT_EQ(key.str(), "test.ext.1.0");

  // Verify optional params default to nullopt
  EXPECT_FALSE(desc.int_to_params_fn().has_value());
  EXPECT_FALSE(desc.resolve_params_fn().has_value());
}

// Dummy parameter functions for testing using std::map API
static bool dummy_int_to_params(int64_t value,
                                std::map<std::string, std::string> &params,
                                char *error_msg) {
  if (value <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "value must be positive");
    return true;
  }
  params["dimension"] = std::to_string(value);
  return false;
}

static bool dummy_resolve_params(
    const std::map<std::string, std::string> & /*params*/,
    villagesql::ResolvedTypeParams *result, char * /*error_msg*/) {
  result->persisted_length = 6144;
  result->max_decode_buffer_length = 32768;
  return false;
}

// VDF wrapper for dummy_int_to_params: (INT) -> STRING "key=value,..."
static void dummy_int_to_params_vdf(vef_context_t * /*ctx*/,
                                    vef_vdf_args_t *args,
                                    vef_vdf_result_t *result) {
  std::map<std::string, std::string> params;
  if (dummy_int_to_params(args->values[0]->int_value, params,
                          result->error_msg)) {
    result->type = VEF_RESULT_ERROR;
    return;
  }
  size_t pos = 0;
  for (const auto &[key, value] : params) {
    if (pos > 0) result->str_buf[pos++] = ',';
    memcpy(result->str_buf + pos, key.c_str(), key.size());
    pos += key.size();
    result->str_buf[pos++] = '=';
    memcpy(result->str_buf + pos, value.c_str(), value.size());
    pos += value.size();
  }
  result->type = VEF_RESULT_VALUE;
  result->actual_len = pos;
}

// VDF wrapper for dummy_resolve_params: (STRING) -> STRING "N,N"
static void dummy_resolve_params_vdf(vef_context_t * /*ctx*/,
                                     vef_vdf_args_t *args,
                                     vef_vdf_result_t *result) {
  std::string input(args->values[0]->str_value, args->values[0]->str_len);
  std::map<std::string, std::string> params;
  size_t start = 0;
  while (start < input.size()) {
    size_t comma = input.find(',', start);
    if (comma == std::string::npos) comma = input.size();
    size_t eq = input.find('=', start);
    if (eq != std::string::npos && eq < comma) {
      params[input.substr(start, eq - start)] =
          input.substr(eq + 1, comma - eq - 1);
    }
    start = comma + 1;
  }
  villagesql::ResolvedTypeParams resolved = {};
  if (dummy_resolve_params(params, &resolved, result->error_msg)) {
    result->type = VEF_RESULT_ERROR;
    return;
  }
  int written =
      snprintf(result->str_buf, result->max_str_len, "%" PRId64 ",%" PRId64,
               resolved.persisted_length, resolved.max_decode_buffer_length);
  result->type = VEF_RESULT_VALUE;
  result->actual_len = static_cast<size_t>(written);
}

// Test TypeDescriptor construction with non-null param functions
TEST_F(TypeDescriptorTest, ConstructionWithParamFunctions) {
  // Build mock VDF descriptors for int_to_params and resolve_params.
  static vef_type_t itp_param = {VEF_TYPE_INT, nullptr};
  static vef_signature_t itp_sig = {1, &itp_param, {VEF_TYPE_STRING, nullptr}};
  static vef_func_desc_t itp_fd = {VEF_PROTOCOL_3,
                                   "dummy_int_to_params",
                                   &itp_sig,
                                   &dummy_int_to_params_vdf,
                                   nullptr,
                                   nullptr,
                                   VEF_MAX_TYPE_PARAMS_STRING_LEN,
                                   false,
                                   nullptr,
                                   nullptr,
                                   0};

  static vef_type_t rp_param = {VEF_TYPE_STRING, nullptr};
  static vef_signature_t rp_sig = {1, &rp_param, {VEF_TYPE_STRING, nullptr}};
  static vef_func_desc_t rp_fd = {VEF_PROTOCOL_3,
                                  "dummy_resolve_params",
                                  &rp_sig,
                                  &dummy_resolve_params_vdf,
                                  nullptr,
                                  nullptr,
                                  VEF_MAX_TYPE_PARAMS_STRING_LEN,
                                  false,
                                  nullptr,
                                  nullptr,
                                  0};

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3,
      1,   // implementation_type
      -1,  // persisted_length (variable-length)
      0,   // max_decode_buffer_length (determined by params)
      0,   // max_persisted_length (not used by these tests)
      villagesql::LengthKind::Fixed,  // length_kind
      villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare),
      villagesql::HashFunction(dummy_hash), std::nullopt,
      villagesql::IntToParamsFunction(&itp_fd),
      villagesql::ResolveParamsFunction(&rp_fd));

  EXPECT_EQ(desc.persisted_length(), -1);
  EXPECT_TRUE(desc.int_to_params_fn().has_value());
  EXPECT_TRUE(desc.resolve_params_fn().has_value());

  // Verify int_to_params callback produces canonical string
  std::string params_str;
  char error_msg[VEF_MAX_ERROR_LEN] = {0};
  EXPECT_FALSE(desc.int_to_params_fn()->invoke(1536, &params_str, error_msg));
  EXPECT_EQ(params_str, "dimension=1536");

  // Verify resolve_params callback computes storage sizes
  villagesql::ResolvedTypeParams resolved = {};
  EXPECT_FALSE(
      desc.resolve_params_fn()->invoke(params_str, &resolved, error_msg));
  EXPECT_EQ(resolved.persisted_length, 6144);
  EXPECT_EQ(resolved.max_decode_buffer_length, 32768);
}

}  // namespace villagesql_unittest
