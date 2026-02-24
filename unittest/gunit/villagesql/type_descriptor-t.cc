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
      1,    // implementation_type
      16,   // persisted_length
      256,  // max_decode_buffer_length
      villagesql::EncodeOp(dummy_encode), villagesql::DecodeOp(dummy_decode),
      villagesql::CompareOp(dummy_compare), villagesql::HashOp(dummy_hash));

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

  // Verify ops are set and dispatch to wrapped functions
  EXPECT_EQ(desc.compare_op().invoke(nullptr, 0, nullptr, 0), 0);
  EXPECT_EQ(desc.hash_op()->invoke(nullptr, 0), 42u);
}

// Test TypeDescriptor with nullptr hash (optional)
TEST_F(TypeDescriptorTest, ConstructionWithNullHash) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("NOHASH", "ext", "1.0"), 0, 8, 64,
      villagesql::EncodeOp(dummy_encode), villagesql::DecodeOp(dummy_decode),
      villagesql::CompareOp(dummy_compare));

  EXPECT_FALSE(desc.hash_op().has_value());
  EXPECT_EQ(desc.compare_op().invoke(nullptr, 0, nullptr, 0), 0);
  EXPECT_EQ(desc.int_to_params(), nullptr);
  EXPECT_EQ(desc.resolve_params(), nullptr);
}

// Test that TypeDescriptor can be used with SystemTableMap (compile check)
// This verifies the key_type typedef and key() method work correctly
TEST_F(TypeDescriptorTest, KeyTypeCompatibility) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("TEST", "ext", "1.0"), 0, 8, 64,
      villagesql::EncodeOp(dummy_encode), villagesql::DecodeOp(dummy_decode),
      villagesql::CompareOp(dummy_compare));

  // Verify key_type is TypeDescriptorKey
  static_assert(std::is_same_v<villagesql::TypeDescriptor::key_type,
                               villagesql::TypeDescriptorKey>,
                "key_type should be TypeDescriptorKey");

  // Verify key() returns the right type
  const villagesql::TypeDescriptorKey &key = desc.key();
  EXPECT_EQ(key.str(), "test.ext.1.0");

  // Verify optional params default to nullptr
  EXPECT_EQ(desc.int_to_params(), nullptr);
  EXPECT_EQ(desc.resolve_params(), nullptr);
}

// Dummy parameter functions for testing
static bool dummy_int_to_params(int64_t value, vef_type_param_t *params,
                                size_t *param_count, char *error_msg) {
  if (value <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "value must be positive");
    return true;
  }
  // Return key-value pairs. Keys/values are string literals (static lifetime).
  static char dim_buf[32];
  snprintf(dim_buf, sizeof(dim_buf), "%" PRId64, value);
  params[0] = {"dimension", dim_buf};
  *param_count = 1;
  return false;
}

static bool dummy_resolve_params(const vef_type_param_t * /*params*/,
                                 size_t /*param_count*/,
                                 vef_type_resolved_params_t *result,
                                 char * /*error_msg*/) {
  result->persisted_length = 6144;
  result->max_decode_buffer_length = 32768;
  return false;
}

// Test TypeDescriptor construction with non-null param functions
TEST_F(TypeDescriptorTest, ConstructionWithParamFunctions) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      1,   // implementation_type
      -1,  // persisted_length (variable-length)
      0,   // max_decode_buffer_length (determined by params)
      villagesql::EncodeOp(dummy_encode), villagesql::DecodeOp(dummy_decode),
      villagesql::CompareOp(dummy_compare), villagesql::HashOp(dummy_hash),
      dummy_int_to_params, dummy_resolve_params);

  EXPECT_EQ(desc.persisted_length(), -1);
  EXPECT_EQ(desc.int_to_params(), dummy_int_to_params);
  EXPECT_EQ(desc.resolve_params(), dummy_resolve_params);

  // Verify int_to_params callback produces key-value pairs
  vef_type_param_t params[VEF_MAX_TYPE_PARAMS];
  size_t param_count = 0;
  char error_msg[VEF_MAX_ERROR_LEN] = {0};
  EXPECT_FALSE(desc.int_to_params()(1536, params, &param_count, error_msg));
  EXPECT_EQ(param_count, 1u);
  EXPECT_STREQ(params[0].key, "dimension");
  EXPECT_STREQ(params[0].value, "1536");

  // Verify resolve_params callback computes storage sizes
  vef_type_resolved_params_t resolved = {};
  EXPECT_FALSE(
      desc.resolve_params()(params, param_count, &resolved, error_msg));
  EXPECT_EQ(resolved.persisted_length, 6144);
  EXPECT_EQ(resolved.max_decode_buffer_length, 32768);
}

}  // namespace villagesql_unittest
