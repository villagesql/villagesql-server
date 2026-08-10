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

// Unit tests for parse_extension_registration().
//
// These tests exercise the pure parsing path — no THD, no VictionaryClient,
// no .so loading — by constructing vef_registration_t structs directly.

#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "sql/sql_const.h"
#include "unittest/gunit/test_utils.h"
#include "villagesql/sdk/include/villagesql/abi/preview/index.h"
#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"
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
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
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
  reg.deprecated_extension_name = "empty_ext";

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "empty_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->types.empty());
  EXPECT_TRUE(result->funcs.empty());
}

// A null registration pointer (extension registered nothing) is allowed.
TEST_F(ValidateExtensionRegistrationTest, NullRegistrationPointer) {
  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
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
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
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
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("max_decode_buffer_length"), std::string::npos);
  EXPECT_NE(error.find("MYTYPE"), std::string::npos);
}

// The widest storage footprint a custom type may declare. A custom column is
// backed by a VARBINARY field, and MySQL's 65535-byte row budget also pays for
// the field's 2 length bytes and the row's null byte. Spelled as a literal
// rather than recomputed from MAX_FIELD_VARCHARLENGTH.
static constexpr int64_t kExpectedMaxDeclaredLength = 65532;

// A declared footprint above the cap is rejected, naming the type and the
// supported maximum.
TEST_F(ValidateExtensionRegistrationTest,
       PersistedLengthAboveDeclaredLengthCap) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  td.persisted_length = kExpectedMaxDeclaredLength + 1;
  vef_type_desc_t *types[] = {&td};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("MYTYPE"), std::string::npos);
  EXPECT_NE(error.find(std::to_string(kExpectedMaxDeclaredLength)),
            std::string::npos);
}

// A variable-length type sizes its backing field from max_persisted_length
// rather than persisted_length.
TEST_F(ValidateExtensionRegistrationTest,
       MaxPersistedLengthAboveDeclaredLengthCap) {
  vef_type_desc_t td = make_v1_type("MYVARTYPE");
  td.protocol = VEF_PROTOCOL_4;
  td.variable_length = true;
  td.persisted_length = 0;  // variable-length types must not fix a footprint
  td.max_persisted_length = kExpectedMaxDeclaredLength + 1;
  vef_type_desc_t *types[] = {&td};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_4;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_4), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("MYVARTYPE"), std::string::npos);
  EXPECT_NE(error.find(std::to_string(kExpectedMaxDeclaredLength)),
            std::string::npos);
}

// The cap is inclusive: a type sitting exactly on it registers.
TEST_F(ValidateExtensionRegistrationTest, PersistedLengthAtDeclaredLengthCap) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  td.persisted_length = kExpectedMaxDeclaredLength;
  vef_type_desc_t *types[] = {&td};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value()) << error;
  EXPECT_EQ(result->types.size(), 1U);
}

// A null pointer in the funcs array fails with a descriptive error.
TEST_F(ValidateExtensionRegistrationTest, NullFuncDescriptor) {
  vef_func_desc_t *funcs[] = {nullptr};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.deprecated_extension_name = "my_ext";
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("NULL"), std::string::npos);
}

// Setting clear without accumulate (protocol 2) fails.
TEST_F(ValidateExtensionRegistrationTest, ClearWithoutAccumulate) {
  vef_type_t ret = {VEF_TYPE_INT, nullptr};
  vef_signature_t sig = {0, nullptr, ret};
  vef_func_desc_t fd = make_scalar_func("bad_agg", &sig);
  fd.protocol = VEF_PROTOCOL_3;
  fd.clear = stub_clear;
  fd.accumulate = nullptr;
  vef_func_desc_t *funcs[] = {&fd};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

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
  fd.protocol = VEF_PROTOCOL_3;
  fd.clear = nullptr;
  fd.accumulate = stub_accumulate;
  vef_func_desc_t *funcs[] = {&fd};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

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
  reg.deprecated_extension_name = "my_ext";
  reg.func_count = 1;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->funcs.size(), 1u);
}

// A v2 type whose encode_vdf_name is malformed fails validation.
TEST_F(ValidateExtensionRegistrationTest, V2TypeBadVdfName) {
  struct TestCase {
    const char *bad_name;
    const char *expected_error;
  };

  const TestCase cases[] = {
      {"MYTYPE::transform",
       "type 'MYTYPE' failed validation"},  // unrecognised method suffix
      {"WRONGTYPE::from_string",
       "type 'MYTYPE' failed validation"},  // prefix does not match type name
      {"::MYTPE::transform", "type 'MYTYPE' failed validation"},
      {"MYTPE::::transform", "type 'MYTYPE' failed validation"},
  };

  for (const auto &tc : cases) {
    SCOPED_TRACE(std::string("bad_name=") + tc.bad_name);

    vef_type_desc_t td = {};
    td.protocol = VEF_PROTOCOL_3;
    td.name = "MYTYPE";
    td.persisted_length = 16;
    td.max_decode_buffer_length = 256;
    td.encode_vdf_name = tc.bad_name;

    vef_type_desc_t *types[] = {&td};
    vef_registration_t reg = {};
    reg.protocol = VEF_PROTOCOL_3;
    reg.deprecated_extension_name = "my_ext";
    reg.type_count = 1;
    reg.types = types;

    std::string error;
    auto result = villagesql::veb::parse_extension_registration(
        make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(error, tc.expected_error);
  }
}

// A v2 type registration using valid VDF names succeeds end-to-end.
TEST_F(ValidateExtensionRegistrationTest, V2TypeValidVdfNames) {
  vef_type_desc_t td = {};
  td.protocol = VEF_PROTOCOL_3;
  td.name = "MYTYPE";
  td.persisted_length = 16;
  td.max_decode_buffer_length = 256;
  td.encode_vdf_name = "MYTYPE::from_string";
  td.decode_vdf_name = "MYTYPE::to_string";
  td.compare_vdf_name = "MYTYPE::compare";

  vef_type_t str_type = {VEF_TYPE_STRING, nullptr};
  vef_type_t int_type = {VEF_TYPE_INT, nullptr};
  vef_type_t custom_mytype = {VEF_TYPE_CUSTOM, "MYTYPE"};

  vef_type_t encode_params[] = {str_type};
  vef_signature_t encode_sig = {1, encode_params, custom_mytype};
  vef_func_desc_t encode_fd = {};
  encode_fd.protocol = VEF_PROTOCOL_3;
  encode_fd.name = "MYTYPE::from_string";
  encode_fd.signature = &encode_sig;
  encode_fd.vdf = stub_vdf;

  vef_type_t decode_params[] = {custom_mytype};
  vef_signature_t decode_sig = {1, decode_params, str_type};
  vef_func_desc_t decode_fd = {};
  decode_fd.protocol = VEF_PROTOCOL_3;
  decode_fd.name = "MYTYPE::to_string";
  decode_fd.signature = &decode_sig;
  decode_fd.vdf = stub_vdf;

  vef_type_t compare_params[] = {custom_mytype, custom_mytype};
  vef_signature_t compare_sig = {2, compare_params, int_type};
  vef_func_desc_t compare_fd = {};
  compare_fd.protocol = VEF_PROTOCOL_3;
  compare_fd.name = "MYTYPE::compare";
  compare_fd.signature = &compare_sig;
  compare_fd.vdf = stub_vdf;

  vef_type_desc_t *types[] = {&td};
  vef_func_desc_t *funcs[] = {&encode_fd, &decode_fd, &compare_fd};

  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.func_count = 3;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(result->types.size(), 1u);
  EXPECT_EQ(result->types[0].type_name(), "MYTYPE");
}

// Stub column storage function pointers — never called during validation.
static bool stub_storage_create(vef_storage_space_ref_t, vef_storage_trx_ref_t,
                                uint32_t, vef_storage_arena_t *,
                                vef_storage_arena_func_t, vef_storage_ctx_t **,
                                char *, uint32_t) {
  return false;
}
static bool stub_storage_drop(vef_storage_ctx_t *, vef_storage_trx_ref_t,
                              char *, uint32_t) {
  return false;
}
static bool stub_storage_load(vef_storage_ref_t, vef_storage_arena_t *,
                              vef_storage_arena_func_t, vef_storage_ctx_t **,
                              char *, uint32_t) {
  return false;
}
static bool stub_storage_insert(vef_storage_ctx_t *, vef_storage_mtr_ref_t,
                                vef_storage_trx_ref_t, vef_storage_col_data_t,
                                vef_storage_col_data_t, vef_storage_col_ref_t *,
                                char *, uint32_t) {
  return false;
}
static bool stub_storage_select(vef_storage_ctx_t *, vef_storage_mtr_ref_t,
                                vef_storage_col_ref_t, vef_storage_col_data_t *,
                                vef_storage_col_data_t *,
                                vef_storage_trx_ref_t *, bool *, char *,
                                uint32_t) {
  return false;
}
static bool stub_storage_mark_delete(vef_storage_ctx_t *, vef_storage_mtr_ref_t,
                                     vef_storage_trx_ref_t,
                                     vef_storage_col_ref_t, bool, char *,
                                     uint32_t) {
  return false;
}
static bool stub_storage_purge(vef_storage_ctx_t *, vef_storage_mtr_ref_t,
                               vef_storage_trx_ref_t, vef_storage_col_ref_t,
                               char *, uint32_t) {
  return false;
}

static vef_type_storage_intf_t make_storage_intf(const char *type_name) {
  vef_type_storage_intf_t si = {};
  si.version = VEF_STORAGE_TYPE_INTF_VERSION;
  si.type_name = type_name;
  si.create = stub_storage_create;
  si.drop = stub_storage_drop;
  si.load = stub_storage_load;
  si.insert = stub_storage_insert;
  si.select = stub_storage_select;
  si.mark_delete = stub_storage_mark_delete;
  si.purge = stub_storage_purge;
  return si;
}

// A v2 extension with a column_store capability wires storage_intf into the
// matching TypeDescriptor. This is the regression test for the accidental
// deletion of the wiring block in parse_extension_registration() (commit
// 736f6a7afca removed it as a side effect of removing
// validate_sys_var_descriptors in the same diff hunk).
TEST_F(ValidateExtensionRegistrationTest, ColumnStorageWiredToType) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  vef_type_desc_t *types[] = {&td};

  vef_type_storage_intf_t si = make_storage_intf("MYTYPE");
  const vef_type_storage_intf_t *storages[] = {&si};

  vef_preview_column_store_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_COLUMN_STORE_INTF_VERSION;
  ext_desc.type_storage_count = 1;
  ext_desc.type_storages = storages;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_COLUMN_STORE_NAME;
  cap.capability_config = &ext_desc;
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(result->types.size(), 1u);
  EXPECT_TRUE(result->types[0].storage_intf().has_value());
}

// Column store wiring is skipped entirely for protocol-1 negotiations even if
// a column_store capability is present (the field didn't exist in v1).
TEST_F(ValidateExtensionRegistrationTest,
       ColumnStorageNotWiredForProtocol1Negotiation) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  vef_type_desc_t *types[] = {&td};

  vef_type_storage_intf_t si = make_storage_intf("MYTYPE");
  const vef_type_storage_intf_t *storages[] = {&si};

  vef_preview_column_store_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_COLUMN_STORE_INTF_VERSION;
  ext_desc.type_storage_count = 1;
  ext_desc.type_storages = storages;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_COLUMN_STORE_NAME;
  cap.capability_config = &ext_desc;
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->types.size(), 1u);
  EXPECT_FALSE(result->types[0].storage_intf().has_value());
}

// Column store capability referencing a type not registered by this extension
// fails validation.
TEST_F(ValidateExtensionRegistrationTest, ColumnStorageUnknownTypeFails) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  vef_type_desc_t *types[] = {&td};

  vef_type_storage_intf_t si = make_storage_intf("OTHERTYPE");
  const vef_type_storage_intf_t *storages[] = {&si};

  vef_preview_column_store_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_COLUMN_STORE_INTF_VERSION;
  ext_desc.type_storage_count = 1;
  ext_desc.type_storages = storages;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_COLUMN_STORE_NAME;
  cap.capability_config = &ext_desc;
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("OTHERTYPE"), std::string::npos);
}

// Column store capability with incomplete storage functions (missing purge)
// fails validation.
TEST_F(ValidateExtensionRegistrationTest, ColumnStorageMissingFunctionFails) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  vef_type_desc_t *types[] = {&td};

  vef_type_storage_intf_t si = make_storage_intf("MYTYPE");
  si.purge = nullptr;
  const vef_type_storage_intf_t *storages[] = {&si};

  vef_preview_column_store_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_COLUMN_STORE_INTF_VERSION;
  ext_desc.type_storage_count = 1;
  ext_desc.type_storages = storages;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_COLUMN_STORE_NAME;
  cap.capability_config = &ext_desc;
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("MYTYPE"), std::string::npos);
}

// A null entry in the type_storages array fails validation.
TEST_F(ValidateExtensionRegistrationTest, ColumnStorageNullDescriptorFails) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  vef_type_desc_t *types[] = {&td};

  const vef_type_storage_intf_t *storages[] = {nullptr};

  vef_preview_column_store_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_COLUMN_STORE_INTF_VERSION;
  ext_desc.type_storage_count = 1;
  ext_desc.type_storages = storages;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_COLUMN_STORE_NAME;
  cap.capability_config = &ext_desc;
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("NULL"), std::string::npos);
}

// A column_store capability with a mismatched version field fails validation.
TEST_F(ValidateExtensionRegistrationTest, ColumnStorageVersionMismatchFails) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  vef_type_desc_t *types[] = {&td};

  vef_type_storage_intf_t si = make_storage_intf("MYTYPE");
  const vef_type_storage_intf_t *storages[] = {&si};

  vef_preview_column_store_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_COLUMN_STORE_INTF_VERSION + 99;
  ext_desc.type_storage_count = 1;
  ext_desc.type_storages = storages;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_COLUMN_STORE_NAME;
  cap.capability_config = &ext_desc;
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("unsupported version"), std::string::npos);
}

// Two storage descriptors claiming the same type name fail validation.
TEST_F(ValidateExtensionRegistrationTest, ColumnStorageDuplicateTypeFails) {
  vef_type_desc_t td = make_v1_type("MYTYPE");
  vef_type_desc_t *types[] = {&td};

  vef_type_storage_intf_t si1 = make_storage_intf("MYTYPE");
  vef_type_storage_intf_t si2 = make_storage_intf("MYTYPE");
  const vef_type_storage_intf_t *storages[] = {&si1, &si2};

  vef_preview_column_store_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_COLUMN_STORE_INTF_VERSION;
  ext_desc.type_storage_count = 2;
  ext_desc.type_storages = storages;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_COLUMN_STORE_NAME;
  cap.capability_config = &ext_desc;
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 1;
  reg.types = types;
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("duplicate"), std::string::npos);
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
  reg.deprecated_extension_name = "my_ext";
  reg.type_count = 2;
  reg.types = types;
  reg.func_count = 3;
  reg.funcs = funcs;

  std::string error;
  auto result = villagesql::veb::parse_extension_registration(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "2.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->types.size(), 2u);
  EXPECT_EQ(result->funcs.size(), 3u);
}

// ---- Stub function pointers for vef_type_index_intf_t ----------------------

static bool stub_idx_create(const vef_index_ctx_t *, vef_storage_space_ref_t,
                            vef_storage_trx_ref_t, vef_storage_arena_t *,
                            vef_storage_arena_func_t, vef_storage_ctx_t **,
                            char *, uint32_t) {
  return false;
}
static bool stub_idx_drop(const vef_index_ctx_t *, vef_storage_ctx_t *,
                          vef_storage_trx_ref_t, char *, uint32_t) {
  return false;
}
static bool stub_idx_load(const vef_index_ctx_t *, vef_storage_ref_t,
                          vef_storage_arena_t *, vef_storage_arena_func_t,
                          vef_storage_ctx_t **, char *, uint32_t) {
  return false;
}
static bool stub_idx_insert(const vef_index_ctx_t *, vef_storage_ctx_t *,
                            vef_storage_trx_ref_t, vef_storage_col_data_t *,
                            vef_storage_col_data_t *, vef_storage_col_ref_t *,
                            char *, uint32_t) {
  return false;
}
static bool stub_idx_mark_delete(const vef_index_ctx_t *, vef_storage_ctx_t *,
                                 vef_storage_trx_ref_t, vef_storage_col_ref_t *,
                                 vef_storage_col_data_t *,
                                 vef_storage_col_data_t *, bool, char *,
                                 uint32_t) {
  return false;
}
static bool stub_idx_purge(const vef_index_ctx_t *, vef_storage_ctx_t *,
                           vef_storage_trx_ref_t, vef_storage_col_ref_t *,
                           vef_storage_col_data_t *, vef_storage_col_data_t *,
                           char *, uint32_t) {
  return false;
}
static bool stub_idx_scan_begin(const vef_index_ctx_t *, vef_storage_ctx_t *,
                                vef_storage_mtr_ref_t,
                                const vef_index_scan_desc_t *,
                                vef_index_cursor_ref_t *, bool *, char *,
                                uint32_t) {
  return false;
}
static bool stub_idx_scan_position(vef_index_cursor_ref_t,
                                   vef_index_cursor_op_t, bool *, char *,
                                   uint32_t) {
  return false;
}
static bool stub_idx_scan_fetch(vef_index_cursor_ref_t, vef_storage_col_ref_t *,
                                vef_storage_col_data_t *,
                                vef_storage_col_data_t *, char *, uint32_t) {
  return false;
}
static bool stub_idx_scan_save(vef_index_cursor_ref_t, char *, uint32_t) {
  return false;
}
static bool stub_idx_scan_restore(vef_index_cursor_ref_t, vef_storage_mtr_ref_t,
                                  bool *, char *, uint32_t) {
  return false;
}
static void stub_idx_scan_end(vef_index_cursor_ref_t *) {}

static vef_type_index_intf_t make_index_intf() {
  vef_type_index_intf_t intf = {};
  intf.version = VEF_INDEX_TYPE_INTF_VERSION;
  intf.create = stub_idx_create;
  intf.drop = stub_idx_drop;
  intf.load = stub_idx_load;
  intf.insert = stub_idx_insert;
  intf.mark_delete = stub_idx_mark_delete;
  intf.purge = stub_idx_purge;
  intf.scan_begin = stub_idx_scan_begin;
  intf.scan_position = stub_idx_scan_position;
  intf.scan_fetch = stub_idx_scan_fetch;
  intf.scan_save = stub_idx_scan_save;
  intf.scan_restore = stub_idx_scan_restore;
  intf.scan_end = stub_idx_scan_end;
  return intf;
}

class ValidatePreviewCapabilitiesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    villagesql::test_set_lower_case_table_names(0);
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }

  static villagesql::veb::ExtensionRegistration make_ext_reg(
      vef_registration_t *reg, vef_protocol_t protocol) {
    villagesql::veb::ExtensionRegistration ext_reg;
    ext_reg.registration = reg;
    ext_reg.negotiated_protocol = protocol;
    ext_reg.dlhandle = nullptr;
    ext_reg.unregister_func = nullptr;
    return ext_reg;
  }
};

// A null registration pointer produces an empty result, not an error.
TEST_F(ValidatePreviewCapabilitiesTest, NullRegistrationReturnsEmpty) {
  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(nullptr, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->index_types.empty());
  EXPECT_TRUE(result->index_profiles.empty());
}

// A protocol older than VEF_PROTOCOL_2 skips parsing entirely.
TEST_F(ValidatePreviewCapabilitiesTest, OldProtocolReturnsEmpty) {
  vef_type_index_intf_t intf = make_index_intf();
  vef_index_type_reg_t entry = {};
  entry.name = "HNSW";
  entry.intf = &intf;

  vef_preview_index_type_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_TYPE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.types = &entry;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_TYPE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_1), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->index_types.empty());
  EXPECT_TRUE(result->index_profiles.empty());
}

// A registration with no capabilities produces an empty result.
TEST_F(ValidatePreviewCapabilitiesTest, NoCapabilitiesReturnsEmpty) {
  vef_registration_t reg = {};

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->index_types.empty());
  EXPECT_TRUE(result->index_profiles.empty());
}

// A valid index_type capability produces an IndexTypeDescriptor with the
// correct name and extension.
TEST_F(ValidatePreviewCapabilitiesTest, ValidIndexTypeCapability) {
  vef_type_index_intf_t intf = make_index_intf();
  vef_index_type_reg_t entry = {};
  entry.name = "HNSW";
  entry.intf = &intf;

  vef_preview_index_type_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_TYPE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.types = &entry;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_TYPE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(result->index_types.size(), 1u);
  EXPECT_EQ(result->index_types[0].index_type_name(), "HNSW");
  EXPECT_EQ(result->index_types[0].extension_name(), "my_ext");
  EXPECT_EQ(result->index_types[0].extension_version(), "1.0.0");
  EXPECT_TRUE(result->index_profiles.empty());
}

// A valid index_profile capability (no function bindings) produces an
// IndexProfileDescriptor with the correct name and unqualified type refs.
TEST_F(ValidatePreviewCapabilitiesTest, ValidIndexProfileCapability) {
  vef_index_profile_reg_t profile = {};
  profile.name = "mytype_hnsw";
  profile.type_name = "MYTYPE";
  profile.index_type_name = "HNSW";
  profile.function_count = 0;
  profile.ordering = VEF_INDEX_ORDERING_ASC;
  profile.default_for_type = 1;

  vef_preview_index_profile_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.profiles = &profile;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_PROFILE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(error.empty());
  EXPECT_TRUE(result->index_types.empty());
  ASSERT_EQ(result->index_profiles.size(), 1u);
  EXPECT_EQ(result->index_profiles[0].profile_name(), "mytype_hnsw");
  EXPECT_EQ(result->index_profiles[0].extension_name(), "my_ext");
}

// Both index_type and index_profile capabilities in one registration are both
// parsed independently.
TEST_F(ValidatePreviewCapabilitiesTest,
       BothCapabilitiesProduceBothDescriptors) {
  vef_type_index_intf_t intf = make_index_intf();
  vef_index_type_reg_t type_entry = {};
  type_entry.name = "HNSW";
  type_entry.intf = &intf;

  vef_preview_index_type_ext_desc_t type_ext_desc = {};
  type_ext_desc.version = VEF_PREVIEW_INDEX_TYPE_ABI_VERSION;
  type_ext_desc.count = 1;
  type_ext_desc.types = &type_entry;

  vef_index_profile_reg_t profile = {};
  profile.name = "mytype_hnsw";
  profile.type_name = "MYTYPE";
  profile.index_type_name = "HNSW";

  vef_preview_index_profile_ext_desc_t profile_ext_desc = {};
  profile_ext_desc.version = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION;
  profile_ext_desc.count = 1;
  profile_ext_desc.profiles = &profile;

  vef_required_capability_t caps[2] = {};
  caps[0].name = VEF_PREVIEW_INDEX_TYPE_NAME;
  caps[0].capability_config = &type_ext_desc;
  caps[1].name = VEF_PREVIEW_INDEX_PROFILE_NAME;
  caps[1].capability_config = &profile_ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 2;
  reg.required_capabilities = caps;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(error.empty());
  EXPECT_EQ(result->index_types.size(), 1u);
  EXPECT_EQ(result->index_profiles.size(), 1u);
}

// A qualified type_name ("other_ext.MYTYPE") produces a type_ref with the
// extension component set.
TEST_F(ValidatePreviewCapabilitiesTest, IndexProfileQualifiedTypeRef) {
  vef_index_profile_reg_t profile = {};
  profile.name = "mytype_hnsw";
  profile.type_name = "other_ext.MYTYPE";
  profile.index_type_name = "HNSW";

  vef_preview_index_profile_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.profiles = &profile;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_PROFILE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->index_profiles.size(), 1u);
  EXPECT_EQ(result->index_profiles[0].type_ref().extension_name(), "other_ext");
}

// index_type capability with a mismatched version fails validation.
TEST_F(ValidatePreviewCapabilitiesTest, IndexTypeVersionMismatchFails) {
  vef_preview_index_type_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_TYPE_ABI_VERSION + 99;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_TYPE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("unsupported version"), std::string::npos);
}

// A null name or intf pointer in an index type entry fails validation.
TEST_F(ValidatePreviewCapabilitiesTest, IndexTypeNullEntryFails) {
  vef_index_type_reg_t entry = {};
  entry.name = nullptr;
  entry.intf = nullptr;

  vef_preview_index_type_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_TYPE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.types = &entry;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_TYPE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("NULL"), std::string::npos);
}

// An index type entry with one missing function pointer fails validation.
TEST_F(ValidatePreviewCapabilitiesTest, IndexTypeMissingFunctionPointerFails) {
  vef_type_index_intf_t intf = make_index_intf();
  intf.scan_end = nullptr;

  vef_index_type_reg_t entry = {};
  entry.name = "HNSW";
  entry.intf = &intf;

  vef_preview_index_type_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_TYPE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.types = &entry;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_TYPE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("not all required function pointers"),
            std::string::npos);
  EXPECT_NE(error.find("HNSW"), std::string::npos);
}

// index_profile capability with a mismatched version fails validation.
TEST_F(ValidatePreviewCapabilitiesTest, IndexProfileVersionMismatchFails) {
  vef_preview_index_profile_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION + 99;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_PROFILE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("unsupported version"), std::string::npos);
}

// An index profile entry with a null name/type_name/index_type_name fails.
TEST_F(ValidatePreviewCapabilitiesTest, IndexProfileNullFieldFails) {
  vef_index_profile_reg_t profile = {};
  profile.name = nullptr;
  profile.type_name = "MYTYPE";
  profile.index_type_name = "HNSW";

  vef_preview_index_profile_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.profiles = &profile;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_PROFILE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("NULL"), std::string::npos);
}

// A function binding with a null name fails with a descriptive error.
TEST_F(ValidatePreviewCapabilitiesTest, IndexProfileNullFunctionNameFails) {
  vef_index_profile_fn_binding_t fn = {};
  fn.name = nullptr;
  fn.vdf = stub_vdf;

  vef_index_profile_reg_t profile = {};
  profile.name = "mytype_hnsw";
  profile.type_name = "MYTYPE";
  profile.index_type_name = "HNSW";
  profile.function_count = 1;
  profile.functions = &fn;

  vef_preview_index_profile_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.profiles = &profile;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_PROFILE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("NULL name"), std::string::npos);
}

// A function binding with a null vdf pointer fails with a descriptive error.
TEST_F(ValidatePreviewCapabilitiesTest, IndexProfileNullFunctionVdfFails) {
  vef_index_profile_fn_binding_t fn = {};
  fn.name = "mytype_distance";
  fn.vdf = nullptr;

  vef_index_profile_reg_t profile = {};
  profile.name = "mytype_hnsw";
  profile.type_name = "MYTYPE";
  profile.index_type_name = "HNSW";
  profile.function_count = 1;
  profile.functions = &fn;

  vef_preview_index_profile_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.profiles = &profile;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_PROFILE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("NULL vdf pointer"), std::string::npos);
  EXPECT_NE(error.find("mytype_distance"), std::string::npos);
}

// A function binding with non-zero param_count but NULL params fails
// validation.
TEST_F(ValidatePreviewCapabilitiesTest, IndexProfileNullParamsFails) {
  vef_index_profile_fn_binding_t fn = {};
  fn.name = "mytype_distance";
  fn.vdf = stub_vdf;
  fn.signature.param_count = 1;
  // fn.signature.params intentionally left NULL

  vef_index_profile_reg_t profile = {};
  profile.name = "mytype_hnsw";
  profile.type_name = "MYTYPE";
  profile.index_type_name = "HNSW";
  profile.function_count = 1;
  profile.functions = &fn;

  vef_preview_index_profile_ext_desc_t ext_desc = {};
  ext_desc.version = VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION;
  ext_desc.count = 1;
  ext_desc.profiles = &profile;

  vef_required_capability_t cap = {};
  cap.name = VEF_PREVIEW_INDEX_PROFILE_NAME;
  cap.capability_config = &ext_desc;

  vef_registration_t reg = {};
  reg.required_capability_count = 1;
  reg.required_capabilities = &cap;

  std::string error;
  auto result = villagesql::veb::parse_preview_capabilities(
      make_ext_reg(&reg, VEF_PROTOCOL_3), "my_ext", "1.0.0", error);

  EXPECT_FALSE(result.has_value());
  EXPECT_NE(error.find("mytype_distance"), std::string::npos);
  EXPECT_NE(error.find("params"), std::string::npos);
}

}  // namespace villagesql_unittest
