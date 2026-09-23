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

// Unit tests for check_vef_registration().
//
// open_vef_extension() itself needs a real .so, but the checks it runs on the
// registration that vef_register returns are pure, so they are exercised here
// by constructing vef_registration_t structs directly.

#include <gtest/gtest.h>

#include <string>

#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/veb/veb_file.h"

namespace villagesql_unittest {

using villagesql::veb::check_vef_registration;

static void stub_vdf(vef_context_t *, vef_vdf_args_t *, vef_vdf_result_t *) {}

// A well-formed scalar VDF: (STRING) -> INT. Value-initialized and then
// filled in rather than partially designated, so that adding a field to the
// ABI struct does not leave this an incomplete initializer.
static const vef_type_t g_params[] = {{VEF_TYPE_STRING, nullptr}};
static vef_signature_t g_signature = {1, g_params, {VEF_TYPE_INT, nullptr}};

static vef_func_desc_t make_valid_func_desc() {
  vef_func_desc_t fd = {};
  fd.protocol = VEF_PROTOCOL_3;
  fd.name = "vdf_ok";
  fd.signature = &g_signature;
  fd.vdf = stub_vdf;
  return fd;
}
static vef_func_desc_t g_func_desc = make_valid_func_desc();

// A well-formed type descriptor. Only the name and the decode buffer size are
// the registration check's business; the rest is the builders'.
static vef_type_desc_t make_valid_type_desc() {
  vef_type_desc_t td = {};
  td.protocol = VEF_PROTOCOL_3;
  td.name = "type_ok";
  td.persisted_length = 16;
  td.max_decode_buffer_length = 256;
  return td;
}
static vef_type_desc_t g_type_desc = make_valid_type_desc();

static vef_func_desc_t *g_funcs[] = {&g_func_desc};
static vef_type_desc_t *g_types[] = {&g_type_desc};

// Arrays whose second element is missing.
static vef_func_desc_t *g_funcs_hole[] = {&g_func_desc, nullptr};
static vef_type_desc_t *g_types_hole[] = {&g_type_desc, nullptr};

class CheckVefRegistrationTest : public ::testing::Test {
 protected:
  // A registration holding exactly one function descriptor, so a test can
  // damage a copy of the valid descriptor and see what the check makes of it.
  static vef_registration_t with_one_func(vef_func_desc_t *desc,
                                          vef_func_desc_t **slot) {
    *slot = desc;
    vef_registration_t reg = {};
    reg.protocol = VEF_PROTOCOL_3;
    reg.func_count = 1;
    reg.funcs = slot;
    return reg;
  }

  // The same for a single type descriptor.
  static vef_registration_t with_one_type(vef_type_desc_t *desc,
                                          vef_type_desc_t **slot) {
    *slot = desc;
    vef_registration_t reg = {};
    reg.protocol = VEF_PROTOCOL_3;
    reg.type_count = 1;
    reg.types = slot;
    return reg;
  }
};

// ---------------------------------------------------------------------------
// Counts and array pointers
// ---------------------------------------------------------------------------

// An extension that registers nothing leaves both arrays as nullptr, which is
// well formed as long as the counts agree.
TEST_F(CheckVefRegistrationTest, EmptyRegistrationIsWellFormed) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
  EXPECT_TRUE(error.empty());
}

TEST_F(CheckVefRegistrationTest, PopulatedRegistrationIsWellFormed) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.func_count = 1;
  reg.funcs = g_funcs;
  reg.type_count = 1;
  reg.types = g_types;

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
  EXPECT_TRUE(error.empty());
}

TEST_F(CheckVefRegistrationTest, NullFuncsArrayWithNonZeroCountIsError) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.func_count = 2;
  reg.funcs = nullptr;

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("funcs"), std::string::npos) << error;
  EXPECT_NE(error.find("2"), std::string::npos) << error;
}

TEST_F(CheckVefRegistrationTest, NullTypesArrayWithNonZeroCountIsError) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.type_count = 3;
  reg.types = nullptr;

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("types"), std::string::npos) << error;
  EXPECT_NE(error.find("3"), std::string::npos) << error;
}

// A valid funcs array must not mask a nullptr types array.
TEST_F(CheckVefRegistrationTest, NullTypesArrayIsCaughtAfterValidFuncs) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.func_count = 1;
  reg.funcs = g_funcs;
  reg.type_count = 1;
  reg.types = nullptr;

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("types"), std::string::npos) << error;
}

// A zero count with a non-nullptr array is harmless: nothing is indexed.
TEST_F(CheckVefRegistrationTest, ZeroCountWithNonNullArrayIsWellFormed) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.func_count = 0;
  reg.funcs = g_funcs;
  reg.type_count = 0;
  reg.types = g_types;

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
}

// A v1 extension never sets the capability fields; the check must not care.
TEST_F(CheckVefRegistrationTest, ProtocolV1RegistrationIsWellFormed) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_1;
  reg.type_count = 1;
  reg.types = g_types;

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
}

// ---------------------------------------------------------------------------
// Array elements
// ---------------------------------------------------------------------------

// A nullptr descriptor inside an otherwise valid array: the
// extension_registration system view dereferences these without a check of its
// own.
TEST_F(CheckVefRegistrationTest, NullFuncDescriptorIsError) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.func_count = 2;
  reg.funcs = g_funcs_hole;

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("func descriptor"), std::string::npos) << error;
  EXPECT_NE(error.find("index 1"), std::string::npos) << error;
}

TEST_F(CheckVefRegistrationTest, NullTypeDescriptorIsError) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.type_count = 2;
  reg.types = g_types_hole;

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("type descriptor"), std::string::npos) << error;
  EXPECT_NE(error.find("index 1"), std::string::npos) << error;
}

// A count shorter than the array must not reach the nullptr element past it.
TEST_F(CheckVefRegistrationTest, ElementsPastTheCountAreNotInspected) {
  vef_registration_t reg = {};
  reg.protocol = VEF_PROTOCOL_3;
  reg.func_count = 1;
  reg.funcs = g_funcs_hole;
  reg.type_count = 1;
  reg.types = g_types_hole;

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
  EXPECT_TRUE(error.empty());
}

// ---------------------------------------------------------------------------
// Function descriptor contents
// ---------------------------------------------------------------------------

// An unnamed descriptor has to be reported by slot, since there is nothing
// else to call it.
TEST_F(CheckVefRegistrationTest, FuncWithoutNameIsError) {
  vef_func_desc_t desc = g_func_desc;
  desc.name = nullptr;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("no name"), std::string::npos) << error;
  EXPECT_NE(error.find("index 0"), std::string::npos) << error;
}

// vdf_handler calls this pointer once per row with no nullptr check.
TEST_F(CheckVefRegistrationTest, FuncWithoutVdfPointerIsError) {
  vef_func_desc_t desc = g_func_desc;
  desc.vdf = nullptr;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("vdf_ok"), std::string::npos) << error;
  EXPECT_NE(error.find("vdf function pointer"), std::string::npos) << error;
}

// parse_extension_registration() dereferences signature unconditionally.
TEST_F(CheckVefRegistrationTest, FuncWithoutSignatureIsError) {
  vef_func_desc_t desc = g_func_desc;
  desc.signature = nullptr;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("vdf_ok"), std::string::npos) << error;
  EXPECT_NE(error.find("signature"), std::string::npos) << error;
}

TEST_F(CheckVefRegistrationTest, NullParamsArrayWithNonZeroCountIsError) {
  vef_signature_t sig = {2, nullptr, {VEF_TYPE_INT, nullptr}};
  vef_func_desc_t desc = g_func_desc;
  desc.signature = &sig;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("params array is a nullptr"), std::string::npos)
      << error;
  EXPECT_NE(error.find("2"), std::string::npos) << error;
}

// A varargs signature carries the sentinel as its count and params is nullptr;
// neither may be treated as a length or indexed.
TEST_F(CheckVefRegistrationTest, VarargsSignatureIsWellFormed) {
  vef_signature_t sig = {
      VEF_PARAM_VARARGS, nullptr, {VEF_TYPE_STRING, nullptr}};
  vef_func_desc_t desc = g_func_desc;
  desc.signature = &sig;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
  EXPECT_TRUE(error.empty());
}

TEST_F(CheckVefRegistrationTest, ZeroParamSignatureIsWellFormed) {
  vef_signature_t sig = {0, nullptr, {VEF_TYPE_INT, nullptr}};
  vef_func_desc_t desc = g_func_desc;
  desc.signature = &sig;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
}

// Resolving a CUSTOM param reads custom_type; outside a debug build the only
// thing standing between a nullptr here and the resolver is this check.
TEST_F(CheckVefRegistrationTest, CustomParamWithoutTypeNameIsError) {
  const vef_type_t params[] = {{VEF_TYPE_STRING, nullptr},
                               {VEF_TYPE_CUSTOM, nullptr}};
  vef_signature_t sig = {2, params, {VEF_TYPE_INT, nullptr}};
  vef_func_desc_t desc = g_func_desc;
  desc.signature = &sig;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("param 2"), std::string::npos) << error;
  EXPECT_NE(error.find("CUSTOM"), std::string::npos) << error;
}

TEST_F(CheckVefRegistrationTest, CustomReturnTypeWithoutTypeNameIsError) {
  vef_signature_t sig = {0, nullptr, {VEF_TYPE_CUSTOM, nullptr}};
  vef_func_desc_t desc = g_func_desc;
  desc.signature = &sig;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("returns a CUSTOM type"), std::string::npos) << error;
}

TEST_F(CheckVefRegistrationTest, NamedCustomTypesAreWellFormed) {
  const vef_type_t params[] = {{VEF_TYPE_CUSTOM, "point"}};
  vef_signature_t sig = {1, params, {VEF_TYPE_CUSTOM, "point"}};
  vef_func_desc_t desc = g_func_desc;
  desc.signature = &sig;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
  EXPECT_TRUE(error.empty());
}

// A v1 descriptor leaves every field above buffer_size unset; none of them
// are read here, so it must still pass.
TEST_F(CheckVefRegistrationTest, ProtocolV1FuncDescriptorIsWellFormed) {
  vef_func_desc_t desc = {};
  desc.protocol = VEF_PROTOCOL_1;
  desc.name = "v1_vdf";
  desc.signature = &g_signature;
  desc.vdf = stub_vdf;
  vef_func_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_func(&desc, &slot);

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
}

// ---------------------------------------------------------------------------
// Type descriptor contents
// ---------------------------------------------------------------------------

// RunUpdatePreCheck reads type names to decide which types a target version
// keeps; an unnamed one is silently skipped there and so reads as dropped.
TEST_F(CheckVefRegistrationTest, TypeWithoutNameIsError) {
  vef_type_desc_t desc = g_type_desc;
  desc.name = nullptr;
  vef_type_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_type(&desc, &slot);

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("no name"), std::string::npos) << error;
  EXPECT_NE(error.find("index 0"), std::string::npos) << error;
}

// TypeDecoder allocates a buffer of this size and only asserts it is positive.
TEST_F(CheckVefRegistrationTest, ZeroMaxDecodeBufferLengthIsError) {
  vef_type_desc_t desc = g_type_desc;
  desc.max_decode_buffer_length = 0;
  vef_type_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_type(&desc, &slot);

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("type_ok"), std::string::npos) << error;
  EXPECT_NE(error.find("max_decode_buffer_length"), std::string::npos) << error;
}

// The field is a signed int64 the decoder casts to size_t, so a negative is
// the more dangerous half of the same check.
TEST_F(CheckVefRegistrationTest, NegativeMaxDecodeBufferLengthIsError) {
  vef_type_desc_t desc = g_type_desc;
  desc.max_decode_buffer_length = -1;
  vef_type_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_type(&desc, &slot);

  std::string error;
  EXPECT_TRUE(check_vef_registration(&reg, error));
  EXPECT_NE(error.find("-1"), std::string::npos) << error;
}

// persisted_length's shape is protocol-dependent and belongs to the builders,
// so the registration check must not judge it either way.
TEST_F(CheckVefRegistrationTest, PersistedLengthIsNotJudgedHere) {
  vef_type_desc_t desc = g_type_desc;
  desc.persisted_length = 0;  // a v4 variable-length type declares no footprint
  vef_type_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_type(&desc, &slot);

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
  EXPECT_TRUE(error.empty());
}

// A v1 type descriptor sets no field above hash_func; none is read here.
TEST_F(CheckVefRegistrationTest, ProtocolV1TypeDescriptorIsWellFormed) {
  vef_type_desc_t desc = {};
  desc.protocol = VEF_PROTOCOL_1;
  desc.name = "v1_type";
  desc.persisted_length = 8;
  desc.max_decode_buffer_length = 64;
  vef_type_desc_t *slot = nullptr;
  vef_registration_t reg = with_one_type(&desc, &slot);

  std::string error;
  EXPECT_FALSE(check_vef_registration(&reg, error));
}

TEST_F(CheckVefRegistrationTest, NullRegistrationIsError) {
  std::string error;
  EXPECT_TRUE(check_vef_registration(nullptr, error));
  EXPECT_FALSE(error.empty());
}

}  // namespace villagesql_unittest
