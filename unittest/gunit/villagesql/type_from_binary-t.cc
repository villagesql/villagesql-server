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

// Tests for the optional from_binary type function: that the builder records
// it and raises the required protocol, that it survives the descriptor ->
// context -> EncodeOp binding, and that a type without one is unaffected.
//
// The charset-driven selection between from_binary and the string converter
// lives in EncodeStringForField, which needs a Field and a THD; that path is
// covered by the mysql-test suite rather than here.

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <optional>
#include <string>

#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/sdk/include/villagesql/vsql/type_builder.h"

namespace villagesql_unittest {

namespace {

bool dummy_encode(unsigned char *, size_t, const char *, size_t,
                  size_t *length) {
  *length = 0;
  return false;
}

bool dummy_decode(const unsigned char *, size_t, char *, size_t,
                  size_t *to_length) {
  *to_length = 0;
  return false;
}

int dummy_compare(const unsigned char *, size_t, const unsigned char *,
                  size_t) {
  return 0;
}

// Reads the "n" parameter and requires from_len to be exactly n bytes, so the
// tests can show that a parameterized type validates against the target rather
// than inferring a size from the payload.
bool n_bytes_from_binary(unsigned char *buffer, size_t buffer_size,
                         const unsigned char *from, size_t from_len,
                         const vef_type_params_t *params, size_t *length) {
  int64_t n = -1;
  for (unsigned int i = 0; i < params->count; ++i) {
    if (std::strcmp(params->keys[i], "n") == 0) {
      n = std::stoll(params->values[i]);
      break;
    }
  }
  if (n < 0) return true;
  if (from_len != static_cast<size_t>(n)) return true;
  if (buffer_size < from_len) return true;
  std::memcpy(buffer, from, from_len);
  *length = from_len;
  return false;
}

villagesql::TypeDescriptor make_desc(
    const char *name, vef_protocol_t protocol,
    vef_from_binary_func_t from_binary = nullptr) {
  villagesql::EncodeFunction encode(dummy_encode);
  if (from_binary != nullptr) encode.set_from_binary_fn(from_binary);
  return villagesql::TypeDescriptor(
      villagesql::TypeDescriptorKey(name, "test_ext", "1.0.0"), protocol,
      /*impl_type=*/1, /*persisted_len=*/8, /*max_unpersisted_len=*/256,
      /*max_persisted_len=*/0, villagesql::LengthKind::Fixed, std::move(encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt);
}

}  // namespace

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

namespace builder_types {

void from_string_fn(std::string_view, vsql::CustomResult) {}
void to_string_fn(vsql::CustomArg, vsql::StringResult) {}
int compare_fn(vsql::CustomArg, vsql::CustomArg) { return 0; }

bool from_binary_fn(unsigned char *, size_t, const unsigned char *, size_t,
                    const vef_type_params_t *, size_t *length) {
  *length = 0;
  return false;
}

constexpr const char kPlain[] = "PLAIN";
constexpr const char kBin[] = "BIN";

constexpr auto kPlainType = vsql::make_type<kPlain>()
                                .persisted_length(8)
                                .max_decode_buffer_length(64)
                                .from_string<from_string_fn>()
                                .to_string<to_string_fn>()
                                .compare<compare_fn>()
                                .build();

constexpr auto kBinType = vsql::make_type<kBin>()
                              .persisted_length(8)
                              .max_decode_buffer_length(64)
                              .from_string<from_string_fn>()
                              .to_string<to_string_fn>()
                              .compare<compare_fn>()
                              .from_binary(from_binary_fn)
                              .build();

}  // namespace builder_types

// A type that does not ask for from_binary leaves the field null and stays at
// the protocol its other features require.
TEST(TypeFromBinaryBuilderTest, AbsentByDefault) {
  static_assert(
      builder_types::kPlainType.descriptor.vef_desc.from_binary_func ==
      nullptr);
  static_assert(builder_types::kPlainType.descriptor.vef_desc.protocol <
                VEF_PROTOCOL_4);
  SUCCEED();
}

// Setting it records the pointer and raises the required protocol to 4, since
// the server only reads the field at protocol >= 4.
TEST(TypeFromBinaryBuilderTest, SetRecordsFnAndRaisesProtocol) {
  static_assert(builder_types::kBinType.descriptor.vef_desc.from_binary_func ==
                builder_types::from_binary_fn);
  static_assert(builder_types::kBinType.descriptor.vef_desc.protocol >=
                VEF_PROTOCOL_4);
  SUCCEED();
}

// ---------------------------------------------------------------------------
// ABI layout
// ---------------------------------------------------------------------------

// from_binary_func is appended last, so adding it shifts no existing field.
// A v3-built extension's shorter struct therefore stays readable, which is
// what makes the field safe to add without a protocol roll.
TEST(TypeFromBinaryAbiTest, FieldIsAppendedAfterEveryOlderField) {
  EXPECT_GT(offsetof(vef_type_desc_t, from_binary_func),
            offsetof(vef_type_desc_t, variable_length));
  EXPECT_GT(offsetof(vef_type_desc_t, from_binary_func),
            offsetof(vef_type_desc_t, max_persisted_length));
  EXPECT_GT(offsetof(vef_type_desc_t, from_binary_func),
            offsetof(vef_type_desc_t, encode_func));
  // Nothing may live between it and the end of the struct.
  EXPECT_EQ(offsetof(vef_type_desc_t, from_binary_func) +
                sizeof(vef_from_binary_func_t),
            sizeof(vef_type_desc_t));
}

// ---------------------------------------------------------------------------
// Descriptor -> TypeContext -> EncodeOp binding
// ---------------------------------------------------------------------------

class FromBinaryBindingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }
};

// The EncodeOp a context binds carries the descriptor's from_binary pointer
// alongside the string converter, so TypeEncoder can reach it.
TEST_F(FromBinaryBindingTest, EncodeOpCarriesFromBinaryFn) {
  villagesql::TypeDescriptor desc =
      make_desc("BIN", VEF_PROTOCOL_4, n_bytes_from_binary);
  villagesql::TypeContext ctx(
      villagesql::TypeContextKey("BIN", "test_ext", "1.0.0"), &desc);

  EXPECT_EQ(ctx.encode_op().from_binary_fn(), &n_bytes_from_binary);
  // The string converter is untouched; the two coexist.
  EXPECT_EQ(ctx.encode_op().fn(), &dummy_encode);
}

// A type without from_binary binds a null pointer, which is what makes
// TypeEncoder::has_from_binary() false and keeps every value on the string
// converter, exactly as before the field existed.
TEST_F(FromBinaryBindingTest, EncodeOpFromBinaryFnNullWhenAbsent) {
  villagesql::TypeDescriptor desc = make_desc("PLAIN", VEF_PROTOCOL_3);
  villagesql::TypeContext ctx(
      villagesql::TypeContextKey("PLAIN", "test_ext", "1.0.0"), &desc);

  EXPECT_EQ(ctx.encode_op().from_binary_fn(), nullptr);
  EXPECT_EQ(ctx.encode_op().fn(), &dummy_encode);
}

// ---------------------------------------------------------------------------
// The converter sees the target's parameters
// ---------------------------------------------------------------------------

// The point of passing vef_type_params_t: a parameterized type validates the
// payload against the target instead of inferring a size from from_len. The
// helper accepts exactly "n" bytes, so a short or long payload is an error
// rather than a value silently taken at another size.
TEST_F(FromBinaryBindingTest, ConverterValidatesAgainstTargetParams) {
  villagesql::TypeParameters params("n=4");
  const vef_type_params_t abi_params{params.count(), params.key_data(),
                                     params.value_data()};

  unsigned char buf[8] = {};
  const unsigned char four[] = {1, 2, 3, 4};
  size_t len = 0;

  EXPECT_FALSE(n_bytes_from_binary(buf, sizeof(buf), four, sizeof(four),
                                   &abi_params, &len));
  EXPECT_EQ(len, 4u);
  EXPECT_EQ(std::memcmp(buf, four, sizeof(four)), 0);

  // Three bytes is not a valid n=4 value, even though it would be a valid
  // value at n=3. Without the params the converter could only divide from_len
  // and would accept it.
  EXPECT_TRUE(
      n_bytes_from_binary(buf, sizeof(buf), four, 3, &abi_params, &len));
  // Five likewise.
  const unsigned char five[] = {1, 2, 3, 4, 5};
  EXPECT_TRUE(n_bytes_from_binary(buf, sizeof(buf), five, sizeof(five),
                                  &abi_params, &len));
}

// A type with no parameters gets count == 0 rather than a null params pointer,
// so a converter may always dereference it.
TEST_F(FromBinaryBindingTest, UnparameterizedTargetPassesEmptyParams) {
  villagesql::TypeDescriptor desc =
      make_desc("BIN", VEF_PROTOCOL_4, n_bytes_from_binary);
  villagesql::TypeContext ctx(
      villagesql::TypeContextKey("BIN", "test_ext", "1.0.0"), &desc);

  EXPECT_TRUE(ctx.parameters().empty());
  EXPECT_EQ(ctx.parameters().count(), 0u);
}

}  // namespace villagesql_unittest
