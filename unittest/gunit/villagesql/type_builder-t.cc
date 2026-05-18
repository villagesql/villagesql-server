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

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <type_traits>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/func_builder.h>
#include <villagesql/vsql/type_builder.h>

namespace villagesql_unittest {

// =============================================================================
// Minimal stubs for test types
// =============================================================================

struct TestParams {
  int64_t n;
  static TestParams parse(const std::map<std::string, std::string> &m) {
    auto it = m.find("n");
    return {it != m.end() ? std::stoll(it->second) : 0};
  }
  static void to_strings(const TestParams &p,
                         std::map<std::string, std::string> &out) {
    out["n"] = std::to_string(p.n);
  }
};

// Non-parameterized operation stubs
static void plain_encode(std::string_view, vsql::CustomResult out) {
  out.set_null();
}
static void plain_decode(vsql::CustomArg, vsql::StringResult out) {
  out.set_null();
}
static int plain_compare(vsql::CustomArg, vsql::CustomArg) { return 0; }
static size_t plain_hash(vsql::CustomArg) { return 0; }

// Parameterized operation stubs
static void params_encode(vsql::MaybeParams<TestParams> &, std::string_view,
                          vsql::CustomResult out) {
  out.set_null();
}
static void params_decode(vsql::CustomArgWith<TestParams>,
                          vsql::StringResult out) {
  out.set_null();
}
static int params_compare(vsql::CustomArgWith<TestParams>,
                          vsql::CustomArgWith<TestParams>) {
  return 0;
}
static size_t params_hash(vsql::CustomArgWith<TestParams>) { return 0; }

static constexpr const char kPlainName[] = "TESTPLAIN";
static constexpr const char kParamsName[] = "TESTPARAMS";

// =============================================================================
// TypeOpParamsType trait tests
// =============================================================================
//
// These static_asserts verify that the trait correctly identifies whether a
// type operation function is parameterized, and if so what params type it uses.

namespace trait_tests {

// Non-parameterized: all four operations should yield void.
static_assert(std::is_void_v<vsql::func_builder::TypeOpParamsType<
                  vsql::func_builder::TypeEncodeFunc>::type>);
static_assert(std::is_void_v<vsql::func_builder::TypeOpParamsType<
                  vsql::func_builder::TypeDecodeFunc>::type>);
static_assert(std::is_void_v<vsql::func_builder::TypeOpParamsType<
                  vsql::func_builder::TypeCompareFunc>::type>);
static_assert(std::is_void_v<vsql::func_builder::TypeOpParamsType<
                  vsql::func_builder::TypeHashFunc>::type>);

// Parameterized: all four operations should yield the params type.
static_assert(
    std::is_same_v<
        vsql::func_builder::TypeOpParamsType<
            vsql::func_builder::TypeEncodeWithParamsFunc<TestParams>>::type,
        TestParams>);
static_assert(
    std::is_same_v<
        vsql::func_builder::TypeOpParamsType<
            vsql::func_builder::TypeDecodeWithParamsFunc<TestParams>>::type,
        TestParams>);
static_assert(
    std::is_same_v<
        vsql::func_builder::TypeOpParamsType<
            vsql::func_builder::TypeCompareWithParamsFunc<TestParams>>::type,
        TestParams>);
static_assert(std::is_same_v<
              vsql::func_builder::TypeOpParamsType<
                  vsql::func_builder::TypeHashWithParamsFunc<TestParams>>::type,
              TestParams>);

// Concrete function pointers: verify trait works on actual function addresses.
static_assert(
    std::is_void_v<
        vsql::func_builder::TypeOpParamsType<decltype(&plain_encode)>::type>);
static_assert(
    std::is_same_v<
        vsql::func_builder::TypeOpParamsType<decltype(&params_encode)>::type,
        TestParams>);
static_assert(
    std::is_same_v<
        vsql::func_builder::TypeOpParamsType<decltype(&params_decode)>::type,
        TestParams>);
static_assert(
    std::is_same_v<
        vsql::func_builder::TypeOpParamsType<decltype(&params_compare)>::type,
        TestParams>);
static_assert(
    std::is_same_v<
        vsql::func_builder::TypeOpParamsType<decltype(&params_hash)>::type,
        TestParams>);

}  // namespace trait_tests

// =============================================================================
// TypeBuilder positive-path tests
// =============================================================================

class TypeBuilderTest : public ::testing::Test {};

// Non-parameterized type: from_string/to_string/compare with plain functions.
TEST_F(TypeBuilderTest, NonParameterizedBuildsCorrectly) {
  constexpr auto obj = vsql::make_type<kPlainName>()
                           .persisted_length(8)
                           .max_decode_buffer_length(64)
                           .from_string<&plain_encode>()
                           .to_string<&plain_decode>()
                           .compare<&plain_compare>()
                           .build();

  EXPECT_STREQ(obj.name(), "TESTPLAIN");
  EXPECT_EQ(obj.params_init_fn, nullptr);
  EXPECT_EQ(obj.params_to_strings_init_fn, nullptr);
}

// Non-parameterized type with optional hash.
TEST_F(TypeBuilderTest, NonParameterizedWithHash) {
  constexpr auto obj = vsql::make_type<kPlainName>()
                           .persisted_length(8)
                           .max_decode_buffer_length(64)
                           .from_string<&plain_encode>()
                           .to_string<&plain_decode>()
                           .compare<&plain_compare>()
                           .hash<&plain_hash>()
                           .build();

  EXPECT_STREQ(obj.name(), "TESTPLAIN");
  EXPECT_EQ(obj.params_init_fn, nullptr);
}

// Parameterized type: params() before operations.
TEST_F(TypeBuilderTest, ParameterizedBuildsCorrectly) {
  auto obj =
      vsql::make_type<kParamsName>()
          .max_persisted_length(128)
          .params<TestParams, &TestParams::parse, &TestParams::to_strings>()
          .from_string<&params_encode>()
          .to_string<&params_decode>()
          .compare<&params_compare>()
          .build();

  EXPECT_STREQ(obj.name(), "TESTPARAMS");
  EXPECT_NE(obj.params_init_fn, nullptr);
  EXPECT_NE(obj.params_to_strings_init_fn, nullptr);
}

// Parameterized type with optional hash.
TEST_F(TypeBuilderTest, ParameterizedWithHash) {
  auto obj =
      vsql::make_type<kParamsName>()
          .max_persisted_length(128)
          .params<TestParams, &TestParams::parse, &TestParams::to_strings>()
          .from_string<&params_encode>()
          .to_string<&params_decode>()
          .compare<&params_compare>()
          .hash<&params_hash>()
          .build();

  EXPECT_STREQ(obj.name(), "TESTPARAMS");
  EXPECT_NE(obj.params_init_fn, nullptr);
}

// Operations can be registered in any order after params().
TEST_F(TypeBuilderTest, ParameterizedOperationsAnyOrder) {
  auto obj =
      vsql::make_type<kParamsName>()
          .max_persisted_length(128)
          .params<TestParams, &TestParams::parse, &TestParams::to_strings>()
          .compare<&params_compare>()
          .to_string<&params_decode>()
          .from_string<&params_encode>()
          .build();

  EXPECT_STREQ(obj.name(), "TESTPARAMS");
  EXPECT_NE(obj.params_init_fn, nullptr);
}

// =============================================================================
// Negative-path documentation
// =============================================================================
//
// The following cases are rejected at compile time by static_assert. They are
// listed here as documentation; to verify one, move it outside the #if 0 and
// confirm the expected error message appears.
//
// (1) Parameterized function without .params<P>() first:
//
//   vsql::make_type<kPlainName>()
//       .from_string<&params_encode>()  // ERROR: OpP=TestParams,
//       ParamsType=void
//       ...
//
// (2) Non-parameterized function after .params<P>():
//
//   vsql::make_type<kParamsName>()
//       .params<TestParams, ...>()
//       .from_string<&plain_encode>()   // ERROR: OpP=void,
//       ParamsType=TestParams
//       ...
//
// (3) .params<P>() after operations already registered:
//
//   vsql::make_type<kParamsName>()
//       .from_string<&plain_encode>()
//       .params<TestParams, ...>()      // ERROR: HasFromString=true
//       ...
//
// (4) int_to_params without .params<P>():
//
//   vsql::make_type<kPlainName>()
//       .int_to_params<&some_fn>()
//       .from_string<...>().to_string<...>().compare<...>()
//       .build()                        // ERROR at build():
//       !std::is_void_v<ParamsType> fails

}  // namespace villagesql_unittest
