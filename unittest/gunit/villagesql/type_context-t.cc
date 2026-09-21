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
#include <memory>
#include <optional>
#include <string>
#include <utility>

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
static size_t dummy_hash(const unsigned char *, size_t) { return 42; }

// Encode that always produces a fixed 2-byte value, regardless of input. Models
// a variable-length type (e.g. BITFIELD) whose empty default encodes to a small
// non-empty value (a header). Returns false (success).
static bool two_byte_encode(unsigned char *out, size_t out_cap, const char *,
                            size_t, size_t *written) {
  if (out_cap < 2) {
    ADD_FAILURE() << "out_cap too small: " << out_cap;
    return true;  // false == success, so true == failure
  }
  out[0] = 0xAB;
  out[1] = 0xCD;
  *written = 2;
  return false;
}

// Encode that produces an empty (zero-byte) value. Models a variable-length
// type whose empty default is genuinely zero bytes (no usable default).
static bool empty_encode(unsigned char *, size_t, const char *, size_t,
                         size_t *written) {
  *written = 0;
  return false;
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

// resolve_params reporting the same sizes for every parameterization. Used
// where a test cares about display or dispatch rather than sizing, and where
// deriving sizes from the parameter value would overflow on the deliberately
// extreme values below.
static void resolve_params_fixed_sizes_vdf(vef_context_t * /*ctx*/,
                                           vef_vdf_args_t * /*args*/,
                                           vef_vdf_result_t *result) {
  int written = snprintf(result->str_buf, result->max_str_len, "32,256");
  result->type = VEF_RESULT_VALUE;
  result->actual_len = static_cast<size_t>(written);
}

// resolve_params that succeeds but resolves a zero max_decode_buffer_length,
// which is rejected because it sizes the decode scratch buffer.
static void resolve_params_zero_decode_vdf(vef_context_t * /*ctx*/,
                                           vef_vdf_args_t * /*args*/,
                                           vef_vdf_result_t *result) {
  int written = snprintf(result->str_buf, result->max_str_len, "32,0");
  result->type = VEF_RESULT_VALUE;
  result->actual_len = static_cast<size_t>(written);
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
          nullptr,
          0};
}

// resolve_params using the mutating overload: when the caller supplied only a
// dimension it fills in the "metric=l2" default and reports the rewritten
// canonical string through the trailing rewritten-params section of the VDF
// output ("persisted,decode,<byte-len>,<params>").
static void resolve_params_fills_metric_vdf(vef_context_t * /*ctx*/,
                                            vef_vdf_args_t *args,
                                            vef_vdf_result_t *result) {
  std::string input(args->values[0]->str_value, args->values[0]->str_len);
  std::string rewritten = input;
  if (input.find("metric=") == std::string::npos) rewritten += ",metric=l2";
  int written = snprintf(result->str_buf, result->max_str_len, "32,256,%zu,%s",
                         rewritten.size(), rewritten.c_str());
  result->type = VEF_RESULT_VALUE;
  result->actual_len = static_cast<size_t>(written);
}

// int_to_params: maps TYPE(N) to "dimension=N", a common use case where the
// integer names the type's first (and sometimes only) parameter.
static void int_to_dimension_vdf(vef_context_t * /*ctx*/, vef_vdf_args_t *args,
                                 vef_vdf_result_t *result) {
  int written = snprintf(result->str_buf, result->max_str_len, "dimension=%lld",
                         args->values[0]->int_value);
  result->type = VEF_RESULT_VALUE;
  result->actual_len = static_cast<size_t>(written);
}

// int_to_params where the integer names the *second* parameter: TYPE(N) means
// scale=N, and the dimension is intrinsic to the type. Exercises the loop that
// tries each integer-valued parameter rather than only the first.
static void int_to_scale_vdf(vef_context_t * /*ctx*/, vef_vdf_args_t *args,
                             vef_vdf_result_t *result) {
  int written = snprintf(result->str_buf, result->max_str_len,
                         "dimension=8,scale=%lld", args->values[0]->int_value);
  result->type = VEF_RESULT_VALUE;
  result->actual_len = static_cast<size_t>(written);
}

// int_to_params that always fails, so no shorthand candidate is produced.
static void int_to_params_fail_vdf(vef_context_t * /*ctx*/,
                                   vef_vdf_args_t * /*args*/,
                                   vef_vdf_result_t *result) {
  result->type = VEF_RESULT_ERROR;
  snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "no shorthand for this type");
}

// Build a mock vef_func_desc_t for an int_to_params VDF wrapper: (INT)->STRING.
static vef_func_desc_t make_int_to_params_fd(const char *name,
                                             vef_vdf_func_t vdf) {
  static vef_type_t itp_param = {VEF_TYPE_INT, nullptr};
  static vef_signature_t itp_sig = {1, &itp_param, {VEF_TYPE_STRING, nullptr}};
  return {VEF_PROTOCOL_3,
          name,
          &itp_sig,
          vdf,
          nullptr,
          nullptr,
          VEF_MAX_TYPE_PARAMS_STRING_LEN,
          false,
          nullptr,
          nullptr,
          0};
}

// intrinsic_default VDF: returns the string the server then encodes. Takes no
// arguments; type parameters arrive through result->type_params.
static void intrinsic_default_ab_vdf(vef_context_t * /*ctx*/,
                                     vef_vdf_args_t * /*args*/,
                                     vef_vdf_result_t *result) {
  static constexpr char kDefault[] = "AB";
  static constexpr size_t kLen = sizeof(kDefault) - 1;
  if (result->max_str_len < kLen) {
    result->type = VEF_RESULT_ERROR;
    snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "result buffer too small");
    return;
  }
  memcpy(result->str_buf, kDefault, kLen);
  result->type = VEF_RESULT_VALUE;
  result->actual_len = kLen;
}

// intrinsic_default VDF that reports an error.
static void intrinsic_default_fail_vdf(vef_context_t * /*ctx*/,
                                       vef_vdf_args_t * /*args*/,
                                       vef_vdf_result_t *result) {
  result->type = VEF_RESULT_ERROR;
  snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "no default for this type");
}

// Build a mock vef_func_desc_t for an intrinsic_default VDF: ()->STRING.
static vef_func_desc_t make_intrinsic_default_fd(const char *name,
                                                 vef_vdf_func_t vdf) {
  static vef_signature_t id_sig = {0, nullptr, {VEF_TYPE_STRING, nullptr}};
  return {VEF_PROTOCOL_3,
          name,
          &id_sig,
          vdf,
          nullptr,
          nullptr,
          VEF_MAX_TYPE_PARAMS_STRING_LEN,
          false,
          nullptr,
          nullptr,
          0};
}

// from_string (encode) VDF counterpart of two_byte_encode. It delegates so the
// two paths cannot drift: a test that runs both is then comparing dispatch,
// not two hand-written encoders that happen to agree.
static void two_byte_encode_vdf(vef_context_t * /*ctx*/, vef_vdf_args_t *args,
                                vef_vdf_result_t *result) {
  const char *in = args->values[0]->str_value;
  size_t in_len = args->values[0]->str_len;
  size_t written = 0;
  if (two_byte_encode(result->bin_buf, result->max_bin_len, in, in_len,
                      &written)) {
    result->type = VEF_RESULT_ERROR;
    snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "encode failed");
    return;
  }
  result->type = VEF_RESULT_VALUE;
  result->actual_len = written;
}

// from_string (encode) VDF that reports an error.
static void encode_fail_vdf(vef_context_t * /*ctx*/, vef_vdf_args_t * /*args*/,
                            vef_vdf_result_t *result) {
  result->type = VEF_RESULT_ERROR;
  snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "cannot encode that input");
}

// Build a mock vef_func_desc_t for a from_string VDF: (STRING)->CUSTOM.
static vef_func_desc_t make_encode_fd(const char *name, vef_vdf_func_t vdf) {
  static vef_type_t enc_param = {VEF_TYPE_STRING, nullptr};
  static vef_signature_t enc_sig = {1, &enc_param, {VEF_TYPE_CUSTOM, nullptr}};
  return {VEF_PROTOCOL_3, name,    &enc_sig, vdf, nullptr, nullptr, 0,
          false,          nullptr, nullptr,  0};
}

// Encode that echoes its input, so a test can assert which string the
// intrinsic-default path fed to encode. Returns false (success).
static bool echo_encode(unsigned char *out, size_t out_cap, const char *in,
                        size_t in_len, size_t *written) {
  if (in_len > out_cap) {
    ADD_FAILURE() << "input length " << in_len << " exceeds out_cap "
                  << out_cap;
    return true;  // false == success, so true == failure
  }
  memcpy(out, in, in_len);
  *written = in_len;
  return false;
}

// Encode that always reports failure.
static bool failing_encode(unsigned char *, size_t, const char *, size_t,
                           size_t *) {
  return true;
}

// Encode that writes nothing but claims one byte more than the buffer holds.
// Models a misbehaving extension: the length check must reject the claim
// rather than trust it and read past the buffer.
static bool over_reporting_encode(unsigned char *, size_t out_cap, const char *,
                                  size_t, size_t *written) {
  *written = out_cap + 1;
  return false;
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

// A token with no '=' is kept with an empty value rather than dropped, so the
// parser can reject the valueless parameter instead of silently ignoring it.
TEST_F(TypeParametersTest, FromRawKeepsValuelessToken) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_raw("dimension=1536,metric");
  EXPECT_EQ(params.str(), "dimension=1536,metric=");
  ASSERT_EQ(params.count(), 2u);
  EXPECT_STREQ(params.key_data()[1], "metric");
  EXPECT_STREQ(params.value_data()[1], "");
}

TEST_F(TypeParametersTest, FromRawKeepsLoneValuelessToken) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_raw(" Metric ");
  EXPECT_EQ(params.str(), "metric=");
}

TEST_F(TypeParametersTest, FromRawKeepsEmptyValue) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_raw("dimension=");
  EXPECT_EQ(params.str(), "dimension=");
}

// Duplicates survive canonicalization (sorted next to each other) so the parser
// can see and reject them.
TEST_F(TypeParametersTest, FromRawKeepsDuplicateKeys) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_raw("Dimension=3,dimension=4");
  ASSERT_EQ(params.count(), 2u);
  EXPECT_STREQ(params.key_data()[0], "dimension");
  EXPECT_STREQ(params.key_data()[1], "dimension");
}

// A leading, doubled or trailing comma all yield a nameless entry (sorting
// first), which the parser rejects.
TEST_F(TypeParametersTest, FromRawCommaHandling) {
  EXPECT_EQ(villagesql::TypeParameters::from_raw("dimension=3,").str(),
            "=,dimension=3");
  EXPECT_EQ(villagesql::TypeParameters::from_raw(",dimension=3").str(),
            "=,dimension=3");
  EXPECT_EQ(
      villagesql::TypeParameters::from_raw("dimension=3,,metric=l2").str(),
      "=,dimension=3,metric=l2");
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

// to_json()/from_json() are the on-disk form of a column's type parameters:
// they are what villagesql.custom_columns.type_parameters holds, so a change
// in either direction changes how existing tables read back.
TEST_F(TypeParametersTest, ToJsonEmptyIsEmptyObject) {
  EXPECT_EQ(villagesql::TypeParameters().to_json(), "{}");
}

// Every value is quoted, numeric-looking ones included, because from_json()
// only accepts quoted values. params_to_json() quotes index parameters for the
// same reason.
TEST_F(TypeParametersTest, ToJsonSingleEntry) {
  villagesql::TypeParameters params("dimension=1536");
  EXPECT_EQ(params.to_json(), R"({"dimension":"1536"})");
}

TEST_F(TypeParametersTest, ToJsonMultipleEntries) {
  villagesql::TypeParameters params("dimension=1536,metric=cosine");
  EXPECT_EQ(params.to_json(), R"({"dimension":"1536","metric":"cosine"})");
}

TEST_F(TypeParametersTest, ToJsonKeepsEmptyValue) {
  villagesql::TypeParameters params("metric=");
  EXPECT_EQ(params.to_json(), R"({"metric":""})");
}

// A canonical string is always "k=v,..."; a bare token with no '=' can only be
// built by the explicit constructor, and to_json() drops it rather than
// emitting a key with no value.
TEST_F(TypeParametersTest, ToJsonDropsTokenWithoutSeparator) {
  villagesql::TypeParameters params("metric");
  EXPECT_EQ(params.to_json(), "{}");
}

// '=' inside a value survives: the canonical form splits on the first '=' only,
// and to_json() does the same.
TEST_F(TypeParametersTest, ToJsonKeepsEqualsInsideValue) {
  villagesql::TypeParameters params("a=x=y");
  EXPECT_EQ(params.to_json(), R"({"a":"x=y"})");
}

TEST_F(TypeParametersTest, FromJsonEmptyForms) {
  EXPECT_TRUE(villagesql::TypeParameters::from_json("").empty());
  EXPECT_TRUE(villagesql::TypeParameters::from_json("{}").empty());
}

TEST_F(TypeParametersTest, FromJsonSingleEntry) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_json(R"({"dimension":"1536"})");
  EXPECT_EQ(params.str(), "dimension=1536");
}

// from_json() re-canonicalizes through from_raw(), so a row written with keys
// out of order or in mixed case still loads as the canonical form.
TEST_F(TypeParametersTest, FromJsonCanonicalizesOrderAndCase) {
  villagesql::TypeParameters params = villagesql::TypeParameters::from_json(
      R"({"metric":"COSINE","DIMENSION":"1536"})");
  EXPECT_EQ(params.str(), "dimension=1536,metric=cosine");
}

TEST_F(TypeParametersTest, FromJsonToleratesWhitespace) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_json(R"({ "dimension" : "1536" })");
  EXPECT_EQ(params.str(), "dimension=1536");
}

// The parser skips over ':' rather than requiring it, so a missing colon is
// accepted. This is due to the parser's behavior.
TEST_F(TypeParametersTest, FromJsonAcceptsMissingColon) {
  villagesql::TypeParameters params =
      villagesql::TypeParameters::from_json(R"({"a""1"})");
  EXPECT_EQ(params.str(), "a=1");
}

// Round trip is the property the storage layer depends on.
TEST_F(TypeParametersTest, JsonRoundTripsCanonicalParams) {
  for (const char *canonical :
       {"", "dimension=1536", "dimension=1536,metric=cosine",
        "metric=", "dimension=3,metric=", "a=x=y"}) {
    villagesql::TypeParameters original(canonical);
    villagesql::TypeParameters restored =
        villagesql::TypeParameters::from_json(original.to_json());
    EXPECT_EQ(restored, original) << "canonical form: " << canonical;
    EXPECT_EQ(restored.str(), original.str());
  }
}

// A value the parser cannot read stops the parse, and everything after it is
// dropped with no diagnostic. Dropping one that determines storage size is
// caught later by CheckFieldLengthMatchesType; dropping one that does not is
// not caught at all.
//
// TODO(villagesql-general): from_json() should reject malformed input rather
// than reading what it can and dropping the rest, so a column cannot load
// with fewer parameters than it holds. It is lenient in the other direction
// too, accepting input a JSON parser would refuse, such as a missing ':'.
TEST_F(TypeParametersTest, FromJsonStopsAtUnquotedValue) {
  // Non-string value in the only pair: nothing is recovered.
  EXPECT_TRUE(
      villagesql::TypeParameters::from_json(R"({"dimension":1536})").empty());

  // Non-string value in a later pair: the earlier pairs survive, the rest do
  // not.
  EXPECT_EQ(villagesql::TypeParameters::from_json(R"({"a":"1","b":2})").str(),
            "a=1");
}

TEST_F(TypeParametersTest, FromJsonReturnsEmptyForMalformedInput) {
  // No object at all.
  EXPECT_TRUE(villagesql::TypeParameters::from_json(R"("a":"1")").empty());
  // Unterminated key.
  EXPECT_TRUE(villagesql::TypeParameters::from_json(R"({"a)").empty());
  // Unterminated value.
  EXPECT_TRUE(villagesql::TypeParameters::from_json(R"({"a":"1)").empty());
}

// to_json() writes values verbatim, so a value containing a double quote
// produces JSON that from_json() cannot read back: the value is truncated at
// the embedded quote and every later pair is dropped. Reachable from SQL via
// TYPE('a=x"y'), though the malformed JSON is then rejected by the
// type_parameters column, so today this surfaces as a confusing JSON parse
// error rather than a bad row.
//
// TODO(villagesql-production): escape '"' and '\' in to_json() and unescape
// them in from_json(), the way params_to_json() already does for index
// parameters. Until then a value's legal characters are limited by the
// serialization rather than by what the extension accepts, which is a
// restriction extension authors have no way to discover. The expectations
// below should become a round trip validation once it is fixed.
TEST_F(TypeParametersTest, JsonDoesNotEscapeQuotesInValues) {
  villagesql::TypeParameters original(R"(a=x"y)");
  EXPECT_EQ(original.to_json(), R"({"a":"x"y"})");

  villagesql::TypeParameters restored =
      villagesql::TypeParameters::from_json(original.to_json());
  EXPECT_EQ(restored.str(), "a=x");
  EXPECT_FALSE(restored == original);
}

// key_data()/value_data() hand the ABI arrays of const char* that point into
// this object's own strings, so every copy and move has to rebuild them. The
// tests below read through those accessors afterwards, which is the only way
// the rebuild is observable.
TEST_F(TypeParametersTest, CopyConstructorRebuildsAbiPointers) {
  villagesql::TypeParameters source("dimension=1536,metric=cosine");
  villagesql::TypeParameters copy(source);

  ASSERT_EQ(copy.count(), 2u);
  EXPECT_STREQ(copy.key_data()[0], "dimension");
  EXPECT_STREQ(copy.value_data()[0], "1536");
  EXPECT_STREQ(copy.key_data()[1], "metric");
  EXPECT_STREQ(copy.value_data()[1], "cosine");

  // The copy must point at its own strings, not share the source's.
  EXPECT_NE(copy.key_data()[0], source.key_data()[0]);
  EXPECT_NE(copy.value_data()[0], source.value_data()[0]);
}

TEST_F(TypeParametersTest, CopyAssignmentRebuildsAbiPointers) {
  villagesql::TypeParameters source("dimension=1536,metric=cosine");
  villagesql::TypeParameters target("dimension=3");
  target = source;

  ASSERT_EQ(target.count(), 2u);
  EXPECT_STREQ(target.key_data()[0], "dimension");
  EXPECT_STREQ(target.value_data()[0], "1536");
  EXPECT_STREQ(target.key_data()[1], "metric");
  EXPECT_STREQ(target.value_data()[1], "cosine");

  // Every entry points at the target's own strings, not the source's.
  EXPECT_NE(target.key_data()[0], source.key_data()[0]);
  EXPECT_NE(target.key_data()[1], source.key_data()[1]);
}

TEST_F(TypeParametersTest, MoveConstructorRebuildsAbiPointers) {
  villagesql::TypeParameters source("dimension=1536,metric=cosine");
  villagesql::TypeParameters moved(std::move(source));

  ASSERT_EQ(moved.count(), 2u);
  EXPECT_EQ(moved.str(), "dimension=1536,metric=cosine");
  EXPECT_STREQ(moved.key_data()[0], "dimension");
  EXPECT_STREQ(moved.value_data()[1], "cosine");
}

TEST_F(TypeParametersTest, MoveAssignmentRebuildsAbiPointers) {
  villagesql::TypeParameters source("dimension=1536,metric=cosine");
  villagesql::TypeParameters target("dimension=3");
  target = std::move(source);

  ASSERT_EQ(target.count(), 2u);
  EXPECT_EQ(target.str(), "dimension=1536,metric=cosine");
  EXPECT_STREQ(target.key_data()[0], "dimension");
  EXPECT_STREQ(target.value_data()[1], "cosine");
}

// The ABI arrays outlive the object they were copied from. Under the
// sanitizers this is what catches c_keys_/c_values_ left pointing at freed
// strings.
TEST_F(TypeParametersTest, AbiPointersSurviveSourceDestruction) {
  villagesql::TypeParameters copy;
  {
    villagesql::TypeParameters source("dimension=1536,metric=cosine");
    copy = source;
  }

  ASSERT_EQ(copy.count(), 2u);
  EXPECT_STREQ(copy.key_data()[0], "dimension");
  EXPECT_STREQ(copy.value_data()[0], "1536");
  EXPECT_STREQ(copy.key_data()[1], "metric");
  EXPECT_STREQ(copy.value_data()[1], "cosine");
}

TEST_F(TypeParametersTest, SelfAssignmentLeavesEntriesIntact) {
  villagesql::TypeParameters params("dimension=1536,metric=cosine");
  villagesql::TypeParameters &alias = params;
  params = alias;

  ASSERT_EQ(params.count(), 2u);
  EXPECT_EQ(params.str(), "dimension=1536,metric=cosine");
  EXPECT_STREQ(params.key_data()[0], "dimension");
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

  // Invoke the private init_intrinsic_default (TypeContextTest is a friend).
  static bool init_default(villagesql::TypeContext &ctx, std::string &error) {
    return ctx.init_intrinsic_default(error);
  }
};

TEST_F(TypeContextTest, FixedLengthTypeUsesDescriptorValues) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("COMPLEX", "test_ext", "1.0.0"),
      VEF_PROTOCOL_1, 1, 16, 256, /*max_persisted_length=*/0,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
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
      VEF_PROTOCOL_3, 1, -1, 256, /*max_persisted_length=*/128,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
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
      VEF_PROTOCOL_3, 1, -1, 256, /*max_persisted_length=*/128,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      villagesql::ResolveParamsFunction(&rp_fail_fd));
  villagesql::TypeParameters params("dimension=1536");
  villagesql::TypeContextKey key(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"), params);
  villagesql::TypeContext ctx = make_context(key, &desc);

  // resolve_params_fail returns error, so we fall back to descriptor values.
  // max_decode_buffer_length is non-zero so this distinguishes the fallback
  // from the member never being assigned (it defaults to 0).
  EXPECT_EQ(ctx.persisted_length(), -1);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 256);
}

TEST_F(TypeContextTest, EmptyParamsSkipsResolveCallback) {
  static auto rp_fail_fd2 =
      make_resolve_params_fd("rp_fail2", &resolve_params_fail_vdf);

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VVECTOR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, -1, 256, /*max_persisted_length=*/128,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      villagesql::ResolveParamsFunction(&rp_fail_fd2));
  // No parameters — should use descriptor values directly, not call
  // resolve_params (which would fail)
  villagesql::TypeContextKey key("VVECTOR", "test_ext", "1.0.0");
  villagesql::TypeContext ctx = make_context(key, &desc);

  EXPECT_EQ(ctx.persisted_length(), -1);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 256);
}

TEST_F(TypeContextTest, SameKeysAreCompatible) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("COMPLEX", "test_ext", "1.0.0"),
      VEF_PROTOCOL_1, 1, 16, 256, /*max_persisted_length=*/0,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
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
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeDescriptor desc_b(
      villagesql::TypeDescriptorKey("OTHER", "test_ext", "1.0.0"),
      VEF_PROTOCOL_1, 1, 16, 256, /*max_persisted_length=*/0,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
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
      VEF_PROTOCOL_3, 1, -1, 256, /*max_persisted_length=*/128,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
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
      VEF_PROTOCOL_3, 1, -1, 256, /*max_persisted_length=*/128,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
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
      VEF_PROTOCOL_3, 1, -1, 256, /*max_persisted_length=*/128,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
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

// A variable-length type (persisted_length = -1, LengthKind::Variable) gets an
// intrinsic default by encoding into its max capacity and accepting whatever
// non-empty length the encoder produces.
TEST_F(TypeContextTest, VariableLengthTypeEncodesIntrinsicDefault) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("BITFIELD", "test_ext", "1.0.0"),
      VEF_PROTOCOL_4, 1, /*persisted_len=*/-1, /*max_unpersisted_len=*/256,
      /*max_persisted_len=*/64, villagesql::LengthKind::Variable,
      villagesql::EncodeFunction(two_byte_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeContextKey key("BITFIELD", "test_ext", "1.0.0");
  villagesql::TypeContext ctx = make_context(key, &desc);

  ASSERT_TRUE(ctx.is_variable_length());
  EXPECT_EQ(ctx.field_buffer_length(), 64);

  std::string error;
  EXPECT_FALSE(init_default(ctx, error));
  EXPECT_TRUE(error.empty());
  ASSERT_NE(ctx.intrinsic_default_buffer(), nullptr);
  EXPECT_EQ(ctx.intrinsic_default_size(), 2u);
  EXPECT_EQ(ctx.intrinsic_default_buffer()[0], 0xAB);
  EXPECT_EQ(ctx.intrinsic_default_buffer()[1], 0xCD);
}

// A variable-length type whose encode produces zero bytes has no usable
// default. Like a fixed-length type with no default (NO_DEFAULT_TYPE), this is
// fatal: init reports an error and stores no buffer. Such a type must supply a
// non-empty intrinsic_default_str/fn to be usable.

// TODO(villagesql-production): try to validate on install whether encode("")
// produces valid default - in case no intrinsic_default_str/fn is provided. Or
// even check if encode(instrinsic_default_str) produces a valid default. Note:
// this can only be a valid install-time check for non-parameterized types.
TEST_F(TypeContextTest, VariableLengthEmptyEncodeIsFatal) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("EMPTYVAR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_4, 1, /*persisted_len=*/-1, /*max_unpersisted_len=*/256,
      /*max_persisted_len=*/64, villagesql::LengthKind::Variable,
      villagesql::EncodeFunction(empty_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeContextKey key("EMPTYVAR", "test_ext", "1.0.0");
  villagesql::TypeContext ctx = make_context(key, &desc);

  std::string error;
  EXPECT_TRUE(init_default(ctx, error));
  EXPECT_FALSE(error.empty());
  EXPECT_EQ(ctx.intrinsic_default_buffer(), nullptr);
}

// A fixed-length type encodes its default to exactly persisted_length bytes.
TEST_F(TypeContextTest, FixedLengthTypeEncodesIntrinsicDefault) {
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("PAIR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, /*persisted_len=*/2, /*max_unpersisted_len=*/256,
      /*max_persisted_len=*/0, villagesql::LengthKind::Fixed,
      villagesql::EncodeFunction(two_byte_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeContextKey key("PAIR", "test_ext", "1.0.0");
  villagesql::TypeContext ctx = make_context(key, &desc);

  std::string error;
  EXPECT_FALSE(init_default(ctx, error));
  EXPECT_TRUE(error.empty());
  ASSERT_NE(ctx.intrinsic_default_buffer(), nullptr);
  EXPECT_EQ(ctx.intrinsic_default_size(), 2u);
}

// A parameterized variable-length type declared without parameters (is_unknown)
// cannot encode a default yet and is skipped (non-fatal, no buffer).
TEST_F(TypeContextTest, UnknownParameterizedTypeSkipsIntrinsicDefault) {
  static auto rp_ok_fd =
      make_resolve_params_fd("rp_ok_default", &resolve_params_ok_vdf);

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("VARVECTOR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_4, 1, /*persisted_len=*/-1, /*max_unpersisted_len=*/256,
      /*max_persisted_len=*/64, villagesql::LengthKind::Variable,
      villagesql::EncodeFunction(two_byte_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      villagesql::ResolveParamsFunction(&rp_ok_fd));
  // No parameters: this is an "unknown" parameterized type.
  villagesql::TypeContextKey key("VARVECTOR", "test_ext", "1.0.0");
  villagesql::TypeContext ctx = make_context(key, &desc);
  ASSERT_TRUE(ctx.is_unknown());

  std::string error;
  EXPECT_FALSE(init_default(ctx, error));
  EXPECT_TRUE(error.empty());
  EXPECT_EQ(ctx.intrinsic_default_buffer(), nullptr);
}

// Descriptor builders for the tests below. The full TypeDescriptor constructor
// takes twelve arguments; naming the three call variants these tests need keeps
// each one down to the fields it actually cares about.

static villagesql::TypeDescriptor make_fixed_desc(
    const char *type_name, vef_protocol_t protocol, int64_t persisted_len,
    vef_encode_func_t encode,
    std::optional<villagesql::HashFunction> hash = std::nullopt) {
  return villagesql::TypeDescriptor(
      villagesql::TypeDescriptorKey(type_name, "test_ext", "1.0.0"), protocol,
      /*impl_type=*/1, persisted_len, /*max_unpersisted_len=*/256,
      /*max_persisted_len=*/0, villagesql::LengthKind::Fixed,
      villagesql::EncodeFunction(encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::move(hash));
}

// resolve_params is what makes a type parameterized; int_to_params is optional
// and only enables the TYPE(N) shorthand.
//
// persisted_length is -1 because the footprint comes from resolve_params, which
// registration requires for a parameterized type. The other two lengths are
// positive because registration requires that too: max_decode_buffer_length
// (validate.cc) and, for persisted_length == -1, max_persisted_length
// (veb_register_type_v3.cc). Keeping them positive also keeps them distinct
// from the values a failing resolve_params produces, so a test can tell a
// fallback to these apart from the rejected result.
static villagesql::TypeDescriptor make_parameterized_desc(
    const char *type_name, const vef_func_desc_t *resolve_fd,
    std::optional<villagesql::IntToParamsFunction> int_to_params =
        std::nullopt) {
  return villagesql::TypeDescriptor(
      villagesql::TypeDescriptorKey(type_name, "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, /*impl_type=*/1, /*persisted_len=*/-1,
      /*max_unpersisted_len=*/256, /*max_persisted_len=*/128,
      villagesql::LengthKind::Fixed, villagesql::EncodeFunction(dummy_encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt,
      std::move(int_to_params), villagesql::ResolveParamsFunction(resolve_fd));
}

static villagesql::TypeDescriptor make_variable_desc(
    const char *type_name, int64_t max_persisted_len, vef_encode_func_t encode,
    std::optional<villagesql::ResolveParamsFunction> resolve = std::nullopt) {
  return villagesql::TypeDescriptor(
      villagesql::TypeDescriptorKey(type_name, "test_ext", "1.0.0"),
      VEF_PROTOCOL_4, /*impl_type=*/1, /*persisted_len=*/-1,
      /*max_unpersisted_len=*/256, max_persisted_len,
      villagesql::LengthKind::Variable, villagesql::EncodeFunction(encode),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare), std::nullopt, std::nullopt,
      std::move(resolve));
}

// Builds the TypeContextKey for one of the descriptors above.
static villagesql::TypeContextKey context_key(const char *type_name,
                                              const char *canonical_params) {
  return villagesql::TypeContextKey(
      villagesql::TypeDescriptorKey(type_name, "test_ext", "1.0.0"),
      villagesql::TypeParameters(canonical_params));
}

// The key is what the victionary stores a TypeContext under, so its normalized
// form decides which instantiations share a cache entry.
TEST_F(TypeContextTest, KeyDefaultConstructedIsEmpty) {
  villagesql::TypeContextKey key;
  EXPECT_TRUE(key.str().empty());
  EXPECT_TRUE(key.parameters().empty());
  EXPECT_TRUE(key.descriptor_key().str().empty());
}

TEST_F(TypeContextTest, KeyWithoutParametersIsJustTheDescriptorKey) {
  villagesql::TypeContextKey key("COMPLEX", "test_ext", "1.0.0");
  EXPECT_EQ(key.str(), "complex.test_ext.1.0.0");
  EXPECT_EQ(key.descriptor_key().str(), "complex.test_ext.1.0.0");
  EXPECT_TRUE(key.parameters().empty());
}

TEST_F(TypeContextTest, KeyAppendsParametersAfterADot) {
  villagesql::TypeContextKey key = context_key("VVECTOR", "dimension=3");
  EXPECT_EQ(key.str(), "vvector.test_ext.1.0.0.dimension=3");
  EXPECT_EQ(key.parameters().str(), "dimension=3");

  // A parameterized instantiation never collides with the bare type.
  EXPECT_FALSE(key ==
               villagesql::TypeContextKey("VVECTOR", "test_ext", "1.0.0"));
}

TEST_F(TypeContextTest, KeyComparisonUsesTheNormalizedForm) {
  villagesql::TypeContextKey lower(
      villagesql::TypeDescriptorKey("vvector", "test_ext", "1.0.0"),
      villagesql::TypeParameters("dimension=3"));
  villagesql::TypeContextKey upper(
      villagesql::TypeDescriptorKey("VVECTOR", "TEST_EXT", "1.0.0"),
      villagesql::TypeParameters("dimension=3"));
  villagesql::TypeContextKey other = context_key("VVECTOR", "dimension=4");

  EXPECT_TRUE(lower == upper);
  EXPECT_FALSE(lower == other);
  EXPECT_TRUE(lower < other);
  EXPECT_FALSE(other < lower);
}

// qualified_name() is what error messages and SHOW CREATE TABLE display, and
// producing the short TYPE(N) form takes a round trip through the extension's
// int_to_params and resolve_params callbacks.
TEST_F(TypeContextTest, QualifiedNameOfNonParameterizedTypeHasNoParens) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("COMPLEX", VEF_PROTOCOL_1, 16, dummy_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("COMPLEX", "test_ext", "1.0.0"), &desc);

  // Names keep the case they were registered with; only keys are case-folded.
  EXPECT_EQ(ctx.qualified_name(), "test_ext.COMPLEX");
  EXPECT_EQ(ctx.qualified_base_name(), "test_ext.COMPLEX");
}

TEST_F(TypeContextTest, QualifiedNameUsesShorthandWhenIntToParamsRoundTrips) {
  static auto rp_fd =
      make_resolve_params_fd("rp_short", &resolve_params_ok_vdf);
  static auto itp_fd =
      make_int_to_params_fd("itp_short", &int_to_dimension_vdf);
  villagesql::TypeDescriptor desc = make_parameterized_desc(
      "VVECTOR", &rp_fd, villagesql::IntToParamsFunction(&itp_fd));
  villagesql::TypeContext ctx =
      make_context(context_key("VVECTOR", "dimension=1536"), &desc);

  EXPECT_EQ(ctx.qualified_name(), "test_ext.VVECTOR(1536)");
  EXPECT_EQ(ctx.qualified_base_name(), "test_ext.VVECTOR");
}

// int_to_params here only knows about the dimension, and this resolve_params
// fills nothing in, so TYPE(1536) cannot reproduce the stored parameters and
// the explicit form is displayed instead.
TEST_F(TypeContextTest, QualifiedNameFallsBackWhenShorthandDoesNotMatch) {
  static auto rp_fd =
      make_resolve_params_fd("rp_nofill", &resolve_params_ok_vdf);
  static auto itp_fd =
      make_int_to_params_fd("itp_nofill", &int_to_dimension_vdf);
  villagesql::TypeDescriptor desc = make_parameterized_desc(
      "VVECTOR", &rp_fd, villagesql::IntToParamsFunction(&itp_fd));
  villagesql::TypeContext ctx = make_context(
      context_key("VVECTOR", "dimension=1536,metric=cosine"), &desc);

  EXPECT_EQ(ctx.qualified_name(),
            "test_ext.VVECTOR('dimension=1536,metric=cosine')");
}

// The mutating resolve_params overload fills in defaults, and the shorthand
// check runs int_to_params' output back through it before comparing. So
// TYPE(1536) is recognized even though the stored parameters also carry the
// metric that resolve_params supplied.
TEST_F(TypeContextTest, QualifiedNameShorthandUsesRewrittenParams) {
  static auto rp_fd =
      make_resolve_params_fd("rp_fill", &resolve_params_fills_metric_vdf);
  static auto itp_fd = make_int_to_params_fd("itp_fill", &int_to_dimension_vdf);
  villagesql::TypeDescriptor desc = make_parameterized_desc(
      "VVECTOR", &rp_fd, villagesql::IntToParamsFunction(&itp_fd));
  villagesql::TypeContext ctx =
      make_context(context_key("VVECTOR", "dimension=1536,metric=l2"), &desc);

  EXPECT_EQ(ctx.qualified_name(), "test_ext.VVECTOR(1536)");
  // The same callback sizes the type for the filled-in parameterization.
  EXPECT_EQ(ctx.persisted_length(), 32);
  EXPECT_EQ(ctx.max_decode_buffer_length(), 256);
}

// TYPE(N) may name a parameter other than the first: each integer-valued
// parameter is tried, in canonical (key-sorted) order.
TEST_F(TypeContextTest, QualifiedNameShorthandTriesEachIntegerParameter) {
  static auto rp_fd =
      make_resolve_params_fd("rp_scale", &resolve_params_fixed_sizes_vdf);
  static auto itp_fd = make_int_to_params_fd("itp_scale", &int_to_scale_vdf);
  villagesql::TypeDescriptor desc = make_parameterized_desc(
      "MATRIX", &rp_fd, villagesql::IntToParamsFunction(&itp_fd));
  villagesql::TypeContext ctx =
      make_context(context_key("MATRIX", "dimension=8,scale=2"), &desc);

  // dimension=8 is tried first and yields "dimension=8,scale=8", which does
  // not match; scale=2 yields "dimension=8,scale=2", which does.
  EXPECT_EQ(ctx.qualified_name(), "test_ext.MATRIX(2)");
}

TEST_F(TypeContextTest, QualifiedNameUsesLongFormWithoutIntToParams) {
  static auto rp_fd = make_resolve_params_fd("rp_long", &resolve_params_ok_vdf);
  villagesql::TypeDescriptor desc = make_parameterized_desc("VVECTOR", &rp_fd);
  villagesql::TypeContext ctx =
      make_context(context_key("VVECTOR", "dimension=1536"), &desc);

  EXPECT_EQ(ctx.qualified_name(), "test_ext.VVECTOR('dimension=1536')");
}

// A failing int_to_params is not fatal: there is simply no shorthand to show.
TEST_F(TypeContextTest, QualifiedNameFallsBackWhenIntToParamsFails) {
  static auto rp_fd =
      make_resolve_params_fd("rp_itpfail", &resolve_params_ok_vdf);
  static auto itp_fd =
      make_int_to_params_fd("itp_fail", &int_to_params_fail_vdf);
  villagesql::TypeDescriptor desc = make_parameterized_desc(
      "VVECTOR", &rp_fd, villagesql::IntToParamsFunction(&itp_fd));
  villagesql::TypeContext ctx =
      make_context(context_key("VVECTOR", "dimension=1536"), &desc);

  EXPECT_EQ(ctx.qualified_name(), "test_ext.VVECTOR('dimension=1536')");
}

// Only a parameter whose value is entirely an in-range integer is a TYPE(N)
// candidate. A non-numeric value, trailing garbage, and a value too large for
// int64 are each skipped rather than mis-parsed into a wrong shorthand.
TEST_F(TypeContextTest, QualifiedNameSkipsNonIntegerParameterValues) {
  static auto rp_fd =
      make_resolve_params_fd("rp_skip", &resolve_params_fixed_sizes_vdf);
  static auto itp_fd = make_int_to_params_fd("itp_skip", &int_to_dimension_vdf);
  villagesql::TypeDescriptor desc = make_parameterized_desc(
      "VVECTOR", &rp_fd, villagesql::IntToParamsFunction(&itp_fd));

  for (const char *canonical : {"dimension=cosine", "dimension=3abc",
                                "dimension=99999999999999999999999"}) {
    villagesql::TypeContext ctx =
        make_context(context_key("VVECTOR", canonical), &desc);
    EXPECT_EQ(ctx.qualified_name(),
              std::string("test_ext.VVECTOR('") + canonical + "')")
        << "parameters: " << canonical;
  }
}

// field_buffer_length() declares the width of the backing VARBINARY field, so
// it is what a column's storage footprint is charged against.
TEST_F(TypeContextTest, FieldBufferLengthFollowsPersistedLengthWhenFixed) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("COMPLEX", VEF_PROTOCOL_3, 16, dummy_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("COMPLEX", "test_ext", "1.0.0"), &desc);

  EXPECT_FALSE(ctx.is_variable_length());
  EXPECT_EQ(ctx.field_buffer_length(), 16);
}

// A parameterized fixed-length type is sized to the length resolve_params
// computed for this instantiation, not to the descriptor's placeholder.
TEST_F(TypeContextTest,
       FieldBufferLengthFollowsResolvedLengthWhenParameterized) {
  static auto rp_fd =
      make_resolve_params_fd("rp_resolved", &resolve_params_ok_vdf);
  villagesql::TypeDescriptor desc = make_parameterized_desc("VVECTOR", &rp_fd);
  villagesql::TypeContext ctx =
      make_context(context_key("VVECTOR", "dimension=3"), &desc);

  EXPECT_EQ(ctx.persisted_length(), 3 * 4);
  EXPECT_EQ(ctx.field_buffer_length(), 3 * 4);
}

// A variable-length type's backing field is sized to the type's declared upper
// bound even when resolve_params computed a smaller persisted_length: the
// per-value length is decided at encode time, so the field has to hold the
// widest value the parameterization allows.
TEST_F(TypeContextTest, FieldBufferLengthUsesMaxPersistedLengthWhenVariable) {
  static auto rp_fd =
      make_resolve_params_fd("rp_var", &resolve_params_fixed_sizes_vdf);
  villagesql::TypeDescriptor desc =
      make_variable_desc("VARVECTOR", 64, two_byte_encode,
                         villagesql::ResolveParamsFunction(&rp_fd));
  villagesql::TypeContext ctx =
      make_context(context_key("VARVECTOR", "dimension=3"), &desc);

  ASSERT_TRUE(ctx.is_variable_length());
  EXPECT_EQ(ctx.persisted_length(), 32);
  EXPECT_EQ(ctx.field_buffer_length(), 64);
}

// resolve_params can also fail by resolving a non-positive
// max_decode_buffer_length, which sizes the decode scratch buffer. That is
// treated the same as an outright VDF error: fall back to the descriptor's
// values rather than trust a zero-sized buffer.
TEST_F(TypeContextTest, NonPositiveDecodeBufferFallsBackToDescriptor) {
  static auto rp_fd =
      make_resolve_params_fd("rp_zerodecode", &resolve_params_zero_decode_vdf);
  villagesql::TypeDescriptor desc = make_parameterized_desc("VVECTOR", &rp_fd);
  villagesql::TypeContext ctx =
      make_context(context_key("VVECTOR", "dimension=3"), &desc);

  // 256 is the descriptor's value and 0 is what resolve_params returned, so
  // this distinguishes the fallback from the rejected result being used.
  EXPECT_EQ(ctx.max_decode_buffer_length(), 256);
  // The descriptor's persisted_length is the -1 placeholder a parameterized
  // type registers, which is why falling back leaves the type unusable until
  // the column is altered (see the TODO in resolve_cached_values()).
  EXPECT_EQ(ctx.persisted_length(), -1);
}

TEST_F(TypeContextTest, AccessorsDelegateToDescriptor) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("COMPLEX", VEF_PROTOCOL_3, 16, dummy_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("COMPLEX", "test_ext", "1.0.0"), &desc);

  EXPECT_EQ(ctx.descriptor(), &desc);
  EXPECT_EQ(ctx.type_name(), "COMPLEX");
  EXPECT_EQ(ctx.extension_name(), "test_ext");
  EXPECT_EQ(ctx.extension_version(), "1.0.0");
  EXPECT_FALSE(ctx.is_parameterized());
  EXPECT_FALSE(ctx.is_variable_length());
  EXPECT_FALSE(ctx.storage_intf().has_value());
}

// A non-parameterized type with no parameters is fully known; only a
// parameterized type still awaiting its parameters is "unknown".
TEST_F(TypeContextTest, NonParameterizedTypeIsNeverUnknown) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("COMPLEX", VEF_PROTOCOL_3, 16, dummy_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("COMPLEX", "test_ext", "1.0.0"), &desc);

  EXPECT_TRUE(ctx.parameters().empty());
  EXPECT_FALSE(ctx.is_unknown());
}

// The bound Ops carry the descriptor's callables plus this context's
// parameters. hash is optional and absent unless the descriptor registers one.
TEST_F(TypeContextTest, BoundOpsExposeDescriptorCallables) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("PAIR", VEF_PROTOCOL_3, 2, two_byte_encode,
                      villagesql::HashFunction(&dummy_hash));
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("PAIR", "test_ext", "1.0.0"), &desc);

  EXPECT_EQ(ctx.encode_op().fn(), &two_byte_encode);
  EXPECT_EQ(ctx.encode_op().vdf(), nullptr);
  EXPECT_EQ(ctx.decode_op().fn(), &dummy_decode);
  EXPECT_TRUE(ctx.hash_op().has_value());

  // The Ops read the parameters living inside this context's key.
  EXPECT_EQ(&ctx.encode_op().parameters(), &ctx.parameters());
  EXPECT_EQ(&ctx.decode_op().parameters(), &ctx.parameters());
}

TEST_F(TypeContextTest, HashOpAbsentWhenDescriptorRegistersNoHash) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("PAIR", VEF_PROTOCOL_3, 2, two_byte_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("PAIR", "test_ext", "1.0.0"), &desc);

  EXPECT_FALSE(ctx.hash_op().has_value());
}

// init_intrinsic_default() tries three sources in order: the intrinsic_default
// VDF, then the extension-supplied string literal, then encode(""). Whatever
// the source, the resulting string is encoded and the encoded length is checked
// against the type's storage size.

// Protocol-1 types define no intrinsic default; MySQL's built-in default
// handling applies instead, so init stores nothing and reports no error.
TEST_F(TypeContextTest, ProtocolOneSkipsIntrinsicDefault) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("LEGACY", VEF_PROTOCOL_1, 2, two_byte_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("LEGACY", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_FALSE(init_default(ctx, error));
  EXPECT_TRUE(error.empty());
  EXPECT_EQ(ctx.intrinsic_default_buffer(), nullptr);
}

// Source 1: the VDF supplies the string, which is then encoded. echo_encode
// makes the string that arrived at encode observable.
TEST_F(TypeContextTest, IntrinsicDefaultFnSuppliesTheEncodedString) {
  static auto id_fd =
      make_intrinsic_default_fd("id_ab", &intrinsic_default_ab_vdf);
  villagesql::TypeDescriptor desc =
      make_fixed_desc("PAIR", VEF_PROTOCOL_3, 2, echo_encode);
  desc.set_intrinsic_default_fn(villagesql::IntrinsicDefaultFunction(&id_fd));
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("PAIR", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_FALSE(init_default(ctx, error));
  EXPECT_TRUE(error.empty());
  ASSERT_NE(ctx.intrinsic_default_buffer(), nullptr);
  ASSERT_EQ(ctx.intrinsic_default_size(), 2u);
  EXPECT_EQ(ctx.intrinsic_default_buffer()[0], 'A');
  EXPECT_EQ(ctx.intrinsic_default_buffer()[1], 'B');
}

// A failing intrinsic_default VDF is fatal, and the message names both the
// source and what the extension reported.
TEST_F(TypeContextTest, IntrinsicDefaultFnFailureIsFatal) {
  static auto id_fd =
      make_intrinsic_default_fd("id_fail", &intrinsic_default_fail_vdf);
  villagesql::TypeDescriptor desc =
      make_fixed_desc("PAIR", VEF_PROTOCOL_3, 2, echo_encode);
  desc.set_intrinsic_default_fn(villagesql::IntrinsicDefaultFunction(&id_fd));
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("PAIR", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_TRUE(init_default(ctx, error));
  EXPECT_NE(error.find("intrinsic_default function failed"), std::string::npos);
  EXPECT_NE(error.find("no default for this type"), std::string::npos);
  EXPECT_EQ(ctx.intrinsic_default_buffer(), nullptr);
}

// Source 2: the extension-supplied string literal.
TEST_F(TypeContextTest, IntrinsicDefaultStrIsEncoded) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("PAIR", VEF_PROTOCOL_3, 2, echo_encode);
  desc.set_intrinsic_default_str("XY");
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("PAIR", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_FALSE(init_default(ctx, error));
  ASSERT_EQ(ctx.intrinsic_default_size(), 2u);
  EXPECT_EQ(ctx.intrinsic_default_buffer()[0], 'X');
  EXPECT_EQ(ctx.intrinsic_default_buffer()[1], 'Y');
}

// A type registering both gets the VDF: it can vary the default per
// parameterization, which a fixed literal cannot.
// TODO(villagesql-general): decide if this should be allowed. Perhaps we should
// only allow one.
TEST_F(TypeContextTest, IntrinsicDefaultFnTakesPrecedenceOverStr) {
  static auto id_fd =
      make_intrinsic_default_fd("id_both", &intrinsic_default_ab_vdf);
  villagesql::TypeDescriptor desc =
      make_fixed_desc("PAIR", VEF_PROTOCOL_3, 2, echo_encode);
  desc.set_intrinsic_default_fn(villagesql::IntrinsicDefaultFunction(&id_fd));
  desc.set_intrinsic_default_str("XY");
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("PAIR", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_FALSE(init_default(ctx, error));
  ASSERT_EQ(ctx.intrinsic_default_size(), 2u);
  EXPECT_EQ(ctx.intrinsic_default_buffer()[0], 'A');
}

// A fixed-length type must encode to exactly persisted_length bytes. A shorter
// value is rejected, and the message reports both lengths so the extension
// author can see the mismatch.
TEST_F(TypeContextTest, FixedLengthWrongEncodedLengthIsFatal) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("QUAD", VEF_PROTOCOL_3, 4, two_byte_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("QUAD", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_TRUE(init_default(ctx, error));
  EXPECT_NE(error.find("to 2 bytes"), std::string::npos) << error;
  EXPECT_NE(error.find("expected persisted_length=4"), std::string::npos)
      << error;
  EXPECT_EQ(ctx.intrinsic_default_buffer(), nullptr);
}

// A variable-length type accepts 1..max_persisted_length bytes, so an encode
// claiming more than the buffer holds is rejected rather than trusted.
TEST_F(TypeContextTest, VariableLengthOverReportedLengthIsFatal) {
  villagesql::TypeDescriptor desc =
      make_variable_desc("BITFIELD", 8, over_reporting_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("BITFIELD", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_TRUE(init_default(ctx, error));
  EXPECT_NE(error.find("expected 1..8 bytes"), std::string::npos) << error;
  EXPECT_EQ(ctx.intrinsic_default_buffer(), nullptr);
}

TEST_F(TypeContextTest, EncodeFailureIsFatal) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("BROKEN", VEF_PROTOCOL_3, 2, failing_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("BROKEN", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_TRUE(init_default(ctx, error));
  EXPECT_NE(error.find("encode function failed"), std::string::npos) << error;
  EXPECT_EQ(ctx.intrinsic_default_buffer(), nullptr);
}

// With no known storage size there is nothing to encode a default against, and
// that is not an error -- it is how a fixed-length type whose parameter
// resolution produced no length is skipped.
// TODO(villagesql-general): decide if this should be allowed.
TEST_F(TypeContextTest, ZeroCapacitySkipsIntrinsicDefault) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("SIZELESS", VEF_PROTOCOL_3, 0, two_byte_encode);
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("SIZELESS", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_FALSE(init_default(ctx, error));
  EXPECT_TRUE(error.empty());
  EXPECT_EQ(ctx.intrinsic_default_buffer(), nullptr);
}

// A type whose from_string is a VDF rather than a function pointer takes the
// other branch of the encode dispatch and must store the same default.
TEST_F(TypeContextTest, VdfEncodeSuppliesIntrinsicDefault) {
  static auto enc_fd = make_encode_fd("enc_two", &two_byte_encode_vdf);
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("PAIR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, /*persisted_len=*/2, /*max_unpersisted_len=*/256,
      /*max_persisted_len=*/0, villagesql::LengthKind::Fixed,
      villagesql::EncodeFunction(&enc_fd),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("PAIR", "test_ext", "1.0.0"), &desc);

  ASSERT_NE(ctx.encode_op().vdf(), nullptr);
  EXPECT_EQ(ctx.encode_op().fn(), nullptr);

  std::string error;
  EXPECT_FALSE(init_default(ctx, error));
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(ctx.intrinsic_default_size(), 2u);
  EXPECT_EQ(ctx.intrinsic_default_buffer()[0], 0xAB);
  EXPECT_EQ(ctx.intrinsic_default_buffer()[1], 0xCD);
}

TEST_F(TypeContextTest, VdfEncodeFailureIsFatal) {
  static auto enc_fd = make_encode_fd("enc_fail", &encode_fail_vdf);
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("PAIR", "test_ext", "1.0.0"),
      VEF_PROTOCOL_3, 1, /*persisted_len=*/2, /*max_unpersisted_len=*/256,
      /*max_persisted_len=*/0, villagesql::LengthKind::Fixed,
      villagesql::EncodeFunction(&enc_fd),
      villagesql::DecodeFunction(dummy_decode),
      villagesql::CompareFunction(dummy_compare));
  villagesql::TypeContext ctx = make_context(
      villagesql::TypeContextKey("PAIR", "test_ext", "1.0.0"), &desc);

  std::string error;
  EXPECT_TRUE(init_default(ctx, error));
  EXPECT_NE(error.find("from_string VDF failed to encode"), std::string::npos)
      << error;
  EXPECT_EQ(ctx.intrinsic_default_buffer(), nullptr);
}

// create() is how the victionary builds a TypeContext, and it runs
// init_intrinsic_default as part of construction -- the tests above reach that
// through the friend declaration instead, bypassing this wrapper.
//
// Only the paths that report nothing are covered here: create() reports an
// init failure through my_printf_error, which this test target does not set up
// server error handling for. The failure behavior itself is covered by
// VariableLengthEmptyEncodeIsFatal and the rejection tests above.

TEST_F(TypeContextTest, CreateReturnsNullForNullDescriptor) {
  EXPECT_EQ(
      villagesql::TableTraits<villagesql::TypeContext>::create(
          villagesql::TypeContextKey("COMPLEX", "test_ext", "1.0.0"), nullptr),
      nullptr);
}

TEST_F(TypeContextTest, CreateInitializesTheIntrinsicDefault) {
  villagesql::TypeDescriptor desc =
      make_fixed_desc("PAIR", VEF_PROTOCOL_3, 2, two_byte_encode);
  std::shared_ptr<villagesql::TypeContext> ctx =
      villagesql::TableTraits<villagesql::TypeContext>::create(
          villagesql::TypeContextKey("PAIR", "test_ext", "1.0.0"), &desc);

  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(ctx->qualified_name(), "test_ext.PAIR");
  ASSERT_NE(ctx->intrinsic_default_buffer(), nullptr);
  EXPECT_EQ(ctx->intrinsic_default_size(), 2u);
}

}  // namespace villagesql_unittest
