// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// Coverage fixture for the typed bind_and_check_types API
// (vsql::BindArgs / vsql::BindResult).
//
// TAG is a custom type carrying an 8-byte big-endian counter, parameterized by
// a 'label' that is type metadata only -- it is not part of the stored value.
// Its text form is "<label>#<number>".
//
// Two functions install bind hooks, between them covering every BindArgType
// accessor:
//
//   tag_make(label, n)      the return type's label comes from the VALUE of a
//                           constant argument, which no built-in rule can do.
//                           Exercises has_const_value() / const_value().
//
//   tag_relabel(tag, label) the hook reads the incoming argument's already
//                           resolved parameters and combines them with a
//                           constant. Exercises is_custom(), custom_type(),
//                           has_params() and params<P>().
//
// Both are the shape the built-in rules cannot express: TD1 only propagates
// parameters between same-typed arguments, and TD2 only copies an argument's
// parameters to a same-typed return.

#include <villagesql/vsql.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

// A TAG value is always exactly this many bytes.
constexpr int64_t kTagBytes = 8;
// Longest label a TAG may declare.
constexpr size_t kMaxLabelLen = 16;
// "<label>#<20 digits of uint64>" plus a terminator.
constexpr int64_t kTagMaxDecodedLen =
    static_cast<int64_t>(kMaxLabelLen) + 1 + 20 + 1;

// Parameters: the label the type was declared with.
struct TagParams {
  std::string label;

  static TagParams parse(const std::map<std::string, std::string> &params) {
    TagParams p;
    auto it = params.find("label");
    if (it != params.end()) p.label = it->second;
    return p;
  }

  static void to_strings(const TagParams &p,
                         std::map<std::string, std::string> &out) {
    out["label"] = p.label;
  }
};

// A label is 1..kMaxLabelLen characters of [a-z_].
static bool valid_label(std::string_view label) {
  if (label.empty() || label.size() > kMaxLabelLen) return false;
  for (char c : label) {
    if ((c < 'a' || c > 'z') && c != '_') return false;
  }
  return true;
}

// Big-endian so that memcmp of the stored bytes orders values numerically.
static void store_be64(unsigned char *buf, uint64_t v) {
  for (int i = 0; i < 8; i++) {
    buf[i] = static_cast<unsigned char>(v >> ((7 - i) * 8));
  }
}

static uint64_t load_be64(const unsigned char *buf) {
  uint64_t v = 0;
  for (int i = 0; i < 8; i++) {
    v = (v << 8) | static_cast<uint64_t>(buf[i]);
  }
  return v;
}

// resolve_params: a TAG column must declare a well-formed label.
bool tag_resolve_params(const std::map<std::string, std::string> &params,
                        vsql::ResolvedTypeParams *result, char *error_msg) {
  auto it = params.find("label");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "tag_resolve_params: TAG requires a label parameter");
    return true;
  }
  if (!valid_label(it->second)) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "tag_resolve_params: invalid label '%s' (lowercase [a-z] and '_', "
             "1..%d chars)",
             it->second.c_str(), static_cast<int>(kMaxLabelLen));
    return true;
  }
  // Every TAG value is the same size whatever the label; only the metadata
  // varies. The size is still published from here because the type declares
  // persisted_length(-1), i.e. resolved per parameterization.
  result->persisted_length = kTagBytes;
  result->max_decode_buffer_length = kTagMaxDecodedLen;
  return false;
}

// STRING -> binary: "<label>#<number>". The label must agree with the target's
// label when that is already known; when it is not (the constant-string
// inference path) the label is published back through MaybeParams.
void tag_from_string(vsql::MaybeParams<TagParams> &p, std::string_view from,
                     vsql::CustomResult out) {
  auto buf = out.buffer();
  if (buf.size() < static_cast<size_t>(kTagBytes)) {
    out.warning("tag_from_string: output buffer too small");
    return;
  }

  // The empty string is the intrinsic default the server encodes when a TAG
  // instantiation is created: counter 0 under the already known label.
  if (from.empty()) {
    if (!p.is_known()) {
      out.warning("tag_from_string: cannot default a TAG with no label");
      return;
    }
    store_be64(buf.data(), 0);
    out.set_length(static_cast<size_t>(kTagBytes));
    return;
  }

  const size_t hash = from.find('#');
  if (hash == std::string_view::npos) {
    out.warning("tag_from_string: expected '<label>#<number>'");
    return;
  }
  const std::string_view label = from.substr(0, hash);
  if (!valid_label(label)) {
    out.warning("tag_from_string: invalid label");
    return;
  }
  if (p.is_known()) {
    if (p.value().label != label) {
      out.warning("tag_from_string: label does not match the column's label");
      return;
    }
  } else {
    p.set(TagParams{std::string(label)});
  }

  const std::string digits(from.substr(hash + 1));
  if (digits.empty()) {
    out.warning("tag_from_string: missing number");
    return;
  }
  char *endptr = nullptr;
  const unsigned long long v = strtoull(digits.c_str(), &endptr, 10);
  if (endptr == digits.c_str() || *endptr != '\0') {
    out.warning("tag_from_string: invalid number");
    return;
  }

  store_be64(buf.data(), static_cast<uint64_t>(v));
  out.set_length(static_cast<size_t>(kTagBytes));
}

// binary -> STRING: the label comes from the type parameters, the number from
// the stored bytes.
void tag_to_string(vsql::CustomArgWith<TagParams> in, vsql::StringResult out) {
  auto data = in.value();
  if (data.size() < static_cast<size_t>(kTagBytes)) {
    out.warning("tag_to_string: short value");
    return;
  }
  auto buf = out.buffer();
  const int written =
      snprintf(buf.data(), buf.size(), "%s#%llu", in.params().label.c_str(),
               static_cast<unsigned long long>(load_be64(data.data())));
  if (written <= 0 || static_cast<size_t>(written) >= buf.size()) {
    out.warning("tag_to_string: output buffer too small");
    return;
  }
  out.set_length(static_cast<size_t>(written));
}

// Big-endian storage makes the byte order the numeric order.
int tag_compare(vsql::CustomArgWith<TagParams> a,
                vsql::CustomArgWith<TagParams> b) {
  return memcmp(a.value().data(), b.value().data(),
                static_cast<size_t>(kTagBytes));
}

// tag_make(label, n) -> TAG. The label argument is type metadata: the hook
// binds it onto the return type and the body never looks at it.
void tag_make(vsql::StringArg label, vsql::IntArg n, vsql::CustomResult out) {
  (void)label;
  if (n.is_null()) {
    out.set_null();
    return;
  }
  if (n.value() < 0) {
    out.error("tag_make: the counter must not be negative");
    return;
  }
  auto buf = out.buffer();
  if (buf.size() < static_cast<size_t>(kTagBytes)) {
    out.error("tag_make: output buffer too small");
    return;
  }
  store_be64(buf.data(), static_cast<uint64_t>(n.value()));
  out.set_length(static_cast<size_t>(kTagBytes));
}

// The return type's label is the VALUE of the first argument, so it exists only
// as a constant seen at resolution time. This is what has_const_value() and
// const_value() are for.
void tag_make_bind(vsql::BindArgs args, vsql::BindResult out) {
  if (args.size() != 2) {
    out.error("tag_make expects 2 arguments");
    return;
  }
  const vsql::BindArgType label = args.at(0);
  if (!label.has_const_value()) {
    out.error("tag_make: the label must be a constant string");
    return;
  }
  const std::string_view text = label.const_value();
  if (!valid_label(text)) {
    out.error("tag_make: invalid label '" + std::string(text) +
              "' (lowercase [a-z] and '_', 1..16 chars)");
    return;
  }
  out.set_return(TagParams{std::string(text)});
}

// tag_relabel(tag, label) -> TAG. The counter passes through unchanged; only
// the type's label differs.
void tag_relabel(vsql::CustomArgWith<TagParams> in, vsql::StringArg label,
                 vsql::CustomResultWith<TagParams> out) {
  (void)label;
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto data = in.value();
  auto buf = out.buffer();
  if (buf.size() < data.size()) {
    out.error("tag_relabel: output buffer too small");
    return;
  }
  memcpy(buf.data(), data.data(), data.size());
  out.set_length(data.size());
}

// Reads the incoming argument's resolved parameters and a constant, which is
// the second thing a bind hook needs and the built-in rules cannot combine.
void tag_relabel_bind(vsql::BindArgs args, vsql::BindResult out) {
  if (args.size() != 2) {
    out.error("tag_relabel expects 2 arguments");
    return;
  }
  const vsql::BindArgType source = args.at(0);
  if (!source.is_custom() || source.custom_type() != "TAG") {
    out.error("tag_relabel: the first argument must be a TAG");
    return;
  }
  const TagParams *source_params = source.params<TagParams>();
  if (source_params == nullptr) {
    out.error("tag_relabel: the source TAG's label is not known here");
    return;
  }
  const vsql::BindArgType label = args.at(1);
  if (!label.has_const_value()) {
    out.error("tag_relabel: the new label must be a constant string");
    return;
  }
  const std::string_view text = label.const_value();
  if (!valid_label(text)) {
    out.error("tag_relabel: invalid label '" + std::string(text) + "'");
    return;
  }
  if (source_params->label == text) {
    out.error("tag_relabel: the new label matches the old one");
    return;
  }
  out.set_return(TagParams{std::string(text)});
}

// Observers, so a test can see which label the hook bound.
void tag_label(vsql::CustomArgWith<TagParams> in, vsql::StringResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  out.set(in.params().label);
}

void tag_counter(vsql::CustomArgWith<TagParams> in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  out.set(static_cast<long long>(load_be64(in.value().data())));
}

static constexpr const char kTagTypeName[] = "TAG";

constexpr auto TAG =
    vsql::make_type<kTagTypeName>()
        .persisted_length(-1)
        .max_persisted_length(kTagBytes)
        .max_decode_buffer_length(kTagMaxDecodedLen)
        .params<TagParams, &TagParams::parse, &TagParams::to_strings>()
        .resolve_params<&tag_resolve_params>()
        .from_string<&tag_from_string>()
        .to_string<&tag_to_string>()
        .compare<&tag_compare>()
        .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .type(TAG)
        .func(make_func<&tag_make>("tag_make")
                  .returns(TAG)
                  .param(STRING)
                  .param(INT)
                  .bind_and_check_types<&tag_make_bind>()
                  .deterministic()
                  .build())
        .func(make_func<&tag_relabel>("tag_relabel")
                  .returns(TAG)
                  .param(TAG)
                  .param(STRING)
                  .bind_and_check_types<&tag_relabel_bind>()
                  .deterministic()
                  .build())
        .func(make_func<&tag_label>("tag_label")
                  .returns(STRING)
                  .param(TAG)
                  .buffer_size(kMaxLabelLen + 1)
                  .deterministic()
                  .build())
        .func(make_func<&tag_counter>("tag_counter")
                  .returns(INT)
                  .param(TAG)
                  .deterministic()
                  .build()))
