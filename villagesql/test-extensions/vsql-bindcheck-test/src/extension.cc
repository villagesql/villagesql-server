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
//
// The first argument is declared TAG, but a bare string literal written there
// reaches the hook as a STRING: it has no type context yet, so the server has
// nothing to report but its result type. Recovering the label from the
// literal's text and answering with set_arg() is what makes
// tag_relabel('user#5', 'archived') work -- the server then encodes the
// literal as a TAG under that label, exactly as if a sibling argument had
// donated it.
void tag_relabel_bind(vsql::BindArgs args, vsql::BindResult out) {
  if (args.size() != 2) {
    out.error("tag_relabel expects 2 arguments");
    return;
  }
  const vsql::BindArgType source = args.at(0);
  std::string source_label;
  if (source.is_custom()) {
    if (source.custom_type() != "TAG") {
      out.error("tag_relabel: the first argument must be a TAG");
      return;
    }
    const TagParams *source_params = source.params<TagParams>();
    if (source_params == nullptr) {
      out.error("tag_relabel: the source TAG's label is not known here");
      return;
    }
    source_label = source_params->label;
  } else if (source.has_const_value()) {
    // A literal in TAG position: its label is whatever its text says.
    const std::string_view source_text = source.const_value();
    const size_t hash = source_text.find('#');
    if (hash == std::string_view::npos ||
        !valid_label(source_text.substr(0, hash))) {
      out.error("tag_relabel: '" + std::string(source_text) +
                "' is not a TAG literal (expected <label>#<number>)");
      return;
    }
    source_label = std::string(source_text.substr(0, hash));
    // Tell the server how to encode that literal. Without this it has no
    // parameters to encode it under and the call cannot be resolved.
    out.set_arg(0, TagParams{source_label});
  } else {
    out.error("tag_relabel: the first argument must be a TAG");
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
  if (source_label == text) {
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

// ---------------------------------------------------------------------------
// TAGLIST: the case that only set_arg() can serve.
// ---------------------------------------------------------------------------
//
// A TAGLIST is a variable-length run of counters that all share one label. The
// label is a type parameter, exactly as for TAG -- but unlike TAG, a TAGLIST's
// text form "[1,2,3]" does not mention it. That single difference is what
// makes this type interesting:
//
//   * its own text cannot supply the label, so the constant-string inference
//     path cannot resolve a TAGLIST literal;
//   * TD1 donates only between arguments of the SAME type, so a TAG argument
//     can never hand its label to a TAGLIST one;
//   * writing TAGLIST::from_string('[1,2,3]') explicitly does not help either,
//     for the first reason.
//
// So in taglist_prepend(TAG, TAGLIST) the second argument's parameters are a
// function of the FIRST argument's -- a different type -- and the only channel
// that can express that is BindResult::set_arg().

// At most this many counters in one TAGLIST value.
constexpr int64_t kTagListMaxElems = 8;
constexpr int64_t kTagListMaxBytes = kTagListMaxElems * kTagBytes;
// "[" + 8 * (20 digits + comma) + "]" + terminator.
constexpr int64_t kTagListMaxDecodedLen = 1 + kTagListMaxElems * 21 + 2;

// Deliberately a distinct C++ type from TagParams, so the two types have
// separate parameter caches and nothing can be shared by accident.
struct TagListParams {
  std::string label;

  static TagListParams parse(const std::map<std::string, std::string> &params) {
    TagListParams p;
    auto it = params.find("label");
    if (it != params.end()) p.label = it->second;
    return p;
  }

  static void to_strings(const TagListParams &p,
                         std::map<std::string, std::string> &out) {
    out["label"] = p.label;
  }
};

bool taglist_resolve_params(const std::map<std::string, std::string> &params,
                            vsql::ResolvedTypeParams *result, char *error_msg) {
  auto it = params.find("label");
  if (it == params.end() || !valid_label(it->second)) {
    snprintf(
        error_msg, VEF_MAX_ERROR_LEN,
        "taglist_resolve_params: TAGLIST requires a valid label parameter");
    return true;
  }
  result->max_decode_buffer_length = kTagListMaxDecodedLen;
  return false;
}

// STRING -> binary: "[n,n,n]". Note what is missing -- nothing in the text
// names the label, so an unparameterized call cannot be resolved from it.
void taglist_from_string(vsql::MaybeParams<TagListParams> &p,
                         std::string_view from, vsql::CustomResult out) {
  if (from.empty()) {
    out.set_length(0);
    return;
  }
  if (!p.is_known()) {
    out.warning(
        "taglist_from_string: a TAGLIST literal cannot say which label it "
        "belongs to");
    return;
  }
  std::string input(from);
  const char *s = input.c_str();
  while (*s == ' ') s++;
  if (*s != '[') {
    out.warning("taglist_from_string: expected '['");
    return;
  }
  s++;
  auto buf = out.buffer();
  size_t count = 0;
  while (*s != '\0') {
    while (*s == ' ') s++;
    if (*s == ']') break;
    if (count >= static_cast<size_t>(kTagListMaxElems)) {
      out.warning("taglist_from_string: too many elements");
      return;
    }
    char *endptr = nullptr;
    const unsigned long long v = strtoull(s, &endptr, 10);
    if (endptr == s) {
      out.warning("taglist_from_string: parse error");
      return;
    }
    store_be64(buf.data() + count * static_cast<size_t>(kTagBytes), v);
    count++;
    s = endptr;
    while (*s == ' ') s++;
    if (*s == ',') s++;
  }
  if (*s != ']') {
    out.warning("taglist_from_string: missing ']'");
    return;
  }
  out.set_length(count * static_cast<size_t>(kTagBytes));
}

void taglist_to_string(vsql::CustomArgWith<TagListParams> in,
                       vsql::StringResult out) {
  auto data = in.value();
  const size_t count = data.size() / static_cast<size_t>(kTagBytes);
  auto buf = out.buffer();
  size_t pos = 0;
  if (pos >= buf.size()) return;
  buf[pos++] = '[';
  for (size_t i = 0; i < count; i++) {
    if (i > 0) {
      if (pos >= buf.size()) return;
      buf[pos++] = ',';
    }
    const int written = snprintf(
        buf.data() + pos, buf.size() - pos, "%llu",
        static_cast<unsigned long long>(
            load_be64(data.data() + i * static_cast<size_t>(kTagBytes))));
    if (written <= 0 || pos + static_cast<size_t>(written) >= buf.size())
      return;
    pos += static_cast<size_t>(written);
  }
  if (pos >= buf.size()) return;
  buf[pos++] = ']';
  out.set_length(pos);
}

int taglist_compare(vsql::CustomArgWith<TagListParams> a,
                    vsql::CustomArgWith<TagListParams> b) {
  auto va = a.value();
  auto vb = b.value();
  const size_t n = va.size() < vb.size() ? va.size() : vb.size();
  const int c = memcmp(va.data(), vb.data(), n);
  if (c != 0) return c;
  if (va.size() == vb.size()) return 0;
  return va.size() < vb.size() ? -1 : 1;
}

// taglist_prepend(tag, rest) -> TAGLIST. Puts the tag's counter at the front
// of the list.
void taglist_prepend(vsql::CustomArgWith<TagParams> head,
                     vsql::CustomArgWith<TagListParams> rest,
                     vsql::CustomResultWith<TagListParams> out) {
  if (head.is_null() || rest.is_null()) {
    out.set_null();
    return;
  }
  auto tail = rest.value();
  auto buf = out.buffer();
  if (buf.size() < tail.size() + static_cast<size_t>(kTagBytes)) {
    out.error("taglist_prepend: result would exceed the maximum list length");
    return;
  }
  memcpy(buf.data(), head.value().data(), static_cast<size_t>(kTagBytes));
  if (tail.size() > 0) {
    memcpy(buf.data() + kTagBytes, tail.data(), tail.size());
  }
  out.set_length(tail.size() + static_cast<size_t>(kTagBytes));
}

// The whole point of this fixture. Argument 2 is a TAGLIST whose label is a
// function of argument 1's parameters -- a TAG. Nothing else in the system can
// make that connection: the literal's text is silent about the label and TD1
// only donates between arguments of the same type.
void taglist_prepend_bind(vsql::BindArgs args, vsql::BindResult out) {
  if (args.size() != 2) {
    out.error("taglist_prepend expects 2 arguments");
    return;
  }
  const TagParams *head = args.at(0).params<TagParams>();
  if (head == nullptr) {
    out.error("taglist_prepend: the tag's label is not known here");
    return;
  }
  // If the list argument already arrived resolved (a column), its label must
  // agree; if it arrived bare (a literal), this is where it gets one.
  const vsql::BindArgType rest = args.at(1);
  if (rest.has_params()) {
    const TagListParams *rp = rest.params<TagListParams>();
    if (rp != nullptr && rp->label != head->label) {
      out.error("taglist_prepend: cannot prepend a '" + head->label +
                "' tag to a '" + rp->label + "' list");
      return;
    }
  } else {
    out.set_arg(1, TagListParams{head->label});
  }
  out.set_return(TagListParams{head->label});
}

void taglist_label(vsql::CustomArgWith<TagListParams> in,
                   vsql::StringResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  out.set(in.params().label);
}

void taglist_size(vsql::CustomArgWith<TagListParams> in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  out.set(static_cast<long long>(in.value().size() / kTagBytes));
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

static constexpr const char kTagListTypeName[] = "TAGLIST";

constexpr auto TAGLIST = vsql::make_type<kTagListTypeName>()
                             .variable_length_type()
                             .max_persisted_length(kTagListMaxBytes)
                             .max_decode_buffer_length(kTagListMaxDecodedLen)
                             .params<TagListParams, &TagListParams::parse,
                                     &TagListParams::to_strings>()
                             .resolve_params<&taglist_resolve_params>()
                             .from_string<&taglist_from_string>()
                             .to_string<&taglist_to_string>()
                             .compare<&taglist_compare>()
                             .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .type(TAG)
        .type(TAGLIST)
        .func(make_func<&taglist_prepend>("taglist_prepend")
                  .returns(TAGLIST)
                  .param(TAG)
                  .param(TAGLIST)
                  .bind_and_check_types<&taglist_prepend_bind>()
                  .deterministic()
                  .build())
        .func(make_func<&taglist_label>("taglist_label")
                  .returns(STRING)
                  .param(TAGLIST)
                  .buffer_size(kMaxLabelLen + 1)
                  .deterministic()
                  .build())
        .func(make_func<&taglist_size>("taglist_size")
                  .returns(INT)
                  .param(TAGLIST)
                  .deterministic()
                  .build())
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
