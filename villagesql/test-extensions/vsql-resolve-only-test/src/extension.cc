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

// Minimal fixture for one regression: a FIXED-length parameterized custom type
// that provides resolve_params but NOT int_to_params.
//
// RPARAM has persisted_length(-1) (the concrete width is resolved from the
// 'len' parameter) and provides resolve_params, so it is parameterized. With no
// int_to_params, the bare declaration `CREATE TABLE t (v ext.RPARAM)` has no
// way to obtain a width: resolve_params is never invoked (no parameters) and
// persisted_length stays -1. This used to crash the server (assert(len > 0) in
// PT_custom_type / make_field); the type now must be rejected at parse time.
//
// The type ops are no-op stubs: they exist only to satisfy the builder. The
// column under test is never populated, because the bare declaration is
// rejected before any value is stored, so there is nothing to encode/decode.

#include <villagesql/vsql.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

constexpr int64_t kRparamMaxLen = 64;

// 'len' parameter: the fixed stored byte width for the column.
struct RparamParams {
  int64_t len = 0;

  static RparamParams parse(const std::map<std::string, std::string> &params) {
    RparamParams p;
    auto it = params.find("len");
    if (it != params.end()) p.len = strtoll(it->second.c_str(), nullptr, 10);
    return p;
  }

  static void to_strings(const RparamParams &p,
                         std::map<std::string, std::string> &out) {
    out["len"] = std::to_string(p.len);
  }
};

// resolve_params makes the type parameterized and, for a fixed-length type
// (persisted_length(-1)), resolves the concrete width. It is never invoked for
// the bare form (no parameters) -- the case under test.
bool rparam_resolve_params(const std::map<std::string, std::string> &params,
                           vsql::ResolvedTypeParams *result, char *error_msg) {
  auto it = params.find("len");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "RPARAM requires a 'len' parameter");
    return true;
  }
  int64_t len = strtoll(it->second.c_str(), nullptr, 10);
  if (len <= 0 || len > kRparamMaxLen) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "RPARAM 'len' must be a positive integer <= %lld",
             static_cast<long long>(kRparamMaxLen));
    return true;
  }
  result->persisted_length = len;
  result->max_decode_buffer_length = len;
  return false;
}

// Minimal type ops: required by the builder. A fixed-length type must encode to
// exactly persisted_length bytes (the server validates the intrinsic default),
// so from_string fills the whole out buffer (already sized to persisted_length)
// with zeros. to_string/compare need only exist.
void rparam_from_string(vsql::MaybeParams<RparamParams> &, std::string_view,
                        vsql::CustomResult out) {
  auto buf = out.buffer();
  memset(buf.data(), 0, buf.size());
  out.set_length(buf.size());
}

void rparam_to_string(vsql::CustomArgWith<RparamParams>,
                      vsql::StringResult out) {
  out.set_length(0);
}

int rparam_compare(vsql::CustomArgWith<RparamParams>,
                   vsql::CustomArgWith<RparamParams>) {
  return 0;
}

static constexpr const char kRparamTypeName[] = "RPARAM";

constexpr auto RPARAM =
    vsql::make_type<kRparamTypeName>()
        .persisted_length(-1)
        .max_persisted_length(kRparamMaxLen)
        .max_decode_buffer_length(kRparamMaxLen)
        .params<RparamParams, &RparamParams::parse, &RparamParams::to_strings>()
        .resolve_params<&rparam_resolve_params>()
        .from_string<&rparam_from_string>()
        .to_string<&rparam_to_string>()
        .compare<&rparam_compare>()
        .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(make_extension().type(RPARAM))
