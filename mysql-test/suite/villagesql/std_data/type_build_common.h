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

// Shared stub type-op functions for the build-error test extensions.
// the op bodies are irrelevant to what is under test, so they are
// defined once here.

#ifndef VILLAGESQL_TEST_STD_DATA_TYPE_BUILD_COMMON_H
#define VILLAGESQL_TEST_STD_DATA_TYPE_BUILD_COMMON_H

#include <villagesql/vsql.h>

#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

namespace tbc {

constexpr int64_t kStubSize = 8;

// ---- Non-parameterized ops ----------------------------------------------
inline void stub_encode(std::string_view /*from*/, vsql::CustomResult out) {
  auto buf = out.buffer();
  if (buf.empty()) return;
  memset(buf.data(), 0, buf.size());
  out.set_length(buf.size());
}

inline void stub_decode(vsql::CustomArg /*in*/, vsql::StringResult out) {
  out.set("");
}

inline int stub_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto va = a.value();
  auto vb = b.value();
  size_t n = va.size() < vb.size() ? va.size() : vb.size();
  return memcmp(va.data(), vb.data(), n);
}

// ---- Intrinsic-default VDF ------------------------------------------------
inline std::string stub_default(char * /*error_msg*/) { return ""; }

// ---- Parameterized ('length') ops ----------------------------------------
struct LenParam {
  int64_t length;

  static LenParam parse(const std::map<std::string, std::string> &params) {
    auto it = params.find("length");
    return LenParam{it == params.end() ? 0 : std::stoll(it->second)};
  }

  static void to_strings(const LenParam &p,
                         std::map<std::string, std::string> &out) {
    out["length"] = std::to_string(p.length);
  }
};

inline void stub_param_encode(vsql::MaybeParams<LenParam> & /*params*/,
                              std::string_view /*from*/,
                              vsql::CustomResult out) {
  auto buf = out.buffer();
  if (buf.empty()) return;
  out.set_length(buf.size());
}

inline void stub_param_decode(vsql::CustomArgWith<LenParam> /*in*/,
                              vsql::StringResult out) {
  out.set_length(0);
}

inline int stub_param_compare(vsql::CustomArgWith<LenParam>,
                              vsql::CustomArgWith<LenParam>) {
  return 0;
}

inline bool stub_int_to_params(int64_t value,
                               std::map<std::string, std::string> &params,
                               char *) {
  params["length"] = std::to_string(value);
  return false;
}

inline bool stub_resolve_params(const std::map<std::string, std::string> &,
                                vsql::ResolvedTypeParams *result, char *) {
  result->persisted_length = kStubSize;
  result->max_decode_buffer_length = 16;
  return false;
}

}  // namespace tbc

#endif  // VILLAGESQL_TEST_STD_DATA_TYPE_BUILD_COMMON_H
