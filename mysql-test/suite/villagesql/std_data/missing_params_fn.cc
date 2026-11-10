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

// Test extension: parameterized encode VDF registered without .params<>().
// vef_register should fail immediately rather than crashing on first VDF call.

#include <villagesql/vsql.h>

#include <cstddef>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

struct FakeParams {
  int value;

  static FakeParams parse(const std::map<std::string, std::string> &) {
    return FakeParams{0};
  }
};

// Parameterized encode: takes MaybeParams<FakeParams>& as first argument, so
// the SDK will route through the params cache. .params<FakeParams,
// &FakeParams::parse>() is intentionally omitted from the type builder below.
void faketype_encode(vsql::MaybeParams<FakeParams> &, std::string_view from,
                     vsql::CustomResult out) {
  auto buf = out.buffer();
  size_t n = from.size() < buf.size() ? from.size() : buf.size();
  memcpy(buf.data(), from.data(), n);
  out.set_length(n);
}

void faketype_decode(vsql::CustomArg in, vsql::StringResult out) {
  auto data = in.value();
  auto buf = out.buffer();
  size_t n = data.size() < buf.size() ? data.size() : buf.size();
  memcpy(buf.data(), data.data(), n);
  out.set_length(n);
}

int faketype_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto va = a.value();
  auto vb = b.value();
  size_t n = va.size() < vb.size() ? va.size() : vb.size();
  int r = memcmp(va.data(), vb.data(), n);
  if (r != 0) return r;
  return static_cast<int>(va.size()) - static_cast<int>(vb.size());
}

static constexpr const char kFakeTypeName[] = "FAKETYPE";

// .params<FakeParams, &FakeParams::parse>() is deliberately omitted so that
// the params cache is never bound. vef_register should detect this and fail.
constexpr auto FAKETYPE = vsql::make_type<kFakeTypeName>()
                              .persisted_length(64)
                              .max_decode_buffer_length(64)
                              .from_string<&faketype_encode>()
                              .to_string<&faketype_decode>()
                              .compare<&faketype_compare>()
                              .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(make_extension().type(FAKETYPE))
