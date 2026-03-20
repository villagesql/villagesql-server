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

// Compile-fail test: parameterized hash VDF without .params<>().
// The SDK static_assert fires at the make_type_hash .func() call.

#include <villagesql/extension.h>

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

bool faketype_encode(std::string_view from, villagesql::Span<unsigned char> buf,
                     size_t *length) {
  size_t n = from.size() < buf.size() ? from.size() : buf.size();
  memcpy(buf.data(), from.data(), n);
  *length = n;
  return false;
}

bool faketype_decode(villagesql::Span<const unsigned char> data,
                     villagesql::Span<char> out, size_t *out_len) {
  size_t n = data.size() < out.size() ? data.size() : out.size();
  memcpy(out.data(), data.data(), n);
  *out_len = n;
  return false;
}

int faketype_compare(villagesql::Span<const unsigned char> a,
                     villagesql::Span<const unsigned char> b) {
  size_t n = a.size() < b.size() ? a.size() : b.size();
  int r = memcmp(a.data(), b.data(), n);
  if (r != 0) return r;
  return static_cast<int>(a.size()) - static_cast<int>(b.size());
}

size_t faketype_hash(const FakeParams &,
                     villagesql::Span<const unsigned char> data) {
  size_t h = 0;
  for (size_t i = 0; i < data.size(); ++i) h = h * 31 + data[i];
  return h;
}

constexpr const char *FAKETYPE = "FAKETYPE";

VEF_GENERATE_ENTRY_POINTS(
    make_extension(VEF_EXTENSION_NAME, "0.0.1-devtest")
        .type(
            make_type(FAKETYPE)
                .persisted_length(64)
                .max_decode_buffer_length(64)
                .encode("faketype_encode")
                .decode("faketype_decode")
                .compare("faketype_compare")
                .hash("faketype_hash")
                .build())  // .params<FakeParams, &FakeParams::parse>() omitted
        .func(make_type_encode<&faketype_encode>("faketype_encode", FAKETYPE))
        .func(make_type_decode<&faketype_decode>("faketype_decode", FAKETYPE))
        .func(make_type_compare<&faketype_compare>("faketype_compare",
                                                   FAKETYPE))
        .func(make_type_hash<&faketype_hash>("faketype_hash", FAKETYPE)))
