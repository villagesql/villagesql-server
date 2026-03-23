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

// Registration-fail test: VDF registered for FAKETYPE_B but uses a params
// type (FakeParams) that was registered for FAKETYPE_A. Compiles fine
// because FakeParams IS in the TypeTuple, but vef_register returns an error
// because the VDF does not operate on FAKETYPE_A.

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

// Hash VDF for FAKETYPE_B that uses FakeParams — which was registered for
// FAKETYPE_A. This mismatch is caught at registration time.
size_t faketype_b_hash(const FakeParams &,
                       villagesql::Span<const unsigned char> data) {
  size_t h = 0;
  for (size_t i = 0; i < data.size(); ++i) h = h * 31 + data[i];
  return h;
}

constexpr const char *FAKETYPE_A = "FAKETYPE_A";
constexpr const char *FAKETYPE_B = "FAKETYPE_B";

VEF_GENERATE_ENTRY_POINTS(
    make_extension(VEF_EXTENSION_NAME, "0.0.1-devtest")
        .type(make_type(FAKETYPE_A)
                  .persisted_length(64)
                  .max_decode_buffer_length(64)
                  .encode("faketype_a_encode")
                  .decode("faketype_a_decode")
                  .compare("faketype_a_compare")
                  .params<FakeParams, &FakeParams::parse>()
                  .build())
        .type(make_type(FAKETYPE_B)
                  .persisted_length(64)
                  .max_decode_buffer_length(64)
                  .encode("faketype_b_encode")
                  .decode("faketype_b_decode")
                  .compare("faketype_b_compare")
                  .hash("faketype_b_hash")
                  .build())
        .func(make_type_encode<&faketype_encode>("faketype_a_encode", FAKETYPE_A))
        .func(make_type_decode<&faketype_decode>("faketype_a_decode", FAKETYPE_A))
        .func(make_type_compare<&faketype_compare>("faketype_a_compare", FAKETYPE_A))
        .func(make_type_encode<&faketype_encode>("faketype_b_encode", FAKETYPE_B))
        .func(make_type_decode<&faketype_decode>("faketype_b_decode", FAKETYPE_B))
        .func(make_type_compare<&faketype_compare>("faketype_b_compare", FAKETYPE_B))
        .func(make_type_hash<&faketype_b_hash>("faketype_b_hash", FAKETYPE_B)))
