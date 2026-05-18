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
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// Compile-fail test: .params<P>() called after operations are already
// registered. Expected static_assert: "vsql::TypeBuilder: .params<P>() must be
// called before .from_string(), .to_string(), and .compare()"

#include <villagesql/vsql.h>

#include <map>
#include <string>
#include <string_view>

struct FakeParams3 {
  int n;
  static FakeParams3 parse(const std::map<std::string, std::string> &) {
    return {0};
  }
  static void to_strings(const FakeParams3 &,
                         std::map<std::string, std::string> &) {}
};

static void plain_encode(std::string_view, vsql::CustomResult out) {
  out.set_null();
}
static void plain_decode(vsql::CustomArg, vsql::StringResult out) {
  out.set_null();
}
static int plain_compare(vsql::CustomArg, vsql::CustomArg) { return 0; }

static constexpr const char kName[] = "FAKETYPE3";

// from_string registered first, then .params<>() — ordering is wrong.
constexpr auto FAKETYPE3 =
    vsql::make_type<kName>()
        .max_persisted_length(64)
        .from_string<&plain_encode>()
        .params<FakeParams3, &FakeParams3::parse,
                &FakeParams3::to_strings>()  // ERROR: HasFromString=true
        .to_string<&plain_decode>()
        .compare<&plain_compare>()
        .build();

VEF_GENERATE_ENTRY_POINTS(vsql::make_extension().type(FAKETYPE3))
