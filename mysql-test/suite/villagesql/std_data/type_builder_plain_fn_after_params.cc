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

// Compile-fail test: non-parameterized operation registered after .params<P>().
// Expected static_assert: "vsql::TypeBuilder::from_string(): function params
// type is inconsistent with the declared params type"

#include <villagesql/vsql.h>

#include <map>
#include <string>
#include <string_view>

struct FakeParams2 {
  int n;
  static FakeParams2 parse(const std::map<std::string, std::string> &) {
    return {0};
  }
  static void to_strings(const FakeParams2 &,
                         std::map<std::string, std::string> &) {}
};

// Plain (non-parameterized) operations — no MaybeParams / CustomArgWith.
static void plain_encode(std::string_view, vsql::CustomResult out) {
  out.set_null();
}
static void plain_decode(vsql::CustomArg, vsql::StringResult out) {
  out.set_null();
}
static int plain_compare(vsql::CustomArg, vsql::CustomArg) { return 0; }

static constexpr const char kName[] = "FAKETYPE2";

// .params<FakeParams2>() declares a parameterized type, so all operations must
// use the WithParams variants. Using plain TypeEncodeFunc triggers the assert.
constexpr auto FAKETYPE2 =
    vsql::make_type<kName>()
        .max_persisted_length(64)
        .params<FakeParams2, &FakeParams2::parse, &FakeParams2::to_strings>()
        .from_string<&plain_encode>()  // ERROR: OpP=void,
                                       // ParamsType=FakeParams2
        .to_string<&plain_decode>()
        .compare<&plain_compare>()
        .build();

VEF_GENERATE_ENTRY_POINTS(vsql::make_extension().type(FAKETYPE2))
