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

// VillageSQL Extension Template (v3 SDK)
//
// This is a template for creating VillageSQL extensions using the stable v3
// SDK. See villagesql/vsql.h for the full typed C++ API.

#include <villagesql/vsql.h>

using namespace vsql;

// Example function: adds two integers
void add_impl(IntArg a, IntArg b, IntResult out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  out.set(a.value() + b.value());
}

VEF_GENERATE_ENTRY_POINTS(make_extension().func(
    make_func<&add_impl>("my_add").returns(INT).param(INT).param(INT).build()))
