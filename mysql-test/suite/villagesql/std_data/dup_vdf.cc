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

// Bad-registration test: an extension that registers two VDFs with the SAME
// name ("DUP_VDF"). Registration must reject the extension at INSTALL EXTENSION
// time. The impl bodies are irrelevant -- registration fails on the duplicate
// before any function is ever called.

#include <villagesql/vsql.h>

using namespace vsql;

static void dup_impl_1(vsql::IntArg a, vsql::IntResult out) {
  if (a.is_null()) {
    out.set_null();
    return;
  }
  out.set(a.value());
}

static void dup_impl_2(vsql::IntArg a, vsql::IntResult out) {
  if (a.is_null()) {
    out.set_null();
    return;
  }
  out.set(a.value());
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&dup_impl_1>("DUP_VDF").returns(INT).param(INT).build())
        .func(
            make_func<&dup_impl_2>("DUP_VDF").returns(INT).param(INT).build()))
