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

// Minimal VEF extension that tests extensions with function names > 64 chars
// don't get registered.

#include <villagesql/vsql.h>

using namespace vsql;

void foo(vsql::IntResult out) { out.set(0); }

VEF_GENERATE_ENTRY_POINTS(make_extension().func(
    make_func<&foo>("really_"
                    "looooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                    "oooooooooooooooooong_function_name")
        .returns(INT)
        .no_params()
        .deterministic()
        .build()))
