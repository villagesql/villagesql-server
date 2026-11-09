// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

#include <villagesql/vsql.h>
#include <string>
#include <string_view>

static constexpr const char kComplexTypeName[] = "COMPLEX";

void complex_from_string(std::string_view from, vsql::CustomResult out) {}

void complex_to_string(vsql::CustomArg in, vsql::StringResult out) {}

int complex_compare(vsql::CustomArg a, vsql::CustomArg b) { return 0; }

size_t complex_hash(vsql::CustomArg a) { return 0; }

constexpr auto COMPLEX = vsql::make_type<kComplexTypeName>()
                             .persisted_length(16)
                             .max_decode_buffer_length(64)
                             .from_string<&complex_from_string>()
                             .to_string<&complex_to_string>()
                             .compare<&complex_compare>()
                             .hash<&complex_hash>()
                             .intrinsic_default_str("(0, 0).")
                             .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(COMPLEX));
