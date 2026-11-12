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

// Build-error test: a type that omits to_string(). build() must static_assert
// "to_string() is required before build()". Only the make_type chain matters;
// the op stubs live in type_build_common.h.

#include "type_build_common.h"

using namespace vsql;

static constexpr const char kName[] = "MISSING_TO_STRING";

// .to_string<>() is deliberately omitted.
constexpr auto MISSING_TO_STRING = make_type<kName>()
                                       .persisted_length(8)
                                       .max_decode_buffer_length(8)
                                       .from_string<&tbc::stub_encode>()
                                       .compare<&tbc::stub_compare>()
                                       .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(MISSING_TO_STRING))
