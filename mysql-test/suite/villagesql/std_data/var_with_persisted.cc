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

// Build-error test: a variable-length type that also declares
// persisted_length() (mutually exclusive). build() must static_assert
// "variable-length types must not ...". Only the make_type chain matters; the
// op stubs live in type_build_common.h.

#include "type_build_common.h"

using namespace vsql;

static constexpr const char kName[] = "VAR_WITH_PERSISTED";

// variable_length_type() with persisted_length() is contradictory.
constexpr auto VAR_WITH_PERSISTED = make_type<kName>()
                                        .variable_length_type()
                                        .persisted_length(16)
                                        .max_decode_buffer_length(16)
                                        .max_persisted_length(32)
                                        .from_string<&tbc::stub_encode>()
                                        .to_string<&tbc::stub_decode>()
                                        .compare<&tbc::stub_compare>()
                                        .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(VAR_WITH_PERSISTED))
