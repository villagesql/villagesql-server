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

// Build-error test: a variable-length type that omits max_persisted_length().
// build() must static_assert "variable-length types must call
// .max_persisted_length(N)". Only the make_type chain matters; the op stubs
// live in type_build_common.h.

#include "type_build_common.h"

using namespace vsql;

static constexpr const char kName[] = "VAR_NO_MAX";

// .max_persisted_length() is deliberately omitted for a variable-length type.
constexpr auto VAR_NO_MAX = make_type<kName>()
                                .variable_length_type()
                                .max_decode_buffer_length(16)
                                .from_string<&tbc::stub_encode>()
                                .to_string<&tbc::stub_decode>()
                                .compare<&tbc::stub_compare>()
                                .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(VAR_NO_MAX))
