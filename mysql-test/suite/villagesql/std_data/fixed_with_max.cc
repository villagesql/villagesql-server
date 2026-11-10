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

// Build-error test: a fixed, non-parameterized type that sets
// max_persisted_length (only valid for variable-length or parameterized types).
// build() must static_assert "max_persisted_length() is only valid for ...".
// Only the make_type chain matters; the op stubs live in type_build_common.h.

#include "type_build_common.h"

using namespace vsql;

static constexpr const char kName[] = "FIXED_MAX";

// Fixed footprint + max_persisted_length, but neither variable-length nor
// parameterized; build() must reject this.
constexpr auto FIXED_MAX = make_type<kName>()
                               .persisted_length(16)
                               .max_decode_buffer_length(16)
                               .max_persisted_length(8)
                               .from_string<&tbc::stub_encode>()
                               .to_string<&tbc::stub_decode>()
                               .compare<&tbc::stub_compare>()
                               .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(FIXED_MAX))
