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

// Build-error test: a parameterized type declares params<P>() but no
// resolve_params() (and no int_to_params()). A parameterized type must provide
// a resolver, so build() must static_assert "resolve_params() is required when
// params<P>() is used". Only the make_type chain matters; the op stubs live in
// type_build_common.h.

#include "type_build_common.h"

using namespace vsql;

static constexpr const char kName[] = "PARAMS_NO_RESOLVE";

// .resolve_params<>() is deliberately omitted.
constexpr auto PARAMS_NO_RESOLVE =
    make_type<kName>()
        .max_persisted_length(16)
        .params<tbc::LenParam, &tbc::LenParam::parse,
                &tbc::LenParam::to_strings>()
        .from_string<&tbc::stub_param_encode>()
        .to_string<&tbc::stub_param_decode>()
        .compare<&tbc::stub_param_compare>()
        .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(PARAMS_NO_RESOLVE))
