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

// Bad-registration test: an extension that registers two types with the SAME
// name ("DUP_TYPE"). Registration must reject the extension at INSTALL
// EXTENSION time.

#include "type_build_common.h"

using namespace vsql;

static constexpr const char kDupName1[] = "DUP_TYPE";
static constexpr const char kDupName2[] = "DUP_TYPE";

constexpr auto DUP_TYPE_1 = make_type<kDupName1>()
                                .persisted_length(8)
                                .max_decode_buffer_length(8)
                                .from_string<&tbc::stub_encode>()
                                .to_string<&tbc::stub_decode>()
                                .compare<&tbc::stub_compare>()
                                .build();

constexpr auto DUP_TYPE_2 = make_type<kDupName2>()
                                .persisted_length(8)
                                .max_decode_buffer_length(8)
                                .from_string<&tbc::stub_encode>()
                                .to_string<&tbc::stub_decode>()
                                .compare<&tbc::stub_compare>()
                                .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(DUP_TYPE_1).type(DUP_TYPE_2))
