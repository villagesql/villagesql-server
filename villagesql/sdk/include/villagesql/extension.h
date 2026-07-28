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

#ifndef VILLAGESQL_SDK_EXTENSION_H
#define VILLAGESQL_SDK_EXTENSION_H

// Do not write new code against this header. It is included only for backward
// compatibility and will be removed before Beta. Use the C++ API in
// <villagesql/vsql.h> instead, which is where the current extension-authoring
// documentation lives.

// V1 extension entry point. Supports raw vef_vdf_func_t VDFs and raw function
// pointer type operations (RawFromStringFunc / RawToStringFunc).

#include <villagesql/extension_builder.h>
#include <villagesql/func_builder.h>

#endif  // VILLAGESQL_SDK_EXTENSION_H
