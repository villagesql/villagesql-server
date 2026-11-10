/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */
// ABI dev compile-time layout check.
//
// Checks the dev headers (villagesql/sdk/include/villagesql/abi/types.h)
// against every stable layout list and against abi_dev_layout.inc.  The
// stable lists use minimum sizes, so the dev ABI keeps every stable offset but
// may append fields.  The dev list uses exact sizes, so a new field must be
// recorded there.  See abi_layout_check.h.

#include <gtest/gtest.h>

#include "villagesql/sdk/include/villagesql/abi/types.h"

// Minimum sizes and the dev headers' field names.  See abi_layout_check.h.
#define ABI_EXACT_SIZES 0
#define ABI_SNAPSHOT 0
#include "unittest/gunit/villagesql/abi_layout_check.h"

#include "unittest/gunit/villagesql/abi_v1_layout.inc"
#include "unittest/gunit/villagesql/abi_v3_layout.inc"

// The dev list records the dev sizes, so check them exactly.
#undef ABI_EXACT_SIZES
#define ABI_EXACT_SIZES 1
#include "unittest/gunit/villagesql/abi_dev_layout.inc"

namespace {

// The checks are static_asserts; this test exists so the binary links and
// runs.
TEST(AbiDevCheck, CompileTimeLayoutsVerified) { SUCCEED(); }

}  // namespace
