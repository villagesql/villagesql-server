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
// ABI v5 compile-time layout check.
//
// Checks the frozen villagesql/stable_sdk/v5 snapshot against every stable
// layout list.  The v1 and v3 lists use minimum sizes, because protocol 5
// appended fields to their structs.  The v5 list uses exact sizes, so the
// snapshot cannot change.  A failure here means a change broke extensions
// compiled against the v5 headers.  See abi_layout_check.h.

#include <gtest/gtest.h>

#include "villagesql/stable_sdk/v5/include/villagesql/abi/types.h"

// Minimum sizes, and the field names the dev headers had when v5 was frozen,
// which are the snapshot's.  See abi_layout_check.h.
#define ABI_EXACT_SIZES 0
#define ABI_SNAPSHOT 0
#include "unittest/gunit/villagesql/abi_layout_check.h"

#include "unittest/gunit/villagesql/abi_v1_layout.inc"
#include "unittest/gunit/villagesql/abi_v3_layout.inc"

// The v5 list records the v5 sizes, so check them exactly.
#undef ABI_EXACT_SIZES
#define ABI_EXACT_SIZES 1
#include "unittest/gunit/villagesql/abi_v5_layout.inc"

namespace {

// The checks are static_asserts; this test exists so the binary links and
// runs.
TEST(AbiV5Check, CompileTimeLayoutsVerified) { SUCCEED(); }

}  // namespace
