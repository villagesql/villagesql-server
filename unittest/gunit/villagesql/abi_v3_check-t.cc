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
// ABI v3 compile-time layout check.
//
// Checks abi_v3_layout.inc against the frozen villagesql/stable_sdk/v3
// snapshot with exact sizes, so the snapshot cannot change.  A failure here
// means a change broke extensions compiled against the v3 headers.  See
// abi_layout_check.h.

#include <gtest/gtest.h>

#include "villagesql/stable_sdk/v3/include/villagesql/abi/types.h"

// Exact sizes and the snapshot's field names.  See abi_layout_check.h.
#define ABI_EXACT_SIZES 1
#define ABI_SNAPSHOT 1
#include "unittest/gunit/villagesql/abi_layout_check.h"

#include "unittest/gunit/villagesql/abi_v3_layout.inc"

namespace {

// The checks are static_asserts; this test exists so the binary links and
// runs.
TEST(AbiV3Check, CompileTimeLayoutsVerified) { SUCCEED(); }

}  // namespace
