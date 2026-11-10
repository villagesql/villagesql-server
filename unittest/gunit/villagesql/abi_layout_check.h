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

#ifndef UNITTEST_GUNIT_VILLAGESQL_ABI_LAYOUT_CHECK_H
#define UNITTEST_GUNIT_VILLAGESQL_ABI_LAYOUT_CHECK_H

// Macros for the ABI layout lists (abi_v*_layout.inc, abi_dev_layout.inc).
//
// Each list records the layout of one protocol version's structs and is
// compiled twice:
//   - abi_vN_check-t.cc includes it against the frozen stable_sdk/vN snapshot
//     with ABI_EXACT_SIZES 1, so the snapshot cannot change at all.
//   - abi_dev_check-t.cc includes it against the dev headers with
//     ABI_EXACT_SIZES 0, so the dev headers keep every stable offset but may
//     append fields.
//
// The including file defines, before including this header:
//   ABI_EXACT_SIZES  1: ABI_SIZE requires sizeof == n.  0: sizeof >= n.
//   ABI_SNAPSHOT     1: ABI_FIELD_RENAMED checks the snapshot's field name.
//                    0: it checks the dev headers' field name.
// and each list defines ABI_LIST, the version named in failure messages.
//
//   ABI_SIZE(T, n)        Size of a struct that is only reached through a
//                         pointer, so later protocols may append fields.
//   ABI_SIZE_FIXED(T, n)  Size of a struct embedded by value in another
//                         struct or laid out in an array.  It can never grow.
//   ABI_FIELD(T, f, off)  Offset of field f.
//   ABI_FIELD_RENAMED(T, snapshot_name, dev_name, off)
//                         Offset of a field that the dev headers renamed.
//
// A later list never removes or changes an earlier list's entries.  It only
// adds to them.  Allowed differences from the previous stable list:
//   - New fields appended to a struct, and the struct's larger ABI_SIZE.
//   - New structs.
//   - New union members at existing offsets (they move nothing).
//   - A field renamed at the same offset (an API change, not an ABI change).
//     Check it with ABI_FIELD_RENAMED.
//   - A struct name reused for a new type that the protocol version selects,
//     after the old type is renamed (v1's vef_invalue_t is now
//     vef_invalue_v1_t).
// ABI_SIZE_FIXED sizes never change.  abi_dev_check-t.cc enforces this by
// checking the dev headers against every stable list.
//
// Sizes and offsets assume the 64-bit LP64 layout (Linux/macOS x86-64 and
// ARM64): pointer/size_t = 8 bytes (align 8), int/unsigned int = 4 bytes
// (align 4), bool = 1 byte, double = 8 bytes, long long = 8 bytes,
// enum : int / enum : unsigned int = 4 bytes (align 4).
//
// Empty structs (vef_postrun_result_t before protocol 5) are not listed: their
// size differs between C and C++ and the server never reads them.

#include <cstddef>

// Expands macro arguments such as ABI_V1_INVALUE_T before stringizing them.
#define ABI_STR_(x) #x
#define ABI_STR(x) ABI_STR_(x)

constexpr bool abi_size_ok(size_t actual, size_t expected, bool exact) {
  return exact ? actual == expected : actual >= expected;
}

#define ABI_SIZE_MSG(T) "ABI " ABI_LIST " break: " ABI_STR(T) " size changed"
#define ABI_FIELD_MSG(T, f) \
  "ABI " ABI_LIST " break: " ABI_STR(T) "::" #f " offset changed"

#define ABI_SIZE(T, n) \
  static_assert(abi_size_ok(sizeof(T), n, ABI_EXACT_SIZES), ABI_SIZE_MSG(T))
#define ABI_SIZE_FIXED(T, n) static_assert(sizeof(T) == n, ABI_SIZE_MSG(T))
#define ABI_FIELD(T, f, off) \
  static_assert(offsetof(T, f) == off, ABI_FIELD_MSG(T, f))

// A snapshot check uses the field's name in that snapshot.  The dev check uses
// its current name.  Both must find the field at the same offset.
#if ABI_SNAPSHOT
#define ABI_FIELD_RENAMED(T, snapshot_name, dev_name, off) \
  ABI_FIELD(T, snapshot_name, off)
#else
#define ABI_FIELD_RENAMED(T, snapshot_name, dev_name, off) \
  ABI_FIELD(T, dev_name, off)
#endif

#endif  // UNITTEST_GUNIT_VILLAGESQL_ABI_LAYOUT_CHECK_H
