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

#ifndef VILLAGESQL_INCLUDE_BYTEORDER_H
#define VILLAGESQL_INCLUDE_BYTEORDER_H

// VillageSQL: Helper functions for processing raw bytes along the lines of
// @file include/my_byteorder.h.

#include <string.h>

#include "my_inttypes.h"

// Return unsigned bytes pointed to by ptr in an ulonglong. Endianness
// agnostic as the byte layout of memory pointed to by ptr should be the
// same as the byte layout of ulonglong.
inline ulonglong ulonglongget(const uchar *ptr) {
  ulonglong val;
  memcpy(&val, ptr, sizeof(val));
  return val;
}

// Return implicitly unsigned bytes pointed to by ptr in an ulonglong.
inline ulonglong ulonglongget(const char *ptr) {
  return ulonglongget(
      static_cast<const uchar *>(static_cast<const void *>(ptr)));
}

#endif  // VILLAGESQL_INCLUDE_BYTEORDER_H
