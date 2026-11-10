// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

#ifndef SQLCOM_COMPACT_INDEX_H
#define SQLCOM_COMPACT_INDEX_H

#include "my_sqlcommand.h"

// Compact indexing for com_stat arrays. VSQL commands start at enum value 1024
// to keep stable IDs, but com_stat arrays pack them contiguously to avoid a
// gap. MySQL commands (0..SQLCOM_MYSQL_COUNT-1) keep their enum value as the
// index. VSQL commands are packed immediately after, starting at index
// SQLCOM_MYSQL_COUNT.

inline constexpr unsigned int sqlcom_compact_index(unsigned int cmd) {
  return (cmd < (unsigned int)SQLCOM_MYSQL_COUNT)
             ? cmd
             : (unsigned int)SQLCOM_MYSQL_COUNT +
                   (cmd - (unsigned int)SQLCOM_VSQL_FIRST);
}

inline constexpr unsigned int sqlcom_from_compact_index(unsigned int idx) {
  return (idx < (unsigned int)SQLCOM_MYSQL_COUNT)
             ? idx
             : (unsigned int)SQLCOM_VSQL_FIRST +
                   (idx - (unsigned int)SQLCOM_MYSQL_COUNT);
}

inline constexpr unsigned int SQLCOM_COMPACT_COUNT =
    (unsigned int)SQLCOM_MYSQL_COUNT +
    ((unsigned int)SQLCOM_END - (unsigned int)SQLCOM_VSQL_FIRST);

#endif  // SQLCOM_COMPACT_INDEX_H
