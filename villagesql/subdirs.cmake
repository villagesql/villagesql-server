# Copyright (c) 2026 VillageSQL Contributors
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <https://www.gnu.org/licenses/>.

# VillageSQL subdirectories included from sql/CMakeLists.txt.
# Binary dirs are relative to sql/'s build directory (CMAKE_CURRENT_BINARY_DIR).
# Kept here so the listing lives alongside the villagesql source tree.
ADD_SUBDIRECTORY(../villagesql/common ${CMAKE_CURRENT_BINARY_DIR}/villagesql/common)
ADD_SUBDIRECTORY(../villagesql/schema ${CMAKE_CURRENT_BINARY_DIR}/villagesql/schema)
ADD_SUBDIRECTORY(../villagesql/services ${CMAKE_CURRENT_BINARY_DIR}/villagesql/services)
ADD_SUBDIRECTORY(../villagesql/sql ${CMAKE_CURRENT_BINARY_DIR}/villagesql/sql)
ADD_SUBDIRECTORY(../villagesql/system_views ${CMAKE_CURRENT_BINARY_DIR}/villagesql/system_views)
ADD_SUBDIRECTORY(../villagesql/types ${CMAKE_CURRENT_BINARY_DIR}/villagesql/types)
ADD_SUBDIRECTORY(../villagesql/vdf ${CMAKE_CURRENT_BINARY_DIR}/villagesql/vdf)
ADD_SUBDIRECTORY(../villagesql/veb ${CMAKE_CURRENT_BINARY_DIR}/villagesql/veb)

IF(MY_COMPILER_IS_GNU_OR_CLANG)
  FOREACH(vsql_target
    villagesql_common
    villagesql_schema
    villagesql_services
    villagesql_sql
    villagesql_system_views
    villagesql_types
    villagesql_vdf
    villagesql_veb
  )
    TARGET_COMPILE_OPTIONS(${vsql_target} PRIVATE -Werror)
  ENDFOREACH()
ENDIF()
