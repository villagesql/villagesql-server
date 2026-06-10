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

SET(VILLAGESQL_VEB_SOURCES
  extension_uninstall_checks.cc
  register.cc
  sql_extension.cc
  validate.cc
  veb_file.cc
  veb_register_type_v1.cc
  veb_register_type_v3.cc
  veb_register_type_v4.cc
)

# Object library for compilation into sql_main (avoids circular dependencies)
ADD_LIBRARY(villagesql_veb OBJECT
  ${VILLAGESQL_VEB_SOURCES})

# Link to libraries (OBJECT libraries can have link dependencies for include paths and compile defs)
TARGET_LINK_LIBRARIES(villagesql_veb PRIVATE extra::rapidjson extra::unordered_dense)

# Include paths for libarchive (no target to link to, must add manually)
TARGET_INCLUDE_DIRECTORIES(villagesql_veb SYSTEM PRIVATE
  ${CMAKE_SOURCE_DIR}/extra/libarchive/libarchive-3.8.1/libarchive
  ${CMAKE_BINARY_DIR}/extra/libarchive/libarchive-3.8.1/libarchive)

ADD_DEPENDENCIES(villagesql_veb GenError)
ADD_DEPENDENCIES(villagesql_veb GenServerSource)
ADD_DEPENDENCIES(villagesql_veb villagesql_common)
