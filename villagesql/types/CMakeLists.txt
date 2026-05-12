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

SET(VILLAGESQL_TYPES_SOURCES
  from_string_inference.cc
  type_decoder.cc
  type_encoder.cc
  type_function.cc
  type_op.cc
  util.cc
)

# Object library for compilation into sql_main (avoids circular dependencies)
ADD_LIBRARY(villagesql_types OBJECT
  ${VILLAGESQL_TYPES_SOURCES})

TARGET_LINK_LIBRARIES(villagesql_types PRIVATE extra::rapidjson)

ADD_DEPENDENCIES(villagesql_types GenError)
ADD_DEPENDENCIES(villagesql_types GenServerSource)
