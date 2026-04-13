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

# VillageSQL INFORMATION_SCHEMA system views

SET(VILLAGESQL_SYSTEM_VIEWS_SOURCES
  columns.cc
  extension_registration.cc
  extensions.cc
)

# Object library for compilation into sql_main (avoids circular dependencies)
ADD_LIBRARY(villagesql_system_views OBJECT
  ${VILLAGESQL_SYSTEM_VIEWS_SOURCES})

ADD_DEPENDENCIES(villagesql_system_views GenError)
ADD_DEPENDENCIES(villagesql_system_views GenServerSource)

TARGET_LINK_LIBRARIES(villagesql_system_views PRIVATE extra::rapidjson)
