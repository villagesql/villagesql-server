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

# Links VillageSQL object files into sql_main.
# Included from sql/CMakeLists.txt after ADD_STATIC_LIBRARY(sql_main ...).
TARGET_SOURCES(sql_main PRIVATE
  $<TARGET_OBJECTS:villagesql_common>
  $<TARGET_OBJECTS:villagesql_schema>
  $<TARGET_OBJECTS:villagesql_services>
  $<TARGET_OBJECTS:villagesql_sql>
  $<TARGET_OBJECTS:villagesql_system_views>
  $<TARGET_OBJECTS:villagesql_types>
  $<TARGET_OBJECTS:villagesql_vdf>
)
ADD_DEPENDENCIES(sql_main villagesql_common)
ADD_DEPENDENCIES(sql_main villagesql_schema)
ADD_DEPENDENCIES(sql_main villagesql_services)
ADD_DEPENDENCIES(sql_main villagesql_sql)
ADD_DEPENDENCIES(sql_main villagesql_system_views)
ADD_DEPENDENCIES(sql_main villagesql_types)
ADD_DEPENDENCIES(sql_main villagesql_vdf)
