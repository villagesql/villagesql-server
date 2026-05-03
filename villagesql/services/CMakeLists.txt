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

SET(VILLAGESQL_SERVICES_SOURCES
  background_thread.cc
  keyring.cc
  status_vars.cc
  sys_vars.cc
)

# Object library for compilation into sql_main (avoids circular dependencies)
ADD_LIBRARY(villagesql_services OBJECT
  ${VILLAGESQL_SERVICES_SOURCES})

TARGET_LINK_LIBRARIES(villagesql_services PRIVATE extra::rapidjson extra::unordered_dense)

ADD_DEPENDENCIES(villagesql_services GenError)
ADD_DEPENDENCIES(villagesql_services GenServerSource)
ADD_DEPENDENCIES(villagesql_services villagesql_common)
