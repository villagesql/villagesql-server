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

SET(VILLAGESQL_SCHEMA_SOURCES
  schema_manager.cc
  util.cc
  upgrade.cc
  victionary_client.cc
  descriptor/extension_descriptor.cc
  descriptor/func_descriptor.cc
  descriptor/type_context.cc
  descriptor/type_descriptor.cc
  systable/custom_columns.cc
  systable/custom_sp_params.cc
  systable/extensions.cc
  systable/helpers.cc
  systable/properties.cc
)

# Object library for compilation into sql_main (avoids circular dependencies)
ADD_LIBRARY(villagesql_schema OBJECT
  ${VILLAGESQL_SCHEMA_SOURCES})

TARGET_LINK_LIBRARIES(villagesql_schema PRIVATE extra::rapidjson)

ADD_DEPENDENCIES(villagesql_schema GenError)
ADD_DEPENDENCIES(villagesql_schema GenServerSource)
TARGET_LINK_LIBRARIES(villagesql_schema PRIVATE extra::unordered_dense)

# Configure VillageSQL schema file with version substitution for build directory
CONFIGURE_FILE(villagesql_schema.sql.in
  ${CMAKE_BINARY_DIR}/${INSTALL_MYSQLSHAREDIR}/villagesql_schema.sql
  @ONLY
)

# Install the configured VillageSQL schema file
INSTALL(FILES ${CMAKE_BINARY_DIR}/${INSTALL_MYSQLSHAREDIR}/villagesql_schema.sql
  DESTINATION ${INSTALL_MYSQLSHAREDIR}
  COMPONENT Server
)
