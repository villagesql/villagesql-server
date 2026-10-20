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

SET(VILLAGESQL_COMMON_SOURCES
  semver.cc
)

# Generated version header. The generator target is always considered out of
# date, so every build regenerates version.h from the
# current VSQL_VERSION in the source tree. configure_file() only rewrites the
# header when its content changes, so the files that include it (see
# villagesql_version_gen dependents) recompile only when the version changes.
#
# The pre-release identifier is resolved at configure time (honoring the
# -DVSQL_PRE_RELEASE_VERSION="" release-strip override) and forwarded to the
# generator; the core version numbers are read live from VSQL_VERSION.
SET(VILLAGESQL_VERSION_H ${CMAKE_BINARY_DIR}/villagesql/include/version.h)

ADD_CUSTOM_TARGET(villagesql_version_gen
  BYPRODUCTS ${VILLAGESQL_VERSION_H}
  COMMAND ${CMAKE_COMMAND}
    -DSRC_DIR=${CMAKE_SOURCE_DIR}
    -DIN_FILE=${CMAKE_SOURCE_DIR}/villagesql/include/version.h.in
    -DOUT_FILE=${VILLAGESQL_VERSION_H}
    -DVSQL_PRE_RELEASE_VERSION=${VSQL_PRE_RELEASE_VERSION}
    -P ${CMAKE_SOURCE_DIR}/villagesql/cmake/gen_version.cmake
  COMMENT "Generating VillageSQL version header"
  VERBATIM)

# Object library for compilation into sql_main (avoids circular dependencies)
ADD_LIBRARY(villagesql_common OBJECT
  ${VILLAGESQL_COMMON_SOURCES})

TARGET_LINK_LIBRARIES(villagesql_common PRIVATE extra::rapidjson)

ADD_DEPENDENCIES(villagesql_common GenError)
ADD_DEPENDENCIES(villagesql_common GenServerSource)
