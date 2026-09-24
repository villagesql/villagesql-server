# Copyright (c) 2026 VillageSQL Contributors
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have either included with
# the program or referenced in the documentation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# Generate "something" to trigger cmake rerun when VSQL_VERSION changes
CONFIGURE_FILE(
  ${CMAKE_SOURCE_DIR}/VSQL_VERSION
  ${CMAKE_BINARY_DIR}/VSQL_VERSION.dep
)

# Read value for a variable from VSQL_VERSION.
# Variables can be overridden at cmake time, e.g.:
#   cmake -DVSQL_PRE_RELEASE_VERSION="" ...
# This is useful for release builds to strip the "-dev" suffix.

MACRO(VSQL_GET_CONFIG_VALUE keyword var)
 IF(NOT DEFINED ${var})
   FILE (STRINGS ${CMAKE_SOURCE_DIR}/VSQL_VERSION str REGEX "^[ ]*${keyword}=")
   IF(str)
     STRING(REPLACE "${keyword}=" "" str ${str})
     STRING(REGEX REPLACE  "[ ].*" ""  str "${str}")
     SET(${var} ${str})
   ENDIF()
 ENDIF()
ENDMACRO()

# Read VillageSQL version for configure script

MACRO(GET_VSQL_VERSION)
  VSQL_GET_CONFIG_VALUE("VSQL_CODE_BASE" VSQL_CODE_BASE)
  VSQL_GET_CONFIG_VALUE("VSQL_MAJOR_VERSION" VSQL_MAJOR_VERSION)
  VSQL_GET_CONFIG_VALUE("VSQL_MINOR_VERSION" VSQL_MINOR_VERSION)
  VSQL_GET_CONFIG_VALUE("VSQL_PATCH_VERSION" VSQL_PATCH_VERSION)
  VSQL_GET_CONFIG_VALUE("VSQL_PRE_RELEASE_VERSION" VSQL_PRE_RELEASE_VERSION)

  IF(NOT DEFINED VSQL_CODE_BASE OR
     NOT DEFINED VSQL_MAJOR_VERSION OR
     NOT DEFINED VSQL_MINOR_VERSION OR
     NOT DEFINED VSQL_PATCH_VERSION)
    MESSAGE(FATAL_ERROR "VSQL_VERSION file cannot be parsed.")
  ENDIF()

  # Construct the abbreviated semver string (no code base) used in the MySQL
  # version suffix.
  SET(VSQL_ABBR_VERSION "${VSQL_MAJOR_VERSION}.${VSQL_MINOR_VERSION}.${VSQL_PATCH_VERSION}")
  IF(VSQL_PRE_RELEASE_VERSION)
    SET(VSQL_ABBR_VERSION "${VSQL_ABBR_VERSION}-${VSQL_PRE_RELEASE_VERSION}")
    SET(VSQL_HAS_PRE_RELEASE 1)
  ELSE()
    SET(VSQL_HAS_PRE_RELEASE 0)
  ENDIF()

  # Construct the full semver string, including the code base prefix.
  SET(VSQL_VERSION_STRING "${VSQL_CODE_BASE}_${VSQL_ABBR_VERSION}")

  # Set EXTRA_VERSION for mysql_version.cmake to use. This becomes the suffix
  # in the MySQL version string (e.g., 8.4.6-villagesql-0.1.0-dev).
  #
  # Deliberately no git hash here: EXTRA_VERSION lands in
  # include/mysql_version.h, which nearly every TU includes, so a hash would
  # rebuild the world on every HEAD move and miss in a ccache shared across
  # checkouts sitting at different commits. The commit is available at runtime
  # via @@villagesql_build_info instead.
  SET(EXTRA_VERSION "-villagesql-${VSQL_ABBR_VERSION}")
ENDMACRO()

# Get VillageSQL version
GET_VSQL_VERSION()
