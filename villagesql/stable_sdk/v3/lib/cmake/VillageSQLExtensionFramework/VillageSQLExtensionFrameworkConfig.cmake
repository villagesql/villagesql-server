# Copyright (c) 2026 VillageSQL Contributors
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# VillageSQLExtensionFramework CMake Config — ABI v3
#
# This file is frozen as part of the ABI v3 snapshot. When ABI v4 is
# introduced, this file stays here unchanged so that extensions compiled
# against v3 continue to resolve the correct v3 headers.
#
# Usage:
#   find_package(VillageSQLExtensionFramework REQUIRED)
#
# Set CMAKE_PREFIX_PATH to the stable_sdk/v3/ directory so cmake finds this file.

set(VillageSQLExtensionFramework_FOUND TRUE)

# Compute the installation prefix from this file's location.
# This file is at: <prefix>/lib/cmake/VillageSQLExtensionFramework/<file>
# Walk up to find the prefix (stable_sdk/v3/).
get_filename_component(_vef_prefix "${CMAKE_CURRENT_LIST_FILE}" PATH)  # .../VillageSQLExtensionFramework
get_filename_component(_vef_prefix "${_vef_prefix}" PATH)              # .../cmake
get_filename_component(_vef_prefix "${_vef_prefix}" PATH)              # .../lib
get_filename_component(_vef_prefix "${_vef_prefix}" PATH)              # .../  (stable_sdk/v3/)

# Set the include directory to the v3 SDK headers
set(VillageSQLExtensionFramework_INCLUDE_DIR "${_vef_prefix}/include")

unset(_vef_prefix)

FUNCTION(VEF_CREATE_VEB)
  SET(VEB_OPTIONS)
  SET(VEB_ONE_VALUE_KW
    NAME
    LIBRARY_TARGET
    MANIFEST
    INSTALL_SQL
  )
  SET(VEB_MULTI_VALUE_KW
    EXTRA_FILES
  )

  CMAKE_PARSE_ARGUMENTS(ARG
    "${VEB_OPTIONS}"
    "${VEB_ONE_VALUE_KW}"
    "${VEB_MULTI_VALUE_KW}"
    ${ARGN}
  )

  # Validate required arguments
  IF(NOT ARG_NAME)
    MESSAGE(FATAL_ERROR "VEF_CREATE_VEB: NAME is required")
  ENDIF()
  IF(NOT ARG_LIBRARY_TARGET)
    MESSAGE(FATAL_ERROR "VEF_CREATE_VEB: LIBRARY_TARGET is required")
  ENDIF()
  IF(NOT ARG_MANIFEST)
    MESSAGE(FATAL_ERROR "VEF_CREATE_VEB: MANIFEST is required")
  ENDIF()
  IF(NOT EXISTS "${ARG_MANIFEST}")
    MESSAGE(FATAL_ERROR "VEF_CREATE_VEB: MANIFEST file not found: ${ARG_MANIFEST}")
  ENDIF()
  IF(ARG_INSTALL_SQL AND NOT EXISTS "${ARG_INSTALL_SQL}")
    MESSAGE(FATAL_ERROR "VEF_CREATE_VEB: INSTALL_SQL file not found: ${ARG_INSTALL_SQL}")
  ENDIF()

  # Set up staging directory
  SET(VEB_STAGING_DIR ${CMAKE_BINARY_DIR}/${ARG_NAME}_veb_staging)
  FILE(MAKE_DIRECTORY ${VEB_STAGING_DIR})
  FILE(MAKE_DIRECTORY ${VEB_STAGING_DIR}/lib)

  # Configure the library target to output to staging/lib
  # TODO(villagesql-windows): Handle DLL when building for Windows
  SET_TARGET_PROPERTIES(${ARG_LIBRARY_TARGET} PROPERTIES
    OUTPUT_NAME ${ARG_NAME}
    LIBRARY_OUTPUT_DIRECTORY ${VEB_STAGING_DIR}/lib
    PREFIX ""
    SUFFIX ".so"
  )

  # Get the library output path for dependency tracking
  GET_TARGET_PROPERTY(LIB_OUTPUT_NAME ${ARG_LIBRARY_TARGET} OUTPUT_NAME)
  IF(NOT LIB_OUTPUT_NAME)
    SET(LIB_OUTPUT_NAME ${ARG_LIBRARY_TARGET})
  ENDIF()
  SET(LIB_OUTPUT_PATH ${VEB_STAGING_DIR}/lib/${LIB_OUTPUT_NAME}.so)

  # Build lists of:
  # - VEB_STAGED_FILES: Full paths to staged files (for CMake dependencies)
  # - VEB_TAR_CONTENTS: Relative paths to include in tar (explicit list)
  SET(VEB_STAGED_FILES)
  SET(VEB_TAR_CONTENTS)

  # Copy manifest.json at build time with dependency tracking
  SET(STAGED_MANIFEST ${VEB_STAGING_DIR}/manifest.json)
  ADD_CUSTOM_COMMAND(
    OUTPUT ${STAGED_MANIFEST}
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${ARG_MANIFEST} ${STAGED_MANIFEST}
    DEPENDS ${ARG_MANIFEST}
    COMMENT "Staging manifest.json for ${ARG_NAME}"
  )
  LIST(APPEND VEB_STAGED_FILES ${STAGED_MANIFEST})
  LIST(APPEND VEB_TAR_CONTENTS "manifest.json")

  # Copy any extra files
  FOREACH(EXTRA_FILE ${ARG_EXTRA_FILES})
    GET_FILENAME_COMPONENT(EXTRA_FILENAME ${EXTRA_FILE} NAME)
    SET(STAGED_EXTRA ${VEB_STAGING_DIR}/${EXTRA_FILENAME})
    ADD_CUSTOM_COMMAND(
      OUTPUT ${STAGED_EXTRA}
      COMMAND ${CMAKE_COMMAND} -E copy_if_different ${EXTRA_FILE} ${STAGED_EXTRA}
      DEPENDS ${EXTRA_FILE}
      COMMENT "Staging ${EXTRA_FILENAME} for ${ARG_NAME}"
    )
    LIST(APPEND VEB_STAGED_FILES ${STAGED_EXTRA})
    LIST(APPEND VEB_TAR_CONTENTS "${EXTRA_FILENAME}")
  ENDFOREACH()

  # Add the lib directory (contains the .so file)
  LIST(APPEND VEB_TAR_CONTENTS "lib")

  # Create the VEB file (tar archive) with explicit file list
  SET(VEB_OUTPUT_FILE ${CMAKE_BINARY_DIR}/${ARG_NAME}.veb)

  ADD_CUSTOM_COMMAND(
    OUTPUT ${VEB_OUTPUT_FILE}
    COMMAND tar -cf ${VEB_OUTPUT_FILE} ${VEB_TAR_CONTENTS}
    DEPENDS ${ARG_LIBRARY_TARGET} ${VEB_STAGED_FILES}
    WORKING_DIRECTORY ${VEB_STAGING_DIR}
    COMMENT "Creating ${ARG_NAME}.veb"
  )

  # Create a target that builds the VEB as part of 'all'
  ADD_CUSTOM_TARGET(veb ALL
    DEPENDS ${VEB_OUTPUT_FILE}
  )

  # Utility target to display VEB contents
  ADD_CUSTOM_TARGET(show_veb
    COMMAND tar -tf ${VEB_OUTPUT_FILE}
    DEPENDS veb
    COMMENT "Contents of ${ARG_NAME}.veb:"
  )

  # Export VEB_OUTPUT to parent scope for install() commands
  SET(VEB_OUTPUT ${VEB_OUTPUT_FILE} PARENT_SCOPE)

  # Automatically add install rule if VillageSQL_VEB_INSTALL_DIR is set
  IF(DEFINED VillageSQL_VEB_INSTALL_DIR)
    INSTALL(FILES ${VEB_OUTPUT_FILE} DESTINATION ${VillageSQL_VEB_INSTALL_DIR})
    MESSAGE(STATUS "VEB will be installed to: ${VillageSQL_VEB_INSTALL_DIR}")
  ENDIF()

  MESSAGE(STATUS "VEB will be created at: ${VEB_OUTPUT_FILE}")
ENDFUNCTION()
