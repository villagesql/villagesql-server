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

# All VillageSQL subdirectories are managed by sql/CMakeLists.txt to avoid circular dependencies

# =============================================================================
# VillageSQL Extension SDK Packaging
# =============================================================================
# Creates a standalone SDK tarball for extension developers.
# Built automatically as part of 'make' (or explicitly with 'make sdk').
#
# The tarball follows the standard CMake package layout so that
# find_package(VillageSQLExtensionFramework) works automatically when
# CMAKE_PREFIX_PATH points to the extracted SDK directory.
# The SDK name should not include the code base.

set(SDK_VERSION "${VSQL_ABBR_VERSION}")
set(SDK_NAME "villagesql-extension-sdk-${VSQL_ABBR_VERSION}")
set(SDK_STAGING_DIR "${CMAKE_BINARY_DIR}/${SDK_NAME}")

# The stable SDK snapshot to package and use for ABI compliance tests.
# Update this variable when cutting a new stable ABI version.
set(STABLE_SDK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/stable_sdk/v3")

# Generate villagesql_config script with version substituted
configure_file(
  "${STABLE_SDK_DIR}/bin/villagesql_config.in"
  "${CMAKE_BINARY_DIR}/sdk/villagesql_config"
  @ONLY
)

# Generate sdk_version.h with the VillageSQL version embedded
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/sdk/include/villagesql/sdk_version.h.in"
  "${CMAKE_BINARY_DIR}/sdk/include/villagesql/sdk_version.h"
  @ONLY
)

# Generate test environment file for ABI compatibility tests.
# Sourced by mysql-tests to locate SDK paths.
file(WRITE "${CMAKE_BINARY_DIR}/villagesql_test_env.sh"
  "VILLAGESQL_SOURCE_DIR=${CMAKE_SOURCE_DIR}\n"
  "VILLAGESQL_STABLE_SDK=${STABLE_SDK_DIR}\n"
  "VILLAGESQL_STABLE_SDK_V1=${CMAKE_CURRENT_SOURCE_DIR}/stable_sdk/v1\n"
  "VILLAGESQL_DEV_SDK_INCLUDE=${SDK_STAGING_DIR}/include-dev\n"
)

# Collect SDK input files for dependency tracking so the sdk target reruns
# whenever any of these change.
file(GLOB_RECURSE SDK_STABLE_HEADERS CONFIGURE_DEPENDS
  "${STABLE_SDK_DIR}/include/villagesql/*.h"
)
file(GLOB_RECURSE SDK_DEV_HEADERS CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/sdk/include/villagesql/*.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/sdk/include/villagesql/*.h.in"
)
file(GLOB_RECURSE SDK_TEMPLATE_FILES CONFIGURE_DEPENDS
  "${STABLE_SDK_DIR}/template/*"
)

# SDK packaging command - reruns whenever tracked input files change
add_custom_command(
  OUTPUT "${CMAKE_BINARY_DIR}/${SDK_NAME}.tar.gz"
  DEPENDS
    ${SDK_STABLE_HEADERS}
    ${SDK_DEV_HEADERS}
    ${SDK_TEMPLATE_FILES}
    "${CMAKE_BINARY_DIR}/sdk/include/villagesql/sdk_version.h"
    "${CMAKE_BINARY_DIR}/sdk/villagesql_config"
    "${CMAKE_SOURCE_DIR}/cmake/VillageSQLExtensionFrameworkConfig.cmake"
    "${CMAKE_SOURCE_DIR}/cmake/FindVillageSQL.cmake"

  # Clean and create staging directory structure
  COMMAND ${CMAKE_COMMAND} -E remove_directory "${SDK_STAGING_DIR}"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/bin"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/lib/cmake/VillageSQLExtensionFramework"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/include/villagesql"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/include-dev/villagesql"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/template/src"

  # Copy villagesql_config script and make executable
  COMMAND ${CMAKE_COMMAND} -E copy
    "${CMAKE_BINARY_DIR}/sdk/villagesql_config"
    "${SDK_STAGING_DIR}/bin/"
  COMMAND chmod +x "${SDK_STAGING_DIR}/bin/villagesql_config"

  # Copy CMake files
  COMMAND ${CMAKE_COMMAND} -E copy
    "${CMAKE_SOURCE_DIR}/cmake/VillageSQLExtensionFrameworkConfig.cmake"
    "${CMAKE_SOURCE_DIR}/cmake/FindVillageSQL.cmake"
    "${SDK_STAGING_DIR}/lib/cmake/VillageSQLExtensionFramework/"

  # Copy stable SDK headers (v3 includes the full feature set with subdirectories)
  COMMAND ${CMAKE_COMMAND} -E copy_directory
    "${STABLE_SDK_DIR}/include/villagesql"
    "${SDK_STAGING_DIR}/include/villagesql"

  # Overwrite the snapshot's frozen sdk_version.h with one generated from the
  # release version (like villagesql_config's @SDK_VERSION@), so an extension
  # built against the shipped SDK reports the version it was actually built from
  # rather than the value pinned in the frozen v3 snapshot. (The frozen value is
  # only seen when building directly against stable_sdk/v3, as the abi_v3 tests
  # do.)
  COMMAND ${CMAKE_COMMAND} -E copy
    "${CMAKE_BINARY_DIR}/sdk/include/villagesql/sdk_version.h"
    "${SDK_STAGING_DIR}/include/villagesql/"

  # Copy dev (unstable) SDK headers (all subdirectories picked up automatically)
  COMMAND ${CMAKE_COMMAND} -E copy_directory
    "${CMAKE_CURRENT_SOURCE_DIR}/sdk/include/villagesql"
    "${SDK_STAGING_DIR}/include-dev/villagesql"
  COMMAND ${CMAKE_COMMAND} -E copy
    "${CMAKE_BINARY_DIR}/sdk/include/villagesql/sdk_version.h"
    "${SDK_STAGING_DIR}/include-dev/villagesql/"

  # Copy template project
  COMMAND ${CMAKE_COMMAND} -E copy
    "${STABLE_SDK_DIR}/template/CMakeLists.txt"
    "${STABLE_SDK_DIR}/template/manifest.json"
    "${SDK_STAGING_DIR}/template/"
  COMMAND ${CMAKE_COMMAND} -E copy
    "${STABLE_SDK_DIR}/template/src/extension.cc"
    "${SDK_STAGING_DIR}/template/src/"

  # Create the tarball
  COMMAND tar -czf "${CMAKE_BINARY_DIR}/${SDK_NAME}.tar.gz" -C "${CMAKE_BINARY_DIR}" "${SDK_NAME}"

  COMMENT "Creating VillageSQL Extension SDK: ${SDK_NAME}.tar.gz"
  VERBATIM
)

# Named target so 'make sdk' still works; depends on tarball for file tracking
add_custom_target(sdk ALL DEPENDS "${CMAKE_BINARY_DIR}/${SDK_NAME}.tar.gz")

# Install SDK components from the staged SDK directory.
# This ensures 'make install' installs exactly what the SDK tarball contains.
# NOTE: If you add a new top-level directory to the SDK staging target above,
# add a corresponding install() rule here.
install(DIRECTORY "${SDK_STAGING_DIR}/include/villagesql"
  DESTINATION "${INSTALL_INCLUDEDIR}"
)
install(DIRECTORY "${SDK_STAGING_DIR}/include-dev/villagesql"
  DESTINATION "${INSTALL_INCLUDEDIR}-dev"
)
install(DIRECTORY "${SDK_STAGING_DIR}/lib/cmake/VillageSQLExtensionFramework"
  DESTINATION "${INSTALL_LIBDIR}/cmake"
)
install(PROGRAMS "${SDK_STAGING_DIR}/bin/villagesql_config"
  DESTINATION "${INSTALL_BINDIR}"
)

# Add VillageSQL example and test extensions
IF(NOT WITHOUT_SERVER)
  include(ExternalProject)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/VsqlExtension.cmake)

  # Example extensions (each has its own CMakeLists.txt for standalone use)
  vsql_add_extension(
    DIR_NAME    vsql-simple
    EXT_TARGET  vsql_simple_extension
    VEB_NAME    vsql_simple
    SOURCE_BASE ${CMAKE_CURRENT_SOURCE_DIR}/examples
    BINARY_BASE ${CMAKE_CURRENT_BINARY_DIR}/examples
    INSTALL_DEST      ${INSTALL_VEBDIR}
    INSTALL_COMPONENT ExampleVebs
  )
  vsql_add_extension(
    DIR_NAME    vsql-complex
    EXT_TARGET  vsql_complex_extension
    VEB_NAME    vsql_complex
    SOURCE_BASE ${CMAKE_CURRENT_SOURCE_DIR}/examples
    BINARY_BASE ${CMAKE_CURRENT_BINARY_DIR}/examples
    INSTALL_DEST      ${INSTALL_VEBDIR}
    INSTALL_COMPONENT ExampleVebs
  )
  vsql_add_extension(
    DIR_NAME    vsql-tvector
    EXT_TARGET  vsql_tvector_extension
    VEB_NAME    vsql_tvector
    SOURCE_BASE ${CMAKE_CURRENT_SOURCE_DIR}/examples
    BINARY_BASE ${CMAKE_CURRENT_BINARY_DIR}/examples
    INSTALL_DEST ${INSTALL_VEBDIR}
  )
  # Test extensions (share a single CMakeLists.txt template)
  vsql_add_test_extension(vsql-config-vars-test  vsql_config_vars_test)
  vsql_add_test_extension(vsql-slow-query-log    vsql_slow_query_log)
  vsql_add_test_extension(vsql-usage-counter      vsql_usage_counter  VERSION 0.0.1)
  vsql_add_test_extension(vsql-range-clamp-test   vsql_range_clamp_test)
  vsql_add_test_extension(vsql-test-only          vsql_test_only)
  vsql_add_test_extension(vsql-storage-test       vsql_storage_test)
  vsql_add_test_extension(vsql-index-test         vsql_index_test)

  include(test-extensions/CMakeLists.txt)
ENDIF()
