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

set(SDK_VERSION "${VSQL_VERSION_STRING}")
set(SDK_NAME "villagesql-extension-sdk-${SDK_VERSION}")
set(SDK_STAGING_DIR "${CMAKE_BINARY_DIR}/${SDK_NAME}")

# The stable SDK snapshot to package and use for ABI compliance tests.
# Update this variable when cutting a new stable ABI version.
set(STABLE_SDK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/stable_sdk/v1")

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
  "VILLAGESQL_DEV_SDK_INCLUDE=${SDK_STAGING_DIR}/include-dev\n"
)

# SDK packaging target - built by default (ALL)
add_custom_target(sdk ALL
  # Clean and create staging directory structure
  COMMAND ${CMAKE_COMMAND} -E remove_directory "${SDK_STAGING_DIR}"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/bin"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/lib/cmake/VillageSQLExtensionFramework"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/include/villagesql/abi"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/include-dev/villagesql/abi"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${SDK_STAGING_DIR}/include-dev/villagesql/vsql"
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

  # Copy stable SDK headers
  COMMAND ${CMAKE_COMMAND} -E copy
    "${STABLE_SDK_DIR}/include/villagesql/extension.h"
    "${STABLE_SDK_DIR}/include/villagesql/extension_builder.h"
    "${STABLE_SDK_DIR}/include/villagesql/func_builder.h"
    "${STABLE_SDK_DIR}/include/villagesql/type_builder.h"
    "${SDK_STAGING_DIR}/include/villagesql/"
  COMMAND ${CMAKE_COMMAND} -E copy
    "${STABLE_SDK_DIR}/include/villagesql/abi/types.h"
    "${SDK_STAGING_DIR}/include/villagesql/abi/"

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

# Add VillageSQL example extensions
IF(NOT WITHOUT_SERVER)
  include(ExternalProject)

  ExternalProject_Add(vsql_simple_extension
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/examples/vsql-simple
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-simple-build
    CMAKE_GENERATOR ${CMAKE_GENERATOR}
    CMAKE_ARGS
      "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
      "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
      "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
      "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    DEPENDS sdk
    BUILD_ALWAYS ON
    INSTALL_COMMAND ""
  )

  add_custom_target(copy_vsql_simple_veb
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-simple-build/vsql_simple.veb
      ${CMAKE_BINARY_DIR}/veb_output_directory/
    DEPENDS vsql_simple_extension
    COMMENT "Copying vsql_simple.veb to veb_output_directory"
  )

  add_custom_target(simple_veb ALL)
  add_dependencies(simple_veb copy_vsql_simple_veb)

  # Install VEB to lib/veb directory
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-simple-build/vsql_simple.veb
    DESTINATION ${INSTALL_VEBDIR}
    COMPONENT ExampleVebs
  )

  ExternalProject_Add(vsql_complex_extension
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/examples/vsql-complex
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-complex-build
    CMAKE_GENERATOR ${CMAKE_GENERATOR}
    CMAKE_ARGS
      "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
      "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
      "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
      "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    DEPENDS sdk
    BUILD_ALWAYS ON
    INSTALL_COMMAND ""
  )

  add_custom_target(copy_vsql_complex_veb
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-complex-build/vsql_complex.veb
      ${CMAKE_BINARY_DIR}/veb_output_directory/
    DEPENDS vsql_complex_extension
    COMMENT "Copying vsql_complex.veb to veb_output_directory"
  )

  add_custom_target(complex_veb ALL)
  add_dependencies(complex_veb copy_vsql_complex_veb)

  # Install VEB to lib/veb directory
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-complex-build/vsql_complex.veb
    DESTINATION ${INSTALL_VEBDIR}
    COMPONENT ExampleVebs
  )

  ExternalProject_Add(vsql_tvector_extension
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/examples/vsql-tvector
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-tvector-build
    CMAKE_GENERATOR ${CMAKE_GENERATOR}
    CMAKE_ARGS
      "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
      "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
      "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
      "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    DEPENDS sdk
    BUILD_ALWAYS ON
    INSTALL_COMMAND ""
  )

  add_custom_target(copy_vsql_tvector_veb
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-tvector-build/vsql_tvector.veb
      ${CMAKE_BINARY_DIR}/veb_output_directory/
    DEPENDS vsql_tvector_extension
    COMMENT "Copying vsql_tvector.veb to veb_output_directory"
  )

  add_custom_target(tvector_veb ALL)
  add_dependencies(tvector_veb copy_vsql_tvector_veb)

  # Install VEB to lib/veb directory
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-tvector-build/vsql_tvector.veb
    DESTINATION ${INSTALL_VEBDIR}
  )

  ExternalProject_Add(vsql_config_vars_test_extension
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/vsql-config-vars-test
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-config-vars-test-build
    CMAKE_GENERATOR ${CMAKE_GENERATOR}
    CMAKE_ARGS
      "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
      "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
      "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
      "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    DEPENDS sdk
    BUILD_ALWAYS ON
    INSTALL_COMMAND ""
  )

  ExternalProject_Add(vsql_keyring_reader_extension
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/examples/vsql-keyring-reader
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-keyring-reader-build
    CMAKE_GENERATOR ${CMAKE_GENERATOR}
    CMAKE_ARGS
      "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
      "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
      "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
      "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    DEPENDS sdk
    BUILD_ALWAYS ON
    INSTALL_COMMAND ""
  )

  add_custom_target(copy_vsql_config_vars_test_veb
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-config-vars-test-build/vsql_config_vars_test.veb
      ${CMAKE_BINARY_DIR}/veb_output_directory/
    DEPENDS vsql_config_vars_test_extension
    COMMENT "Copying vsql_config_vars_test.veb to veb_output_directory"
  )

  add_custom_target(config_vars_test_veb ALL)
  add_dependencies(config_vars_test_veb copy_vsql_config_vars_test_veb)

  add_custom_target(copy_vsql_keyring_reader_veb
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_CURRENT_BINARY_DIR}/examples/vsql-keyring-reader-build/vsql_keyring_reader.veb
      ${CMAKE_BINARY_DIR}/veb_output_directory/
    DEPENDS vsql_keyring_reader_extension
    COMMENT "Copying vsql_keyring_reader.veb to veb_output_directory"
  )

  add_custom_target(keyring_reader_veb ALL)
  add_dependencies(keyring_reader_veb copy_vsql_keyring_reader_veb)

  ExternalProject_Add(vsql_test_only_extension
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/vsql-test-only
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-test-only-build
    CMAKE_GENERATOR ${CMAKE_GENERATOR}
    CMAKE_ARGS
      "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
      "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
      "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
    DEPENDS sdk
    BUILD_ALWAYS ON
    INSTALL_COMMAND ""
  )

  add_custom_target(copy_vsql_test_only_veb
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-test-only-build/vsql_test_only.veb
      ${CMAKE_BINARY_DIR}/veb_output_directory/
    DEPENDS vsql_test_only_extension
    COMMENT "Copying vsql_test_only.veb to veb_output_directory"
  )

  add_custom_target(test_only_veb ALL)
  add_dependencies(test_only_veb copy_vsql_test_only_veb)
ENDIF()
