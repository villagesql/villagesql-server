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

# vsql_add_extension(
#   DIR_NAME       <subdir name under SOURCE_BASE>
#   EXT_TARGET     <CMake ExternalProject target name>
#   VEB_NAME       <stem of the .veb file, e.g. vsql_simple>
#   SOURCE_BASE    <absolute path containing DIR_NAME>
#   BINARY_BASE    <absolute path for build output>
#   [INSTALL_DEST  <destination for install(); omit to skip>]
#   [INSTALL_COMPONENT <component name>]
# )
#
# For standalone extensions that carry their own CMakeLists.txt (e.g. examples).
function(vsql_add_extension)
  cmake_parse_arguments(ARG
    ""
    "DIR_NAME;EXT_TARGET;VEB_NAME;SOURCE_BASE;BINARY_BASE;INSTALL_DEST;INSTALL_COMPONENT"
    ""
    ${ARGN}
  )

  # Under gcov, instrument the extension .so so running its tests produces
  # coverage.
  set(_ext_cov_args "")
  if(ENABLE_GCOV)
    set(_ext_cov_args
      "-DCMAKE_CXX_FLAGS=--coverage"
      "-DCMAKE_C_FLAGS=--coverage"
      "-DCMAKE_SHARED_LINKER_FLAGS=--coverage"
      "-DCMAKE_EXE_LINKER_FLAGS=--coverage")
  endif()

  ExternalProject_Add(${ARG_EXT_TARGET}
    SOURCE_DIR ${ARG_SOURCE_BASE}/${ARG_DIR_NAME}
    BINARY_DIR ${ARG_BINARY_BASE}/${ARG_DIR_NAME}-build
    CMAKE_GENERATOR ${CMAKE_GENERATOR}
    CMAKE_ARGS
      "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
      "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
      "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
      "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
      ${_ext_cov_args}
    DEPENDS sdk
    BUILD_ALWAYS ON
    INSTALL_COMMAND ""
  )

  add_custom_target(copy_${ARG_VEB_NAME}_veb
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy
      ${ARG_BINARY_BASE}/${ARG_DIR_NAME}-build/${ARG_VEB_NAME}.veb
      ${CMAKE_BINARY_DIR}/veb_output_directory/
    DEPENDS ${ARG_EXT_TARGET}
  )

  add_custom_target(${ARG_VEB_NAME}_veb ALL)
  add_dependencies(${ARG_VEB_NAME}_veb copy_${ARG_VEB_NAME}_veb)

  if(ARG_INSTALL_DEST)
    set(_install_args
      FILES ${ARG_BINARY_BASE}/${ARG_DIR_NAME}-build/${ARG_VEB_NAME}.veb
      DESTINATION ${ARG_INSTALL_DEST}
    )
    if(ARG_INSTALL_COMPONENT)
      list(APPEND _install_args COMPONENT ${ARG_INSTALL_COMPONENT})
    endif()
    install(${_install_args})
  endif()
endfunction()

# vsql_add_test_extension(DIR_NAME VEB_NAME [ABI <abi>] [VERSION <ver>])
#
# Shorthand for test extensions. Uses the shared CMakeLists.txt template at
# test-extensions/shared/ so individual extensions need no CMakeLists.txt.
# SOURCE_BASE, BINARY_BASE, and EXT_TARGET are derived automatically.
#
# Keyword options:
#   ABI <abi>     - selects which SDK headers the fixture is built against:
#                     (default) / "dev" - current unstable dev SDK
#                                         (include-dev), latest protocol
#                     "v3"              - frozen stable v3 SDK, so the
#                                         fixture registers at VEF_PROTOCOL_3
#   VERSION <ver> - when supplied, the staged output filename is
#                   <VEB_NAME>-<VERSION>.veb (matching what the server's
#                   find_veb_version() looks for). The .so inside the .veb
#                   keeps the plain name. CMake target names are suffixed
#                   with -<VERSION> so multiple versions of the same
#                   VEB_NAME can coexist.
#   MYSQL_HEADERS - flag; when present, MySQL's source include (service
#                   definitions, e.g. <mysql/components/services/*.h>) and its
#                   build-tree generated include (e.g. mysqld_error.h) are
#                   added to the fixture's include path, and the build is
#                   ordered after GenError. The mysql_services capability test
#                   extensions need this; ordinary VEF-only extensions don't.
macro(vsql_add_test_extension DIR_NAME VEB_NAME)
  cmake_parse_arguments(_ext "MYSQL_HEADERS" "ABI;VERSION" "" ${ARGN})

  # v3 fixtures build against the frozen v3 ABI snapshot directly (its headers
  # plus its frozen sdk_version.h), so they exercise the v3 ABI as a pinned
  # extension sees it. Everything else builds against the current dev ABI
  # (include-dev/ from the built SDK).
  if(_ext_ABI STREQUAL "v3")
    set(_vsql_test_ext_include "${CMAKE_SOURCE_DIR}/villagesql/stable_sdk/v3/include")
  else()
    set(_vsql_test_ext_include "${SDK_STAGING_DIR}/include-dev")
  endif()

  if(_ext_VERSION)
    set(_ext_target_suffix "-${_ext_VERSION}")
    set(_ext_veb_basename ${VEB_NAME}-${_ext_VERSION})
  else()
    set(_ext_target_suffix "")
    set(_ext_veb_basename ${VEB_NAME})
  endif()

  set(_ext_proj_target ${VEB_NAME}${_ext_target_suffix}_extension)
  set(_ext_copy_target copy_${VEB_NAME}${_ext_target_suffix}_veb)
  set(_ext_veb_target ${VEB_NAME}${_ext_target_suffix}_veb)

  # Under gcov, instrument the extension .so so that installing it during mtr
  # produces coverage of the SDK headers it compiles in. The .gcno/.gcda live
  # in this ExternalProject's BINARY_DIR (under the main build tree), so
  # fastcov-report collects them; villagesql_delta_coverage.py then remaps the
  # packaged include-dev path back onto villagesql/sdk and folds it into the
  # delta. The .so writes .gcda to this baked-in build path at runtime, so the
  # build dir must remain writable by whoever runs mtr.
  set(_ext_cov_args "")
  if(ENABLE_GCOV)
    # EXE_LINKER_FLAGS is needed too: CMake's compiler check links a test
    # executable, which must resolve the gcov symbols pulled in by --coverage.
    set(_ext_cov_args
      "-DCMAKE_CXX_FLAGS=--coverage"
      "-DCMAKE_C_FLAGS=--coverage"
      "-DCMAKE_SHARED_LINKER_FLAGS=--coverage"
      "-DCMAKE_EXE_LINKER_FLAGS=--coverage")
  endif()

  set(_ext_cmake_args
    "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
    "-DVillageSQLExtensionFramework_INCLUDE_DIR=${_vsql_test_ext_include}"
    "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    "-DEXTENSION_NAME=${VEB_NAME}"
    "-DEXTENSION_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/${DIR_NAME}"
    ${_ext_cov_args}
  )
  # Extensions that consume/provide MySQL component services need MySQL's
  # headers. Two locations: the source tree ({src}/include) for the service
  # definitions, and the build tree ({build}/include) for generated headers
  # such as mysqld_error.h (error-code constants), produced by GenError.
  if(_ext_MYSQL_HEADERS)
    list(APPEND _ext_cmake_args
      "-DMYSQL_INCLUDE_DIR=${CMAKE_SOURCE_DIR}/include"
      "-DMYSQL_GENERATED_INCLUDE_DIR=${CMAKE_BINARY_DIR}/include")
  endif()

  ExternalProject_Add(${_ext_proj_target}
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/shared
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/${DIR_NAME}-shared-build
    CMAKE_GENERATOR ${CMAKE_GENERATOR}
    CMAKE_ARGS ${_ext_cmake_args}
    DEPENDS sdk
    BUILD_ALWAYS ON
    INSTALL_COMMAND ""
  )

  # Order the build after GenError so the generated MySQL headers exist before
  # this extension compiles. Use add_dependencies (not ExternalProject_Add's
  # DEPENDS) because GenError, defined in utilities/, is created after this
  # subdirectory is configured — add_dependencies resolves the target lazily,
  # ExternalProject_Add's DEPENDS requires it to already exist.
  if(_ext_MYSQL_HEADERS)
    add_dependencies(${_ext_proj_target} GenError)
  endif()

  add_custom_target(${_ext_copy_target}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/${DIR_NAME}-shared-build/${VEB_NAME}.veb
      ${CMAKE_BINARY_DIR}/veb_output_directory/${_ext_veb_basename}.veb
    DEPENDS ${_ext_proj_target}
  )

  add_custom_target(${_ext_veb_target} ALL)
  add_dependencies(${_ext_veb_target} ${_ext_copy_target})
endmacro()
