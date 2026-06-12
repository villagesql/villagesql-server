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

  ExternalProject_Add(${ARG_EXT_TARGET}
    SOURCE_DIR ${ARG_SOURCE_BASE}/${ARG_DIR_NAME}
    BINARY_DIR ${ARG_BINARY_BASE}/${ARG_DIR_NAME}-build
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

# vsql_add_test_extension(DIR_NAME VEB_NAME [ABI])
#
# Shorthand for test extensions. Uses the shared CMakeLists.txt template at
# test-extensions/shared/ so individual extensions need no CMakeLists.txt.
# SOURCE_BASE, BINARY_BASE, and EXT_TARGET are derived automatically.
#
# The optional ABI argument selects which SDK headers the fixture is built
# against:
#   (default) / "dev" - current unstable dev SDK (include-dev), latest protocol
#   "v3"              - frozen stable v3 SDK (include), so the fixture registers
#                       at VEF_PROTOCOL_3. Use this to exercise the v3 register
#                       path (build_type_descriptor_v3) from a fixture that a
#                       real v3-compiled extension would produce.
macro(vsql_add_test_extension DIR_NAME VEB_NAME)
  set(_vsql_test_ext_abi "${ARGN}")
  if(_vsql_test_ext_abi STREQUAL "v3")
    set(_vsql_test_ext_include "${SDK_STAGING_DIR}/include")
  else()
    set(_vsql_test_ext_include "${SDK_STAGING_DIR}/include-dev")
  endif()
  ExternalProject_Add(${VEB_NAME}_extension
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/shared
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/${DIR_NAME}-shared-build
    CMAKE_GENERATOR ${CMAKE_GENERATOR}
    CMAKE_ARGS
      "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
      "-DVillageSQLExtensionFramework_INCLUDE_DIR=${_vsql_test_ext_include}"
      "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
      "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
      "-DEXTENSION_NAME=${VEB_NAME}"
      "-DEXTENSION_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/${DIR_NAME}"
    DEPENDS sdk
    BUILD_ALWAYS ON
    INSTALL_COMMAND ""
  )

  add_custom_target(copy_${VEB_NAME}_veb
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/${DIR_NAME}-shared-build/${VEB_NAME}.veb
      ${CMAKE_BINARY_DIR}/veb_output_directory/
    DEPENDS ${VEB_NAME}_extension
  )

  add_custom_target(${VEB_NAME}_veb ALL)
  add_dependencies(${VEB_NAME}_veb copy_${VEB_NAME}_veb)
endmacro()
