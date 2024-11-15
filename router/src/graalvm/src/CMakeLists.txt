# Copyright (c) 2021, 2024, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

INCLUDE(GenerateExportHeader)

SET(graalvm_plugin_SOURCE
  graalvm_common_context.cc
  graalvm_plugin.cc
  graalvm_javascript_context.cc
  graalvm_javascript.cc
  graalvm_component.cc
  graalvm_type_conversion.cc  
  polyglot_wrappers/types_polyglot.cc
  native_wrappers/polyglot_array_wrapper.cc
  native_wrappers/polyglot_collectable.cc
  native_wrappers/polyglot_file_system_wrapper.cc
  native_wrappers/polyglot_iterator_wrapper.cc
  native_wrappers/polyglot_map_wrapper.cc
  native_wrappers/polyglot_seekable_channel_wrapper.cc
  languages/polyglot_language.cc
  languages/polyglot_javascript.cc
  languages/polyglot_common_context.cc
  languages/polyglot_garbage_collector.cc
  utils/polyglot_store.cc
  utils/polyglot_error.cc
  utils/polyglot_utils.cc
  utils/native_value.cc
  utils/polyglot_store.cc
  utils/utils_json.cc
  utils/utils_string.cc
  utils/utils_path.cc
  utils/utils_encoding.cc
  utils/polyglot_scope.cc
  utils/profiling.cc
  )

IF(UNIX)
    LIST(APPEND graalvm_plugin_SOURCE utils/utils_path_unix.cc)
ELSEIF(WIN32)
    LIST(APPEND graalvm_plugin_SOURCE utils/utils_path_win32.cc)
ENDIF()

FIND_LIBRARY(POLYGLOT_LIBRARY
  NAMES polyglot
  HINTS "${BUNDLED_POLYGLOT_DIR}"
  NO_DEFAULT_PATH
  )

MESSAGE(STATUS "Polyglot Library: ${POLYGLOT_LIBRARY}")

GET_FILENAME_COMPONENT(LIBPOLYGLOT_NAME "${POLYGLOT_LIBRARY}" NAME)

ADD_HARNESS_PLUGIN(graalvm_plugin
  OUTPUT_NAME "graalvm"
  SOURCES ${graalvm_plugin_SOURCE}
  REQUIRES harness_stdx ${POLYGLOT_LIBRARY}
  DESTINATION "${ROUTER_INSTALL_PLUGINDIR}"
  )

TARGET_INCLUDE_DIRECTORIES(graalvm_plugin PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../include/
  ${CMAKE_CURRENT_BINARY_DIR}/../include/
  ${BUNDLED_POLYGLOT_DIR}
  )

# So that dlopen() can find libpolyglot.so at runtime.
IF(LINUX)
  ADD_INSTALL_RPATH(graalvm_plugin "\$ORIGIN/../private")
ELSEIF(APPLE)
  # Fix the polyglot library name to be @rpath/libpolyglot.dylib
  ADD_CUSTOM_COMMAND(TARGET graalvm_plugin PRE_BUILD
    COMMAND install_name_tool -id "@rpath/${LIBPOLYGLOT_NAME}" "${POLYGLOT_LIBRARY}"
  )
ENDIF()

IF(NOT APPLE AND NOT WIN32)
  # Debug versions of plugins may be installed to <root>/lib/plugin/debug
  # and we need the correct RPATH to libpolyglot.so after 'make install'.
  FOREACH(LINK_FLAG
      CMAKE_MODULE_LINKER_FLAGS_DEBUG
      CMAKE_SHARED_LINKER_FLAGS_DEBUG
      )
    IF(${LINK_FLAG} MATCHES "ORIGIN/\.\./\.\./private")
      # Nothing, already done in top level CMakeLists.txt
    ELSE()
      STRING_APPEND(${LINK_FLAG} " -Wl,-rpath,'\$ORIGIN/../../private'")
    ENDIF()
    MESSAGE(STATUS "${LINK_FLAG} ${${LINK_FLAG}}")
  ENDFOREACH()
ENDIF()

IF(APPLE)
  INSTALL(FILES
    ${POLYGLOT_LIBRARY}
    DESTINATION "${ROUTER_INSTALL_PLUGINDIR}" COMPONENT Router
    PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    )
ELSEIF(WIN32)
  GET_FILENAME_COMPONENT(LIBPOLYGLOT_NAME_WE "${POLYGLOT_LIBRARY}" NAME_WE)
  GET_FILENAME_COMPONENT(LIBPOLYGLOT_DIR "${POLYGLOT_LIBRARY}" DIRECTORY)
  INSTALL(FILES
    "${LIBPOLYGLOT_DIR}/${LIBPOLYGLOT_NAME_WE}.dll"
    DESTINATION "${ROUTER_INSTALL_PLUGINDIR}" COMPONENT Router
    PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    )
ELSEIF(LINUX)
  INSTALL(FILES
    ${POLYGLOT_LIBRARY}
    DESTINATION "${ROUTER_INSTALL_LIBDIR}" COMPONENT Router
    PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    )
ENDIF()

GENERATE_EXPORT_HEADER(graalvm_plugin
  EXPORT_FILE_NAME ${CMAKE_CURRENT_BINARY_DIR}/../include/mysqlrouter/graalvm_plugin_export.h)
