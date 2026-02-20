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

cmake_minimum_required(VERSION 3.16)
project(vsql_simple_extension)

# TL;DR:
#   mkdir build; cd build; cmake .. ; make; make install
#
# Build vs Install:
# * `make` creates the .veb artifact in the build directory
# * `make install` copies it to the VEB directory of your VillageSQL installation
#
# Configuration:
# This build auto-detects VillageSQL by finding `mysql_config` on your PATH.
# For different configurations, use these options (in precedence order):
#
#   1. Development build (editing VillageSQL source):
#      -DVSQL_BUILD_DIR=/path/to/build
#      (Reads configuration from CMakeCache.txt)
#
#   2. Specific installation:
#      -DVSQL_BASE_DIR=/path/to/installation
#      (Uses $VSQL_BASE_DIR/bin/mysql_config)
#
#   3. Default: Uses mysql_config from PATH

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Extension name
set(EXTENSION_NAME "vsql_simple")

# Find VillageSQLExtensionFramework for VEF_CREATE_VEB function
# Search order: CMAKE_PREFIX_PATH, then in-tree location for development builds
find_package(VillageSQLExtensionFramework QUIET
    PATHS "${CMAKE_CURRENT_SOURCE_DIR}/../../../cmake"
)
if(NOT VillageSQLExtensionFramework_FOUND)
    message(FATAL_ERROR "VillageSQLExtensionFramework not found. "
        "Set CMAKE_PREFIX_PATH to your VillageSQL installation, "
        "or build from within the source tree.")
endif()

#
# Build Flags Configuration
#

# Level 1: Check if user provided explicit flags
if(NOT DEFINED MYSQL_INCLUDE_FLAGS OR NOT DEFINED MYSQL_CXXFLAGS)
    set(NEED_BUILD_FLAGS TRUE)
else()
    set(NEED_BUILD_FLAGS FALSE)
    message(STATUS "Using explicitly provided build flags")
endif()

# Level 2: Try VSQL_BUILD_DIR
if(NEED_BUILD_FLAGS AND DEFINED VSQL_BUILD_DIR)
    set(CACHE_FILE "${VSQL_BUILD_DIR}/CMakeCache.txt")
    if(EXISTS "${CACHE_FILE}")
        load_cache("${VSQL_BUILD_DIR}" READ_WITH_PREFIX VSQL_ CMAKE_HOME_DIRECTORY)
        if(VSQL_CMAKE_HOME_DIRECTORY)
            set(VSQL_SOURCE_DIR "${VSQL_CMAKE_HOME_DIRECTORY}")
            set(MYSQL_INCLUDE_FLAGS "-I${VSQL_SOURCE_DIR}/include -I${VSQL_BUILD_DIR}/include")
            set(MYSQL_CXXFLAGS "${MYSQL_INCLUDE_FLAGS}")
            if(NOT DEFINED MYSQL_LIBS)
                set(MYSQL_LIBS "-L${VSQL_BUILD_DIR}/library_output_directory -lmysqlclient")
            endif()
            set(NEED_BUILD_FLAGS FALSE)
            message(STATUS "Using VillageSQL development build (from CMakeCache.txt)")
            message(STATUS "  Source: ${VSQL_SOURCE_DIR}")
            message(STATUS "  Build: ${VSQL_BUILD_DIR}")
        endif()
    endif()
endif()

# Level 3: Try VSQL_BASE_DIR
if(NEED_BUILD_FLAGS AND DEFINED VSQL_BASE_DIR)
    find_program(MYSQL_CONFIG mysql_config PATHS ${VSQL_BASE_DIR}/bin NO_DEFAULT_PATH)
    if(MYSQL_CONFIG)
        execute_process(COMMAND ${MYSQL_CONFIG} --include OUTPUT_VARIABLE MYSQL_INCLUDE_FLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)
        execute_process(COMMAND ${MYSQL_CONFIG} --cxxflags OUTPUT_VARIABLE MYSQL_CXXFLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)
        execute_process(COMMAND ${MYSQL_CONFIG} --libs OUTPUT_VARIABLE MYSQL_LIBS OUTPUT_STRIP_TRAILING_WHITESPACE)
        set(NEED_BUILD_FLAGS FALSE)
        message(STATUS "Using mysql_config from VSQL_BASE_DIR: ${MYSQL_CONFIG}")
    else()
        message(WARNING "VSQL_BASE_DIR specified but mysql_config not found in ${VSQL_BASE_DIR}/bin")
    endif()
endif()

# Level 4: Default - use mysql_config from PATH
if(NEED_BUILD_FLAGS)
    find_program(MYSQL_CONFIG mysql_config)
    if(NOT MYSQL_CONFIG)
        message(FATAL_ERROR "mysql_config not found in PATH. Please set VSQL_BASE_DIR or VSQL_BUILD_DIR, or ensure mysql_config is in PATH.")
    endif()
    execute_process(COMMAND ${MYSQL_CONFIG} --include OUTPUT_VARIABLE MYSQL_INCLUDE_FLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND ${MYSQL_CONFIG} --cxxflags OUTPUT_VARIABLE MYSQL_CXXFLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND ${MYSQL_CONFIG} --libs OUTPUT_VARIABLE MYSQL_LIBS OUTPUT_STRIP_TRAILING_WHITESPACE)
    message(STATUS "Using mysql_config from PATH: ${MYSQL_CONFIG}")
endif()

message(STATUS "Build flags:")
message(STATUS "  MYSQL_INCLUDE_FLAGS: ${MYSQL_INCLUDE_FLAGS}")
message(STATUS "  MYSQL_CXXFLAGS: ${MYSQL_CXXFLAGS}")
message(STATUS "  MYSQL_LIBS: ${MYSQL_LIBS}")

#
# Install Destination Configuration
#

set(VEB_INSTALL_DIRS "")

if(DEFINED VEB_INSTALL_DIR)
    list(APPEND VEB_INSTALL_DIRS "${VEB_INSTALL_DIR}")
    message(STATUS "Primary VEB install directory: ${VEB_INSTALL_DIR}")
elseif(DEFINED VSQL_BUILD_DIR)
    list(APPEND VEB_INSTALL_DIRS "${VSQL_BUILD_DIR}/lib/veb")
    message(STATUS "Primary VEB install directory (from VSQL_BUILD_DIR): ${VSQL_BUILD_DIR}/lib/veb")
elseif(DEFINED VSQL_BASE_DIR)
    list(APPEND VEB_INSTALL_DIRS "${VSQL_BASE_DIR}/lib/veb")
    message(STATUS "Primary VEB install directory (from VSQL_BASE_DIR): ${VSQL_BASE_DIR}/lib/veb")
else()
    if(MYSQL_CONFIG)
        execute_process(COMMAND ${MYSQL_CONFIG} --vebdir OUTPUT_VARIABLE VEB_DIR_FROM_CONFIG OUTPUT_STRIP_TRAILING_WHITESPACE)
        list(APPEND VEB_INSTALL_DIRS "${VEB_DIR_FROM_CONFIG}")
        message(STATUS "Primary VEB install directory (from mysql_config): ${VEB_DIR_FROM_CONFIG}")
    else()
        message(FATAL_ERROR "Cannot determine VEB install directory")
    endif()
endif()

if(DEFINED VEB_ADDITIONAL_INSTALL_DIRS)
    foreach(ADDITIONAL_DIR ${VEB_ADDITIONAL_INSTALL_DIRS})
        list(APPEND VEB_INSTALL_DIRS "${ADDITIONAL_DIR}")
        message(STATUS "Additional VEB install directory: ${ADDITIONAL_DIR}")
    endforeach()
endif()

# Set compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MYSQL_CXXFLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

# VillageSQL SDK headers - set by parent build via VSQL_SDK_INCLUDE_DIR
include_directories("${VSQL_SDK_INCLUDE_DIR}")

add_library(extension SHARED
    src/extension.cc
)

VEF_CREATE_VEB(
    NAME ${EXTENSION_NAME}
    LIBRARY_TARGET extension
    MANIFEST ${CMAKE_CURRENT_SOURCE_DIR}/manifest.json
)

set(INSTALL_VEB_COMMANDS "")
foreach(INSTALL_DIR ${VEB_INSTALL_DIRS})
    list(APPEND INSTALL_VEB_COMMANDS COMMAND ${CMAKE_COMMAND} -E copy ${VEB_OUTPUT} ${INSTALL_DIR}/)
endforeach()

add_custom_target(install_veb
    ${INSTALL_VEB_COMMANDS}
    DEPENDS veb
    COMMENT "Installing ${EXTENSION_NAME}.veb to configured directories"
)

foreach(INSTALL_DIR ${VEB_INSTALL_DIRS})
    install(FILES ${VEB_OUTPUT} DESTINATION ${INSTALL_DIR})
    install(CODE "message(STATUS \"${EXTENSION_NAME} installed to: ${INSTALL_DIR}/${EXTENSION_NAME}.veb\")")
endforeach()

message(STATUS "VEB will be installed to: ${VEB_INSTALL_DIRS}")
