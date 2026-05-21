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

# ABI v3 Test Extension
#
# This extension is built during the abi_v3_compliance test to verify that an
# extension compiled against the v3 SDK headers loads and runs correctly
# against the current server.
#
# Build with CMAKE_PREFIX_PATH pointing to stable_sdk/v3/ so that
# find_package(VillageSQLExtensionFramework) resolves the v3 headers:
#
#   cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/stable_sdk/v3
#   cmake --build build

cmake_minimum_required(VERSION 3.16)
project(abi_v3_test)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Resolve VillageSQLExtensionFramework from CMAKE_PREFIX_PATH (stable_sdk/v3/).
# The cmake config at stable_sdk/v3/lib/cmake/VillageSQLExtensionFramework/ sets
# VillageSQLExtensionFramework_INCLUDE_DIR to stable_sdk/v3/include/.
find_package(VillageSQLExtensionFramework REQUIRED)

include_directories(${VillageSQLExtensionFramework_INCLUDE_DIR})

add_library(abi_v3_test_lib SHARED src/abi_v3_extension.cc)

target_compile_options(abi_v3_test_lib PRIVATE -fPIC)

VEF_CREATE_VEB(
  NAME abi_v3_test
  LIBRARY_TARGET abi_v3_test_lib
  MANIFEST ${CMAKE_CURRENT_SOURCE_DIR}/manifest.json
)
