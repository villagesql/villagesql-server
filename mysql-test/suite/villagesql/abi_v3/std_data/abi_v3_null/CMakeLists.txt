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

# An extension built against the stable v3 SDK whose prerun admits a NULL
# literal the way a v3 extension has to: as STRING.

cmake_minimum_required(VERSION 3.16)
project(abi_v3_null)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(VillageSQLExtensionFramework REQUIRED)

include_directories(${VillageSQLExtensionFramework_INCLUDE_DIR})

add_library(abi_v3_null_lib SHARED src/extension.cc)

target_compile_options(abi_v3_null_lib PRIVATE -fPIC)

VEF_CREATE_VEB(
  NAME abi_v3_null
  LIBRARY_TARGET abi_v3_null_lib
  MANIFEST ${CMAKE_CURRENT_SOURCE_DIR}/manifest.json
)
