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

cmake_minimum_required(VERSION 3.16)
project(vsql_index_test_extension)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(EXTENSION_NAME "vsql_index_test")

find_package(VillageSQLExtensionFramework REQUIRED)

include_directories(${VillageSQLExtensionFramework_INCLUDE_DIR})

add_library(vsql_index_test SHARED
    src/extension.cc
)

target_compile_options(vsql_index_test PRIVATE -fPIC)

# vef_storage_* symbols are provided by the server process at load time.
# On macOS the linker requires explicit permission to leave them unresolved.
if(APPLE)
  target_link_options(vsql_index_test PRIVATE -undefined dynamic_lookup)
endif()

VEF_CREATE_VEB(
    NAME ${EXTENSION_NAME}
    LIBRARY_TARGET vsql_index_test
    MANIFEST ${CMAKE_CURRENT_SOURCE_DIR}/manifest.json
)

install(FILES ${VEB_OUTPUT} DESTINATION ".")
