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
project(vsql_keyring_reader_extension)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(EXTENSION_NAME "vsql_keyring_reader")

find_package(VillageSQLExtensionFramework REQUIRED)

include_directories(${VillageSQLExtensionFramework_INCLUDE_DIR})

add_library(extension SHARED
    src/extension.cc
)

target_compile_options(extension PRIVATE -fPIC)

VEF_CREATE_VEB(
    NAME ${EXTENSION_NAME}
    LIBRARY_TARGET extension
    MANIFEST ${CMAKE_CURRENT_SOURCE_DIR}/manifest.json
)

install(FILES ${VEB_OUTPUT} DESTINATION ".")
