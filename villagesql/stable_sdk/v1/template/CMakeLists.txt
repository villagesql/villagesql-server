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

# VillageSQL Extension Template
#
# Quick start:
#   mkdir build && cd build
#   cmake ..
#   make
#
# The extension SDK is found automatically if:
#   - CMAKE_PREFIX_PATH points to the SDK directory, OR
#   - The SDK is installed in a standard system location
#
# To install the .veb to a VillageSQL server:
#   make install DESTDIR=/path/to/villagesql/lib/veb

cmake_minimum_required(VERSION 3.16)
project(my_extension)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Extension name - change this for your extension
set(EXTENSION_NAME "my_extension")

# Find VillageSQL Extension Framework
# First check if we're inside the SDK tarball (template/../lib/cmake exists)
get_filename_component(_sdk_root "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)
find_package(VillageSQLExtensionFramework REQUIRED
    PATHS "${_sdk_root}"
)
unset(_sdk_root)

# Add SDK include directory
include_directories(${VillageSQLExtensionFramework_INCLUDE_DIR})

# Build the extension shared library
add_library(${EXTENSION_NAME}_lib SHARED
    src/extension.cc
)

# Compiler flags for shared library
target_compile_options(${EXTENSION_NAME}_lib PRIVATE -fPIC)

# Create the VEB (VillageSQL Extension Bundle)
VEF_CREATE_VEB(
    NAME ${EXTENSION_NAME}
    LIBRARY_TARGET ${EXTENSION_NAME}_lib
    MANIFEST ${CMAKE_CURRENT_SOURCE_DIR}/manifest.json
)

# Install target
install(FILES ${VEB_OUTPUT} DESTINATION ".")
