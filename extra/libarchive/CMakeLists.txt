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

# This CMakeLists.txt integrates libarchive into the MySQL build system

# Disable building of libarchive utilities - we only need the library
SET(ENABLE_CPIO OFF CACHE BOOL "Disable cpio" FORCE)
SET(ENABLE_CAT OFF CACHE BOOL "Disable cat" FORCE)
SET(ENABLE_TAR OFF CACHE BOOL "Disable tar" FORCE)
SET(ENABLE_UNZIP OFF CACHE BOOL "Disable unzip" FORCE)
SET(ENABLE_TEST OFF CACHE BOOL "Disable libarchive tests" FORCE)
SET(ENABLE_INSTALL OFF CACHE BOOL "Disable libarchive install" FORCE)

# Set libarchive to build as a static library
SET(ENABLE_SHARED OFF CACHE BOOL "Build shared library" FORCE)
SET(ENABLE_STATIC ON CACHE BOOL "Build static library" FORCE)

# Disable external dependencies we don't need
SET(ENABLE_BZip2 OFF CACHE BOOL "Disable BZip2" FORCE)
SET(ENABLE_LIBXML2 OFF CACHE BOOL "Disable libxml2" FORCE)
SET(ENABLE_EXPAT OFF CACHE BOOL "Disable expat" FORCE)
SET(ENABLE_PCREPOSIX OFF CACHE BOOL "Disable PCRE" FORCE)
SET(ENABLE_LibGCC OFF CACHE BOOL "Disable LibGCC" FORCE)
SET(ENABLE_XATTR OFF CACHE BOOL "Disable extended attributes" FORCE)
SET(ENABLE_ACL OFF CACHE BOOL "Disable ACL support" FORCE)
SET(ENABLE_ICONV OFF CACHE BOOL "Disable iconv" FORCE)

# Use MySQL's bundled zlib
SET(ENABLE_ZLIB ON CACHE BOOL "Enable zlib" FORCE)
SET(ZLIB_FOUND TRUE)
SET(ZLIB_INCLUDE_DIR ${ZLIB_INCLUDE_DIR})
SET(ZLIB_LIBRARIES zlib)

# Use MySQL's bundled lz4 if desired
SET(ENABLE_LZ4 ON CACHE BOOL "Enable lz4" FORCE)
SET(LZ4_FOUND TRUE)
SET(LZ4_INCLUDE_DIR ${LZ4_INCLUDE_DIR})
SET(LZ4_LIBRARY lz4_lib)

# Use MySQL's bundled zstd if desired
SET(ENABLE_ZSTD ON CACHE BOOL "Enable zstd" FORCE)
SET(Zstd_FOUND TRUE)
SET(Zstd_INCLUDE_DIR ${ZSTD_INCLUDE_DIR})
SET(Zstd_LIBRARY zstd)

# Add the libarchive subdirectory
ADD_SUBDIRECTORY(libarchive-3.8.1)

# Suppress warnings in bundled third-party libarchive source code.
MY_CHECK_C_COMPILER_FLAG("-Wdeprecated-declarations" HAVE_C_DEPRECATED_DECLARATIONS)
IF(HAVE_C_DEPRECATED_DECLARATIONS)
  TARGET_COMPILE_OPTIONS(archive_static PRIVATE "-Wno-deprecated-declarations")
ENDIF()

MY_CHECK_C_COMPILER_FLAG("-Wconditional-uninitialized" HAVE_C_CONDITIONAL_UNINITIALIZED)
IF(HAVE_C_CONDITIONAL_UNINITIALIZED)
  TARGET_COMPILE_OPTIONS(archive_static PRIVATE "-Wno-conditional-uninitialized")
ENDIF()

MY_CHECK_C_COMPILER_FLAG("-Wunused-variable" HAVE_C_UNUSED_VARIABLE)
IF(HAVE_C_UNUSED_VARIABLE)
  TARGET_COMPILE_OPTIONS(archive_static PRIVATE "-Wno-unused-variable")
ENDIF()

# Create an interface library for easier consumption
ADD_LIBRARY(archive_interface INTERFACE)
TARGET_LINK_LIBRARIES(archive_interface INTERFACE archive_static)
TARGET_INCLUDE_DIRECTORIES(archive_interface INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/libarchive-3.8.1/libarchive
  ${CMAKE_CURRENT_BINARY_DIR}/libarchive-3.8.1/libarchive
)

# Export the include directories for parent scope
SET(LIBARCHIVE_INCLUDE_DIRS
  ${CMAKE_CURRENT_SOURCE_DIR}/libarchive-3.8.1/libarchive
  ${CMAKE_CURRENT_BINARY_DIR}/libarchive-3.8.1/libarchive
  PARENT_SCOPE
)
