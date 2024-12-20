# Copyright (c) 2024, Oracle and/or its affiliates.
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

# Always build xxhash library, which is used even if WITH_LZ4="system".
# Also build lz4 library if WITH_LZ4="bundled".

ADD_LIBRARY(xxhash_interface INTERFACE)
TARGET_LINK_LIBRARIES(xxhash_interface INTERFACE xxhash_lib)
ADD_LIBRARY(ext::xxhash ALIAS xxhash_interface)

ADD_STATIC_LIBRARY(xxhash_lib
  ${BUNDLED_LZ4_PATH}/xxhash.c
  COMPILE_DEFINITIONS PRIVATE XXH_NAMESPACE=MY_
  )

IF(UNIX)
  TARGET_COMPILE_OPTIONS(xxhash_lib PRIVATE "-fvisibility=hidden")
ENDIF()

IF(WITH_LZ4 STREQUAL "system")
  RETURN()
ENDIF()

ADD_STATIC_LIBRARY(lz4_lib
  ${BUNDLED_LZ4_PATH}/lz4.c
  ${BUNDLED_LZ4_PATH}/lz4frame.c
  ${BUNDLED_LZ4_PATH}/lz4hc.c
  ${BUNDLED_LZ4_PATH}/xxhash.c
  )
