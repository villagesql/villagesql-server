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

# Sources carrying VillageSQL build metadata (git SHA, work-tree file counts,
# build timestamp, host).
#
# These compile directly into the mysqld executable rather than into sql_main,
# because the generated source changes on every build (the timestamp is always
# fresh). In sql_main it would drag every one of the ~75 targets that link
# sql_main into a relink; in mysqld it costs mysqld's own link plus, on macOS,
# the plugins that name mysqld via -bundle_loader. Measured on a warm debug
# tree: 21.9s versus 13.4s, against an 8.4s do-nothing floor.
#
# The sysvar that reads the metadata moves with the data, so that nothing in
# sql_main references GetBuildInfo(). Otherwise sql_main would carry an
# undefined symbol and every binary linking it would fail to link --
# server_unittest_library is a shared library, so it must resolve everything.
#
# Sets ${out_var} to the sources to add to the mysqld executable, and defines
# the villagesql_build_info_gen target they depend on.
MACRO(VSQL_BUILD_INFO_SOURCES out_var)
  SET(_vsql_build_info_cc ${CMAKE_BINARY_DIR}/villagesql/common/build_info.cc)

  # Production releases (no pre-release suffix) blank the volatile,
  # non-reproducible fields (build timestamp and host) so release binaries are
  # reproducible; dev builds keep them. The single template is shared by both.
  IF(VSQL_HAS_PRE_RELEASE)
    SET(_vsql_build_info_production OFF)
  ELSE()
    SET(_vsql_build_info_production ON)
  ENDIF()

  # The generator target has no dependencies, so it is always out of date and
  # every build refreshes the timestamp. Callers include the stable, committed
  # build_info.h, so only the generated source recompiles.
  ADD_CUSTOM_TARGET(villagesql_build_info_gen
    BYPRODUCTS ${_vsql_build_info_cc}
    COMMAND ${CMAKE_COMMAND}
      -DSRC_DIR=${CMAKE_SOURCE_DIR}
      -DIN_FILE=${CMAKE_SOURCE_DIR}/villagesql/common/build_info.cc.in
      -DOUT_FILE=${_vsql_build_info_cc}
      -DBUILD_OS=${CMAKE_HOST_SYSTEM}
      -DBUILD_ARCH=${CMAKE_HOST_SYSTEM_PROCESSOR}
      -DPRODUCTION_BUILD=${_vsql_build_info_production}
      -P ${CMAKE_SOURCE_DIR}/villagesql/cmake/gen_build_info.cmake
    COMMENT "Generating VillageSQL build info"
    VERBATIM)

  SET_SOURCE_FILES_PROPERTIES(${_vsql_build_info_cc} PROPERTIES GENERATED TRUE)

  SET(${out_var}
    ${_vsql_build_info_cc}
    ${CMAKE_SOURCE_DIR}/villagesql/sql/build_info_sysvar.cc)
ENDMACRO()
