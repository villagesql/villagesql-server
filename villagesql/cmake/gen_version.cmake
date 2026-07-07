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

# Generates villagesql/include/version.h from version.h.in with the current
# VillageSQL version. Run at build time via `cmake -P` (not at configure time)
# so the header always reflects the value in the source tree's VSQL_VERSION on
# every build, without needing a reconfigure.
#
# The core version (code base, major, minor, patch) is read live from
# VSQL_VERSION here. The pre-release identifier is passed in via -D because it
# can be overridden at configure time (release builds strip it with
# -DVSQL_PRE_RELEASE_VERSION=""); that resolved value is forwarded by the
# caller. Parameters passed in via -D by the caller:
#   IN_FILE                  - path to version.h.in
#   OUT_FILE                 - path to the generated version.h
#   SRC_DIR                  - source tree containing VSQL_VERSION
#   VSQL_PRE_RELEASE_VERSION - resolved pre-release identifier ("" strips it)
#
# configure_file() only rewrites OUT_FILE when the content changes, so this
# target running on every build does not force spurious recompiles of the
# files that include version.h.

# Read the value of a keyword from the VSQL_VERSION file. Mirrors
# VSQL_GET_CONFIG_VALUE in cmake/vsql_version.cmake: the first matching line
# wins and any trailing content after a space is dropped.
function(vsql_read_version_value keyword outvar)
  file(STRINGS "${SRC_DIR}/VSQL_VERSION" _lines REGEX "^[ ]*${keyword}=")
  if(_lines)
    list(GET _lines 0 _line)
    string(REGEX REPLACE "^[ ]*${keyword}=" "" _value "${_line}")
    string(REGEX REPLACE "[ ].*" "" _value "${_value}")
    set(${outvar} "${_value}" PARENT_SCOPE)
  endif()
endfunction()

vsql_read_version_value("VSQL_CODE_BASE" VSQL_CODE_BASE)
vsql_read_version_value("VSQL_MAJOR_VERSION" VSQL_MAJOR_VERSION)
vsql_read_version_value("VSQL_MINOR_VERSION" VSQL_MINOR_VERSION)
vsql_read_version_value("VSQL_PATCH_VERSION" VSQL_PATCH_VERSION)

if(NOT DEFINED VSQL_CODE_BASE OR
   NOT DEFINED VSQL_MAJOR_VERSION OR
   NOT DEFINED VSQL_MINOR_VERSION OR
   NOT DEFINED VSQL_PATCH_VERSION)
  message(FATAL_ERROR "VSQL_VERSION file cannot be parsed.")
endif()

# The pre-release identifier is normally forwarded via -D; fall back to the
# source-tree value if the caller did not pass one.
if(NOT DEFINED VSQL_PRE_RELEASE_VERSION)
  vsql_read_version_value("VSQL_PRE_RELEASE_VERSION" VSQL_PRE_RELEASE_VERSION)
endif()

configure_file("${IN_FILE}" "${OUT_FILE}" @ONLY)
