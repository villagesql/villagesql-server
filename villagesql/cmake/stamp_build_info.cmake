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

# Stamps build metadata into an already-linked binary and (on macOS) re-signs
# it, since patching invalidates the linker's ad-hoc code signature. Run at
# build time via `cmake -P` from a POST_BUILD step, so values reflect the
# moment the binary was linked; a build that does not relink does not restamp.
#
# Volatile values (git SHA, work-tree file counts, timestamp, host) are
# computed here. Stable values are passed in via -D by the caller:
#   BINARY_FILE      - binary to stamp
#   STAMPER          - path to the vsql_build_info_stamper tool
#   SRC_DIR          - source tree to inspect with git
#   BUILD_OS         - host OS string (CMAKE_HOST_SYSTEM)
#   BUILD_ARCH       - host architecture (CMAKE_HOST_SYSTEM_PROCESSOR)
#   PRODUCTION_BUILD - if true, blank the volatile, non-reproducible fields
#                      (build timestamp and host) for reproducible releases
#   APPLE_CODESIGN   - if true, re-sign the binary after stamping

find_package(Git QUIET)

set(GIT_SHA "unknown")
set(GIT_FILES_ADDED 0)
set(GIT_FILES_DELETED 0)
set(GIT_FILES_MODIFIED 0)

if(GIT_EXECUTABLE)
  execute_process(COMMAND ${GIT_EXECUTABLE} -C "${SRC_DIR}" rev-parse HEAD
    OUTPUT_VARIABLE GIT_SHA OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
  if(NOT GIT_SHA)
    set(GIT_SHA "unknown")
  endif()

  # Count work-tree divergence from HEAD by status code, one bucket per file.
  # Untracked ("??") files count as added alongside staged adds ("A"); a clean
  # tree leaves all three counts at zero.
  execute_process(COMMAND ${GIT_EXECUTABLE} -C "${SRC_DIR}" status --porcelain
    OUTPUT_VARIABLE _status OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
  if(_status)
    string(REPLACE "\n" ";" _lines "${_status}")
    foreach(_line IN LISTS _lines)
      string(SUBSTRING "${_line}" 0 2 _xy)
      if(_xy STREQUAL "??")
        math(EXPR GIT_FILES_ADDED "${GIT_FILES_ADDED} + 1")
      else()
        string(SUBSTRING "${_xy}" 0 1 _x)
        string(SUBSTRING "${_xy}" 1 1 _y)
        if(_x STREQUAL "A" OR _y STREQUAL "A")
          math(EXPR GIT_FILES_ADDED "${GIT_FILES_ADDED} + 1")
        elseif(_x STREQUAL "D" OR _y STREQUAL "D")
          math(EXPR GIT_FILES_DELETED "${GIT_FILES_DELETED} + 1")
        elseif(_x STREQUAL "M" OR _y STREQUAL "M")
          math(EXPR GIT_FILES_MODIFIED "${GIT_FILES_MODIFIED} + 1")
        endif()
      endif()
    endforeach()
  endif()
endif()

# Honors SOURCE_DATE_EPOCH automatically (CMake >= 3.8) for reproducible builds.
string(TIMESTAMP BUILD_TIMESTAMP "%Y-%m-%dT%H:%M:%SZ" UTC)

cmake_host_system_information(RESULT BUILD_HOST QUERY HOSTNAME)

# Production releases blank the volatile, non-reproducible fields so identical
# sources yield identical binaries. The work-tree counts are forced to zero so
# a release always reports clean (is_dirty() == false); git_sha, OS and arch
# stay populated.
if(PRODUCTION_BUILD)
  set(GIT_FILES_ADDED 0)
  set(GIT_FILES_DELETED 0)
  set(GIT_FILES_MODIFIED 0)
  set(BUILD_TIMESTAMP "")
  set(BUILD_HOST "")
endif()

execute_process(
  COMMAND "${STAMPER}" "${BINARY_FILE}"
    "${GIT_SHA}" "${GIT_FILES_ADDED}" "${GIT_FILES_DELETED}"
    "${GIT_FILES_MODIFIED}" "${BUILD_TIMESTAMP}" "${BUILD_HOST}"
    "${BUILD_OS}" "${BUILD_ARCH}"
  RESULT_VARIABLE _stamp_rc)
if(_stamp_rc)
  message(FATAL_ERROR "build-info stamping failed for ${BINARY_FILE}")
endif()

if(APPLE_CODESIGN)
  # Same ad-hoc identity and entitlement preservation as
  # MACOS_ADD_DEVELOPER_ENTITLEMENTS in cmake/mysql_add_executable.cmake.
  execute_process(
    COMMAND codesign --force --preserve-metadata=entitlements --sign -
      "${BINARY_FILE}"
    RESULT_VARIABLE _sign_rc)
  if(_sign_rc)
    message(FATAL_ERROR "re-signing after stamp failed for ${BINARY_FILE}")
  endif()
endif()
