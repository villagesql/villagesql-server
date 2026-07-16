# Copyright (c) 2019, 2026, Oracle and/or its affiliates.
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

# Targets below assume we have gcc and gcov version >= 9

# There is no coverage for the NDBCLUSTER plugin, so disable it.
# Alternatively: use -DWITH_NDB=1, and run cluster test suites also.

# The default mtr test suite has limited coverage of replication,
# and of some plugins.

# cmake <path> -DWITH_DEBUG=1 -DWITH_SYSTEM_LIBS=1 -DENABLE_GCOV=1
#              -DWITH_NDBCLUSTER_STORAGE_ENGINE=0
# make
# make fastcov-clean
# <run some tests>
# make fastcov-report
# make fastcov-html
# open in browser:  ${CMAKE_BINARY_DIR}/code_coverage/index.html

FIND_PROGRAM(FASTCOV_EXECUTABLE NAMES fastcov.py fastcov)

IF(NOT FASTCOV_EXECUTABLE)
  MESSAGE(WARNING "Could not find fastcov.py or fastcov")
  RETURN()
ENDIF()

IF(NOT CMAKE_COMPILER_IS_GNUCXX)
  MESSAGE(WARNING "You should upgrade to gcc version >= 10")
  RETURN()
ENDIF()

IF(ALTERNATIVE_GCC)
  GET_FILENAME_COMPONENT(GCC_B_PREFIX ${ALTERNATIVE_GCC} DIRECTORY)
  MESSAGE(STATUS "Looking for gcov in ${GCC_B_PREFIX}")
  FIND_PROGRAM(GCOV_EXECUTABLE gcov
    NO_DEFAULT_PATH
    PATHS "${GCC_B_PREFIX}")
  # Ensure that fastcov can find tools in PATH.
  IF(GCOV_EXECUTABLE)
    SET(FASTCOV_PATH_PREFIX
      ${CMAKE_COMMAND} -E env "PATH=${GCC_B_PREFIX}:$ENV{PATH}"
      )
  ENDIF()
ENDIF()

FIND_PROGRAM(GCOV_EXECUTABLE NAMES gcov)
IF(NOT GCOV_EXECUTABLE)
  MESSAGE(FATAL_ERROR "gcov not found")
ENDIF()

EXECUTE_PROCESS(
  COMMAND ${GCOV_EXECUTABLE} --version
  OUTPUT_VARIABLE stdout
  ERROR_VARIABLE  stderr
  RESULT_VARIABLE result
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# gcov --version output samples on Linux:
# gcov (Debian 9-20190208-1) 9.0.1 20190208 (experimental)
# gcov (GCC) 8.3.1 20190223 (Red Hat 8.3.1-2)
STRING(REPLACE "\n" ";" GCOV_OUTPUT_LIST "${stdout}")
UNSET(GCOV_VERSION)
LIST(GET GCOV_OUTPUT_LIST 0 FIRST_LINE)
STRING(REGEX MATCH "gcov [(].*[)] ([0-9\.]+).*" XXX ${FIRST_LINE})
IF(CMAKE_MATCH_1)
  SET(GCOV_VERSION "${CMAKE_MATCH_1}")
ENDIF()

IF(GCOV_VERSION AND GCOV_VERSION VERSION_LESS 10)
  MESSAGE(FATAL_ERROR "${GCOV_EXECUTABLE} has version ${GCOV_VERSION}\n"
    "At least version 10 is required")
ENDIF()

# We may be running gcov in-source.
IF(NOT THIS_IS_AN_IN_SOURCE_BUILD)
  FOREACH(FILE
      # InnoDB generated parsers are checked in as source.
      ${CMAKE_SOURCE_DIR}/storage/innobase/fts/fts0blex.cc
      ${CMAKE_SOURCE_DIR}/storage/innobase/fts/fts0blex.l
      ${CMAKE_SOURCE_DIR}/storage/innobase/fts/fts0pars.cc
      ${CMAKE_SOURCE_DIR}/storage/innobase/fts/fts0pars.y
      ${CMAKE_SOURCE_DIR}/storage/innobase/fts/fts0tlex.cc
      ${CMAKE_SOURCE_DIR}/storage/innobase/fts/fts0tlex.l
      ${CMAKE_SOURCE_DIR}/storage/innobase/pars/lexyy.cc
      ${CMAKE_SOURCE_DIR}/storage/innobase/pars/pars0grm.cc
      ${CMAKE_SOURCE_DIR}/storage/innobase/pars/pars0grm.y
      ${CMAKE_SOURCE_DIR}/storage/innobase/pars/pars0lex.l
      )
    GET_FILENAME_COMPONENT(filename "${FILE}" NAME)
    IF(CMAKE_GENERATOR MATCHES "Ninja")
      EXECUTE_PROCESS(
        COMMAND ${CMAKE_COMMAND} -E create_symlink ${FILE} ${filename}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        )
    ELSE()
      EXECUTE_PROCESS(
        COMMAND ${CMAKE_COMMAND} -E create_symlink ${FILE} ${filename}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/storage/innobase
        )
    ENDIF()
  ENDFOREACH()
ENDIF()

# Ignore std, boost and 3rd-party code when doing coverage analysis.
SET(FASTCOV_EXCLUDE_LIST "--exclude")
FOREACH(FASTCOV_EXCLUDE
    "/usr/include"
    "/usr/lib"
    "${BOOST_INCLUDE_DIR}"
    "${BOOST_PATCHES_DIR}"
    ${GMOCK_INCLUDE_DIRS}
    "${CMAKE_SOURCE_DIR}/extra/rapidjson"
    "${CMAKE_SOURCE_DIR}/extra/xxhash"
    # libarchive is vendored by VillageSQL (for .veb bundle handling); it is
    # third-party code, so exclude it from coverage like the other extra/ libs.
    "${CMAKE_SOURCE_DIR}/extra/libarchive"
    # Test code is not product code.
    "${CMAKE_SOURCE_DIR}/unittest/gunit/villagesql"
    "${CMAKE_SOURCE_DIR}/villagesql/test-extensions"
    # stable_sdk is the frozen stable ABI
    "${CMAKE_SOURCE_DIR}/villagesql/stable_sdk"
    )
  LIST(APPEND FASTCOV_EXCLUDE_LIST "${FASTCOV_EXCLUDE}")
ENDFOREACH()

ADD_CUSTOM_TARGET(fastcov-clean
  COMMAND ${FASTCOV_PATH_PREFIX}
          ${FASTCOV_EXECUTABLE} --gcov ${GCOV_EXECUTABLE} --zerocounters
  COMMENT "Running ${FASTCOV_EXECUTABLE} --zerocounters"
  VERBATIM
  )
ADD_CUSTOM_TARGET(fastcov-report
  COMMAND ${FASTCOV_PATH_PREFIX}
          ${FASTCOV_EXECUTABLE} --gcov ${GCOV_EXECUTABLE}
          ${FASTCOV_EXCLUDE_LIST} --lcov -o report.info
  COMMENT "Running ${FASTCOV_EXECUTABLE} --lcov -o report.info"
  VERBATIM
  )
ADD_CUSTOM_TARGET(fastcov-html
  COMMAND genhtml -o code_coverage report.info
  COMMENT "Running genhtml -o code_coverage report.info"
  VERBATIM
  )

# VillageSQL differential ("diff") coverage.
#
# Restricts coverage to lines added or changed since VillageSQL forked from
# upstream MySQL. scripts/villagesql_delta_coverage.py filters the report.info
# produced by fastcov-report down to those lines (upstream code excluded; new
# files in full, modified files by changed line), then genhtml renders the
# familiar navigable report scoped to the VillageSQL delta.
#
# make fastcov-clean
# <run some tests>
# make fastcov-report
# make fastcov-diff
# open in browser:  ${CMAKE_BINARY_DIR}/coverage-delta/index.html

FIND_PROGRAM(PYTHON3_EXECUTABLE NAMES python3)

# The base is the upstream MySQL release that HEAD is built on -- the tree
# VillageSQL layered its changes onto. HEAD tracks a specific MySQL version
# (VillageSQL periodically merges the matching "mysql-X.Y.Z" upstream tag), so
# diffing against that tag isolates the VillageSQL delta: upstream code is
# byte-identical in both trees and drops out. Do NOT use the fork point: the
# tree has since absorbed several upstream patch releases (8.4.8 -> 8.4.10),
# and diffing that far back would misattribute all that upstream drift to
# VillageSQL. Computed once and cached; override with
# -DVILLAGESQL_COVERAGE_BASE=<ref> if needed.
IF(NOT VILLAGESQL_COVERAGE_BASE AND GIT_EXECUTABLE)
  SET(UPSTREAM_MYSQL_TAG
    "mysql-${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}")
  EXECUTE_PROCESS(
    COMMAND ${GIT_EXECUTABLE} -C ${CMAKE_SOURCE_DIR}
            rev-parse --verify --quiet "${UPSTREAM_MYSQL_TAG}^{commit}"
    OUTPUT_VARIABLE UPSTREAM_MYSQL_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE GIT_REVPARSE_RESULT
    ERROR_QUIET
    )
  IF(GIT_REVPARSE_RESULT EQUAL 0 AND UPSTREAM_MYSQL_COMMIT)
    SET(VILLAGESQL_COVERAGE_BASE "${UPSTREAM_MYSQL_COMMIT}"
      CACHE STRING "Base commit for VillageSQL differential coverage")
  ELSE()
    MESSAGE(STATUS "Upstream tag ${UPSTREAM_MYSQL_TAG} not found; "
      "set -DVILLAGESQL_COVERAGE_BASE=<ref> for 'make fastcov-diff'")
  ENDIF()
ENDIF()

IF(NOT PYTHON3_EXECUTABLE)
  MESSAGE(STATUS "python3 not found; 'make fastcov-diff' unavailable")
ELSEIF(NOT VILLAGESQL_COVERAGE_BASE)
  MESSAGE(STATUS "Could not determine upstream MySQL base; "
    "'make fastcov-diff' unavailable (set -DVILLAGESQL_COVERAGE_BASE=<ref>)")
ELSE()
  # Filter report.info to the VillageSQL delta, then render with genhtml. The
  # filter script resolves git paths against the source tree (--repo).
  ADD_CUSTOM_TARGET(fastcov-diff
    COMMAND ${PYTHON3_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/scripts/villagesql_delta_coverage.py
            ${CMAKE_BINARY_DIR}/report.info ${VILLAGESQL_COVERAGE_BASE}
            ${CMAKE_BINARY_DIR}/delta.info --repo ${CMAKE_SOURCE_DIR}
    # --prefix strips the common source root so directories show relative
    # (villagesql/schema, sql/dd/impl, ...). The delta report contains only
    # in-source paths, so a single --prefix cleanly applies to all of them.
    COMMAND genhtml ${CMAKE_BINARY_DIR}/delta.info
            --output-directory ${CMAKE_BINARY_DIR}/coverage-delta
            --prefix ${CMAKE_SOURCE_DIR} --legend
            --title "VillageSQL delta coverage"
    COMMENT
      "VillageSQL delta coverage since ${VILLAGESQL_COVERAGE_BASE} "
      "-> coverage-delta/index.html"
    VERBATIM
    )
ENDIF()
