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

# Unit tests for VillageSQL-specific internal code

# List of all VillageSQL unit tests.
# Place additional test here as necesssary.
SET(VILLAGESQL_UNIT_TESTS
  abi_v1_check-t
  abi_v2_check-t
  custom_indexes-t
  identifier_names-t
  semver-t
  system_views_columns-t
  type_builder-t
  type_context-t
  type_descriptor-t
  validate-t
  victionary_client-t
)

# Helper macro to add VillageSQL unit tests
# Note: We use a custom villagesql_test_main.cc instead of gunit_small to avoid
# duplicate symbol issues with system_charset_info on macOS. The macOS dynamic
# linker uses two-level namespace, which causes the test and server_unittest_library
# to have separate copies of the symbol when both define it.
#
# ADD_TEST registers the ctest test in all builds (so villagesql-unit-tests and
# "ctest -L villagesql" work). ADD_GCOV_TEST additionally clears EXCLUDE_FROM_ALL
# under ENABLE_GCOV so these tests are built by "make all" and folded into the
# standard gcov coverage sweep, matching the gunit small/server tests.
MACRO(ADD_VILLAGESQL_TEST test_name)
  MYSQL_ADD_EXECUTABLE(${test_name}
    ${test_name}.cc
    villagesql_test_main.cc
    COMPONENT Test
    EXCLUDE_FROM_ALL
    SKIP_INSTALL
    DEPENDENCIES ${GTEST_LIBRARIES}
    ADD_TEST ${test_name}
    ADD_GCOV_TEST ${test_name}
  )

  TARGET_INCLUDE_DIRECTORIES(${test_name} PRIVATE
    ${CMAKE_SOURCE_DIR}/villagesql/sdk/include
  )

  TARGET_LINK_LIBRARIES(${test_name}
    server_unittest_library
    mysys
    strings
    ${GTEST_LIBRARIES}
  )

  ADD_DEPENDENCIES(${test_name} GenError)

  # Set test properties for CTest
  SET_TESTS_PROPERTIES(${test_name} PROPERTIES
    TIMEOUT 300
    LABELS "villagesql"
  )
ENDMACRO()

# Register all VillageSQL unit tests from the list above.
FOREACH(test_name ${VILLAGESQL_UNIT_TESTS})
  ADD_VILLAGESQL_TEST(${test_name})
ENDFOREACH()

# Custom target to build all VillageSQL unit tests
ADD_CUSTOM_TARGET(villagesql-unit-tests
  DEPENDS ${VILLAGESQL_UNIT_TESTS}
)

# Also add them to the main test-unit dependencies if we're building unit tests
IF(TARGET test-unit)
  ADD_DEPENDENCIES(test-unit villagesql-unit-tests)
ENDIF()
