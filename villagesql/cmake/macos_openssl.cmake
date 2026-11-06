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

# On macOS with no WITH_SSL path, use Homebrew's openssl@3. Homebrew's
# unversioned openssl is OpenSSL 4, which cmake/ssl.cmake cannot read.

IF(APPLE AND (NOT WITH_SSL OR WITH_SSL STREQUAL "system"))
  FIND_PROGRAM(VILLAGESQL_BREW_EXECUTABLE brew)
  IF(VILLAGESQL_BREW_EXECUTABLE)
    EXECUTE_PROCESS(COMMAND ${VILLAGESQL_BREW_EXECUTABLE} --prefix openssl@3
      OUTPUT_VARIABLE VILLAGESQL_OPENSSL3 OUTPUT_STRIP_TRAILING_WHITESPACE)
    # brew --prefix prints the path even when openssl@3 is not installed.
    IF(NOT EXISTS "${VILLAGESQL_OPENSSL3}/include/openssl/ssl.h")
      MESSAGE(FATAL_ERROR "OpenSSL 3 not found. Run: brew install openssl@3")
    ENDIF()
    SET(WITH_SSL "${VILLAGESQL_OPENSSL3}" CACHE STRING "OpenSSL path" FORCE)
    MESSAGE(STATUS "Using Homebrew openssl@3 at ${WITH_SSL}")
  ENDIF()
ENDIF()
