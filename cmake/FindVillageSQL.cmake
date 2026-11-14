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

#[=======================================================================[.rst:
FindVillageSQL
--------------

Find the VillageSQL Extension SDK.

This module finds the VillageSQL Extension SDK using the following methods
(in order of priority):

1. ``VillageSQL_BUILD_DIR`` for development builds (points to VillageSQL build tree)
2. ``VillageSQL_SDK_DIR`` if explicitly set by the user
3. The ``villagesql_config`` script if found in PATH
4. The prebuilt install in ``~/.villagesql/prebuilt`` (as laid down by the
   install script)

If none of these find the SDK, ``VillageSQL_FOUND`` is false, and
``find_package(VillageSQL REQUIRED)`` stops configuration with an error.

Imported Targets
^^^^^^^^^^^^^^^^

This module does not define imported targets directly. Instead, it locates
the SDK and delegates to ``VillageSQLExtensionFrameworkConfig.cmake`` which
provides the ``VEF_CREATE_VEB()`` function for building extensions.

Result Variables
^^^^^^^^^^^^^^^^

This module defines the following variables:

``VillageSQL_FOUND``
  True if the SDK was found.

``VillageSQL_VERSION``
  The version of the SDK found (if available).

``VillageSQL_PREFIX``
  The installation prefix of the SDK.

``VillageSQL_INCLUDE_DIR``
  The include directory for SDK headers.

``VillageSQL_CXX_FLAGS``
  Compiler flags for building extensions.

``VILLAGESQL_CONFIG_EXECUTABLE``
  Path to the villagesql_config script (if found).

``VillageSQL_VEB_INSTALL_DIR``
  Directory for installing VEB files. Automatically set when using
  ``VillageSQL_BUILD_DIR`` or ``~/.villagesql/prebuilt`` to point to
  ``lib/veb`` in that tree, the directory the server loads VEBs from.

Cache Variables
^^^^^^^^^^^^^^^

``VillageSQL_BUILD_DIR``
  Set this to the VillageSQL build directory for development builds.
  The SDK will be found in the staged SDK directory, and
  ``VillageSQL_VEB_INSTALL_DIR`` will be set to ``lib/veb`` in the build tree.

``VillageSQL_SDK_DIR``
  Set this variable to the SDK installation directory to override
  automatic detection.

Requirements
^^^^^^^^^^^^

The VillageSQL Extension SDK requires C++17 or later. This module will
check that ``CMAKE_CXX_STANDARD`` is set appropriately.

Example Usage
^^^^^^^^^^^^^

  # Automatic detection
  find_package(VillageSQL REQUIRED)

  # Development build (building against VillageSQL build tree)
  cmake -DVillageSQL_BUILD_DIR=/path/to/villagesql/build ..

  # Or with explicit SDK path
  cmake -DVillageSQL_SDK_DIR=/path/to/sdk ..

#]=======================================================================]

set(_villagesql_found FALSE)
# Locations tried, reported if the SDK is not found
set(_villagesql_searched "")

# Use the newest staged SDK (villagesql-extension-sdk-*) under a VillageSQL
# build tree or prebuilt install. Both lay out the SDK and lib/veb the same
# way. Sets _villagesql_found on success and
# _villagesql_no_staged_sdk if no SDK directory exists under _root.
macro(_villagesql_find_staged_sdk _root)
  set(_villagesql_no_staged_sdk FALSE)
  # Sort descending so the newest version is used.
  file(GLOB _sdk_dirs_all "${_root}/villagesql-extension-sdk-*")
  set(_sdk_dirs "")
  foreach(_d IN LISTS _sdk_dirs_all)
    if(IS_DIRECTORY "${_d}")
      list(APPEND _sdk_dirs "${_d}")
    endif()
  endforeach()
  unset(_sdk_dirs_all)
  if(_sdk_dirs)
    list(SORT _sdk_dirs ORDER DESCENDING)
    list(GET _sdk_dirs 0 _sdk_dir)
    if(EXISTS "${_sdk_dir}/include/villagesql/vsql.h")
      set(VillageSQL_PREFIX "${_sdk_dir}")
      set(VillageSQL_INCLUDE_DIR "${_sdk_dir}/include")
      set(VillageSQL_CXX_FLAGS "-I${_sdk_dir}/include")
      # Try to get version from villagesql_config if present
      if(EXISTS "${_sdk_dir}/bin/villagesql_config")
        set(VILLAGESQL_CONFIG_EXECUTABLE "${_sdk_dir}/bin/villagesql_config")
        execute_process(
          COMMAND ${VILLAGESQL_CONFIG_EXECUTABLE} --version
          OUTPUT_VARIABLE VillageSQL_VERSION
          OUTPUT_STRIP_TRAILING_WHITESPACE
          ERROR_QUIET
        )
      endif()
      # Auto-set VEB install directory to where the server loads VEBs from
      if(NOT DEFINED VillageSQL_VEB_INSTALL_DIR)
        set(VillageSQL_VEB_INSTALL_DIR "${_root}/lib/veb")
      endif()
      set(_villagesql_found TRUE)
      message(STATUS "Using VillageSQL SDK: ${_sdk_dir}")
      message(STATUS "VEB install directory: ${VillageSQL_VEB_INSTALL_DIR}")
    endif()
    unset(_sdk_dir)
  else()
    set(_villagesql_no_staged_sdk TRUE)
  endif()
  unset(_sdk_dirs)
endmacro()

# Method 1: Use VillageSQL_BUILD_DIR for development builds
if(VillageSQL_BUILD_DIR AND NOT _villagesql_found)
  list(APPEND _villagesql_searched
    "VillageSQL_BUILD_DIR: ${VillageSQL_BUILD_DIR}/villagesql-extension-sdk-*")
  _villagesql_find_staged_sdk("${VillageSQL_BUILD_DIR}")
  if(_villagesql_no_staged_sdk)
    message(WARNING
      "VillageSQL_BUILD_DIR is set to '${VillageSQL_BUILD_DIR}' "
      "but no staged SDK (villagesql-extension-sdk-*) was found. "
      "Make sure the VillageSQL build has completed."
    )
  endif()
endif()

# Method 2: Use VillageSQL_SDK_DIR if explicitly set
if(VillageSQL_SDK_DIR AND NOT _villagesql_found)
  list(APPEND _villagesql_searched "VillageSQL_SDK_DIR: ${VillageSQL_SDK_DIR}")
  if(EXISTS "${VillageSQL_SDK_DIR}/include/villagesql/vsql.h")
    set(VillageSQL_PREFIX "${VillageSQL_SDK_DIR}")
    set(VillageSQL_INCLUDE_DIR "${VillageSQL_SDK_DIR}/include")
    set(VillageSQL_CXX_FLAGS "-I${VillageSQL_SDK_DIR}/include")
    # Try to get version from villagesql_config if present
    if(EXISTS "${VillageSQL_SDK_DIR}/bin/villagesql_config")
      set(VILLAGESQL_CONFIG_EXECUTABLE "${VillageSQL_SDK_DIR}/bin/villagesql_config")
      execute_process(
        COMMAND ${VILLAGESQL_CONFIG_EXECUTABLE} --version
        OUTPUT_VARIABLE VillageSQL_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
      )
    endif()
    set(_villagesql_found TRUE)
  else()
    message(WARNING
      "VillageSQL_SDK_DIR is set to '${VillageSQL_SDK_DIR}' "
      "but SDK headers were not found there."
    )
  endif()
endif()

# Method 3: Look for villagesql_config in PATH
if(NOT _villagesql_found)
  find_program(VILLAGESQL_CONFIG_EXECUTABLE
    NAMES villagesql_config
    DOC "Path to villagesql_config script"
  )

  if(VILLAGESQL_CONFIG_EXECUTABLE)
    execute_process(
      COMMAND ${VILLAGESQL_CONFIG_EXECUTABLE} --version
      OUTPUT_VARIABLE VillageSQL_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
    )

    execute_process(
      COMMAND ${VILLAGESQL_CONFIG_EXECUTABLE} --prefix
      OUTPUT_VARIABLE VillageSQL_PREFIX
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
    )

    execute_process(
      COMMAND ${VILLAGESQL_CONFIG_EXECUTABLE} --include
      OUTPUT_VARIABLE VillageSQL_INCLUDE_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
    )

    execute_process(
      COMMAND ${VILLAGESQL_CONFIG_EXECUTABLE} --cxxflags
      OUTPUT_VARIABLE VillageSQL_CXX_FLAGS
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
    )

    if(VillageSQL_PREFIX AND VillageSQL_INCLUDE_DIR)
      set(_villagesql_found TRUE)
    else()
      list(APPEND _villagesql_searched
        "villagesql_config in PATH: ${VILLAGESQL_CONFIG_EXECUTABLE} did not report a prefix and include directory")
    endif()
  else()
    list(APPEND _villagesql_searched "villagesql_config in PATH: not found")
  endif()
endif()

# Method 4: Fall back to the prebuilt install in ~/.villagesql/prebuilt
if(NOT _villagesql_found)
  list(APPEND _villagesql_searched
    "$ENV{HOME}/.villagesql/prebuilt/villagesql-extension-sdk-*")
  _villagesql_find_staged_sdk("$ENV{HOME}/.villagesql/prebuilt")
endif()

string(JOIN "\n    " _villagesql_searched_text ${_villagesql_searched})
string(CONCAT _villagesql_failure_message
  "Searched:\n    ${_villagesql_searched_text}\n"
  "Set VillageSQL_SDK_DIR to the SDK directory, or VillageSQL_BUILD_DIR to a "
  "VillageSQL build directory.")

unset(_villagesql_found)
unset(_villagesql_no_staged_sdk)
unset(_villagesql_searched)
unset(_villagesql_searched_text)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(VillageSQL
  REQUIRED_VARS
    VillageSQL_PREFIX
    VillageSQL_INCLUDE_DIR
  VERSION_VAR VillageSQL_VERSION
  REASON_FAILURE_MESSAGE "${_villagesql_failure_message}"
)
unset(_villagesql_failure_message)

if(VillageSQL_FOUND)
  # Check C++ standard requirement
  if(DEFINED CMAKE_CXX_STANDARD AND CMAKE_CXX_STANDARD LESS 17)
    message(FATAL_ERROR
      "VillageSQL Extension SDK requires C++17 or later. "
      "CMAKE_CXX_STANDARD is set to ${CMAKE_CXX_STANDARD}."
    )
  endif()

  # Chain to the full CMake config for VEF_CREATE_VEB, etc.
  find_package(VillageSQLExtensionFramework REQUIRED
    PATHS "${VillageSQL_PREFIX}"
    NO_DEFAULT_PATH
  )

  # Re-export the include dir from the framework config for convenience
  if(NOT VillageSQLExtensionFramework_INCLUDE_DIR)
    set(VillageSQLExtensionFramework_INCLUDE_DIR "${VillageSQL_INCLUDE_DIR}")
  endif()
endif()

mark_as_advanced(VILLAGESQL_CONFIG_EXECUTABLE)
