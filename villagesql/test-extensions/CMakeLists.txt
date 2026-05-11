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

# Test extension builds.
#
# Each extension is an independent ExternalProject so it only sees the SDK
# headers, not server internals — matching the experience of an external
# extension author.
#
# TODO(villagesql): migrate the existing test extensions from villagesql/CMakeLists.txt
# into this file using the same pattern.

include(ExternalProject)

ExternalProject_Add(vsql_preview_ping_test_extension
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/vsql-preview-ping-test
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-preview-ping-test-build
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  CMAKE_ARGS
    "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
    "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
    "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  DEPENDS sdk
  BUILD_ALWAYS ON
  INSTALL_COMMAND ""
)

add_custom_target(copy_vsql_preview_ping_test_veb
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
  COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-preview-ping-test-build/vsql_preview_ping_test.veb
    ${CMAKE_BINARY_DIR}/veb_output_directory/
  DEPENDS vsql_preview_ping_test_extension
  COMMENT "Copying vsql_preview_ping_test.veb to veb_output_directory"
)

add_custom_target(preview_ping_test_veb ALL)
add_dependencies(preview_ping_test_veb copy_vsql_preview_ping_test_veb)

ExternalProject_Add(vsql_preview_unknown_cap_test_extension
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/vsql-preview-unknown-cap-test
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-preview-unknown-cap-test-build
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  CMAKE_ARGS
    "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
    "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
    "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  DEPENDS sdk
  BUILD_ALWAYS ON
  INSTALL_COMMAND ""
)

add_custom_target(copy_vsql_preview_unknown_cap_test_veb
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
  COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-preview-unknown-cap-test-build/vsql_preview_unknown_cap_test.veb
    ${CMAKE_BINARY_DIR}/veb_output_directory/
  DEPENDS vsql_preview_unknown_cap_test_extension
  COMMENT "Copying vsql_preview_unknown_cap_test.veb to veb_output_directory"
)

add_custom_target(preview_unknown_cap_test_veb ALL)
add_dependencies(preview_unknown_cap_test_veb copy_vsql_preview_unknown_cap_test_veb)

ExternalProject_Add(vsql_keyring_reader_extension
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/vsql-keyring-reader
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-keyring-reader-build
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  CMAKE_ARGS
    "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
    "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
    "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  DEPENDS sdk
  BUILD_ALWAYS ON
  INSTALL_COMMAND ""
)

add_custom_target(copy_vsql_keyring_reader_veb
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
  COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-keyring-reader-build/vsql_keyring_reader.veb
    ${CMAKE_BINARY_DIR}/veb_output_directory/
  DEPENDS vsql_keyring_reader_extension
  COMMENT "Copying vsql_keyring_reader.veb to veb_output_directory"
)

add_custom_target(keyring_reader_veb ALL)
add_dependencies(keyring_reader_veb copy_vsql_keyring_reader_veb)

ExternalProject_Add(vsql_lifecycle_test_extension
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/vsql-lifecycle-test
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-lifecycle-test-build
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  CMAKE_ARGS
    "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
    "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
    "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  DEPENDS sdk
  BUILD_ALWAYS ON
  INSTALL_COMMAND ""
)

add_custom_target(copy_vsql_lifecycle_test_veb
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
  COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-lifecycle-test-build/vsql_lifecycle_test.veb
    ${CMAKE_BINARY_DIR}/veb_output_directory/
  DEPENDS vsql_lifecycle_test_extension
  COMMENT "Copying vsql_lifecycle_test.veb to veb_output_directory"
)

add_custom_target(lifecycle_test_veb ALL)
add_dependencies(lifecycle_test_veb copy_vsql_lifecycle_test_veb)

ExternalProject_Add(vsql_thread_worker_test_extension
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/vsql-thread-worker-test
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-thread-worker-test-build
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  CMAKE_ARGS
    "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
    "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
    "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  DEPENDS sdk
  BUILD_ALWAYS ON
  INSTALL_COMMAND ""
)

add_custom_target(copy_vsql_thread_worker_test_veb
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
  COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-thread-worker-test-build/vsql_thread_worker_test.veb
    ${CMAKE_BINARY_DIR}/veb_output_directory/
  DEPENDS vsql_thread_worker_test_extension
  COMMENT "Copying vsql_thread_worker_test.veb to veb_output_directory"
)

add_custom_target(thread_worker_test_veb ALL)
add_dependencies(thread_worker_test_veb copy_vsql_thread_worker_test_veb)

ExternalProject_Add(vsql_preview_ping_no_cap_test_extension
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/vsql-preview-ping-no-cap-test
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-preview-ping-no-cap-test-build
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  CMAKE_ARGS
    "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
    "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
    "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  DEPENDS sdk
  BUILD_ALWAYS ON
  INSTALL_COMMAND ""
)

add_custom_target(copy_vsql_preview_ping_no_cap_test_veb
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
  COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-preview-ping-no-cap-test-build/vsql_preview_ping_no_cap_test.veb
    ${CMAKE_BINARY_DIR}/veb_output_directory/
  DEPENDS vsql_preview_ping_no_cap_test_extension
  COMMENT "Copying vsql_preview_ping_no_cap_test.veb to veb_output_directory"
)

add_custom_target(preview_ping_no_cap_test_veb ALL)
add_dependencies(preview_ping_no_cap_test_veb copy_vsql_preview_ping_no_cap_test_veb)

ExternalProject_Add(vsql_preview_ping_v2_test_extension
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test-extensions/vsql-preview-ping-v2-test
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-preview-ping-v2-test-build
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  CMAKE_ARGS
    "-DCMAKE_PREFIX_PATH=${CMAKE_SOURCE_DIR}"
    "-DVillageSQLExtensionFramework_INCLUDE_DIR=${SDK_STAGING_DIR}/include-dev"
    "-DVillageSQL_VEB_INSTALL_DIR=${CMAKE_BINARY_DIR}/lib/veb"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  DEPENDS sdk
  BUILD_ALWAYS ON
  INSTALL_COMMAND ""
)

add_custom_target(copy_vsql_preview_ping_v2_test_veb
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/veb_output_directory
  COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/test-extensions/vsql-preview-ping-v2-test-build/vsql_preview_ping_v2_test.veb
    ${CMAKE_BINARY_DIR}/veb_output_directory/
  DEPENDS vsql_preview_ping_v2_test_extension
  COMMENT "Copying vsql_preview_ping_v2_test.veb to veb_output_directory"
)

add_custom_target(preview_ping_v2_test_veb ALL)
add_dependencies(preview_ping_v2_test_veb copy_vsql_preview_ping_v2_test_veb)
