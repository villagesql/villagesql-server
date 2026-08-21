if(NOT WITH_COMPONENT_KEYRING_VAULT)
  message(STATUS "Not building Keyring Vault Component unit tests")
  return()
endif()

message(STATUS "Building Keyring Vault Component unit tests")

set(SRC_FILES)
list(APPEND SRC_FILES
  ${CMAKE_SOURCE_DIR}/components/keyrings/common/data/meta.cc
  ${CMAKE_SOURCE_DIR}/components/keyrings/common/data/data.cc
  ${CMAKE_SOURCE_DIR}/components/keyrings/common/utils/utils.cc
  ${CMAKE_SOURCE_DIR}/components/keyrings/keyring_vault/config/config.cc
  ${CMAKE_SOURCE_DIR}/components/keyrings/keyring_vault/backend/backend.cc
  ${CMAKE_SOURCE_DIR}/components/keyrings/keyring_vault/backend/vault_base64.cc
  ${CMAKE_SOURCE_DIR}/components/keyrings/keyring_vault/backend/vault_curl.cc
  ${CMAKE_SOURCE_DIR}/components/keyrings/keyring_vault/backend/vault_parser_composer.cc
  ${CMAKE_SOURCE_DIR}/components/keyrings/keyring_vault/backend/vault_keys_container.cc
  ${CMAKE_SOURCE_DIR}/unittest/gunit/components/keyring_vault/component_callbacks_mock.cc
)

add_library(component_keyring_vault_test STATIC ${SRC_FILES})
add_dependencies(component_keyring_vault_test GenError)
target_link_libraries(component_keyring_vault_test extra::rapidjson library_mysys)
target_compile_definitions(component_keyring_vault_test PUBLIC LOG_COMPONENT_TAG="component_keyring_vault")
target_include_directories(component_keyring_vault_test SYSTEM PUBLIC
  ${CMAKE_SOURCE_DIR}
  ${GMOCK_INCLUDE_DIRS}
  ${BOOST_PATCHES_DIR}
  ${BOOST_INCLUDE_DIR}
)
target_include_directories(component_keyring_vault_test PUBLIC
  ${CMAKE_SOURCE_DIR}/components/keyrings/keyring_vault
)

set(TESTS
  vault_backend
  vault_config
  vault_parser
)

set(ALL_COMPONENT_KEYRING_VAULT_TESTS)
foreach(test ${TESTS})
  list(APPEND ALL_COMPONENT_KEYRING_VAULT_TESTS ${test}-t.cc)
endforeach()

MYSQL_ADD_EXECUTABLE(merge_component_keyring_vault_tests-t
  ${ALL_COMPONENT_KEYRING_VAULT_TESTS}
  ENABLE_EXPORTS
  ADD_TEST merge_component_keyring_vault_tests)

target_link_libraries(merge_component_keyring_vault_tests-t
  component_keyring_vault_test
  gunit_large
  perfschema
  sql_main
  strings
  mysys
  sql_gis
  sql_dd
  minchassis
  binlog
  rpl
  rpl_source
  rpl_replica
  ext::curl
  ext::icu
)

add_dependencies(merge_component_keyring_vault_tests-t GenError)

# Unlike upstream's aggregate merge targets, this one links the server libraries
# directly rather than through server_unittest_library, so it inherits none of
# the link-time warning downgrades that whole-server links need. Without them a
# RelWithDebInfo build on a DEB platform fails: dpkg-buildflags supplies
# -flto=auto, and under LTO these diagnostics are emitted at link time, where
# per-source-file suppressions cannot reach them. The same three warning classes
# upstream downgrades elsewhere then become fatal via VILLAGESQL_WERROR --
# boost geometry/spirit and sql/rpl_info_values.cc, the very sites named in the
# server_unittest_library block in the top-level CMakeLists.txt.
#
# Shape and gating copied from the two existing precedents rather than invented:
# unittest/gunit/CMakeLists.txt's merge_large_tests-t for the alloc-size and
# stringop downgrades, and server_unittest_library for the LTO-gated
# maybe-uninitialized one.
#
# TODO(villagesql-rebase): the underlying deviation is the direct link above. If
# this target is ever reworked to link server_unittest_library like
# merge_large_tests-t does, it inherits the downgrades and this block goes away.
IF(TARGET merge_component_keyring_vault_tests-t)
  IF(MY_COMPILER_IS_GNU)
    TARGET_LINK_OPTIONS(merge_component_keyring_vault_tests-t
      PRIVATE -Wno-alloc-size-larger-than)
  ENDIF()
  DOWNGRADE_STRINGOP_WARNINGS(merge_component_keyring_vault_tests-t)
  IF(WITH_LTO OR CMAKE_COMPILER_FLAG_WITH_LTO)
    MY_CHECK_CXX_COMPILER_WARNING("error=maybe-uninitialized" HAS_WARN_FLAG)
    IF(HAS_WARN_FLAG)
      MY_TARGET_LINK_OPTIONS(merge_component_keyring_vault_tests-t
        "${HAS_WARN_FLAG}")
    ENDIF()
  ENDIF()
ENDIF()

foreach(test ${TESTS})
  MYSQL_ADD_EXECUTABLE(${test}-t ${test}-t.cc
    ENABLE_EXPORTS SKIP_INSTALL EXCLUDE_FROM_ALL)

  target_link_libraries(${test}-t component_keyring_vault_test gunit_small server_unittest_library ext::curl ext::icu)
endforeach()
