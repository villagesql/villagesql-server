/*
  Copyright (c) 2024, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is designed to work with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have either included with
  the program or referenced in the documentation.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/
#include <chrono>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "config_builder.h"
#include "mock_server_testutils.h"
#include "mysqlrouter/mysql_session.h"
#include "router_component_test.h"
#include "router_component_testutils.h"
#include "router_test_helpers.h"
#include "tcp_port_pool.h"

using namespace std::chrono_literals;

class RouterMrsConfig : public RouterComponentTest {
 protected:
  void SetUp() override { RouterComponentTest::SetUp(); }

  ProcessWrapper &launch_router(const std::string &conf_dir,
                                const std::string &mrs_section,
                                bool expect_error = false) {
    auto def_section = get_DEFAULT_defaults();
    init_keyring(def_section, conf_dir);

    // launch the router with mrs section
    const std::string conf_file =
        create_config_file(conf_dir, mrs_section, &def_section);
    const int expected_exit_code = expect_error ? EXIT_FAILURE : EXIT_SUCCESS;
    auto &router =
        ProcessManager::launch_router({"-c", conf_file}, expected_exit_code,
                                      true, false, expect_error ? -1s : 5s);

    return router;
  }

  std::string get_mrs_section(
      const std::optional<std::string> &mysql_read_only_route,
      const std::optional<std::string> &mysql_read_write_route,
      const std::optional<std::string> &mysql_user,
      const std::optional<std::string> &mysql_user_data_access,
      const std::optional<std::uint64_t> &router_id) const {
    std::vector<std::pair<std::string, std::string>> options{};

    if (mysql_read_only_route) {
      options.emplace_back("mysql_read_only_route",
                           mysql_read_only_route.value());
    }
    if (mysql_read_write_route) {
      options.emplace_back("mysql_read_write_route",
                           mysql_read_only_route.value());
    }
    if (mysql_user) {
      options.emplace_back("mysql_user", mysql_user.value());
    }
    if (mysql_user_data_access) {
      options.emplace_back("mysql_user_data_access",
                           mysql_user_data_access.value());
    }
    if (router_id) {
      options.emplace_back("router_id", std::to_string(router_id.value()));
    }

    return mysql_harness::ConfigBuilder::build_section("mysql_rest_service",
                                                       options);
  }
};

/**
 * @test Checks that the Router refuses to start if
 * [mysql_rest_service].router_id si not configured
 */
TEST_F(RouterMrsConfig, RouterComponentBootstrapWithDefaultCertsTest) {
  TempDirectory conf_dir("conf");

  const std::string mrs_section =
      get_mrs_section("bootstrap_ro", "bootstrap_rw",
                      "mysql_router_mrs1_ie9u74n75rsb", "", std::nullopt);

  auto &router = launch_router(conf_dir.name(), mrs_section,
                               /*expect_error=*/true);

  check_exit_code(router, EXIT_FAILURE);
  EXPECT_TRUE(wait_log_contains(router,
                                "Configuration error: option router_id in "
                                "\\[mysql_rest_service\\] is required",
                                500ms));
}

int main(int argc, char *argv[]) {
  init_windows_sockets();
  ProcessManager::set_origin(Path(argv[0]).dirname());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
