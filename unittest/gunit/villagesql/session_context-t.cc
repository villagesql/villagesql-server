/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/func_builder.h>
#include <villagesql/vsql/func_types.h>

#include "villagesql/vdf/session_context.h"

namespace villagesql_unittest {

class SessionContextAbiTest : public ::testing::Test {};

void session_vdf(vsql::Session, vsql::IntResult) {}
void regular_vdf(vsql::IntResult) {}

TEST_F(SessionContextAbiTest, KillStatusEnumValues) {
  EXPECT_EQ(0u, VEF_KILL_NOT_KILLED);
  EXPECT_EQ(1u, VEF_KILL_CONNECTION);
  EXPECT_EQ(2u, VEF_KILL_QUERY);
  EXPECT_EQ(3u, VEF_KILL_TIMEOUT);
  EXPECT_EQ(255u, VEF_KILL_UNKNOWN);
}

TEST_F(SessionContextAbiTest, ContextHoldsTier1Fields) {
  vef_context_t ctx{};
  ctx.protocol = VEF_PROTOCOL_4;
  ctx.schema = "mydb";
  ctx.connection_id = 42;
  ctx.priv_user = "root";
  ctx.priv_host = "localhost";
  ctx.kill_status = VEF_KILL_QUERY;

  EXPECT_STREQ("mydb", ctx.schema);
  EXPECT_EQ(42u, ctx.connection_id);
  EXPECT_STREQ("root", ctx.priv_user);
  EXPECT_STREQ("localhost", ctx.priv_host);
  EXPECT_EQ(VEF_KILL_QUERY, ctx.kill_status);
}

TEST_F(SessionContextAbiTest, BuilderOptsInSessionVdfs) {
  constexpr auto session =
      vsql::func_builder::make_func<&session_vdf>("session")
          .returns(vsql::func_builder::INT)
          .no_params()
          .build();
  constexpr auto regular =
      vsql::func_builder::make_func<&regular_vdf>("regular")
          .returns(vsql::func_builder::INT)
          .no_params()
          .build();

  EXPECT_TRUE(session.uses_session_context());
  EXPECT_EQ(VEF_PROTOCOL_4, session.required_protocol());
  EXPECT_FALSE(regular.uses_session_context());
}

class SessionAccessorTest : public ::testing::Test {};

TEST_F(SessionAccessorTest, ReadsFieldsWhenProtocol4) {
  vef_context_t ctx{};
  ctx.protocol = VEF_PROTOCOL_4;
  ctx.schema = "mydb";
  ctx.connection_id = 7;
  ctx.priv_user = "root";
  ctx.priv_host = "localhost";
  ctx.kill_status = VEF_KILL_TIMEOUT;

  vsql::Session s(&ctx);
  EXPECT_TRUE(s.available());
  EXPECT_EQ("mydb", s.schema());
  EXPECT_EQ(7u, s.connection_id());
  EXPECT_EQ("root", s.priv_user());
  EXPECT_EQ("localhost", s.priv_host());
  EXPECT_EQ(vsql::KillStatus::Timeout, s.kill_status());
}

TEST_F(SessionAccessorTest, EmptyWhenProtocolBelow4) {
  vef_context_t ctx{};
  ctx.protocol = VEF_PROTOCOL_3;
  ctx.schema = "should_not_leak";

  vsql::Session s(&ctx);
  EXPECT_FALSE(s.available());
  EXPECT_TRUE(s.schema().empty());
  EXPECT_EQ(0u, s.connection_id());
  EXPECT_EQ(vsql::KillStatus::NotKilled, s.kill_status());
}

TEST_F(SessionAccessorTest, EmptyWhenNullPointers) {
  vef_context_t ctx{};
  ctx.protocol = VEF_PROTOCOL_4;

  vsql::Session s(&ctx);
  EXPECT_TRUE(s.available());
  EXPECT_TRUE(s.schema().empty());
  EXPECT_TRUE(s.priv_user().empty());
  EXPECT_TRUE(s.priv_host().empty());
}

class KillStatusMappingTest : public ::testing::Test {};

TEST_F(KillStatusMappingTest, MapsEachKilledState) {
  using villagesql::vdf::vef_map_kill_status;
  EXPECT_EQ(VEF_KILL_NOT_KILLED, vef_map_kill_status(THD::NOT_KILLED));
  EXPECT_EQ(VEF_KILL_CONNECTION, vef_map_kill_status(THD::KILL_CONNECTION));
  EXPECT_EQ(VEF_KILL_QUERY, vef_map_kill_status(THD::KILL_QUERY));
  EXPECT_EQ(VEF_KILL_TIMEOUT, vef_map_kill_status(THD::KILL_TIMEOUT));
  EXPECT_EQ(VEF_KILL_UNKNOWN, vef_map_kill_status(THD::KILLED_NO_VALUE));
}

}  // namespace villagesql_unittest
