/*
  Copyright (c) 2017, 2024, Oracle and/or its affiliates.

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

#include "dest_round_robin.h"

#include <ostream>
#include <stdexcept>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mysql/harness/destination.h"
#include "mysql/harness/net_ts/io_context.h"
#include "test/helpers.h"  // init_test_logger

using ::testing::StrEq;

class RoundRobinDestinationTest : public ::testing::Test {
 protected:
  net::io_context io_ctx_;
};

TEST_F(RoundRobinDestinationTest, Constructor) {
  DestRoundRobin balancer(io_ctx_);
  ASSERT_EQ(0, balancer.size());
}

TEST_F(RoundRobinDestinationTest, AddTcp) {
  DestRoundRobin balancer(io_ctx_);
  balancer.add(mysql_harness::TcpDestination("addr1", 1));
  ASSERT_EQ(1, balancer.size());
  balancer.add(mysql_harness::TcpDestination("addr2", 2));
  ASSERT_EQ(2, balancer.size());

  // Already added destination
  balancer.add(mysql_harness::TcpDestination("addr1", 1));
  ASSERT_EQ(2, balancer.size());
}

TEST_F(RoundRobinDestinationTest, AddLocal) {
  DestRoundRobin balancer(io_ctx_);
  balancer.add(mysql_harness::LocalDestination("/foo"));
  ASSERT_EQ(1, balancer.size());
  balancer.add(mysql_harness::LocalDestination("/bar"));
  ASSERT_EQ(2, balancer.size());

  // Already added destination
  balancer.add(mysql_harness::LocalDestination("/bar"));
  ASSERT_EQ(2, balancer.size());
}

TEST_F(RoundRobinDestinationTest, Remove) {
  DestRoundRobin balancer(io_ctx_);
  balancer.add(mysql_harness::TcpDestination("addr1", 1));
  balancer.add(mysql_harness::TcpDestination("addr99", 99));
  balancer.add(mysql_harness::TcpDestination("addr2", 2));
  ASSERT_EQ(3, balancer.size());

  balancer.remove(mysql_harness::TcpDestination("addr99", 99));
  ASSERT_EQ(2, balancer.size());

  balancer.remove(mysql_harness::TcpDestination("addr99", 99));
  ASSERT_EQ(2, balancer.size());
}

TEST_F(RoundRobinDestinationTest, GetTcp) {
  mysql_harness::TcpDestination dest_addr1_1("addr1", 1);

  DestRoundRobin balancer(io_ctx_);
  ASSERT_THROW(balancer.get(dest_addr1_1), std::out_of_range);
  balancer.add(dest_addr1_1);
  ASSERT_NO_THROW(balancer.get(dest_addr1_1));

  mysql_harness::Destination dest = balancer.get(dest_addr1_1);
  ASSERT_THAT(dest.as_tcp().hostname(), StrEq("addr1"));
  EXPECT_EQ(dest.as_tcp().port(), 1);

  balancer.remove(dest_addr1_1);
  ASSERT_THAT(dest.as_tcp().hostname(), StrEq("addr1"));
  EXPECT_EQ(dest.as_tcp().port(), 1);
}

TEST_F(RoundRobinDestinationTest, GetLocal) {
  mysql_harness::LocalDestination dest_tmp_foo("/tmp/foo");

  DestRoundRobin balancer(io_ctx_);
  ASSERT_THROW(balancer.get(dest_tmp_foo), std::out_of_range);
  balancer.add(dest_tmp_foo);
  ASSERT_NO_THROW(balancer.get(dest_tmp_foo));

  mysql_harness::Destination dest = balancer.get(dest_tmp_foo);
  ASSERT_THAT(dest.as_local().path(), StrEq("/tmp/foo"));

  balancer.remove(dest_tmp_foo);
  ASSERT_THAT(dest.as_local().path(), StrEq("/tmp/foo"));
}

TEST_F(RoundRobinDestinationTest, SizeTcp) {
  mysql_harness::TcpDestination dest_addr1_1("addr1", 1);

  DestRoundRobin balancer(io_ctx_);
  ASSERT_EQ(0, balancer.size());

  balancer.add(dest_addr1_1);
  ASSERT_EQ(1, balancer.size());

  balancer.remove(dest_addr1_1);
  ASSERT_EQ(0, balancer.size());
}

TEST_F(RoundRobinDestinationTest, SizeLocal) {
  mysql_harness::LocalDestination dest_tmp_foo("/tmp/foo");

  DestRoundRobin balancer(io_ctx_);
  ASSERT_EQ(0, balancer.size());

  balancer.add(dest_tmp_foo);
  ASSERT_EQ(1, balancer.size());

  balancer.remove(dest_tmp_foo);
  ASSERT_EQ(0, balancer.size());
}

TEST_F(RoundRobinDestinationTest, RemoveAll) {
  DestRoundRobin balancer(io_ctx_);

  balancer.add(mysql_harness::TcpDestination("addr1", 1));
  balancer.add(mysql_harness::TcpDestination("addr2", 2));
  balancer.add(mysql_harness::TcpDestination("addr3", 3));
  ASSERT_EQ(3, balancer.size());

  balancer.clear();
  ASSERT_EQ(0, balancer.size());
}

/**
 * @test DestRoundRobin spawns the quarantine thread and
 *       joins it in the destructor. Make sure the destructor
 *       does not block/deadlock and forces the thread close (bug#27145261).
 */
TEST_F(RoundRobinDestinationTest, SpawnAndJoinQuarantineThread) {
  DestRoundRobin balancer(io_ctx_);
  balancer.start(nullptr);
}

bool operator==(const Destinations::value_type &a, const Destination &b) {
  return a->destination() == b.destination();
}

std::ostream &operator<<(std::ostream &os, const Destination &v) {
  os << "{ address: " << v.destination().str() << ", good: " << v.good() << "}";
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const std::unique_ptr<Destination> &v) {
  os << *v;
  return os;
}

MATCHER(IsGoodEq, "") {
  return ::testing::ExplainMatchResult(
      ::testing::Property(&Destination::good, std::get<1>(arg)),
      std::get<0>(arg).get(), result_listener);
}

TEST_F(RoundRobinDestinationTest, RepeatedFetch) {
  DestRoundRobin dest(io_ctx_, Protocol::Type::kClassicProtocol);
  dest.add(mysql_harness::TcpDestination("41", 41));
  dest.add(mysql_harness::TcpDestination("42", 42));
  dest.add(mysql_harness::TcpDestination("43", 43));

  SCOPED_TRACE("// fetch 0, rotate 0");
  {
    auto actual = dest.destinations();
    EXPECT_THAT(actual, ::testing::SizeIs(3));
    EXPECT_THAT(
        actual,
        ::testing::ElementsAre(
            Destination("41", mysql_harness::TcpDestination("41", 41)),
            Destination("42", mysql_harness::TcpDestination("42", 42)),
            Destination("43", mysql_harness::TcpDestination("43", 43))));
    EXPECT_THAT(actual, ::testing::Pointwise(IsGoodEq(), {true, true, true}));
  }

  SCOPED_TRACE("// fetch 1, rotate 1");
  {
    auto actual = dest.destinations();
    EXPECT_THAT(actual, ::testing::SizeIs(3));
    EXPECT_THAT(
        actual,
        ::testing::ElementsAre(
            Destination("42", mysql_harness::TcpDestination("42", 42)),
            Destination("43", mysql_harness::TcpDestination("43", 43)),
            Destination("41", mysql_harness::TcpDestination("41", 41))));
    EXPECT_THAT(actual, ::testing::Pointwise(IsGoodEq(), {true, true, true}));
  }

  SCOPED_TRACE("// fetch 2, rotate 2");
  {
    auto actual = dest.destinations();
    EXPECT_THAT(actual, ::testing::SizeIs(3));
    EXPECT_THAT(
        actual,
        ::testing::ElementsAre(
            Destination("43", mysql_harness::TcpDestination("43", 43)),
            Destination("41", mysql_harness::TcpDestination("41", 41)),
            Destination("42", mysql_harness::TcpDestination("42", 42))));
    EXPECT_THAT(actual, ::testing::Pointwise(IsGoodEq(), {true, true, true}));
  }

  SCOPED_TRACE("// fetch 3, rotate 0");
  {
    auto actual = dest.destinations();
    EXPECT_THAT(actual, ::testing::SizeIs(3));
    EXPECT_THAT(
        actual,
        ::testing::ElementsAre(
            Destination("41", mysql_harness::TcpDestination("41", 41)),
            Destination("42", mysql_harness::TcpDestination("42", 42)),
            Destination("43", mysql_harness::TcpDestination("43", 43))));
    EXPECT_THAT(actual, ::testing::Pointwise(IsGoodEq(), {true, true, true}));
  }
}

int main(int argc, char *argv[]) {
  init_test_logger();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
