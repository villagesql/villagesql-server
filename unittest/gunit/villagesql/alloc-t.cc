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

#include <gtest/gtest.h>

#include <new>
#include <string>
#include <utility>

#include "villagesql/include/alloc.h"

namespace villagesql_unittest {

using namespace villagesql;

class AllocTest : public ::testing::Test {};

namespace {

// Tracks construction and destruction so the tests can verify ownership.
struct Tracked {
  static int live;
  int value;
  std::string name;

  explicit Tracked(int v, std::string n = "") : value(v), name(std::move(n)) {
    ++live;
  }
  Tracked(const Tracked &other) : value(other.value), name(other.name) {
    ++live;
  }
  ~Tracked() { --live; }
};

int Tracked::live = 0;

// Stands in for a type whose own constructor runs out of memory, which is the
// only way to reach the helpers' failure path deterministically.
struct ThrowsBadAlloc {
  ThrowsBadAlloc() { throw std::bad_alloc(); }
};

// A type only the helpers' caller can construct, mirroring TypeContext and
// IndexContext, whose private constructors are why wrap_shared_nothrow exists.
class PrivateCtor {
 public:
  static std::shared_ptr<PrivateCtor> create(int value) {
    return wrap_shared_nothrow(new (std::nothrow) PrivateCtor(value));
  }
  int value() const { return value_; }

 private:
  explicit PrivateCtor(int value) : value_(value) {}
  int value_;
};

}  // namespace

// The helpers must not propagate exceptions: callers on the statement
// execution path have no handler for them.
static_assert(noexcept(make_shared_nothrow<Tracked>(1)));
static_assert(noexcept(make_unique_nothrow<Tracked>(1)));
static_assert(noexcept(wrap_shared_nothrow<Tracked>(nullptr)));

TEST_F(AllocTest, MakeSharedNothrowConstructsAndForwards) {
  ASSERT_EQ(0, Tracked::live);
  {
    auto ptr = make_shared_nothrow<Tracked>(42, "forwarded");
    ASSERT_NE(nullptr, ptr);
    EXPECT_EQ(42, ptr->value);
    EXPECT_EQ("forwarded", ptr->name);
    EXPECT_EQ(1, Tracked::live);
    EXPECT_EQ(1, ptr.use_count());
  }
  EXPECT_EQ(0, Tracked::live);
}

TEST_F(AllocTest, MakeSharedNothrowMovesArguments) {
  std::string name = "moved";
  auto ptr = make_shared_nothrow<Tracked>(7, std::move(name));
  ASSERT_NE(nullptr, ptr);
  EXPECT_EQ("moved", ptr->name);
  EXPECT_TRUE(name.empty());
}

TEST_F(AllocTest, MakeUniqueNothrowConstructsAndForwards) {
  ASSERT_EQ(0, Tracked::live);
  {
    auto ptr = make_unique_nothrow<Tracked>(11, "unique");
    ASSERT_NE(nullptr, ptr);
    EXPECT_EQ(11, ptr->value);
    EXPECT_EQ("unique", ptr->name);
    EXPECT_EQ(1, Tracked::live);
  }
  EXPECT_EQ(0, Tracked::live);
}

// Returning nullptr instead of throwing is the whole point of the helpers:
// callers null-check the result the same way they do for new (std::nothrow).
TEST_F(AllocTest, MakeSharedNothrowReturnsNullOnBadAlloc) {
  auto ptr = make_shared_nothrow<ThrowsBadAlloc>();
  EXPECT_EQ(nullptr, ptr);
}

TEST_F(AllocTest, MakeUniqueNothrowReturnsNullOnBadAlloc) {
  auto ptr = make_unique_nothrow<ThrowsBadAlloc>();
  EXPECT_EQ(nullptr, ptr);
}

TEST_F(AllocTest, WrapSharedNothrowTakesOwnership) {
  ASSERT_EQ(0, Tracked::live);
  {
    auto ptr = wrap_shared_nothrow(new (std::nothrow) Tracked(3, "wrapped"));
    ASSERT_NE(nullptr, ptr);
    EXPECT_EQ(3, ptr->value);
    EXPECT_EQ("wrapped", ptr->name);
    EXPECT_EQ(1, Tracked::live);
  }
  EXPECT_EQ(0, Tracked::live);
}

// A null argument passes straight through, so callers can hand over the result
// of new (std::nothrow) without checking it first.
TEST_F(AllocTest, WrapSharedNothrowPassesNullThrough) {
  Tracked *null_entry = nullptr;
  auto ptr = wrap_shared_nothrow(null_entry);
  EXPECT_EQ(nullptr, ptr);
  EXPECT_EQ(0, Tracked::live);
}

TEST_F(AllocTest, WrapSharedNothrowReachesPrivateConstructor) {
  auto ptr = PrivateCtor::create(5);
  ASSERT_NE(nullptr, ptr);
  EXPECT_EQ(5, ptr->value());
}

}  // namespace villagesql_unittest
