// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/type_params.h>

namespace villagesql_unittest {

// TypeParamsCache read without a lock: concurrent first uses of many
// parameter strings, from several threads in different orders, must give
// every thread the same entry (same address, right value) per string, keep
// earlier entries valid while others are inserted, and run parse at most
// once per thread per string. Run under the thread sanitizer to check the
// lock-free read path.

namespace {

struct CacheParams {
  int max_size;
  std::string type;
};

std::atomic<long> g_cache_parses{0};

CacheParams ParseCacheParams(const std::map<std::string, std::string> &m) {
  g_cache_parses.fetch_add(1, std::memory_order_relaxed);
  return CacheParams{std::stoi(m.at("max_size")), m.at("type")};
}

}  // namespace

TEST(TypeParamsCacheConcurrentTest, SameEntryPerStringAcrossThreads) {
  constexpr int kThreads = 8;
  constexpr int kKeys = 200;  // more than the bucket count: chains are used
  constexpr int kRounds = 5;
  g_cache_parses = 0;
  vsql::TypeParamsCache<CacheParams> cache;
  cache.bind(&ParseCacheParams);

  std::vector<std::string> sizes(kKeys);
  for (int k = 0; k < kKeys; ++k) sizes[k] = std::to_string(k + 1);
  std::vector<std::vector<const CacheParams *>> seen(
      kThreads, std::vector<const CacheParams *>(kKeys, nullptr));
  std::atomic<bool> bad{false};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int round = 0; round < kRounds; ++round) {
        for (int i = 0; i < kKeys; ++i) {
          const int k = (i * 7 + t * 13 + round) % kKeys;
          const char *keys[2] = {"max_size", "type"};
          const char *values[2] = {sizes[k].c_str(), "int32"};
          vef_type_params_t raw{};
          raw.keys = keys;
          raw.values = values;
          raw.count = 2;
          const CacheParams &p = cache.get(raw);
          if (p.max_size != k + 1 || p.type != "int32") bad = true;
          if (seen[t][k] == nullptr) {
            seen[t][k] = &p;
          } else if (seen[t][k] != &p) {
            bad = true;
          }
        }
      }
    });
  }
  for (std::thread &th : threads) th.join();

  EXPECT_FALSE(bad.load());
  for (int t = 1; t < kThreads; ++t) {
    for (int k = 0; k < kKeys; ++k) EXPECT_EQ(seen[t][k], seen[0][k]);
  }
  EXPECT_GE(g_cache_parses.load(), kKeys);
  EXPECT_LE(g_cache_parses.load(), static_cast<long>(kKeys) * kThreads);

  // Unsorted raw params and the map overload reach the same entry.
  const char *keys[2] = {"type", "max_size"};
  const char *values[2] = {"int32", "7"};
  vef_type_params_t raw{};
  raw.keys = keys;
  raw.values = values;
  raw.count = 2;
  EXPECT_EQ(&cache.get(raw), seen[0][6]);
  const std::map<std::string, std::string> m{{"max_size", "7"},
                                             {"type", "int32"}};
  EXPECT_EQ(&cache.get(m, &ParseCacheParams), seen[0][6]);
}

// ===========================================================================
// write_params_to
// ===========================================================================
//
// Serializes a params map into a caller-supplied buffer under the
// vef_inferred_type_params_t contract. Every bind_and_check_types hook reaches
// it through BindResult::set_return / set_arg, and the constant-string
// from_string inference path writes its inferred params back the same way.
//
// Two properties carry weight beyond this function:
//
//   1. The snprintf convention. actual_len is the length the whole string
//      WOULD have taken, not the number of bytes written. Callers must test
//      overflow before reading actual_len bytes out of the buffer, which is
//      why ValidateAndConvertVDFArguments checks it first.
//
//   2. Stable key order. The SDK sorts keys by bytes. The server then uses
//      from_raw() to lowercase, trim and sort them by the parameter collation.

class WriteParamsToTest : public ::testing::Test {
 protected:
  // Runs write_params_to with a declared capacity of `cap`, carved out of a
  // larger sentinel-filled block so a write past max_buf_len is detectable.
  void Run(const std::map<std::string, std::string> &m, size_t cap) {
    // fill with 0xAA bytes (sentinel)
    buf_.assign(kBlock, kSentinel);
    out_ = {};
    out_.buf = buf_.data();
    out_.max_buf_len = cap;
    vsql::write_params_to(&out_, m);
  }

  // The bytes written. Only meaningful when !out_.overflow -- on overflow the
  // contract leaves the buffer undefined.
  std::string Written() const {
    return std::string(buf_.data(), out_.actual_len);
  }

  // True when nothing was written at or past the declared capacity.
  bool GuardIntact(size_t cap) const {
    return buf_.find_first_not_of(kSentinel, cap) == std::string::npos;
  }

  static constexpr size_t kBlock = 256;
  static constexpr char kSentinel = '\xAA';

  std::string buf_;
  vef_inferred_type_params_t out_{};
};

TEST_F(WriteParamsToTest, EmptyMapWritesNothing) {
  Run({}, 64);
  EXPECT_EQ(out_.actual_len, 0u);
  EXPECT_FALSE(out_.overflow);
  EXPECT_TRUE(GuardIntact(0));
}

TEST_F(WriteParamsToTest, SinglePairExactBytes) {
  Run({{"dimension", "3"}}, 64);
  EXPECT_FALSE(out_.overflow);
  std::string written = Written();
  EXPECT_EQ(written, "dimension=3");
  EXPECT_EQ(out_.actual_len, written.length());
  EXPECT_TRUE(GuardIntact(11));
}

TEST_F(WriteParamsToTest, OutputIsInCanonicalSortedOrder) {
  std::map<std::string, std::string> m;
  m["type"] = "float";
  m["dimension"] = "3";
  Run(m, 64);
  EXPECT_FALSE(out_.overflow);
  EXPECT_EQ(Written(), "dimension=3,type=float");
}

TEST_F(WriteParamsToTest, ExactFitDoesNotOverflow) {
  // "dimension=3,type=float" is 22 bytes; a 22-byte buffer must be accepted.
  Run({{"dimension", "3"}, {"type", "float"}}, 22);
  EXPECT_FALSE(out_.overflow);
  std::string written = Written();
  EXPECT_EQ(written, "dimension=3,type=float");
  EXPECT_EQ(out_.actual_len, written.length());
  EXPECT_TRUE(GuardIntact(22));
}

TEST_F(WriteParamsToTest, OneByteShortSetsOverflow) {
  Run({{"dimension", "3"}, {"type", "float"}}, 21);
  EXPECT_TRUE(out_.overflow);
  EXPECT_EQ(out_.actual_len, strlen("dimension=3,type=float"));
  EXPECT_TRUE(GuardIntact(21));
}

// The snprintf convention: actual_len is the length of the WHOLE string, not
// of the part that fitted. Here the first pair (11 bytes) is written and the
// second is not, yet actual_len reports all 22.
TEST_F(WriteParamsToTest, ActualLenCountsWholeStringNotWhatFit) {
  Run({{"dimension", "3"}, {"type", "float"}}, 11);
  EXPECT_TRUE(out_.overflow);
  EXPECT_EQ(out_.actual_len, strlen("dimension=3,type=float"));
  EXPECT_TRUE(GuardIntact(11));
}

TEST_F(WriteParamsToTest, ZeroCapacityNonEmptyMap) {
  Run({{"dimension", "3"}}, 0);
  EXPECT_TRUE(out_.overflow);
  EXPECT_EQ(out_.actual_len, strlen("dimension=3"));
  EXPECT_TRUE(GuardIntact(0));
}

// No NUL is appended: actual_len is the whole story, and the byte just past
// the output is left as the caller had it.
TEST_F(WriteParamsToTest, BufferIsNotNulTerminated) {
  Run({{"dimension", "3"}}, 64);
  ASSERT_FALSE(out_.overflow);
  std::string written = Written();
  EXPECT_EQ(written, "dimension=3");
  ASSERT_EQ(out_.actual_len, written.length());
  EXPECT_EQ(buf_[out_.actual_len], kSentinel);
}

// Both outputs are assigned unconditionally, so a struct that overflowed once
// is safe to reuse.
TEST_F(WriteParamsToTest, ReuseClearsOverflow) {
  Run({{"dimension", "3"}, {"type", "float"}}, 5);
  ASSERT_TRUE(out_.overflow);

  Run({{"dimension", "3"}}, 64);
  ASSERT_FALSE(out_.overflow);
  std::string written = Written();
  EXPECT_EQ(written, "dimension=3");
  EXPECT_EQ(out_.actual_len, written.length());
}

// TypeParamsCache::get from a vef_type_params_t finds its entry without
// building the key string (lookup_raw). These tests pin down that it returns
// the same entry as the key-string and map paths do, for sorted and unsorted
// input, and that near-miss parameters stay distinct.
struct CacheTestParams {
  int id;
};

class TypeParamsCacheRawTest : public ::testing::Test {
 protected:
  static vef_type_params_t Raw(const char **keys, const char **values,
                               unsigned count) {
    vef_type_params_t raw{};
    raw.keys = keys;
    raw.values = values;
    raw.count = count;
    return raw;
  }
  const CacheTestParams &Get(const vef_type_params_t &raw) {
    return cache_.get(raw, [this](const std::map<std::string, std::string> &) {
      return CacheTestParams{++parses_};
    });
  }
  vsql::TypeParamsCache<CacheTestParams> cache_;
  int parses_ = 0;
};

TEST_F(TypeParamsCacheRawTest, SecondGetIsAHitWithoutParsing) {
  const char *keys[] = {"max_size", "type"};
  const char *values[] = {"15", "int32"};
  const CacheTestParams &first = Get(Raw(keys, values, 2));
  const CacheTestParams &second = Get(Raw(keys, values, 2));
  EXPECT_EQ(&first, &second);
  EXPECT_EQ(parses_, 1);
}

TEST_F(TypeParamsCacheRawTest, UnsortedAndMapInputReachTheSameEntry) {
  const char *keys[] = {"max_size", "type"};
  const char *values[] = {"15", "int32"};
  const char *keys_unsorted[] = {"type", "max_size"};
  const char *values_unsorted[] = {"int32", "15"};
  const CacheTestParams &sorted = Get(Raw(keys, values, 2));
  const CacheTestParams &unsorted = Get(Raw(keys_unsorted, values_unsorted, 2));
  const std::map<std::string, std::string> as_map = {{"type", "int32"},
                                                     {"max_size", "15"}};
  const CacheTestParams &from_map =
      cache_.get(as_map, [this](const std::map<std::string, std::string> &) {
        return CacheTestParams{++parses_};
      });
  EXPECT_EQ(&sorted, &unsorted);
  EXPECT_EQ(&sorted, &from_map);
  EXPECT_EQ(parses_, 1);
}

TEST_F(TypeParamsCacheRawTest, NearMissParamsStayDistinct) {
  const char *keys[] = {"max_size", "type"};
  const char *values[] = {"15", "int32"};
  const char *values_longer[] = {"150", "int32"};
  const char *one_key[] = {"max_size"};
  const char *one_value[] = {"15"};
  const CacheTestParams &base = Get(Raw(keys, values, 2));
  const CacheTestParams &longer = Get(Raw(keys, values_longer, 2));
  const CacheTestParams &fewer = Get(Raw(one_key, one_value, 1));
  const CacheTestParams &none = Get(vef_type_params_t{});
  EXPECT_NE(&base, &longer);
  EXPECT_NE(&base, &fewer);
  EXPECT_NE(&fewer, &none);
  EXPECT_EQ(parses_, 4);
  EXPECT_EQ(&none, &Get(vef_type_params_t{}));
  EXPECT_EQ(parses_, 4);
}

}  // namespace villagesql_unittest
