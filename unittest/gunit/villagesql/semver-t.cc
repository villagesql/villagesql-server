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

#include "villagesql/include/semver.h"

namespace villagesql_unittest {

using namespace villagesql;

class SemverTest : public ::testing::Test {};

// Local helper to construct a Semver from components and return it.  Tests do a
// lot of this, otherwise its uses are mostly one-offs. The result is invalid
// (is_valid() == false) if any component fails validation.
Semver from_components(
    unsigned long major, unsigned long minor, unsigned long patch,
    const std::string_view code_base = Semver::kMysql84CodeBase,
    const std::vector<std::string> &prerelease = {},
    const std::vector<std::string> &build_metadata = {}) {
  Semver v;
  v.from_components(major, minor, patch, code_base, prerelease, build_metadata);
  return v;
}

// Test basic parsing of valid semver strings
TEST_F(SemverTest, ParseValidVersions) {
  Semver v1;
  ASSERT_TRUE(v1.parse("mysql-8.4_1.2.3"));
  EXPECT_TRUE(v1.is_valid());
  EXPECT_EQ("mysql-8.4", v1.code_base());
  EXPECT_EQ(1u, v1.major());
  EXPECT_EQ(2u, v1.minor());
  EXPECT_EQ(3u, v1.patch());
  EXPECT_FALSE(v1.has_prerelease());
  EXPECT_FALSE(v1.has_build_metadata());

  Semver v2;
  ASSERT_TRUE(v2.parse("mysql-8.4_0.0.0"));
  EXPECT_TRUE(v2.is_valid());
  EXPECT_EQ(0u, v2.major());
  EXPECT_EQ(0u, v2.minor());
  EXPECT_EQ(0u, v2.patch());

  Semver v3;
  ASSERT_TRUE(v3.parse("percona-9.7_10.20.30"));
  EXPECT_EQ("percona-9.7", v3.code_base());
  EXPECT_EQ(10u, v3.major());
  EXPECT_EQ(20u, v3.minor());
  EXPECT_EQ(30u, v3.patch());
}

// Test parsing with pre-release versions
TEST_F(SemverTest, ParsePrerelease) {
  Semver v1;
  std::string err;
  EXPECT_TRUE(v1.parse("mysql-8.4_1.0.0-alpha", &err));
  EXPECT_EQ(err, "");
  EXPECT_TRUE(v1.is_valid());
  EXPECT_TRUE(v1.has_prerelease());
  EXPECT_FALSE(v1.has_build_metadata());
  EXPECT_EQ(1u, v1.prerelease().size());
  EXPECT_EQ("alpha", v1.prerelease()[0]);

  Semver v2;
  ASSERT_TRUE(v2.parse("mysql-8.4_1.0.0-alpha.1"));
  EXPECT_EQ(2u, v2.prerelease().size());
  EXPECT_EQ("alpha", v2.prerelease()[0]);
  EXPECT_EQ("1", v2.prerelease()[1]);

  Semver v3;
  ASSERT_TRUE(v3.parse("mysql-8.4_1.0.0-0.3.7"));
  EXPECT_EQ(3u, v3.prerelease().size());
  EXPECT_EQ("0", v3.prerelease()[0]);
  EXPECT_EQ("3", v3.prerelease()[1]);
  EXPECT_EQ("7", v3.prerelease()[2]);

  Semver v4;
  ASSERT_TRUE(v4.parse("mysql-8.4_1.0.0-x.7.z.92"));
  EXPECT_EQ(4u, v4.prerelease().size());
  EXPECT_EQ("x", v4.prerelease()[0]);
  EXPECT_EQ("7", v4.prerelease()[1]);
  EXPECT_EQ("z", v4.prerelease()[2]);
  EXPECT_EQ("92", v4.prerelease()[3]);

  Semver v5;
  ASSERT_TRUE(v5.parse("mysql-8.4_1.0.0-x-y-z.--"));
  EXPECT_EQ(2u, v5.prerelease().size());
  EXPECT_EQ("x-y-z", v5.prerelease()[0]);
  EXPECT_EQ("--", v5.prerelease()[1]);
}

// Test parsing with build metadata
TEST_F(SemverTest, ParseBuildMetadata) {
  Semver v1;
  ASSERT_TRUE(v1.parse("mysql-8.4_1.0.0+20130313144700"));
  EXPECT_TRUE(v1.is_valid());
  EXPECT_FALSE(v1.has_prerelease());
  EXPECT_TRUE(v1.has_build_metadata());
  EXPECT_EQ(1u, v1.build_metadata().size());
  EXPECT_EQ("20130313144700", v1.build_metadata()[0]);

  Semver v2;
  ASSERT_TRUE(v2.parse("mysql-8.4_1.0.0+exp.sha.5114f85"));
  EXPECT_EQ(3u, v2.build_metadata().size());
  EXPECT_EQ("exp", v2.build_metadata()[0]);
  EXPECT_EQ("sha", v2.build_metadata()[1]);
  EXPECT_EQ("5114f85", v2.build_metadata()[2]);

  Semver v3;
  ASSERT_TRUE(v3.parse("mysql-8.4_1.0.0-beta+exp.sha.5114f85"));
  EXPECT_TRUE(v3.has_prerelease());
  EXPECT_THAT(v3.prerelease(), ::testing::ElementsAre("beta"));
  EXPECT_TRUE(v3.has_build_metadata());
  EXPECT_THAT(v3.build_metadata(),
              ::testing::ElementsAre("exp", "sha", "5114f85"));
}

// Test string generation
TEST_F(SemverTest, ToString) {
  Semver v1;
  v1.parse("mysql-8.4_1.2.3");
  EXPECT_EQ("mysql-8.4_1.2.3", v1.to_string());

  Semver v2;
  v2.parse("mysql-8.4_1.0.0-alpha");
  EXPECT_EQ("mysql-8.4_1.0.0-alpha", v2.to_string());

  Semver v3;
  v3.parse("mysql-8.4_1.0.0-alpha.1");
  EXPECT_EQ("mysql-8.4_1.0.0-alpha.1", v3.to_string());

  Semver v4;
  v4.parse("mysql-8.4_1.0.0+20130313144700");
  EXPECT_EQ("mysql-8.4_1.0.0+20130313144700", v4.to_string());

  Semver v5;
  v5.parse("mysql-8.4_1.0.0-beta+exp.sha.5114f85");
  EXPECT_EQ("mysql-8.4_1.0.0-beta+exp.sha.5114f85", v5.to_string());
}

// Test invalid version strings
TEST_F(SemverTest, ParseInvalidVersions) {
  std::string error;
  Semver v;

  // Empty string
  EXPECT_FALSE(v.parse("", &error));
  EXPECT_FALSE(error.empty());
  EXPECT_FALSE(v.is_valid());

  // Missing code base prefix
  error.clear();
  EXPECT_FALSE(v.parse("1.2.3", &error));
  EXPECT_FALSE(error.empty());

  // Blank code base prefix
  error.clear();
  EXPECT_FALSE(v.parse("_1.2.3", &error));
  EXPECT_FALSE(error.empty());

  // Missing components
  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.2", &error));
  EXPECT_FALSE(error.empty());

  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1", &error));
  EXPECT_FALSE(error.empty());

  // Too many dots in core version
  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.2.3.4", &error));
  EXPECT_FALSE(error.empty());

  // Non-numeric version numbers
  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_a.b.c", &error));
  EXPECT_FALSE(error.empty());

  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.2.c", &error));
  EXPECT_FALSE(error.empty());

  // Leading zeros in version numbers
  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_01.2.3", &error));
  EXPECT_FALSE(error.empty());

  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.02.3", &error));
  EXPECT_FALSE(error.empty());

  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.2.03", &error));
  EXPECT_FALSE(error.empty());

  // Version numbers exceeding their per-position bounds
  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_11.0.0", &error));
  EXPECT_FALSE(error.empty());

  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_0.101.0", &error));
  EXPECT_FALSE(error.empty());

  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_0.0.1001", &error));
  EXPECT_FALSE(error.empty());

  // Leading zeros in numeric pre-release identifiers
  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.0.0-01", &error));
  EXPECT_FALSE(error.empty());

  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.0.0-alpha.01", &error));
  EXPECT_FALSE(error.empty());

  // Empty pre-release
  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.0.0-", &error));
  EXPECT_FALSE(error.empty());

  // Empty build metadata
  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.0.0+", &error));
  EXPECT_FALSE(error.empty());
}

// Test equality comparison
TEST_F(SemverTest, EqualityComparison) {
  Semver v1, v2;
  v1.parse("mysql-8.4_1.2.3");
  v2.parse("mysql-8.4_1.2.3");
  EXPECT_TRUE(v1 == v2);
  EXPECT_FALSE(v1 != v2);

  v1.parse("mysql-8.4_1.2.3");
  v2.parse("mysql-8.4_1.2.4");
  EXPECT_FALSE(v1 == v2);
  EXPECT_TRUE(v1 != v2);

  // Build metadata should be ignored for equality
  v1.parse("mysql-8.4_1.2.3+build1");
  v2.parse("mysql-8.4_1.2.3+build2");
  EXPECT_TRUE(v1 == v2);

  // Pre-release must match
  v1.parse("mysql-8.4_1.2.3-alpha");
  v2.parse("mysql-8.4_1.2.3-beta");
  EXPECT_FALSE(v1 == v2);

  v1.parse("mysql-8.4_1.2.3-alpha");
  v2.parse("mysql-8.4_1.2.3-alpha");
  EXPECT_TRUE(v1 == v2);
}

// Test less-than comparison
TEST_F(SemverTest, LessThanComparison) {
  Semver v1, v2;

  // Major version comparison
  v1.parse("mysql-8.4_1.0.0");
  v2.parse("mysql-8.4_2.0.0");
  EXPECT_TRUE(v1 < v2);
  EXPECT_FALSE(v2 < v1);

  // Minor version comparison
  v1.parse("mysql-8.4_1.1.0");
  v2.parse("mysql-8.4_1.2.0");
  EXPECT_TRUE(v1 < v2);
  EXPECT_FALSE(v2 < v1);

  // Patch version comparison
  v1.parse("mysql-8.4_1.0.1");
  v2.parse("mysql-8.4_1.0.2");
  EXPECT_TRUE(v1 < v2);
  EXPECT_FALSE(v2 < v1);

  // Pre-release version has lower precedence than normal version
  v1.parse("mysql-8.4_1.0.0-alpha");
  v2.parse("mysql-8.4_1.0.0");
  EXPECT_TRUE(v1 < v2);
  EXPECT_FALSE(v2 < v1);

  // Comparing pre-release versions
  v1.parse("mysql-8.4_1.0.0-alpha");
  v2.parse("mysql-8.4_1.0.0-alpha.1");
  EXPECT_TRUE(v1 < v2);

  v1.parse("mysql-8.4_1.0.0-alpha.1");
  v2.parse("mysql-8.4_1.0.0-alpha.beta");
  EXPECT_TRUE(v1 < v2);

  v1.parse("mysql-8.4_1.0.0-alpha.beta");
  v2.parse("mysql-8.4_1.0.0-beta");
  EXPECT_TRUE(v1 < v2);

  v1.parse("mysql-8.4_1.0.0-beta");
  v2.parse("mysql-8.4_1.0.0-beta.2");
  EXPECT_TRUE(v1 < v2);

  v1.parse("mysql-8.4_1.0.0-beta.2");
  v2.parse("mysql-8.4_1.0.0-beta.11");
  EXPECT_TRUE(v1 < v2);

  v1.parse("mysql-8.4_1.0.0-beta.11");
  v2.parse("mysql-8.4_1.0.0-rc.1");
  EXPECT_TRUE(v1 < v2);

  v1.parse("mysql-8.4_1.0.0-rc.1");
  v2.parse("mysql-8.4_1.0.0");
  EXPECT_TRUE(v1 < v2);

  // Build metadata should be ignored
  v1.parse("mysql-8.4_1.0.0+build1");
  v2.parse("mysql-8.4_1.0.0+build2");
  EXPECT_FALSE(v1 < v2);
  EXPECT_FALSE(v2 < v1);
}

// Test other comparison operators
TEST_F(SemverTest, OtherComparisonOperators) {
  Semver v1, v2;

  v1.parse("mysql-8.4_1.2.3");
  v2.parse("mysql-8.4_1.2.4");

  // Less than or equal
  EXPECT_TRUE(v1 <= v2);
  EXPECT_FALSE(v2 <= v1);
  v2.parse("mysql-8.4_1.2.3");
  EXPECT_TRUE(v1 <= v2);
  EXPECT_TRUE(v2 <= v1);

  // Greater than
  v1.parse("mysql-8.4_2.0.0");
  v2.parse("mysql-8.4_1.0.0");
  EXPECT_TRUE(v1 > v2);
  EXPECT_FALSE(v2 > v1);

  // Greater than or equal
  EXPECT_TRUE(v1 >= v2);
  EXPECT_FALSE(v2 >= v1);
  v2.parse("mysql-8.4_2.0.0");
  EXPECT_TRUE(v1 >= v2);
  EXPECT_TRUE(v2 >= v1);
}

// Test precedence examples from semver.org spec
TEST_F(SemverTest, SemverSpecPrecedenceExamples) {
  // From semver.org section 11:
  // 1.0.0-alpha < 1.0.0-alpha.1 < 1.0.0-alpha.beta < 1.0.0-beta <
  // 1.0.0-beta.2 < 1.0.0-beta.11 < 1.0.0-rc.1 < 1.0.0

  Semver v1, v2, v3, v4, v5, v6, v7, v8;
  v1.parse("mysql-8.4_1.0.0-alpha");
  v2.parse("mysql-8.4_1.0.0-alpha.1");
  v3.parse("mysql-8.4_1.0.0-alpha.beta");
  v4.parse("mysql-8.4_1.0.0-beta");
  v5.parse("mysql-8.4_1.0.0-beta.2");
  v6.parse("mysql-8.4_1.0.0-beta.11");
  v7.parse("mysql-8.4_1.0.0-rc.1");
  v8.parse("mysql-8.4_1.0.0");

  EXPECT_TRUE(v1 < v2);
  EXPECT_TRUE(v2 < v3);
  EXPECT_TRUE(v3 < v4);
  EXPECT_TRUE(v4 < v5);
  EXPECT_TRUE(v5 < v6);
  EXPECT_TRUE(v6 < v7);
  EXPECT_TRUE(v7 < v8);

  // Verify transitivity
  EXPECT_TRUE(v1 < v8);
  EXPECT_TRUE(v1 < v4);
}

// Test numeric vs alphanumeric pre-release comparison
TEST_F(SemverTest, NumericVsAlphanumericPrerelease) {
  Semver v1, v2;

  // Numeric identifiers should be compared numerically
  v1.parse("mysql-8.4_1.0.0-1");
  v2.parse("mysql-8.4_1.0.0-2");
  EXPECT_TRUE(v1 < v2);

  v1.parse("mysql-8.4_1.0.0-2");
  v2.parse("mysql-8.4_1.0.0-10");
  EXPECT_TRUE(v1 < v2);  // Not lexical comparison

  // Numeric < alphanumeric
  v1.parse("mysql-8.4_1.0.0-1");
  v2.parse("mysql-8.4_1.0.0-alpha");
  EXPECT_TRUE(v1 < v2);

  v1.parse("mysql-8.4_1.0.0-99");
  v2.parse("mysql-8.4_1.0.0-a");
  EXPECT_TRUE(v1 < v2);

  // Alphanumeric comparison is lexical
  v1.parse("mysql-8.4_1.0.0-alpha");
  v2.parse("mysql-8.4_1.0.0-beta");
  EXPECT_TRUE(v1 < v2);

  v1.parse("mysql-8.4_1.0.0-abc");
  v2.parse("mysql-8.4_1.0.0-abd");
  EXPECT_TRUE(v1 < v2);
}

// Test edge cases
TEST_F(SemverTest, EdgeCases) {
  Semver v;

  // Maximum allowed version numbers (bounds are inclusive)
  EXPECT_TRUE(v.parse("mysql-8.4_10.100.1000"));
  EXPECT_EQ(10u, v.major());
  EXPECT_EQ(100u, v.minor());
  EXPECT_EQ(1000u, v.patch());

  // Single character identifiers
  EXPECT_TRUE(v.parse("mysql-8.4_1.0.0-a"));
  EXPECT_TRUE(v.parse("mysql-8.4_1.0.0+b"));

  // Hyphens in identifiers
  EXPECT_TRUE(v.parse("mysql-8.4_1.0.0-alpha-beta"));
  EXPECT_THAT(v.prerelease(), ::testing::ElementsAre("alpha-beta"));
  EXPECT_TRUE(v.parse("mysql-8.4_1.0.0+build-123"));
  EXPECT_THAT(v.build_metadata(), ::testing::ElementsAre("build-123"));

  // Mixed identifiers
  EXPECT_TRUE(v.parse("mysql-8.4_1.0.0-0.alpha.1.beta.2"));
  EXPECT_EQ(5u, v.prerelease().size());
}

// Test default constructor
TEST_F(SemverTest, DefaultConstructor) {
  Semver v;
  EXPECT_FALSE(v.is_valid());
  EXPECT_EQ(0u, v.major());
  EXPECT_EQ(0u, v.minor());
  EXPECT_EQ(0u, v.patch());
  EXPECT_FALSE(v.has_prerelease());
  EXPECT_FALSE(v.has_build_metadata());
  EXPECT_EQ("", v.to_string());
}

// Test reusing a Semver object
TEST_F(SemverTest, ReuseObject) {
  Semver v;

  EXPECT_TRUE(v.parse("mysql-8.4_1.0.0-alpha+build"));
  EXPECT_TRUE(v.is_valid());
  EXPECT_TRUE(v.has_prerelease());
  EXPECT_TRUE(v.has_build_metadata());

  // Parse a different version
  EXPECT_TRUE(v.parse("mysql-8.4_2.0.0"));
  EXPECT_TRUE(v.is_valid());
  EXPECT_EQ(2u, v.major());
  EXPECT_EQ(0u, v.minor());
  EXPECT_EQ(0u, v.patch());
  EXPECT_FALSE(v.has_prerelease());
  EXPECT_FALSE(v.has_build_metadata());

  // Parse invalid version
  EXPECT_FALSE(v.parse("invalid"));
  EXPECT_FALSE(v.is_valid());
}

// Test from_components factory method. A code base is mandatory, so every
// valid version carries one.
TEST_F(SemverTest, FromComponents) {
  // Basic version without pre-release or build metadata
  Semver v1 = from_components(1, 2, 3);
  EXPECT_TRUE(v1.is_valid());
  EXPECT_EQ(1u, v1.major());
  EXPECT_EQ(2u, v1.minor());
  EXPECT_EQ(3u, v1.patch());
  EXPECT_EQ("mysql-8.4", v1.code_base());
  EXPECT_FALSE(v1.has_prerelease());
  EXPECT_FALSE(v1.has_build_metadata());
  EXPECT_EQ("mysql-8.4_1.2.3", v1.to_string());

  // Version with pre-release
  Semver v2 = from_components(1, 0, 0, Semver::kMysql84CodeBase, {"alpha"});
  EXPECT_TRUE(v2.is_valid());
  EXPECT_EQ(1u, v2.prerelease().size());
  EXPECT_EQ("alpha", v2.prerelease()[0]);
  EXPECT_EQ("mysql-8.4_1.0.0-alpha", v2.to_string());

  // Version with pre-release and build metadata
  Semver v3 = from_components(2, 1, 0, Semver::kMysql84CodeBase, {"beta", "1"},
                              {"build", "123"});
  EXPECT_TRUE(v3.is_valid());
  EXPECT_TRUE(v3.has_prerelease());
  EXPECT_TRUE(v3.has_build_metadata());
  EXPECT_EQ(2u, v3.prerelease().size());
  EXPECT_EQ("beta", v3.prerelease()[0]);
  EXPECT_EQ("1", v3.prerelease()[1]);
  EXPECT_EQ(2u, v3.build_metadata().size());
  EXPECT_EQ("build", v3.build_metadata()[0]);
  EXPECT_EQ("123", v3.build_metadata()[1]);
  EXPECT_EQ("mysql-8.4_2.1.0-beta.1+build.123", v3.to_string());

  // Version with only build metadata
  Semver v4 =
      from_components(3, 0, 0, Semver::kMysql84CodeBase, {}, {"sha", "abc123"});
  EXPECT_TRUE(v4.is_valid());
  EXPECT_FALSE(v4.has_prerelease());
  EXPECT_TRUE(v4.has_build_metadata());
  EXPECT_EQ("mysql-8.4_3.0.0+sha.abc123", v4.to_string());

  // Zero version
  Semver v5 = from_components(0, 0, 0);
  EXPECT_TRUE(v5.is_valid());
  EXPECT_EQ("mysql-8.4_0.0.0", v5.to_string());
}

// Test from_components with invalid identifiers
TEST_F(SemverTest, FromComponentsInvalid) {
  // Invalid character in pre-release
  Semver v1 =
      from_components(1, 0, 0, Semver::kMysql84CodeBase, {"alpha@beta"});
  EXPECT_FALSE(v1.is_valid());

  // Leading zero in numeric pre-release identifier
  Semver v2 = from_components(1, 0, 0, Semver::kMysql84CodeBase, {"01"});
  EXPECT_FALSE(v2.is_valid());

  // Empty identifier in pre-release
  Semver v3 = from_components(1, 0, 0, Semver::kMysql84CodeBase, {""});
  EXPECT_FALSE(v3.is_valid());

  // Invalid character in build metadata
  Semver v4 =
      from_components(1, 0, 0, Semver::kMysql84CodeBase, {}, {"build$123"});
  EXPECT_FALSE(v4.is_valid());

  // Empty identifier in build metadata
  Semver v5 = from_components(1, 0, 0, Semver::kMysql84CodeBase, {}, {""});
  EXPECT_FALSE(v5.is_valid());

  // Valid numeric identifier with leading zero is OK if it's actually "0"
  Semver v6 =
      from_components(1, 0, 0, Semver::kMysql84CodeBase, {"0", "alpha"});
  EXPECT_TRUE(v6.is_valid());
  EXPECT_EQ("mysql-8.4_1.0.0-0.alpha", v6.to_string());

  // Invalid character in code base
  Semver v7 = from_components(1, 0, 0, "mysql@8.4");
  EXPECT_FALSE(v7.is_valid());

  // Code base is mandatory: a blank code base is invalid
  Semver v8 = from_components(1, 0, 0, "");
  EXPECT_FALSE(v8.is_valid());
}

// Test comparison with versions created from components
TEST_F(SemverTest, FromComponentsComparison) {
  Semver v1 = from_components(1, 2, 3);
  Semver v2 = from_components(1, 2, 3);
  Semver v3 = from_components(1, 2, 4);
  Semver v4 = from_components(1, 2, 3, Semver::kMysql84CodeBase, {"alpha"});

  EXPECT_TRUE(v1 == v2);
  EXPECT_TRUE(v1 < v3);
  EXPECT_TRUE(v4 < v1);  // Pre-release < normal version
  EXPECT_FALSE(v1 < v2);
}

// Test that from_components and parse produce equivalent results
TEST_F(SemverTest, FromComponentsEquivalentToParse) {
  Semver v1 = from_components(1, 2, 3, Semver::kMysql84CodeBase, {"alpha", "1"},
                              {"build", "123"});
  Semver v2;
  v2.parse("mysql-8.4_1.2.3-alpha.1+build.123");

  EXPECT_TRUE(v1.is_valid());
  EXPECT_TRUE(v2.is_valid());
  EXPECT_TRUE(v1 == v2);
  EXPECT_EQ(v1.to_string(), v2.to_string());
}

// Test parsing and round-tripping of the mandatory code base prefix
TEST_F(SemverTest, CodeBaseParsing) {
  // Full form: code base + core + pre-release
  Semver v1;
  v1.parse("mysql-8.4_0.0.5-dev");
  EXPECT_TRUE(v1.is_valid());
  EXPECT_EQ("mysql-8.4", v1.code_base());
  EXPECT_EQ(0u, v1.major());
  EXPECT_EQ(0u, v1.minor());
  EXPECT_EQ(5u, v1.patch());
  ASSERT_EQ(1u, v1.prerelease().size());
  EXPECT_EQ("dev", v1.prerelease()[0]);
  EXPECT_EQ("mysql-8.4_0.0.5-dev", v1.to_string());

  // Code base + core + pre-release + build metadata
  Semver v2;
  v2.parse("percona-9.7_1.0.0-beta+exp.sha.5114f85");
  EXPECT_TRUE(v2.is_valid());
  EXPECT_EQ("percona-9.7", v2.code_base());
  EXPECT_EQ("percona-9.7_1.0.0-beta+exp.sha.5114f85", v2.to_string());

  // A string without a code base prefix is invalid
  std::string error;
  Semver v3;
  v3.parse("0.0.5-dev", &error);
  EXPECT_FALSE(v3.is_valid());
  EXPECT_FALSE(error.empty());

  // from_components round-trips through parse
  Semver v4;
  v4.from_components(0, 0, 5, Semver::kMysql84CodeBase, {"dev"});
  Semver v5;
  v5.parse(v4.to_string());
  EXPECT_EQ(v4, v5);
}

// Test that code base participates in equality but invalidates ordering
TEST_F(SemverTest, CodeBaseComparison) {
  Semver mysql_a = from_components(1, 0, 0, Semver::kMysql84CodeBase);
  Semver mysql_b = from_components(1, 0, 0, Semver::kMysql84CodeBase);
  Semver mysql_newer = from_components(1, 0, 1, Semver::kMysql84CodeBase);
  Semver percona = from_components(1, 0, 0, Semver::kPercona97CodeBase);

  // Equality requires matching code base
  EXPECT_TRUE(mysql_a == mysql_b);
  EXPECT_FALSE(mysql_a == percona);
  EXPECT_TRUE(mysql_a != percona);

  // Ordering is valid within the same code base
  EXPECT_TRUE(mysql_a < mysql_newer);
  EXPECT_TRUE(mysql_newer > mysql_a);

  // Differing code bases are unordered: every relational operator is false
  EXPECT_FALSE(mysql_a < percona);
  EXPECT_FALSE(mysql_a > percona);
  EXPECT_FALSE(mysql_a <= percona);
  EXPECT_FALSE(mysql_a >= percona);
}

// Test parsing versions read from persistent storage, including the historical
// (pre-code-base) format written by older builds.
TEST_F(SemverTest, ParseStoredVersion) {
  // Legacy stored version (no code base, begins with a digit) is parsed with
  // the historical layout and assigned the legacy code base.
  Semver v1;
  ASSERT_TRUE(v1.parse_schema_version("0.0.5-dev"));
  EXPECT_TRUE(v1.is_valid());
  EXPECT_EQ("mysql-8.4", v1.code_base());
  EXPECT_EQ(0u, v1.major());
  EXPECT_EQ(0u, v1.minor());
  EXPECT_EQ(5u, v1.patch());
  EXPECT_EQ("mysql-8.4_0.0.5-dev", v1.to_string());

  // Legacy plain version
  Semver v2;
  ASSERT_TRUE(v2.parse_schema_version("0.0.3"));
  EXPECT_EQ("mysql-8.4", v2.code_base());
  EXPECT_EQ("mysql-8.4_0.0.3", v2.to_string());

  // Current format with an explicit code base is parsed as-is (not overridden)
  Semver v3;
  ASSERT_TRUE(v3.parse_schema_version("percona-9.7_1.0.0"));
  EXPECT_EQ("percona-9.7", v3.code_base());
  EXPECT_EQ("percona-9.7_1.0.0", v3.to_string());

  Semver v4;
  ASSERT_TRUE(v4.parse_schema_version("0.0.5-dev"));
  EXPECT_EQ("mysql-8.4", v4.code_base());
  EXPECT_EQ("mysql-8.4_0.0.5-dev", v4.to_string());

  // Garbage is still rejected
  Semver v5;
  std::string error;
  EXPECT_FALSE(v5.parse_schema_version("invalid", &error));
  EXPECT_FALSE(error.empty());
}

}  // namespace villagesql_unittest
