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

#include "villagesql/include/code_base_version.h"
#include "villagesql/include/semver.h"

namespace villagesql_unittest {

using namespace villagesql;

class CodeBaseVersionTest : public ::testing::Test {};

// Local helper for the two-step build a version needs: make the Semver, then
// pair it with a code base. The result is invalid (is_valid() == false) if any
// component, including the code base, fails validation.
CodeBaseVersion make_version(
    std::string_view code_base, unsigned long major, unsigned long minor,
    unsigned long patch, const std::vector<std::string> &prerelease = {},
    const std::vector<std::string> &build_metadata = {}) {
  Semver semver;
  semver.from_components(major, minor, patch, prerelease, build_metadata);

  CodeBaseVersion v;
  v.from_semver(code_base, semver);
  return v;
}

// Test basic parsing of valid version strings
TEST_F(CodeBaseVersionTest, ParseValidVersions) {
  CodeBaseVersion v1;
  ASSERT_TRUE(v1.parse("mysql-8.4_1.2.3"));
  EXPECT_TRUE(v1.is_valid());
  EXPECT_EQ("mysql-8.4", v1.code_base());
  EXPECT_EQ(1u, v1.semver().major());
  EXPECT_EQ(2u, v1.semver().minor());
  EXPECT_EQ(3u, v1.semver().patch());
  EXPECT_FALSE(v1.semver().has_prerelease());
  EXPECT_FALSE(v1.semver().has_build_metadata());

  CodeBaseVersion v2;
  ASSERT_TRUE(v2.parse("percona-9.7_10.20.30"));
  EXPECT_EQ("percona-9.7", v2.code_base());
  EXPECT_EQ(10u, v2.semver().major());
  EXPECT_EQ(20u, v2.semver().minor());
  EXPECT_EQ(30u, v2.semver().patch());
}

// Test default constructor
TEST_F(CodeBaseVersionTest, DefaultConstructor) {
  CodeBaseVersion v;
  EXPECT_FALSE(v.is_valid());
  EXPECT_TRUE(v.code_base().empty());
  EXPECT_FALSE(v.semver().is_valid());
  EXPECT_EQ("", v.to_string());
}

// Test parsing and round-tripping of the mandatory code base prefix
TEST_F(CodeBaseVersionTest, CodeBaseParsing) {
  // Full form: code base + core + pre-release
  CodeBaseVersion v1;
  ASSERT_TRUE(v1.parse("mysql-8.4_0.0.5-dev"));
  EXPECT_TRUE(v1.is_valid());
  EXPECT_EQ("mysql-8.4", v1.code_base());
  EXPECT_EQ(0u, v1.semver().major());
  EXPECT_EQ(0u, v1.semver().minor());
  EXPECT_EQ(5u, v1.semver().patch());
  ASSERT_EQ(1u, v1.semver().prerelease().size());
  EXPECT_EQ("dev", v1.semver().prerelease()[0]);
  EXPECT_EQ("mysql-8.4_0.0.5-dev", v1.to_string());

  // Code base + core + pre-release + build metadata
  CodeBaseVersion v2;
  ASSERT_TRUE(v2.parse("percona-9.7_1.0.0-beta+exp.sha.5114f85"));
  EXPECT_TRUE(v2.is_valid());
  EXPECT_EQ("percona-9.7", v2.code_base());
  EXPECT_EQ("percona-9.7_1.0.0-beta+exp.sha.5114f85", v2.to_string());

  // A version built from its parts round-trips through parse
  const CodeBaseVersion v3 =
      make_version(CodeBaseVersion::kMysql84CodeBase, 0, 0, 5, {"dev"});
  ASSERT_TRUE(v3.is_valid());
  CodeBaseVersion v4;
  ASSERT_TRUE(v4.parse(v3.to_string()));
  EXPECT_TRUE(v3.same_code_base(v4));
  EXPECT_EQ(v3.semver(), v4.semver());
  EXPECT_EQ(v3.to_string(), v4.to_string());
}

// Test invalid version strings. The semver body itself is covered by
// semver-t.cc; these cases are about the code base prefix and the separator.
TEST_F(CodeBaseVersionTest, ParseInvalidVersions) {
  std::string error;
  CodeBaseVersion v;

  // Empty string
  EXPECT_FALSE(v.parse("", &error));
  EXPECT_FALSE(error.empty());
  EXPECT_FALSE(v.is_valid());

  // Missing code base prefix
  error.clear();
  EXPECT_FALSE(v.parse("1.2.3", &error));
  EXPECT_EQ("Missing codebase prefix", error);

  // Blank code base prefix
  error.clear();
  EXPECT_FALSE(v.parse("_1.2.3", &error));
  EXPECT_FALSE(error.empty());

  // Unknown code base
  error.clear();
  EXPECT_FALSE(v.parse("maria-11.4_1.2.3", &error));
  EXPECT_THAT(error, ::testing::HasSubstr("Invalid codebase"));

  // An invalid semver body is still rejected, with the body's error message
  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_1.2.c", &error));
  EXPECT_THAT(error, ::testing::HasSubstr("must be numeric"));

  error.clear();
  EXPECT_FALSE(v.parse("mysql-8.4_01.2.3", &error));
  EXPECT_FALSE(error.empty());
}

// A code base prefix with nothing after the separator.
TEST_F(CodeBaseVersionTest, ParseRejectsEmptyVersionBody) {
  CodeBaseVersion v;
  std::string error;
  EXPECT_FALSE(v.parse("mysql-8.4_", &error));
  EXPECT_EQ("Empty version string", error);
  EXPECT_FALSE(v.is_valid());
}

// An error message naming an unknown code base escapes anything unprintable in
// it: these strings reach the error log and client-visible errors.
TEST_F(CodeBaseVersionTest, ErrorMessagesEscapeUnprintableCodeBase) {
  CodeBaseVersion v;
  std::string error;
  EXPECT_FALSE(v.parse("mysql\t8.4_1.2.3", &error));
  EXPECT_THAT(error, ::testing::HasSubstr("Invalid codebase"));
  EXPECT_THAT(error, ::testing::HasSubstr("\\t"));
  EXPECT_EQ(std::string::npos, error.find('\t'));
}

// Every known code base maps to its canonical static spelling.
TEST_F(CodeBaseVersionTest, ParseAcceptsAllKnownCodeBases) {
  const std::string_view code_bases[] = {
      CodeBaseVersion::kMysql84CodeBase, CodeBaseVersion::kMysql97CodeBase,
      CodeBaseVersion::kPercona84CodeBase, CodeBaseVersion::kPercona97CodeBase};

  for (const std::string_view cb : code_bases) {
    CodeBaseVersion v;
    const std::string version = std::string(cb) + "_1.2.3";
    std::string error;
    ASSERT_TRUE(v.parse(version, &error)) << version << ": " << error;
    EXPECT_EQ(cb, v.code_base());
    EXPECT_EQ(version, v.to_string());
  }
}

// The code base and the semver are rendered back as one string, across the
// combinations of pre-release and build metadata.
TEST_F(CodeBaseVersionTest, ToString) {
  const CodeBaseVersion v1 =
      make_version(CodeBaseVersion::kMysql84CodeBase, 1, 2, 3);
  EXPECT_TRUE(v1.is_valid());
  EXPECT_EQ("mysql-8.4", v1.code_base());
  EXPECT_EQ(1u, v1.semver().major());
  EXPECT_EQ("mysql-8.4_1.2.3", v1.to_string());

  const CodeBaseVersion v2 =
      make_version(CodeBaseVersion::kMysql84CodeBase, 1, 0, 0, {"alpha"});
  EXPECT_EQ("mysql-8.4_1.0.0-alpha", v2.to_string());

  const CodeBaseVersion v3 =
      make_version(CodeBaseVersion::kPercona97CodeBase, 2, 1, 0, {"beta", "1"},
                   {"build", "123"});
  EXPECT_EQ("percona-9.7_2.1.0-beta.1+build.123", v3.to_string());
}

// from_semver is the only way to build a version from parts: it pairs a code
// base with an already-built Semver and validates both.
TEST_F(CodeBaseVersionTest, FromSemver) {
  Semver version;
  ASSERT_TRUE(version.from_components(1, 2, 3, {"dev"}));

  CodeBaseVersion v1;
  ASSERT_TRUE(v1.from_semver(CodeBaseVersion::kMysql84CodeBase, version));
  EXPECT_EQ("mysql-8.4_1.2.3-dev", v1.to_string());
  EXPECT_EQ(version, v1.semver());

  // An unknown (but well-formed) code base is rejected
  CodeBaseVersion v2;
  EXPECT_FALSE(v2.from_semver("maria-11.4", version));
  EXPECT_FALSE(v2.is_valid());

  // So is one with an invalid character, and a blank one: the code base is
  // mandatory, and only the known values are accepted.
  CodeBaseVersion v3;
  EXPECT_FALSE(v3.from_semver("mysql@8.4", version));
  EXPECT_FALSE(v3.is_valid());
  EXPECT_FALSE(v3.from_semver("", version));
  EXPECT_FALSE(v3.is_valid());

  // An invalid Semver is rejected even with a good code base, so a caller that
  // ignored Semver::from_components() failing still cannot build a bad version.
  CodeBaseVersion v4;
  EXPECT_FALSE(v4.from_semver(CodeBaseVersion::kMysql84CodeBase, Semver()));
  EXPECT_FALSE(v4.is_valid());

  // A failed call leaves the object unchanged
  CodeBaseVersion v5;
  ASSERT_TRUE(v5.parse("mysql-8.4_1.2.3"));
  EXPECT_FALSE(v5.from_semver("maria-11.4", version));
  EXPECT_EQ("mysql-8.4_1.2.3", v5.to_string());
}

// CodeBaseVersion is deliberately not comparable: callers pair
// same_code_base() with a comparison of the semver()s. Only the code base half
// is tested here -- semver precedence, including how invalid versions compare,
// belongs to semver-t.cc.
TEST_F(CodeBaseVersionTest, SameCodeBase) {
  const CodeBaseVersion mysql =
      make_version(CodeBaseVersion::kMysql84CodeBase, 1, 0, 0);
  const CodeBaseVersion mysql_newer =
      make_version(CodeBaseVersion::kMysql84CodeBase, 1, 0, 1);
  const CodeBaseVersion percona =
      make_version(CodeBaseVersion::kPercona97CodeBase, 1, 0, 0);

  EXPECT_TRUE(mysql.same_code_base(mysql_newer));
  EXPECT_FALSE(mysql.same_code_base(percona));

  // mysql and percona carry identical semvers, so the code base is the only
  // thing separating them: a caller that skipped this check would treat a
  // cross-code-base pair as the same version.
  EXPECT_EQ(mysql.semver(), percona.semver());

  // An invalid version has an empty code base. It matches no real code base,
  // but it does match another invalid version, so is_valid() has to be checked
  // before same_code_base() means anything.
  const CodeBaseVersion invalid;
  const CodeBaseVersion other_invalid;
  EXPECT_FALSE(invalid.is_valid());
  EXPECT_FALSE(invalid.same_code_base(mysql));
  EXPECT_TRUE(invalid.same_code_base(other_invalid));
}

// Test parsing versions read from persistent storage, including the historical
// (pre-code-base) format written by older builds.
TEST_F(CodeBaseVersionTest, ParseStoredVersion) {
  // Legacy stored version (no code base, begins with a digit) is parsed as a
  // bare semver and assigned the legacy code base.
  CodeBaseVersion v1;
  ASSERT_TRUE(v1.parse_schema_version("0.0.5-dev"));
  EXPECT_TRUE(v1.is_valid());
  EXPECT_EQ("mysql-8.4", v1.code_base());
  EXPECT_EQ(0u, v1.semver().major());
  EXPECT_EQ(0u, v1.semver().minor());
  EXPECT_EQ(5u, v1.semver().patch());
  EXPECT_EQ("mysql-8.4_0.0.5-dev", v1.to_string());

  // Legacy plain version
  CodeBaseVersion v2;
  ASSERT_TRUE(v2.parse_schema_version("0.0.3"));
  EXPECT_EQ("mysql-8.4", v2.code_base());
  EXPECT_EQ("mysql-8.4_0.0.3", v2.to_string());

  // Current format with an explicit code base is parsed as-is (not overridden)
  CodeBaseVersion v3;
  ASSERT_TRUE(v3.parse_schema_version("percona-9.7_1.0.0"));
  EXPECT_EQ("percona-9.7", v3.code_base());
  EXPECT_EQ("percona-9.7_1.0.0", v3.to_string());

  // Garbage is still rejected
  CodeBaseVersion v4;
  std::string error;
  EXPECT_FALSE(v4.parse_schema_version("invalid", &error));
  EXPECT_FALSE(error.empty());

  // A legacy-layout version (starts with a digit, so no code base prefix) that
  // fails body validation is rejected through the legacy branch.
  CodeBaseVersion v5;
  error.clear();
  EXPECT_FALSE(v5.parse_schema_version("1.2.x", &error));
  EXPECT_THAT(error, ::testing::HasSubstr("must be numeric"));

  // An empty stored value is rejected rather than silently treated as legacy.
  CodeBaseVersion v6;
  error.clear();
  EXPECT_FALSE(v6.parse_schema_version("", &error));
  EXPECT_FALSE(error.empty());
}

// A failed parse leaves the object invalid rather than keeping stale state.
TEST_F(CodeBaseVersionTest, ReuseObject) {
  CodeBaseVersion v;

  ASSERT_TRUE(v.parse("mysql-8.4_1.0.0-alpha+build"));
  EXPECT_TRUE(v.semver().has_prerelease());
  EXPECT_TRUE(v.semver().has_build_metadata());

  ASSERT_TRUE(v.parse("percona-8.4_2.0.0"));
  EXPECT_EQ("percona-8.4", v.code_base());
  EXPECT_EQ(2u, v.semver().major());
  EXPECT_FALSE(v.semver().has_prerelease());
  EXPECT_FALSE(v.semver().has_build_metadata());

  EXPECT_FALSE(v.parse("invalid"));
  EXPECT_FALSE(v.is_valid());
  EXPECT_EQ("", v.to_string());
}

}  // namespace villagesql_unittest
