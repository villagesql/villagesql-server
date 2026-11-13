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

// Tests for PendingAction, the deferred extension version update recorded in
// villagesql.extensions.pending_action.
//
// The JSON wire format is private to the class: ReadFromTable/StoreToTable are
// the only public way in and both need a TABLE. Serialize/Deserialize are
// reachable here through the friend declaration in extensions.h, following the
// same pattern VictionaryClient and TypeContext already use for their tests.
//
// Deserialize's rejection paths matter because the column holds free-form JSON
// that an operator can edit by hand: a malformed cell must produce a precise
// diagnostic rather than a half-populated action. The storage layer turns that
// diagnostic into a failed-apply record (see read_from_table in extensions.cc),
// so these messages reach the operator through PENDING_LAST_ERROR.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "villagesql/schema/systable/extensions.h"

namespace villagesql_unittest {

using villagesql::PendingAction;

class PendingActionTest : public ::testing::Test {
 protected:
  // "YYYY-MM-DDTHH:MM:SS.uuuuuuZ". Written with [0-9] rather than \d: gtest
  // matches with POSIX extended regular expressions here, which have no \d.
  static constexpr const char *kTimestampPattern =
      R"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z)";

  // Thin wrappers: the tests are the friend's members, so they reach the
  // private wire format through these.
  static std::string serialize(const PendingAction &a) { return a.Serialize(); }

  static bool deserialize(const std::string &raw, PendingAction &out,
                          std::string &error) {
    return PendingAction::Deserialize(raw, out, error);
  }
};

TEST_F(PendingActionTest, CreateVersionUpdatePopulatesFields) {
  const PendingAction a = PendingAction::CreateVersionUpdate("2.0.0", "abc123");

  EXPECT_EQ("2.0.0", a.target_version());
  EXPECT_EQ("abc123", a.target_veb_sha256());
  // Stamped from the server clock; only its format is predictable.
  EXPECT_THAT(a.requested_at(), ::testing::MatchesRegex(kTimestampPattern));

  // A freshly created action has not been attempted, so it carries no failure.
  EXPECT_FALSE(a.has_failure());
  EXPECT_EQ("", a.last_error());
  EXPECT_EQ("", a.last_error_at());
}

TEST_F(PendingActionTest, MarkFailedRecordsErrorAndTime) {
  PendingAction a = PendingAction::CreateVersionUpdate("2.0.0", "abc123");

  a.MarkFailed("target .so failed to load");

  EXPECT_TRUE(a.has_failure());
  EXPECT_EQ("target .so failed to load", a.last_error());
  EXPECT_THAT(a.last_error_at(), ::testing::MatchesRegex(kTimestampPattern));

  // The action still targets the same version: a failure annotates, it does
  // not clear the request.
  EXPECT_EQ("2.0.0", a.target_version());
  EXPECT_EQ("abc123", a.target_veb_sha256());

  // A later failure replaces the previous record rather than accumulating.
  a.MarkFailed("second attempt failed");
  EXPECT_EQ("second attempt failed", a.last_error());
}

TEST_F(PendingActionTest, RoundTripWithoutFailure) {
  const PendingAction original =
      PendingAction::CreateVersionUpdate("2.0.0", "deadbeef");

  PendingAction parsed;
  std::string error;
  ASSERT_FALSE(deserialize(serialize(original), parsed, error)) << error;
  EXPECT_EQ("", error);

  EXPECT_EQ(original.target_version(), parsed.target_version());
  EXPECT_EQ(original.target_veb_sha256(), parsed.target_veb_sha256());
  EXPECT_EQ(original.requested_at(), parsed.requested_at());
  EXPECT_FALSE(parsed.has_failure());
}

TEST_F(PendingActionTest, RoundTripWithFailure) {
  PendingAction original = PendingAction::CreateVersionUpdate("3.1.4", "cafe");
  original.MarkFailed("Pending action could not be read: boom");

  PendingAction parsed;
  std::string error;
  ASSERT_FALSE(deserialize(serialize(original), parsed, error)) << error;

  EXPECT_EQ("3.1.4", parsed.target_version());
  EXPECT_EQ("cafe", parsed.target_veb_sha256());
  EXPECT_TRUE(parsed.has_failure());
  EXPECT_EQ(original.last_error(), parsed.last_error());
  EXPECT_EQ(original.last_error_at(), parsed.last_error_at());
}

// An operator can edit this column by hand, so every malformed json must be
// rejected with a message naming what is wrong.
TEST_F(PendingActionTest, DeserializeRejectsMalformedInput) {
  struct {
    const char *raw;
    const char *expected_error;
  } cases[] = {
      {"not json at all", "JSON parse error"},
      {"", "JSON parse error"},
      {"[1, 2, 3]", "not an object"},
      {"\"a string\"", "not an object"},
      {R"({"target_veb_sha256": "abc"})",
       "missing string field 'target_version'"},
      {R"({"target_version": "2.0.0"})",
       "missing string field 'target_veb_sha256'"},
      // Present but of the wrong type is as bad as absent.
      {R"({"target_version": 7, "target_veb_sha256": "abc"})",
       "missing string field 'target_version'"},
      {R"({"target_version": "2.0.0", "target_veb_sha256": null})",
       "missing string field 'target_veb_sha256'"},
      // requested_at is required too: an action always records when it was
      // queued.
      {R"({"target_version": "2.0.0", "target_veb_sha256": "abc"})",
       "missing string field 'requested_at'"},
  };

  for (const auto &c : cases) {
    PendingAction out;
    std::string error;
    EXPECT_TRUE(deserialize(c.raw, out, error)) << "raw: " << c.raw;
    EXPECT_THAT(error, ::testing::HasSubstr(c.expected_error))
        << "raw: " << c.raw;
  }
}

// A hand-written object carrying exactly the three required fields parses;
// the optional failure record is simply absent.
TEST_F(PendingActionTest, DeserializeAcceptsHandWrittenObject) {
  PendingAction out;
  std::string error;
  ASSERT_FALSE(deserialize(R"({"target_version": "2.0.0",
                               "target_veb_sha256": "abc",
                               "requested_at": "2026-01-02T03:04:05.000000Z"})",
                           out, error))
      << error;

  EXPECT_EQ("2.0.0", out.target_version());
  EXPECT_EQ("abc", out.target_veb_sha256());
  EXPECT_EQ("2026-01-02T03:04:05.000000Z", out.requested_at());
  EXPECT_FALSE(out.has_failure());
}

}  // namespace villagesql_unittest
