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

#include <string>

#include "villagesql/include/string_utils.h"

namespace villagesql_unittest {

using namespace villagesql;

class SanitizeForOutputTest : public ::testing::Test {};

// Printable ASCII passes through untouched.
TEST_F(SanitizeForOutputTest, PrintableTextIsUnchanged) {
  EXPECT_EQ("", sanitize_for_output(""));
  EXPECT_EQ("mysql-8.4_1.2.3", sanitize_for_output("mysql-8.4_1.2.3"));
  EXPECT_EQ("a b!\"#$%&'()*+,-./:;<=>?@[]^_`{|}~",
            sanitize_for_output("a b!\"#$%&'()*+,-./:;<=>?@[]^_`{|}~"));
}

// The named escapes are rendered readably, and a backslash is doubled so the
// escaping is unambiguous.
TEST_F(SanitizeForOutputTest, NamedEscapes) {
  EXPECT_EQ("\\t", sanitize_for_output("\t"));
  EXPECT_EQ("\\n", sanitize_for_output("\n"));
  EXPECT_EQ("\\r", sanitize_for_output("\r"));
  EXPECT_EQ("\\\\", sanitize_for_output("\\"));
  EXPECT_EQ("a\\nb", sanitize_for_output("a\nb"));

  // An input that already looks like an escape is not mistaken for one.
  EXPECT_EQ("\\\\n", sanitize_for_output("\\n"));
}

// Anything else unprintable becomes a hex escape, including the high bytes of
// a multi-byte UTF-8 sequence and an embedded NUL.
TEST_F(SanitizeForOutputTest, UnprintableBytesBecomeHexEscapes) {
  EXPECT_EQ("\\x01", sanitize_for_output("\x01"));
  EXPECT_EQ("\\x1B[31m", sanitize_for_output("\x1b[31m"));
  EXPECT_EQ("\\x7F", sanitize_for_output("\x7f"));

  // U+00E9 in UTF-8 is 0xC3 0xA9; both bytes are non-printable ASCII.
  EXPECT_EQ("caf\\xC3\\xA9", sanitize_for_output("caf\xc3\xa9"));

  // An embedded NUL is escaped rather than truncating the result.
  EXPECT_EQ("a\\x00b", sanitize_for_output(std::string_view("a\0b", 3)));
}

// Input longer than the limit is truncated and marked with an ellipsis. The
// limit counts source characters, not output characters, so escapes do not
// shorten how much of the input is shown.
TEST_F(SanitizeForOutputTest, TruncationAppendsEllipsis) {
  const std::string ellipsis = "…";

  EXPECT_EQ("abcde", sanitize_for_output("abcde", 5));
  EXPECT_EQ("abcde" + ellipsis, sanitize_for_output("abcdef", 5));
  EXPECT_EQ(ellipsis, sanitize_for_output("abc", 0));

  // Four source characters, each two characters wide once escaped.
  EXPECT_EQ("\\n\\n\\n\\n" + ellipsis, sanitize_for_output("\n\n\n\n\n", 4));

  // A string exactly at the limit is not marked as truncated.
  const std::string at_limit(kDefaultMaxOutputChars, 'x');
  EXPECT_EQ(at_limit, sanitize_for_output(at_limit));
  EXPECT_EQ(at_limit + ellipsis, sanitize_for_output(at_limit + "x"));
}

// A long, hostile string cannot flood the log: the result is bounded by the
// limit, at four output characters per escaped source character.
TEST_F(SanitizeForOutputTest, OutputIsBounded) {
  const std::string flood(10000, '\x1b');
  const std::string result = sanitize_for_output(flood);
  EXPECT_LE(result.size(), kDefaultMaxOutputChars * 4 + 3);
  EXPECT_EQ(std::string::npos, result.find('\x1b'));
}

}  // namespace villagesql_unittest
