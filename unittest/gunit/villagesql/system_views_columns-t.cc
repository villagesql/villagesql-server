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

// Guards villagesql/system_views/columns.cc against drifting away from the
// upstream definition it was adapted from
// (sql/dd/impl/system_views/columns.cc).
//
// Ours REPLACES upstream's I_S.COLUMNS wholesale once VillageSQL is
// initialized, so anything upstream adds to that view -- a new projection, a
// new MYSQL_TYPE_* arm in the CHARACTER_SET_NAME / COLLATION_NAME CASE
// expressions, a new join -- silently disappears from the served view unless
// somebody notices during a rebase. Nothing else catches that: the funcs_1
// goldens and the I_S schema checksum are re-recorded whenever they change, so
// by construction they detect divergence-from-last-time, never
// divergence-from-upstream.
//
// The source of truth here is upstream's own view definition, compared
// structurally against ours. Ours is allowed to ADD (custom-type columns come
// from a LEFT JOIN on villagesql.custom_columns) and to WRAP an upstream
// projection in a larger expression, but never to drop or alter what upstream
// projects.
//
// If this test fails after a rebase, re-sync villagesql/system_views/columns.cc
// with sql/dd/impl/system_views/columns.cc. Do NOT edit upstream's file: ours
// is the served view.

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "sql/dd/impl/system_views/columns.h"
#include "sql/dd/types/system_view.h"
#include "villagesql/system_views/columns.h"

namespace villagesql_unittest {
namespace {

// Collapse every run of whitespace to a single space and trim the ends, so
// that pure formatting differences between the two definitions (they are built
// from differently wrapped string literals) do not trip the comparison.
std::string Normalize(const std::string &s) {
  std::string out;
  bool pending_space = false;
  for (const char c : s) {
    if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
      pending_space = true;
      continue;
    }
    if (pending_space && !out.empty()) out.push_back(' ');
    pending_space = false;
    out.push_back(c);
  }
  return out;
}

bool EndsWith(const std::string &s, const std::string &suffix) {
  return s.size() >= suffix.size() &&
         s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

// A CREATE VIEW statement broken into its parts. Field expressions are keyed by
// the column name they are projected as.
struct ParsedView {
  std::map<std::string, std::string> fields;
  std::vector<std::string> from_clauses;
  std::vector<std::string> where_clauses;
};

// Parses the text produced by System_view_select_definition_impl::
// build_ddl_create_view(). That builder emits the header on the first line,
// then one projection per line, then one FROM clause per line, then one WHERE
// clause per line; the " FROM" and " WHERE" keywords are appended to the end of
// the last line of the preceding section.
ParsedView ParseCreateView(const std::string &ddl) {
  enum class Section { kFields, kFrom, kWhere };

  ParsedView parsed;
  Section section = Section::kFields;
  bool first_line = true;

  size_t pos = 0;
  while (pos <= ddl.size()) {
    const size_t eol = ddl.find('\n', pos);
    std::string line =
        Normalize(ddl.substr(pos, eol == std::string::npos ? eol : eol - pos));
    pos = (eol == std::string::npos) ? ddl.size() + 1 : eol + 1;

    if (first_line) {
      // "CREATE OR REPLACE ... VIEW information_schema.COLUMNS AS SELECT"
      EXPECT_TRUE(EndsWith(line, " AS SELECT"))
          << "unexpected header: " << line;
      first_line = false;
      continue;
    }
    if (line.empty()) continue;

    Section next_section = section;
    if (section == Section::kFields && EndsWith(line, " FROM")) {
      line.erase(line.size() - 5);
      next_section = Section::kFrom;
    } else if (section == Section::kFrom && EndsWith(line, " WHERE")) {
      line.erase(line.size() - 6);
      next_section = Section::kWhere;
    }
    line = Normalize(line);

    switch (section) {
      case Section::kFields: {
        if (EndsWith(line, ",")) line.erase(line.size() - 1);
        // The projection is "<expression> AS <COLUMN_NAME>".
        const size_t as_pos = line.rfind(" AS ");
        EXPECT_NE(std::string::npos, as_pos) << "unparsable field: " << line;
        if (as_pos == std::string::npos) break;
        parsed.fields[line.substr(as_pos + 4)] = line.substr(0, as_pos);
        break;
      }
      case Section::kFrom:
        parsed.from_clauses.push_back(line);
        break;
      case Section::kWhere:
        parsed.where_clauses.push_back(line);
        break;
    }
    section = next_section;
  }
  return parsed;
}

// Every maximal MYSQL_TYPE_* identifier occurring in the expression. These are
// the CASE arms of CHARACTER_SET_NAME / COLLATION_NAME.
std::set<std::string> TypeArms(const std::string &expression) {
  static const std::string kPrefix = "MYSQL_TYPE_";
  std::set<std::string> arms;
  size_t pos = expression.find(kPrefix);
  while (pos != std::string::npos) {
    size_t end = pos + kPrefix.size();
    while (end < expression.size() &&
           (expression[end] == '_' ||
            (expression[end] >= 'A' && expression[end] <= 'Z') ||
            (expression[end] >= '0' && expression[end] <= '9'))) {
      ++end;
    }
    arms.insert(expression.substr(pos, end - pos));
    pos = expression.find(kPrefix, end);
  }
  return arms;
}

std::string Ddl(const dd::system_views::System_view &view) {
  return std::string(view.view_definition()->build_ddl_create_view().c_str());
}

class SystemViewsColumnsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    upstream_ = ParseCreateView(Ddl(dd::system_views::Columns::instance()));
    ours_ = ParseCreateView(Ddl(villagesql::system_views::Columns::instance()));
  }

  ParsedView upstream_;
  ParsedView ours_;
};

// Nothing upstream projects may vanish from our replacement view.
TEST_F(SystemViewsColumnsTest, ProjectsEveryUpstreamColumn) {
  ASSERT_FALSE(upstream_.fields.empty());
  for (const auto &field : upstream_.fields) {
    EXPECT_TRUE(ours_.fields.count(field.first) != 0)
        << "villagesql/system_views/columns.cc does not project " << field.first
        << ", which upstream's I_S.COLUMNS does. Re-sync it with "
           "sql/dd/impl/system_views/columns.cc.";
  }
}

// Every MYSQL_TYPE_* CASE arm upstream handles must be handled by us too.
// Missing an arm makes our view return NULL where upstream returns a value.
TEST_F(SystemViewsColumnsTest, HandlesEveryUpstreamTypeArm) {
  size_t total_arms = 0;
  for (const auto &field : upstream_.fields) {
    const auto ours = ours_.fields.find(field.first);
    if (ours == ours_.fields.end()) continue;  // reported by the test above
    const std::set<std::string> our_arms = TypeArms(ours->second);
    for (const std::string &arm : TypeArms(field.second)) {
      ++total_arms;
      EXPECT_TRUE(our_arms.count(arm) != 0)
          << "villagesql/system_views/columns.cc is missing the " << arm
          << " arm of " << field.first
          << ", which upstream's I_S.COLUMNS handles. Re-sync it with "
             "sql/dd/impl/system_views/columns.cc.";
    }
  }
  // Guard against the parser silently finding nothing to compare.
  EXPECT_GT(total_arms, 0u);
}

// Catch-all: whatever upstream computes for a column must still be computed the
// same way by us. We may wrap an upstream expression in a larger one (that is
// how custom types are folded into DATA_TYPE / COLUMN_TYPE), so containment
// rather than equality, but we may not rewrite or truncate it.
TEST_F(SystemViewsColumnsTest, PreservesEveryUpstreamExpression) {
  for (const auto &field : upstream_.fields) {
    const auto ours = ours_.fields.find(field.first);
    if (ours == ours_.fields.end()) continue;  // reported above
    EXPECT_NE(std::string::npos, ours->second.find(field.second))
        << "villagesql/system_views/columns.cc computes " << field.first
        << " differently from upstream's I_S.COLUMNS.\n  upstream: "
        << field.second << "\n  ours:     " << ours->second
        << "\nRe-sync it with sql/dd/impl/system_views/columns.cc.";
  }
}

// Our extra LEFT JOIN on villagesql.custom_columns is additive; upstream's
// joins and access-control predicates must all survive.
TEST_F(SystemViewsColumnsTest, KeepsEveryUpstreamFromAndWhereClause) {
  ASSERT_FALSE(upstream_.from_clauses.empty());
  ASSERT_FALSE(upstream_.where_clauses.empty());
  for (const std::string &clause : upstream_.from_clauses) {
    EXPECT_NE(
        ours_.from_clauses.end(),
        std::find(ours_.from_clauses.begin(), ours_.from_clauses.end(), clause))
        << "villagesql/system_views/columns.cc is missing upstream's FROM "
           "clause: "
        << clause;
  }
  for (const std::string &clause : upstream_.where_clauses) {
    EXPECT_NE(ours_.where_clauses.end(),
              std::find(ours_.where_clauses.begin(), ours_.where_clauses.end(),
                        clause))
        << "villagesql/system_views/columns.cc is missing upstream's WHERE "
           "clause: "
        << clause;
  }
}

}  // namespace
}  // namespace villagesql_unittest
