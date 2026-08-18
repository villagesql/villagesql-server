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

#include "mysql/strings/m_ctype.h"
#include "sql/mysqld.h"
#include "sql/mysqld_cs.h"
#include "villagesql/schema/identifier_names.h"
#include "villagesql/schema/systable/helpers.h"

namespace villagesql_unittest {

using namespace villagesql;

class IdentifierNamesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    original_lctn_ = test_get_lower_case_table_names();

    system_charset_info = &my_charset_utf8mb3_general_ci;
  }

  void TearDown() override { test_set_lower_case_table_names(original_lctn_); }

  int original_lctn_;
};

TEST_F(IdentifierNamesTest, FsNameCollationFollowsLowerCaseTableNames) {
  test_set_lower_case_table_names(0);
  EXPECT_EQ(fs_name_collation(), &my_charset_utf8mb3_bin);

  for (int setting : {1, 2}) {
    test_set_lower_case_table_names(setting);
    EXPECT_EQ(fs_name_collation(), &my_charset_utf8mb3_tolower_ci);
  }
}

TEST_F(IdentifierNamesTest, DatabaseAndTableNamesFollowLowerCaseTableNames) {
  test_set_lower_case_table_names(0);
  EXPECT_EQ(canonical_database_name("MyDB"), "MyDB");
  EXPECT_EQ(canonical_table_name("MyTable"), "MyTable");

  for (int setting : {1, 2}) {
    test_set_lower_case_table_names(setting);
    EXPECT_EQ(canonical_database_name("MyDB"), "mydb");
    EXPECT_EQ(canonical_table_name("MyTable"), "mytable");
  }
}

TEST_F(IdentifierNamesTest, ColumnNamesAlwaysLowercased) {
  for (int setting : {0, 1, 2}) {
    test_set_lower_case_table_names(setting);
    EXPECT_EQ(canonical_column_name("MyColumn"), "mycolumn");
    EXPECT_EQ(canonical_column_name("mycolumn"), "mycolumn");
  }
}

TEST_F(IdentifierNamesTest, TypeIndexExtensionNamesAlwaysLowercased) {
  for (int setting : {0, 1, 2}) {
    test_set_lower_case_table_names(setting);
    EXPECT_EQ(canonical_type_name("COMPLEX"), "complex");
    EXPECT_EQ(canonical_index_name("MyIndex"), "myindex");
    EXPECT_EQ(canonical_extension_name("MyExt"), "myext");
  }
}

// Canonical forms must byte-match the data dictionary's lowercasing, or
// lookups against DD-stored names miss.
TEST_F(IdentifierNamesTest, LoweringMatchesDataDictionary) {
  // U+1E9E (capital sharp S) is a case where utf8mb3 lowering (unchanged)
  // differs from utf8mb4_0900 case folding (maps to U+00DF).
  const std::string capital_sharp_s = "STRA\xE1\xBA\x9E\x45";  // STRAẞE
  const std::string dd_lowered = "stra\xE1\xBA\x9E\x65";       // straẞe

  for (int setting : {1, 2}) {
    test_set_lower_case_table_names(setting);
    EXPECT_EQ(canonical_database_name(capital_sharp_s), dd_lowered);
    EXPECT_EQ(canonical_table_name(capital_sharp_s), dd_lowered);
  }

  // Column, type, and index names are lowercased regardless of setting.
  EXPECT_EQ(canonical_column_name(capital_sharp_s), dd_lowered);
  EXPECT_EQ(canonical_type_name(capital_sharp_s), dd_lowered);
  EXPECT_EQ(canonical_index_name(capital_sharp_s), dd_lowered);
}

// Lowercasing preserves accents: café and cafe are distinct identifiers.
TEST_F(IdentifierNamesTest, AccentsPreserved) {
  const std::string cafe_upper = "CAF\xC3\x89";  // CAFÉ
  const std::string cafe_lower = "caf\xC3\xA9";  // café

  EXPECT_EQ(canonical_column_name(cafe_upper), cafe_lower);
  EXPECT_FALSE(column_names_equal(cafe_lower, "cafe"));
  EXPECT_TRUE(column_names_equal(cafe_upper, cafe_lower));

  test_set_lower_case_table_names(2);
  EXPECT_EQ(canonical_table_name(cafe_upper), cafe_lower);
  EXPECT_FALSE(table_names_equal(cafe_lower, "cafe"));
}

// ß and ss compare equal under utf8mb4_0900_ai_ci; identifier comparisons
// must keep them distinct.
TEST_F(IdentifierNamesTest, SharpSDistinctFromSs) {
  const std::string strasse_sharp = "stra\xC3\x9F\x65";  // straße

  EXPECT_FALSE(column_names_equal(strasse_sharp, "strasse"));
  EXPECT_FALSE(type_names_equal(strasse_sharp, "strasse"));

  for (int setting : {0, 2}) {
    test_set_lower_case_table_names(setting);
    EXPECT_FALSE(database_names_equal(strasse_sharp, "strasse"));
    EXPECT_FALSE(table_names_equal(strasse_sharp, "strasse"));
  }

  // ß is already lowercase; canonicalization leaves it unchanged.
  EXPECT_EQ(canonical_column_name(strasse_sharp), strasse_sharp);
}

TEST_F(IdentifierNamesTest, EqualityHelpers) {
  test_set_lower_case_table_names(0);
  EXPECT_FALSE(database_names_equal("MyDB", "mydb"));
  EXPECT_FALSE(table_names_equal("MyTable", "mytable"));
  EXPECT_TRUE(database_names_equal("MyDB", "MyDB"));

  test_set_lower_case_table_names(2);
  EXPECT_TRUE(database_names_equal("MyDB", "mydb"));
  EXPECT_TRUE(table_names_equal("MyTable", "mytable"));

  // Case-insensitive regardless of lower_case_table_names.
  for (int setting : {0, 2}) {
    test_set_lower_case_table_names(setting);
    EXPECT_TRUE(column_names_equal("Price", "PRICE"));
    EXPECT_TRUE(index_names_equal("Idx1", "IDX1"));
    EXPECT_TRUE(type_names_equal("Complex", "COMPLEX"));
    EXPECT_TRUE(extension_names_equal("MyExt", "MYEXT"));
    EXPECT_FALSE(column_names_equal("price", "prices"));
  }
}

TEST_F(IdentifierNamesTest, TypeParameterCollation) {
  EXPECT_EQ(type_parameter_collation(), &my_charset_utf8mb4_0900_ai_ci);
}

// Database and table names compare exact bytes at lower_case_table_names=0
// and fold otherwise; column names always fold.
TEST_F(IdentifierNamesTest, ViewMatchSqlFollowsIdentifierRules) {
  test_set_lower_case_table_names(0);
  EXPECT_EQ(database_name_match_sql("vcc.db_name", "sch.name"),
            "vcc.db_name=CONVERT(sch.name USING utf8mb4) COLLATE utf8mb4_bin");
  EXPECT_EQ(
      table_name_match_sql("vcc.table_name", "tbl.name"),
      "vcc.table_name=CONVERT(tbl.name USING utf8mb4) COLLATE utf8mb4_bin");

  for (int setting : {1, 2}) {
    test_set_lower_case_table_names(setting);
    EXPECT_EQ(database_name_match_sql("vcc.db_name", "sch.name"),
              "LOWER(vcc.db_name)=LOWER(CONVERT(sch.name USING utf8mb4) "
              "COLLATE utf8mb4_bin)");
    EXPECT_EQ(table_name_match_sql("vcc.table_name", "tbl.name"),
              "LOWER(vcc.table_name)=LOWER(CONVERT(tbl.name USING utf8mb4) "
              "COLLATE utf8mb4_bin)");
  }

  for (int setting : {0, 2}) {
    test_set_lower_case_table_names(setting);
    EXPECT_EQ(column_name_match_sql("vcc.column_name", "col.name"),
              "LOWER(vcc.column_name)=LOWER(CONVERT(col.name USING utf8mb4) "
              "COLLATE utf8mb4_bin)");
  }
}

}  // namespace villagesql_unittest
