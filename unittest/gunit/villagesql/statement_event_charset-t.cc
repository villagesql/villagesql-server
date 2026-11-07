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

#include "sql/server_component/mysql_connection_attributes_iterator_imp.h"
#include "sql/sql_class.h"
#include "unittest/gunit/test_utils.h"

namespace villagesql_unittest {

class StatementCharsetTest : public ::testing::Test {
 protected:
  void SetUp() override { initializer.SetUp(); }
  void TearDown() override { initializer.TearDown(); }
  my_testing::Server_initializer initializer;
};

TEST_F(StatementCharsetTest, QueryEncodingSurvivesSessionChange) {
  auto *thd = initializer.thd();
  thd->variables.character_set_client = &my_charset_latin1;
  thd->set_query("SELECT 'caf\xE9'", 13);
  thd->variables.character_set_client = &my_charset_utf8mb4_bin;
  EXPECT_EQ(thd->query_charset(), &my_charset_latin1);
  const auto saved = thd->query();
  const auto *saved_charset = thd->query_charset();
  thd->set_query("SELECT 1", 8);
  EXPECT_EQ(thd->query_charset(), &my_charset_utf8mb4_bin);
  thd->set_query(saved, saved_charset);
  EXPECT_EQ(thd->query_charset(), &my_charset_latin1);
}

TEST_F(StatementCharsetTest, AttributeEncodingAndEmbeddedNulArePreserved) {
  auto *thd = initializer.thd();
  const std::string name = "program_name";
  const std::string value("caf\xE9\0x", 6);  // latin1 café, NUL, x
  thd->m_connection_attributes.push_back(name.size());
  thd->m_connection_attributes.insert(thd->m_connection_attributes.end(),
                                      name.begin(), name.end());
  thd->m_connection_attributes.push_back(value.size());
  thd->m_connection_attributes.insert(thd->m_connection_attributes.end(),
                                      value.begin(), value.end());
  thd->m_connection_attributes_charset = &my_charset_latin1;
  thd->variables.character_set_client = &my_charset_utf8mb4_bin;
  my_h_connection_attributes_iterator iterator;
  ASSERT_FALSE(mysql_connection_attributes_iterator_imp::init(thd, &iterator));
  const char *key, *data, *charset;
  size_t key_length, data_length;
  ASSERT_FALSE(mysql_connection_attributes_iterator_imp::get(
      thd, &iterator, &key, &key_length, &data, &data_length, &charset));
  EXPECT_EQ(std::string(key, key_length), name);
  EXPECT_EQ(std::string(data, data_length), value);
  EXPECT_STREQ(charset, my_charset_latin1.m_coll_name);
  mysql_connection_attributes_iterator_imp::deinit(iterator);
}

}  // namespace villagesql_unittest
