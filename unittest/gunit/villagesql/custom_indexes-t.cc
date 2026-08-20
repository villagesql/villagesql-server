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

#include <string>

#include "unittest/gunit/test_utils.h"
#include "villagesql/schema/systable/custom_index_columns.h"
#include "villagesql/schema/systable/custom_indexes.h"
#include "villagesql/schema/systable/helpers.h"
#include "villagesql/schema/victionary_client.h"

namespace villagesql_unittest {

using namespace villagesql;

// ---- Structural tests (no VictionaryClient) --------------------------------

class CustomIndexesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    villagesql::test_set_lower_case_table_names(0);
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }
};

TEST_F(CustomIndexesTest, IndexKeyNormalization) {
  villagesql::test_set_lower_case_table_names(1);

  IndexKey key("MyDB", "MyTable", "MyIndex");
  EXPECT_EQ(key.str(), "mydb.mytable.myindex");
  EXPECT_EQ(key.db(), "MyDB");
  EXPECT_EQ(key.table(), "MyTable");
  EXPECT_EQ(key.index_name(), "MyIndex");
}

TEST_F(CustomIndexesTest, IndexKeyCaseSensitive) {
  // lower_case_table_names=0 keeps db/table case but index names are always
  // case-folded since MySQL index names are case-insensitive regardless.
  villagesql::test_set_lower_case_table_names(0);

  IndexKey key("MyDB", "MyTable", "MyIndex");
  EXPECT_EQ(key.str(), "MyDB.MyTable.myindex");
}

TEST_F(CustomIndexesTest, IndexKeyPrefix) {
  villagesql::test_set_lower_case_table_names(1);

  IndexKeyPrefix prefix("MyDB", "MyTable");
  EXPECT_EQ(prefix.str(), "mydb.mytable.");

  std::string key1 = IndexKey("MyDB", "MyTable", "idx1").str();
  std::string key2 = IndexKey("MyDB", "MyTable", "idx2").str();
  std::string other = IndexKey("MyDB", "OtherTable", "idx1").str();

  EXPECT_EQ(key1.substr(0, prefix.str().size()), prefix.str());
  EXPECT_EQ(key2.substr(0, prefix.str().size()), prefix.str());
  EXPECT_NE(other.substr(0, prefix.str().size()), prefix.str());
}

TEST_F(CustomIndexesTest, IndexColumnKeyBasics) {
  IndexColumnKey key(42, 3);
  EXPECT_EQ(key.str(), "42.3");
  EXPECT_EQ(key.index_id(), 42u);
  EXPECT_EQ(key.key_position(), 3u);
}

TEST_F(CustomIndexesTest, IndexColumnKeyPrefixOrdering) {
  // The prefix "42." must match "42.0", "42.1", ... but NOT "420.0".
  // This relies on ASCII ordering: '.'(46) < '0'(48), so "42/" (upper bound)
  // correctly excludes "420.*" since '0'(48) > '/'(47).
  IndexColumnKeyPrefix prefix(42);
  EXPECT_EQ(prefix.str(), "42.");

  std::string col0 = IndexColumnKey(42, 0).str();    // "42.0"
  std::string col9 = IndexColumnKey(42, 9).str();    // "42.9"
  std::string other = IndexColumnKey(420, 0).str();  // "420.0"

  EXPECT_LT(prefix.str(), col0);
  EXPECT_LT(col0, col9);

  std::string upper = prefix.str();
  upper.back() = '/';  // '.' + 1 == '/'
  EXPECT_EQ(upper, "42/");

  EXPECT_GT(other, upper);  // "420.0" excluded by upper bound
  EXPECT_LT(col9, upper);   // "42.9" included
}

TEST_F(CustomIndexesTest, IndexEntryDefaults) {
  IndexEntry entry;
  EXPECT_EQ(entry.index_id, 0u);
  EXPECT_EQ(entry.index_type_parameters, "{}");
}

TEST_F(CustomIndexesTest, IndexEntryFullConstruction) {
  IndexEntry entry(IndexKey("db", "tbl", "idx"), 7, "my_ext", "1.0", "hnsw",
                   R"({"dim":128})");
  EXPECT_EQ(entry.index_id, 7u);
  EXPECT_EQ(entry.extension_name, "my_ext");
  EXPECT_EQ(entry.extension_version, "1.0");
  EXPECT_EQ(entry.index_type_name, "hnsw");
  EXPECT_EQ(entry.index_type_parameters, R"({"dim":128})");
  EXPECT_EQ(entry.db_name(), "db");
  EXPECT_EQ(entry.table_name(), "tbl");
  EXPECT_EQ(entry.index_name(), "idx");
}

TEST_F(CustomIndexesTest, IndexColumnEntryConstruction) {
  IndexColumnEntry entry(IndexColumnKey(10, 0), "embedding", "vec_ext", "2.0",
                         "cosine_profile");
  EXPECT_EQ(entry.index_id(), 10u);
  EXPECT_EQ(entry.key_position(), 0u);
  EXPECT_EQ(entry.column_name, "embedding");
  EXPECT_EQ(entry.profile_extension_name, "vec_ext");
  EXPECT_EQ(entry.profile_extension_version, "2.0");
  EXPECT_EQ(entry.profile_name, "cosine_profile");
}

// ---- VictionaryClient integration tests ------------------------------------

class CustomIndexesVictionaryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = &VictionaryClient::instance();
    if (!client_->is_initialized()) {
      ASSERT_FALSE(client_->init_for_testing());
    }
    client_->clear_all_tables();
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }

  void TearDown() override { client_->clear_all_tables(); }

  VictionaryClient *client_;
};

// Test IndexColumnKeyPrefix ASCII ordering correctness end-to-end.
// "42." must match "42.0", "42.1", etc. but must NOT match "420.*" entries
// because '0'(48) > '.'(46) so "420" > "42/" (the upper bound for "42.").
TEST_F(CustomIndexesVictionaryTest, IndexColumnKeyPrefixExcludesLongerIds) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xB000);

  {
    auto guard = client_->get_write_lock();

    IndexColumnEntry e1(IndexColumnKey(42, 0), "col_a", "ext", "1.0", "prof");
    IndexColumnEntry e2(IndexColumnKey(42, 1), "col_b", "ext", "1.0", "prof");
    IndexColumnEntry e3(IndexColumnKey(420, 0), "col_c", "ext", "1.0", "prof");
    IndexColumnEntry e4(IndexColumnKey(421, 0), "col_d", "ext", "1.0", "prof");
    IndexColumnEntry e5(IndexColumnKey(4, 2), "col_e", "ext", "1.0", "prof");

    client_->custom_index_columns().MarkForInsertion(*fake_thd, std::move(e1));
    client_->custom_index_columns().MarkForInsertion(*fake_thd, std::move(e2));
    client_->custom_index_columns().MarkForInsertion(*fake_thd, std::move(e3));
    client_->custom_index_columns().MarkForInsertion(*fake_thd, std::move(e4));
    client_->custom_index_columns().MarkForInsertion(*fake_thd, std::move(e5));
  }
  client_->commit_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();
    auto results = client_->GetColumnsForIndex(42);
    EXPECT_EQ(results.size(), 2u);
    for (const auto *e : results) EXPECT_EQ(e->index_id(), 42u);

    EXPECT_EQ(client_->GetColumnsForIndex(420).size(), 1u);
    EXPECT_EQ(client_->GetColumnsForIndex(4).size(), 1u);
  }
}

// Test allocate_index_id produces strictly increasing unique IDs.
TEST_F(CustomIndexesVictionaryTest, AllocateIndexId) {
  uint64_t id1 = client_->allocate_index_id();
  uint64_t id2 = client_->allocate_index_id();
  uint64_t id3 = client_->allocate_index_id();

  EXPECT_LT(id1, id2);
  EXPECT_LT(id2, id3);
  EXPECT_EQ(id2, id1 + 1);
  EXPECT_EQ(id3, id2 + 1);
}

// Test GetCustomIndexesForTable end-to-end.
TEST_F(CustomIndexesVictionaryTest, GetCustomIndexesForTable) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xB008);

  {
    auto guard = client_->get_write_lock();

    uint64_t id1 = client_->allocate_index_id();
    IndexEntry e1(IndexKey("testdb", "t1", "idx_vec"), id1, "ext", "1.0",
                  "VECTOR_INDEX");

    uint64_t id2 = client_->allocate_index_id();
    IndexEntry e2(IndexKey("testdb", "t1", "idx_hash"), id2, "ext", "1.0",
                  "HASH_INDEX");

    uint64_t id3 = client_->allocate_index_id();
    IndexEntry e3(IndexKey("testdb", "t2", "idx_vec"), id3, "ext", "1.0",
                  "VECTOR_INDEX");

    client_->custom_indexes().MarkForInsertion(*fake_thd, std::move(e1));
    client_->custom_indexes().MarkForInsertion(*fake_thd, std::move(e2));
    client_->custom_indexes().MarkForInsertion(*fake_thd, std::move(e3));
  }
  client_->commit_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();
    EXPECT_EQ(client_->GetCustomIndexesForTable("testdb", "t1").size(), 2u);
    EXPECT_EQ(client_->GetCustomIndexesForTable("testdb", "t2").size(), 1u);
    EXPECT_TRUE(
        client_->GetCustomIndexesForTable("testdb", "no_such_table").empty());
  }
}

// Test that staging an index entry only marks the index maps dirty.
TEST_F(CustomIndexesVictionaryTest, IndexOnlyHasUncommittedOnIndexMaps) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xB010);

  uint64_t id = client_->allocate_index_id();
  IndexEntry entry(IndexKey("db", "t", "idx"), id, "ext", "1.0", "MY_INDEX");

  {
    auto guard = client_->get_write_lock();
    client_->custom_indexes().MarkForInsertion(*fake_thd, std::move(entry));

    EXPECT_TRUE(client_->custom_indexes().has_uncommitted(fake_thd));
    EXPECT_FALSE(client_->custom_index_columns().has_uncommitted(fake_thd));
    EXPECT_FALSE(client_->columns().has_uncommitted(fake_thd));
    EXPECT_FALSE(client_->properties().has_uncommitted(fake_thd));
  }

  client_->rollback_all_tables(fake_thd);
}

// The THD-aware GetColumnsForIndex must see rows staged by this THD (the DDL
// case, where index and column rows are staged together and not yet committed),
// while the committed-only variant must not.
TEST_F(CustomIndexesVictionaryTest, GetColumnsForIndexSeesUncommitted) {
  // Use fake THD pointers for testing that are 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xB018);
  THD *other_thd = reinterpret_cast<THD *>(0xB020);

  {
    auto guard = client_->get_write_lock();
    client_->custom_index_columns().MarkForInsertion(
        *fake_thd,
        IndexColumnEntry(IndexColumnKey(77, 0), "c0", "ext", "1.0", "prof0"));
    client_->custom_index_columns().MarkForInsertion(
        *fake_thd,
        IndexColumnEntry(IndexColumnKey(77, 1), "c1", "ext", "1.0", "prof1"));

    auto staged = client_->GetColumnsForIndex(fake_thd, 77);
    ASSERT_EQ(staged.size(), 2u);
    EXPECT_EQ(staged[0]->column_name, "c0");
    EXPECT_EQ(staged[1]->column_name, "c1");

    // Committed-only view and other sessions see nothing.
    EXPECT_TRUE(client_->GetColumnsForIndex(77).empty());
    EXPECT_TRUE(client_->GetColumnsForIndex(other_thd, 77).empty());
    EXPECT_TRUE(client_->GetColumnsForIndex(fake_thd, 7).empty());
  }

  client_->rollback_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();
    EXPECT_TRUE(client_->GetColumnsForIndex(fake_thd, 77).empty());
  }
}

// A staged delete must hide the committed row from this THD's view, but not
// from the committed-only view or from other sessions.
TEST_F(CustomIndexesVictionaryTest, GetColumnsForIndexHidesStagedDelete) {
  // Use fake THD pointers for testing that are 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xB028);
  THD *other_thd = reinterpret_cast<THD *>(0xB030);

  {
    auto guard = client_->get_write_lock();
    client_->custom_index_columns().MarkForInsertion(
        *fake_thd,
        IndexColumnEntry(IndexColumnKey(88, 0), "c0", "ext", "1.0", "prof0"));
    client_->custom_index_columns().MarkForInsertion(
        *fake_thd,
        IndexColumnEntry(IndexColumnKey(88, 1), "c1", "ext", "1.0", "prof1"));
  }
  client_->commit_all_tables(fake_thd);

  {
    auto guard = client_->get_write_lock();
    client_->custom_index_columns().MarkForDeletion(*fake_thd,
                                                    IndexColumnKey(88, 0));

    auto visible = client_->GetColumnsForIndex(fake_thd, 88);
    ASSERT_EQ(visible.size(), 1u);
    EXPECT_EQ(visible[0]->column_name, "c1");

    EXPECT_EQ(client_->GetColumnsForIndex(88).size(), 2u);
    EXPECT_EQ(client_->GetColumnsForIndex(other_thd, 88).size(), 2u);
  }

  client_->rollback_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();
    EXPECT_EQ(client_->GetColumnsForIndex(fake_thd, 88).size(), 2u);
  }
}

// Staged rows for a different index must not leak into the prefix scan, and the
// "42." prefix must still exclude "420.*" when the staged overlay is applied.
TEST_F(CustomIndexesVictionaryTest, GetColumnsForIndexUncommittedPrefixBounds) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xB038);

  {
    auto guard = client_->get_write_lock();
    client_->custom_index_columns().MarkForInsertion(
        *fake_thd,
        IndexColumnEntry(IndexColumnKey(42, 0), "col_a", "ext", "1.0", "prof"));
    client_->custom_index_columns().MarkForInsertion(
        *fake_thd, IndexColumnEntry(IndexColumnKey(420, 0), "col_b", "ext",
                                    "1.0", "prof"));
    client_->custom_index_columns().MarkForInsertion(
        *fake_thd,
        IndexColumnEntry(IndexColumnKey(4, 2), "col_c", "ext", "1.0", "prof"));

    auto results = client_->GetColumnsForIndex(fake_thd, 42);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0]->column_name, "col_a");
    EXPECT_EQ(client_->GetColumnsForIndex(fake_thd, 420).size(), 1u);
    EXPECT_EQ(client_->GetColumnsForIndex(fake_thd, 4).size(), 1u);
  }

  client_->rollback_all_tables(fake_thd);
}

}  // namespace villagesql_unittest
