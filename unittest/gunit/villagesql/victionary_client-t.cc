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
#include "sql/strfunc.h"
#include "unittest/gunit/temptable/mock_field_varstring.h"
#include "unittest/gunit/test_utils.h"
#include "villagesql/schema/systable/helpers.h"
#include "villagesql/schema/victionary_client.h"

namespace villagesql_unittest {

using namespace villagesql;

class VictionaryClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Each test gets the singleton instance
    client_ = &VictionaryClient::instance();

    // Initialize on first test only (singleton persists across tests)
    // Use init_for_testing() which skips database loading
    if (!client_->is_initialized()) {
      ASSERT_FALSE(client_->init_for_testing());
    }

    // Clear any existing data
    client_->clear_all_tables();

    // Set system character set
    system_charset_info = &my_charset_utf8mb4_0900_ai_ci;
  }

  void TearDown() override {
    // Clean up after each test
    client_->clear_all_tables();
  }

  VictionaryClient *client_;
};

// Test client initialization behavior
TEST_F(VictionaryClientTest, InitializationBehavior) {
  // Client should already be initialized from SetUp
  EXPECT_TRUE(client_->is_initialized());

  // Verify singleton returns the same instance
  EXPECT_EQ(client_, &VictionaryClient::instance());
}

// Test PropertyEntry structure
TEST_F(VictionaryClientTest, PropertyEntryBasics) {
  PropertyEntry entry(PropertyKey("version"));
  entry.value = "1";
  entry.description = "VillageSQL system schema version";

  EXPECT_EQ(entry.name(), "version");
  EXPECT_EQ(entry.value, "1");
  EXPECT_EQ(entry.description, "VillageSQL system schema version");
}

// Test SystemTableMap operations without initialization
TEST_F(VictionaryClientTest, OperationsWithoutInit) {
  // Operations should handle uninitialized state gracefully
  {
    auto guard = client_->get_read_lock();
    const PropertyEntry *entry = client_->properties().get_committed("version");
    EXPECT_EQ(entry, nullptr);
  }

  // Clear should be safe
  client_->clear_all_tables();
}

// Test SystemTableMap basic structure
TEST_F(VictionaryClientTest, SystemTableMapStructure) {
  auto guard = client_->get_read_lock();

  // Initially, map should be empty
  const PropertyEntry *entry = client_->properties().get_committed("version");
  EXPECT_EQ(entry, nullptr);

  // Stats should show a miss
  auto stats = client_->properties().get_stats();
  EXPECT_EQ(stats.hits, 0);
  EXPECT_GE(stats.misses, 1);  // May have accumulated from other tests
  EXPECT_EQ(stats.committed_entries, 0);
}

// Test clear operation
TEST_F(VictionaryClientTest, ClearAllTables) {
  // Clear should reset all tables
  client_->clear_all_tables();

  auto guard = client_->get_read_lock();
  auto stats = client_->properties().get_stats();

  EXPECT_EQ(stats.committed_entries, 0);
}

// Test get_prefix_committed for columns (which uses db.table prefix)
TEST_F(VictionaryClientTest, GetPrefixEntries) {
  auto guard = client_->get_read_lock();

  // Columns support prefix queries via ColumnKeyPrefix
  auto entries =
      client_->columns().get_prefix_committed(ColumnKeyPrefix("testdb", "t1"));
  EXPECT_TRUE(entries.empty());  // No entries added yet
}

// Test has_prefix_committed for columns
TEST_F(VictionaryClientTest, HasPrefixEntries) {
  auto guard = client_->get_read_lock();

  // Columns support prefix queries via ColumnKeyPrefix
  EXPECT_FALSE(
      client_->columns().has_prefix_committed(ColumnKeyPrefix("testdb", "t1")));
}

// Test transaction operations with null THD
TEST_F(VictionaryClientTest, TransactionOperationsWithNullTHD) {
  // These should handle null THD gracefully
  client_->commit_all_tables(nullptr);
  client_->rollback_all_tables(nullptr);
  EXPECT_FALSE(client_->write_all_uncommitted_entries(nullptr));
}

// Test thread safety with multiple operations
// This is a basic test - real thread safety testing would require
// spawning actual threads
TEST_F(VictionaryClientTest, ThreadSafetyBasic) {
  // Multiple operations should not crash
  for (int i = 0; i < 100; ++i) {
    {
      auto guard = client_->get_read_lock();
      client_->properties().get_committed("version");
      // Test prefix queries on columns (properties don't support prefix
      // queries)
      client_->columns().get_prefix_committed(ColumnKeyPrefix("testdb", "t1"));
      client_->columns().has_prefix_committed(ColumnKeyPrefix("testdb", "t1"));
    }

    if (i % 10 == 0) {
      client_->clear_all_tables();
    }
  }

  // Client should still be functional
  auto guard = client_->get_read_lock();
  auto stats = client_->properties().get_stats();

  EXPECT_GE(stats.misses, 0);
}

// Test get_uncommitted_entries
TEST_F(VictionaryClientTest, GetUncommittedEntries) {
  // With the new design, uncommitted operations are tracked in a list
  // We don't expose get_uncommitted_entries anymore
  // Just verify the client works correctly
  auto guard = client_->get_read_lock();
  auto stats = client_->properties().get_stats();
  EXPECT_EQ(stats.uncommitted_entries, 0);
}

// Test commit/rollback with fake THD
TEST_F(VictionaryClientTest, CommitRollbackWithFakeTHD) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x1230);

  // Commit and rollback should be safe even with no entries
  client_->commit_all_tables(fake_thd);
  client_->rollback_all_tables(fake_thd);
}

// Test RAII read lock guard
TEST_F(VictionaryClientTest, ReadLockGuard) {
  {
    auto guard = client_->get_read_lock();

    // Lock should be held, we can safely access data
    const PropertyEntry *entry = client_->properties().get_committed("version");
    EXPECT_EQ(entry, nullptr);  // No entries in empty map

    // Lock automatically released when guard goes out of scope
  }

  // Lock should have been released, we can acquire it again
  {
    auto guard = client_->get_read_lock();
    auto stats = client_->properties().get_stats();
    EXPECT_GE(stats.misses, 1);
  }
}

// Test RAII write lock guard
TEST_F(VictionaryClientTest, WriteLockGuard) {
  PropertyEntry entry(PropertyKey("test_key"));
  entry.value = "test_value";
  entry.description = "Test entry";

  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x1238);

  {
    auto guard = client_->get_write_lock();

    // Lock should be held, we can mark entries for insertion
    bool error =
        client_->properties().MarkForInsertion(*fake_thd, std::move(entry));
    EXPECT_FALSE(error);  // Should succeed

    // Lock automatically released when guard goes out of scope
  }

  // Lock should have been released
  // Clean up the uncommitted entry
  client_->rollback_all_tables(fake_thd);
}

// Test that lock guards work correctly with early returns
TEST_F(VictionaryClientTest, LockGuardEarlyReturn) {
  auto test_function = [this]() -> bool {
    auto guard = client_->get_read_lock();

    // Simulate early return (e.g., error condition)
    const PropertyEntry *entry = client_->properties().get_committed("missing");
    if (!entry) {
      return false;  // Lock should still be released
    }

    return true;
  };

  // Call function multiple times to ensure lock is properly released
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(test_function());
  }

  // Should still be able to acquire lock after all those early returns
  auto guard = client_->get_read_lock();
  auto stats = client_->properties().get_stats();
  EXPECT_GE(stats.misses, 0);
}

// Test nested lock acquisition (read after read is allowed with rwlock)
TEST_F(VictionaryClientTest, NestedReadLocks) {
  // rwlocks allow multiple readers
  auto guard1 = client_->get_read_lock();
  auto guard2 = client_->get_read_lock();

  // Both locks should be held, operations should work
  const PropertyEntry *entry = client_->properties().get_committed("version");
  EXPECT_EQ(entry, nullptr);

  // Both guards will release when they go out of scope
}

// ===== Key Normalization Tests =====

// Test key normalization with lower_case_table_names = 0 (case-sensitive)
TEST_F(VictionaryClientTest, KeyNormalizationCaseSensitive) {
  int original_setting = lower_case_table_names;
  lower_case_table_names = 0;  // Case-sensitive

  ColumnEntry entry1(ColumnKey("MyDB", "MyTable", "MyColumn"));
  ColumnEntry entry2(ColumnKey("mydb", "mytable", "mycolumn"));

  std::string key1 = entry1.key().str();
  std::string key2 = entry2.key().str();

  // With lower_case_table_names=0: DB and table names preserve case
  EXPECT_EQ(key1, "MyDB.MyTable.mycolumn");  // Column always lowercase
  EXPECT_EQ(key2, "mydb.mytable.mycolumn");

  // Keys should be DIFFERENT (case-sensitive)
  EXPECT_NE(key1, key2);

  // But extension/type names should still be case-insensitive
  TypeDescriptor type1(TypeDescriptorKey("COMPLEX", "MyExt", "1.0"));
  TypeDescriptor type2(TypeDescriptorKey("complex", "myext", "1.0"));
  EXPECT_EQ(type1.key().str(), type2.key().str());

  lower_case_table_names = original_setting;
}

// Test key normalization with lower_case_table_names = 1 (lowercase storage)
TEST_F(VictionaryClientTest, KeyNormalizationLowercaseStorage) {
  int original_setting = lower_case_table_names;
  lower_case_table_names = 1;  // Lowercase storage

  ColumnEntry entry1(ColumnKey("MyDB", "MyTable", "MyColumn"));
  ColumnEntry entry2(ColumnKey("MYDB", "MYTABLE", "MYCOLUMN"));
  ColumnEntry entry3(ColumnKey("mydb", "mytable", "mycolumn"));

  std::string key1 = entry1.key().str();
  std::string key2 = entry2.key().str();
  std::string key3 = entry3.key().str();

  // With lower_case_table_names=1: All names normalized to lowercase
  EXPECT_EQ(key1, "mydb.mytable.mycolumn");
  EXPECT_EQ(key2, "mydb.mytable.mycolumn");
  EXPECT_EQ(key3, "mydb.mytable.mycolumn");

  // All keys should be identical (case-insensitive)
  EXPECT_EQ(key1, key2);
  EXPECT_EQ(key2, key3);

  lower_case_table_names = original_setting;
}

// Test key normalization with lower_case_table_names = 2 (case-preserving,
// compare lowercase)
TEST_F(VictionaryClientTest, KeyNormalizationCasePreserving) {
  int original_setting = lower_case_table_names;
  lower_case_table_names = 2;  // Case-preserving but lowercase comparison

  ColumnEntry entry1(ColumnKey("MyDB", "MyTable", "MyColumn"));
  ColumnEntry entry2(ColumnKey("MYDB", "MYTABLE", "MYCOLUMN"));
  ColumnEntry entry3(ColumnKey("mydb", "mytable", "mycolumn"));

  std::string key1 = entry1.key().str();
  std::string key2 = entry2.key().str();
  std::string key3 = entry3.key().str();

  // With lower_case_table_names=2: Names normalized to lowercase for comparison
  EXPECT_EQ(key1, "mydb.mytable.mycolumn");
  EXPECT_EQ(key2, "mydb.mytable.mycolumn");
  EXPECT_EQ(key3, "mydb.mytable.mycolumn");

  // All keys should be identical (normalized for comparison)
  EXPECT_EQ(key1, key2);
  EXPECT_EQ(key2, key3);

  lower_case_table_names = original_setting;
}

// Test key immutability - private key fields prevent corruption
TEST_F(VictionaryClientTest, KeyImmutability) {
  PropertyEntry entry(PropertyKey("test_prop"));
  auto original_key = entry.key().str();

  // Modify public fields (non-key fields)
  entry.value = "new_value";
  entry.description = "new_description";

  // Key should remain unchanged (name is private)
  EXPECT_EQ(entry.key().str(), original_key);

  // Test ColumnEntry immutability
  ColumnEntry col_entry(ColumnKey("db", "table", "column"));
  auto original_col_key = col_entry.key().str();

  // Modify public fields (non-key fields)
  col_entry.extension_name = "new_extension";
  col_entry.extension_version = "2.0";
  col_entry.type_name = "NEW_TYPE";

  // Key should remain unchanged (db/table/column names are private)
  EXPECT_EQ(col_entry.key().str(), original_col_key);
}

// Test KeyPrefix types
TEST_F(VictionaryClientTest, KeyPrefixTypes) {
  int original_setting = test_get_lower_case_table_names();
  test_set_lower_case_table_names(1);  // Use lowercase for consistent behavior

  // Test ColumnKeyPrefix
  {
    ColumnKeyPrefix prefix("MyDB", "MyTable");
    EXPECT_EQ(prefix.db(), "MyDB");
    EXPECT_EQ(prefix.table(), "MyTable");
    // Prefix string should be normalized and end with "."
    EXPECT_EQ(prefix.str(), "mydb.mytable.");
  }

  // Test TypeDescriptorKeyPrefix with type name only
  {
    TypeDescriptorKeyPrefix prefix("COMPLEX");
    EXPECT_EQ(prefix.type_name(), "COMPLEX");
    EXPECT_EQ(prefix.extension_name(), "");
    EXPECT_EQ(prefix.str(), "complex.");
  }

  // Test TypeDescriptorKeyPrefix with type name and extension name
  {
    TypeDescriptorKeyPrefix prefix("VECTOR", "MyExt");
    EXPECT_EQ(prefix.type_name(), "VECTOR");
    EXPECT_EQ(prefix.extension_name(), "MyExt");
    EXPECT_EQ(prefix.str(), "vector.myext.");
  }

  // Test that prefix matches keys correctly
  {
    ColumnKeyPrefix prefix("testdb", "testtable");
    ColumnKey key1("testdb", "testtable", "col1");
    ColumnKey key2("testdb", "testtable", "col2");
    ColumnKey key3("testdb", "other_table", "col1");

    // key1 and key2 should start with prefix, key3 should not
    EXPECT_EQ(key1.str().substr(0, prefix.str().length()), prefix.str());
    EXPECT_EQ(key2.str().substr(0, prefix.str().length()), prefix.str());
    EXPECT_NE(key3.str().substr(0, prefix.str().length()), prefix.str());
  }

  test_set_lower_case_table_names(original_setting);
}

// Test prefix queries work correctly with current normalization
TEST_F(VictionaryClientTest, PrefixQueriesBasic) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x1230);

  {
    auto guard = client_->get_write_lock();

    // Add entries with consistent casing
    ColumnEntry entry1(ColumnKey("testdb", "testtable", "col1"));
    entry1.extension_name = "test_ext";
    entry1.extension_version = "1.0";
    entry1.type_name = "TEST_TYPE";

    ColumnEntry entry2(ColumnKey("testdb", "testtable", "col2"));
    entry2.extension_name = "test_ext";
    entry2.extension_version = "1.0";
    entry2.type_name = "TEST_TYPE";

    client_->columns().MarkForInsertion(*fake_thd, std::move(entry1));
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry2));
  }

  // Commit the entries
  // Grabs its own lock
  client_->commit_all_tables(fake_thd);

  {
    // Prefix query should find both entries for the same table
    auto guard = client_->get_read_lock();
    auto results = client_->columns().get_prefix_committed(
        ColumnKeyPrefix("testdb", "testtable"));
    EXPECT_EQ(results.size(), 2);
  }

  // Clean up - grabs its own lock
  client_->clear_all_tables();
}

// Test normalization functions with all lower_case_table_names settings
TEST_F(VictionaryClientTest, NormalizationFunctionsAllSettings) {
  int original_setting = lower_case_table_names;

  // Column names should ALWAYS be case-insensitive regardless of setting
  for (int setting : {0, 1, 2}) {
    lower_case_table_names = setting;
    EXPECT_EQ(normalize_column_name("MyColumn"), "mycolumn");
    EXPECT_EQ(normalize_column_name("MYCOLUMN"), "mycolumn");
    EXPECT_EQ(normalize_column_name("mycolumn"), "mycolumn");
  }

  // Extension names should ALWAYS be case-insensitive regardless of setting
  for (int setting : {0, 1, 2}) {
    lower_case_table_names = setting;
    EXPECT_EQ(normalize_extension_name("MyExt"), "myext");
    EXPECT_EQ(normalize_extension_name("MYEXT"), "myext");
    EXPECT_EQ(normalize_extension_name("myext"), "myext");
  }

  // Type names should ALWAYS be case-insensitive regardless of setting
  for (int setting : {0, 1, 2}) {
    lower_case_table_names = setting;
    EXPECT_EQ(normalize_type_name("COMPLEX"), "complex");
    EXPECT_EQ(normalize_type_name("Complex"), "complex");
    EXPECT_EQ(normalize_type_name("complex"), "complex");
  }

  // Setting 0: Case-sensitive (preserve case)
  test_set_lower_case_table_names(0);
  EXPECT_EQ(normalize_database_name("MyDB"), "MyDB");     // Preserved
  EXPECT_EQ(normalize_table_name("MyTable"), "MyTable");  // Preserved

  // Setting 1: Case-insensitive (convert to lowercase)
  test_set_lower_case_table_names(1);
  EXPECT_EQ(normalize_database_name("MyDB"), "mydb");     // Lowercased
  EXPECT_EQ(normalize_table_name("MyTable"), "mytable");  // Lowercased

  // Setting 2: Case-insensitive (convert to lowercase)
  test_set_lower_case_table_names(2);
  EXPECT_EQ(normalize_database_name("MyDB"), "mydb");     // Lowercased
  EXPECT_EQ(normalize_table_name("MyTable"), "mytable");  // Lowercased

  test_set_lower_case_table_names(original_setting);
}

// ===== Phase 1: Operation Tracking Tests =====

// Test basic DELETE operation
TEST_F(VictionaryClientTest, DeleteOperation) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x5678);

  // First, add a column to committed state
  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry(key);
  entry.extension_name = "test_ext";
  entry.extension_version = "1.0";
  entry.type_name = "COMPLEX";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry));
  }
  client_->commit_all_tables(fake_thd);

  // Verify it exists
  {
    auto guard = client_->get_read_lock();
    EXPECT_NE(client_->columns().get_committed(key), nullptr);
  }

  // Now mark for deletion
  {
    auto guard = client_->get_write_lock();
    EXPECT_FALSE(client_->columns().MarkForDeletion(*fake_thd, key));
  }

  // Commit the deletion
  client_->commit_all_tables(fake_thd);

  // Verify it's gone
  {
    auto guard = client_->get_read_lock();
    EXPECT_EQ(client_->columns().get_committed(key), nullptr);
  }
}

// Test UPDATE operation with column rename
TEST_F(VictionaryClientTest, UpdateOperationWithRename) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x9AB0);

  // Add a column
  ColumnKey old_key("db1", "table1", "old_name");
  ColumnEntry old_entry(old_key);
  old_entry.extension_name = "test_ext";
  old_entry.extension_version = "1.0";
  old_entry.type_name = "COMPLEX";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(old_entry));
  }
  client_->commit_all_tables(fake_thd);

  // Mark for update (rename)
  ColumnKey new_key("db1", "table1", "new_name");
  ColumnEntry new_entry(new_key);
  new_entry.extension_name = "test_ext";
  new_entry.extension_version = "1.0";
  new_entry.type_name = "COMPLEX";

  {
    auto guard = client_->get_write_lock();
    EXPECT_FALSE(client_->columns().MarkForUpdate(
        *fake_thd, std::move(new_entry), old_key));
  }

  // Commit the update
  client_->commit_all_tables(fake_thd);

  // Verify old key is gone, new key exists
  {
    auto guard = client_->get_read_lock();
    EXPECT_EQ(client_->columns().get_committed(old_key), nullptr);
    EXPECT_NE(client_->columns().get_committed(new_key), nullptr);
  }
}

// Test multiple operations on same key (DELETE then INSERT)
TEST_F(VictionaryClientTest, MultipleOperationsSameKey) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xDEF0);

  // Add initial column
  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry1(key);
  entry1.extension_name = "ext1";
  entry1.extension_version = "1.0";
  entry1.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry1));
  }
  client_->commit_all_tables(fake_thd);

  // Now in same transaction: DELETE then INSERT with same key but different
  // type
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd2 = reinterpret_cast<THD *>(0xDEF8);

  ColumnEntry entry2(key);
  entry2.extension_name = "ext2";
  entry2.extension_version = "2.0";
  entry2.type_name = "TYPE2";

  {
    auto guard = client_->get_write_lock();
    EXPECT_FALSE(client_->columns().MarkForDeletion(*fake_thd2, key));
    client_->columns().MarkForInsertion(*fake_thd2, std::move(entry2));
  }

  // Commit should apply DELETE first, then INSERT
  client_->commit_all_tables(fake_thd2);

  // Verify final state has the new entry
  {
    auto guard = client_->get_read_lock();
    const ColumnEntry *result = client_->columns().get_committed(key);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->extension_name, "ext2");
    EXPECT_EQ(result->type_name, "TYPE2");
  }
}

// Test MarkTableForRename
TEST_F(VictionaryClientTest, MarkTableForRename) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x1110);

  // Define keys and common metadata
  ColumnKey old_key1("db1", "old_table", "col1");
  ColumnKey old_key2("db1", "old_table", "col2");
  ColumnKey new_key1("db1", "new_table", "col1");
  ColumnKey new_key2("db1", "new_table", "col2");

  // Add multiple columns for a table
  ColumnEntry col1(old_key1);
  col1.extension_name = "ext1";
  col1.extension_version = "1.0";
  col1.type_name = "TYPE1";

  ColumnEntry col2(old_key2);
  col2.extension_name = "ext1";
  col2.extension_version = "1.0";
  col2.type_name = "TYPE2";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(col1));
    client_->columns().MarkForInsertion(*fake_thd, std::move(col2));
  }
  client_->commit_all_tables(fake_thd);

  // Rename the table - mark each column for update
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd2 = reinterpret_cast<THD *>(0x2220);
  {
    auto guard = client_->get_write_lock();

    // Rename col1
    ColumnEntry new_col1(new_key1);
    new_col1.extension_name = "ext1";
    new_col1.extension_version = "1.0";
    new_col1.type_name = "TYPE1";
    EXPECT_FALSE(client_->columns().MarkForUpdate(
        *fake_thd2, std::move(new_col1), old_key1));

    // Rename col2
    ColumnEntry new_col2(new_key2);
    new_col2.extension_name = "ext1";
    new_col2.extension_version = "1.0";
    new_col2.type_name = "TYPE2";
    EXPECT_FALSE(client_->columns().MarkForUpdate(
        *fake_thd2, std::move(new_col2), old_key2));
  }

  // Commit the rename
  client_->commit_all_tables(fake_thd2);

  // Verify old entries are gone, new entries exist
  {
    auto guard = client_->get_read_lock();
    EXPECT_EQ(client_->columns().get_committed(old_key1), nullptr);
    EXPECT_EQ(client_->columns().get_committed(old_key2), nullptr);
    EXPECT_NE(client_->columns().get_committed(new_key1), nullptr);
    EXPECT_NE(client_->columns().get_committed(new_key2), nullptr);
  }
}

// Test GetCustomColumnsForTable
TEST_F(VictionaryClientTest, GetCustomColumnsForTable) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x3330);

  // Add columns for multiple tables
  ColumnEntry t1c1(ColumnKey("db1", "table1", "col1"));
  t1c1.extension_name = "ext1";
  t1c1.extension_version = "1.0";
  t1c1.type_name = "TYPE1";

  ColumnEntry t1c2(ColumnKey("db1", "table1", "col2"));
  t1c2.extension_name = "ext1";
  t1c2.extension_version = "1.0";
  t1c2.type_name = "TYPE2";

  ColumnEntry t2c1(ColumnKey("db1", "table2", "col1"));
  t2c1.extension_name = "ext1";
  t2c1.extension_version = "1.0";
  t2c1.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(t1c1));
    client_->columns().MarkForInsertion(*fake_thd, std::move(t1c2));
    client_->columns().MarkForInsertion(*fake_thd, std::move(t2c1));
  }
  client_->commit_all_tables(fake_thd);

  // Get columns for table1
  {
    auto guard = client_->get_read_lock();
    auto columns = client_->GetCustomColumnsForTable("db1", "table1");
    EXPECT_EQ(columns.size(), 2);

    // Verify correct columns returned
    bool found_col1 = false, found_col2 = false;
    for (const ColumnEntry *col : columns) {
      if (col->column_name() == "col1") found_col1 = true;
      if (col->column_name() == "col2") found_col2 = true;
    }
    EXPECT_TRUE(found_col1);
    EXPECT_TRUE(found_col2);
  }

  // Get columns for table2
  {
    auto guard = client_->get_read_lock();
    auto columns = client_->GetCustomColumnsForTable("db1", "table2");
    EXPECT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0]->column_name(), "col1");
  }
}

// Test transaction rollback discards operations
TEST_F(VictionaryClientTest, TransactionRollback) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x4448);

  // Add a column and commit it
  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry(key);
  entry.extension_name = "ext1";
  entry.extension_version = "1.0";
  entry.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry));
  }
  client_->commit_all_tables(fake_thd);

  // Start new transaction, mark for deletion
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd2 = reinterpret_cast<THD *>(0x5558);
  {
    auto guard = client_->get_write_lock();
    EXPECT_FALSE(client_->columns().MarkForDeletion(*fake_thd2, key));
  }

  // Rollback instead of commit
  client_->rollback_all_tables(fake_thd2);

  // Entry should still exist (deletion was rolled back)
  {
    auto guard = client_->get_read_lock();
    EXPECT_NE(client_->columns().get_committed(key), nullptr);
  }
}

// Test operation ordering with complex scenario
TEST_F(VictionaryClientTest, ComplexOperationOrdering) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x6660);

  // Scenario: RENAME col1→col2, DROP col2 (original), ADD col1 (new)
  // This tests that operations are applied in the correct order

  // Define keys
  ColumnKey key1("db1", "table1", "col1");
  ColumnKey key2("db1", "table1", "col2");

  // Setup: col1 and col2 exist
  ColumnEntry col1(key1);
  col1.extension_name = "ext1";
  col1.extension_version = "1.0";
  col1.type_name = "TYPE1";

  ColumnEntry col2(key2);
  col2.extension_name = "ext1";
  col2.extension_version = "1.0";
  col2.type_name = "TYPE2";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(col1));
    client_->columns().MarkForInsertion(*fake_thd, std::move(col2));
  }
  client_->commit_all_tables(fake_thd);

  // Transaction: RENAME col1→col2, DELETE col2, INSERT col1
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd2 = reinterpret_cast<THD *>(0x7778);

  // Operation 1: Rename col1 to col2 (overwrites old col2)
  ColumnEntry col1_renamed(key2);
  col1_renamed.extension_name = "ext1";
  col1_renamed.extension_version = "1.0";
  col1_renamed.type_name = "TYPE1";  // Keep TYPE1 from col1

  // Operation 3: Add new col1
  ColumnEntry new_col1(key1);
  new_col1.extension_name = "ext2";
  new_col1.extension_version = "2.0";
  new_col1.type_name = "TYPE3";

  {
    auto guard = client_->get_write_lock();
    EXPECT_FALSE(client_->columns().MarkForUpdate(
        *fake_thd2, std::move(col1_renamed), key1));

    // Operation 2: Delete original col2
    EXPECT_FALSE(client_->columns().MarkForDeletion(*fake_thd2, key2));

    // Operation 3: Insert new col1
    client_->columns().MarkForInsertion(*fake_thd2, std::move(new_col1));
  }

  // Commit and verify final state
  client_->commit_all_tables(fake_thd2);

  {
    auto guard = client_->get_read_lock();
    // Should have: col1 (TYPE3) and col2 (TYPE1 from original col1)
    const ColumnEntry *final_col1 = client_->columns().get_committed(key1);
    const ColumnEntry *final_col2 = client_->columns().get_committed(key2);

    ASSERT_NE(final_col1, nullptr);
    EXPECT_EQ(final_col1->type_name, "TYPE3");

    // Note: final_col2 might be nullptr if the DELETE removed it after the
    // UPDATE. This tests the actual operation ordering behavior
    (void)final_col2;  // Suppress unused variable warning
  }
}

// Test ExtensionEntry operations (added in PR 129)
TEST_F(VictionaryClientTest, ExtensionEntry_BasicOperations) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x1238);

  // Create test extensions
  ExtensionKey key1("myext");
  ExtensionKey key2("otherext");
  ExtensionEntry ext1(key1);
  ext1.extension_version = "1.0.0";
  ext1.veb_sha256 = "abc123";
  ExtensionEntry ext2(key2);
  ext2.extension_version = "2.0.0";
  ext2.veb_sha256 = "def456";

  {
    // Mark for insertion
    auto guard = client_->get_write_lock();
    client_->extensions().MarkForInsertion(*fake_thd, std::move(ext1));
    client_->extensions().MarkForInsertion(*fake_thd, std::move(ext2));
  }

  // Commit changes
  client_->commit_all_tables(fake_thd);

  {
    // Verify both extensions exist
    auto guard = client_->get_read_lock();
    const ExtensionEntry *retrieved1 =
        client_->extensions().get_committed(key1.str());
    ASSERT_NE(retrieved1, nullptr);
    EXPECT_EQ(retrieved1->extension_name(), "myext");
    EXPECT_EQ(retrieved1->extension_version, "1.0.0");

    const ExtensionEntry *retrieved2 =
        client_->extensions().get_committed(key2.str());
    ASSERT_NE(retrieved2, nullptr);
    EXPECT_EQ(retrieved2->extension_name(), "otherext");
    EXPECT_EQ(retrieved2->extension_version, "2.0.0");
  }

  client_->clear_all_tables();
}

// Test get_all_committed for extensions
TEST_F(VictionaryClientTest, ExtensionEntry_GetAllCommitted) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x5678);

  // Add multiple extensions
  ExtensionEntry ext1(ExtensionKey("ext1"));
  ext1.extension_version = "1.0.0";
  ext1.veb_sha256 = "sha256_1";
  ExtensionEntry ext2(ExtensionKey("ext2"));
  ext2.extension_version = "2.0.0";
  ext2.veb_sha256 = "sha256_2";
  ExtensionEntry ext3(ExtensionKey("ext3"));
  ext3.extension_version = "3.0.0";
  ext3.veb_sha256 = "sha256_3";

  {
    auto guard = client_->get_write_lock();
    client_->extensions().MarkForInsertion(*fake_thd, std::move(ext1));
    client_->extensions().MarkForInsertion(*fake_thd, std::move(ext2));
    client_->extensions().MarkForInsertion(*fake_thd, std::move(ext3));
  }

  client_->commit_all_tables(fake_thd);

  {
    // Get all committed extensions
    auto guard = client_->get_read_lock();
    std::vector<const ExtensionEntry *> all_extensions =
        client_->extensions().get_all_committed();

    EXPECT_EQ(all_extensions.size(), 3);

    // Verify all three are present (order may vary)
    std::set<std::string> extension_names;
    for (const ExtensionEntry *entry : all_extensions) {
      extension_names.insert(entry->extension_name());
    }

    EXPECT_TRUE(extension_names.count("ext1") > 0);
    EXPECT_TRUE(extension_names.count("ext2") > 0);
    EXPECT_TRUE(extension_names.count("ext3") > 0);
  }

  client_->clear_all_tables();
}

// ===== Tests for get() method with uncommitted operations =====

// Test get() with multiple interleaved keys in same transaction
// Validates that operations on different keys don't interfere with each other
// Tests scenarios: UPDATE, DELETE+INSERT (recreate), and DELETE
TEST_F(VictionaryClientTest, GetMultipleInterleavedKeys) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xAAA0);

  // Define keys first for later reference
  ColumnKey key1("db1", "table1", "col1");
  ColumnKey key2("db1", "table1", "col2");
  ColumnKey key3("db1", "table1", "col3");
  ColumnKey key4("db1", "table1", "col4");

  // Create entries for four different columns
  ColumnEntry col1_v1(key1);
  col1_v1.extension_name = "ext1";
  col1_v1.extension_version = "1.0";
  col1_v1.type_name = "TYPE1";

  ColumnEntry col2_v1(key2);
  col2_v1.extension_name = "ext1";
  col2_v1.extension_version = "1.0";
  col2_v1.type_name = "TYPE2";

  ColumnEntry col3_v1(key3);
  col3_v1.extension_name = "ext1";
  col3_v1.extension_version = "1.0";
  col3_v1.type_name = "TYPE3";

  ColumnEntry col4_v1(key4);
  col4_v1.extension_name = "ext1";
  col4_v1.extension_version = "1.0";
  col4_v1.type_name = "TYPE4";

  // Updated versions
  ColumnEntry col1_v2(key1);
  col1_v2.extension_name = "ext2";
  col1_v2.extension_version = "2.0";
  col1_v2.type_name = "TYPE1_UPDATED";

  ColumnEntry col2_v2(key2);
  col2_v2.extension_name = "ext2";
  col2_v2.extension_version = "2.0";
  col2_v2.type_name = "TYPE2_UPDATED";

  // Recreated version of col3 (after delete)
  ColumnEntry col3_v2(key3);
  col3_v2.extension_name = "ext3";
  col3_v2.extension_version = "3.0";
  col3_v2.type_name = "TYPE3_RECREATED";

  {
    auto guard = client_->get_write_lock();

    // Interleave operations on different keys:
    // - col1: INSERT -> UPDATE (simple update)
    // - col2: INSERT -> UPDATE (simple update)
    // - col3: INSERT -> DELETE -> INSERT (delete and recreate)
    // - col4: INSERT -> DELETE (just delete)
    client_->columns().MarkForInsertion(*fake_thd, std::move(col1_v1));
    client_->columns().MarkForInsertion(*fake_thd, std::move(col2_v1));
    client_->columns().MarkForUpdate(*fake_thd, std::move(col1_v2),
                                     key1);  // Update col1
    client_->columns().MarkForInsertion(*fake_thd, std::move(col3_v1));
    client_->columns().MarkForUpdate(*fake_thd, std::move(col2_v2),
                                     key2);  // Update col2
    client_->columns().MarkForInsertion(*fake_thd, std::move(col4_v1));
    client_->columns().MarkForDeletion(*fake_thd, key3);  // Delete col3
    client_->columns().MarkForInsertion(*fake_thd,
                                        std::move(col3_v2));  // Recreate col3
    client_->columns().MarkForDeletion(*fake_thd, key4);      // Delete col4

    // Verify each key returns its most recent version
    const ColumnEntry *result1 = client_->columns().get(fake_thd, key1.str());
    ASSERT_NE(result1, nullptr);
    EXPECT_EQ(result1->type_name, "TYPE1_UPDATED");
    EXPECT_EQ(result1->extension_name, "ext2");

    const ColumnEntry *result2 = client_->columns().get(fake_thd, key2.str());
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result2->type_name, "TYPE2_UPDATED");
    EXPECT_EQ(result2->extension_name, "ext2");

    // col3 was deleted then recreated - should see recreated version
    const ColumnEntry *result3 = client_->columns().get(fake_thd, key3.str());
    ASSERT_NE(result3, nullptr);
    EXPECT_EQ(result3->type_name, "TYPE3_RECREATED");
    EXPECT_EQ(result3->extension_name, "ext3");

    // col4 was deleted - should return nullptr
    const ColumnEntry *result4 = client_->columns().get(fake_thd, key4.str());
    EXPECT_EQ(result4, nullptr);

    // Verify that committed state is still empty for all keys
    EXPECT_EQ(client_->columns().get_committed(key1.str()), nullptr);
    EXPECT_EQ(client_->columns().get_committed(key2.str()), nullptr);
    EXPECT_EQ(client_->columns().get_committed(key3.str()), nullptr);
    EXPECT_EQ(client_->columns().get_committed(key4.str()), nullptr);
  }

  // Commit and verify final state
  client_->commit_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();

    // col1 should be updated
    const ColumnEntry *committed1 =
        client_->columns().get_committed(key1.str());
    ASSERT_NE(committed1, nullptr);
    EXPECT_EQ(committed1->type_name, "TYPE1_UPDATED");

    // col2 should be updated
    const ColumnEntry *committed2 =
        client_->columns().get_committed(key2.str());
    ASSERT_NE(committed2, nullptr);
    EXPECT_EQ(committed2->type_name, "TYPE2_UPDATED");

    // col3 should be recreated version
    const ColumnEntry *committed3 =
        client_->columns().get_committed(key3.str());
    ASSERT_NE(committed3, nullptr);
    EXPECT_EQ(committed3->type_name, "TYPE3_RECREATED");
    EXPECT_EQ(committed3->extension_name, "ext3");

    // col4 should be deleted
    const ColumnEntry *committed4 =
        client_->columns().get_committed(key4.str());
    EXPECT_EQ(committed4, nullptr);
  }

  client_->clear_all_tables();
}

// Test get() returns uncommitted INSERT before commit
TEST_F(VictionaryClientTest, GetUncommittedInsert) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xAAA8);

  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry(key);
  entry.extension_name = "ext1";
  entry.extension_version = "1.0";
  entry.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry));

    // get() should return the uncommitted entry
    const ColumnEntry *result = client_->columns().get(fake_thd, key.str());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->type_name, "TYPE1");

    // get_committed() should still return nullptr
    EXPECT_EQ(client_->columns().get_committed(key.str()), nullptr);
  }

  // After commit, both should return the entry
  client_->commit_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();
    const ColumnEntry *result1 = client_->columns().get(fake_thd, key.str());
    const ColumnEntry *result2 = client_->columns().get_committed(key.str());

    ASSERT_NE(result1, nullptr);
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result1->type_name, result2->type_name);
  }

  client_->clear_all_tables();
}

// Test get() returns nullptr for uncommitted DELETE
TEST_F(VictionaryClientTest, GetUncommittedDelete) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd1 = reinterpret_cast<THD *>(0xBBB0);
  THD *fake_thd2 = reinterpret_cast<THD *>(0xCCC8);

  // First, insert and commit an entry
  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry(key);
  entry.extension_name = "ext1";
  entry.extension_version = "1.0";
  entry.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd1, std::move(entry));
  }
  client_->commit_all_tables(fake_thd1);

  // Now mark for deletion in a new transaction
  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForDeletion(*fake_thd2, key);

    // get() should return nullptr (deleted in this transaction)
    EXPECT_EQ(client_->columns().get(fake_thd2, key.str()), nullptr);

    // But get_committed() should still return the entry
    EXPECT_NE(client_->columns().get_committed(key.str()), nullptr);
  }

  // After commit, both should return nullptr
  client_->commit_all_tables(fake_thd2);

  {
    auto guard = client_->get_read_lock();
    EXPECT_EQ(client_->columns().get(fake_thd2, key.str()), nullptr);
    EXPECT_EQ(client_->columns().get_committed(key.str()), nullptr);
  }

  client_->clear_all_tables();
}

// Test get() returns updated entry for uncommitted UPDATE
TEST_F(VictionaryClientTest, GetUncommittedUpdate) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd1 = reinterpret_cast<THD *>(0xDDD0);
  THD *fake_thd2 = reinterpret_cast<THD *>(0xEEE8);

  // Insert initial entry
  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry(key);
  entry.extension_name = "ext1";
  entry.extension_version = "1.0";
  entry.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd1, std::move(entry));
  }
  client_->commit_all_tables(fake_thd1);

  // Update the entry (change type)
  ColumnEntry updated_entry(key);
  updated_entry.extension_name = "ext2";
  updated_entry.extension_version = "2.0";
  updated_entry.type_name = "TYPE2";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForUpdate(*fake_thd2, std::move(updated_entry), key);

    // get() should return the updated entry
    const ColumnEntry *result = client_->columns().get(fake_thd2, key.str());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->type_name, "TYPE2");
    EXPECT_EQ(result->extension_name, "ext2");

    // get_committed() should still return the old entry
    const ColumnEntry *committed = client_->columns().get_committed(key.str());
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->type_name, "TYPE1");
  }

  client_->clear_all_tables();
}

// Test get() with UPDATE that changes the key (rename)
TEST_F(VictionaryClientTest, GetUncommittedUpdateWithKeyChange) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd1 = reinterpret_cast<THD *>(0xFFF8);
  THD *fake_thd2 = reinterpret_cast<THD *>(0x0008);

  // Insert initial entry
  ColumnKey old_key("db1", "table1", "old_col");
  ColumnKey new_key("db1", "table1", "new_col");
  ColumnEntry old_entry(old_key);
  old_entry.extension_name = "ext1";
  old_entry.extension_version = "1.0";
  old_entry.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd1, std::move(old_entry));
  }
  client_->commit_all_tables(fake_thd1);

  // Rename the column
  ColumnEntry new_entry(new_key);
  new_entry.extension_name = "ext1";
  new_entry.extension_version = "1.0";
  new_entry.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForUpdate(*fake_thd2, std::move(new_entry), old_key);

    // get() with old key should return the renamed entry
    const ColumnEntry *result_old =
        client_->columns().get(fake_thd2, old_key.str());
    ASSERT_NE(result_old, nullptr);
    EXPECT_EQ(result_old->column_name(), "new_col");

    // get() with new key should also return the entry
    const ColumnEntry *result_new =
        client_->columns().get(fake_thd2, new_key.str());
    ASSERT_NE(result_new, nullptr);
    EXPECT_EQ(result_new->column_name(), "new_col");

    // get_committed() with old key should still return old entry
    const ColumnEntry *committed_old =
        client_->columns().get_committed(old_key.str());
    ASSERT_NE(committed_old, nullptr);
    EXPECT_EQ(committed_old->column_name(), "old_col");

    // get_committed() with new key should return nullptr
    EXPECT_EQ(client_->columns().get_committed(new_key.str()), nullptr);
  }

  client_->clear_all_tables();
}

// Test get() returns most recent operation when multiple operations on same key
TEST_F(VictionaryClientTest, GetMultipleUncommittedOperations) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x0020);

  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry1(key);
  entry1.extension_name = "ext1";
  entry1.extension_version = "1.0";
  entry1.type_name = "TYPE1";

  ColumnEntry entry2(key);
  entry2.extension_name = "ext2";
  entry2.extension_version = "2.0";
  entry2.type_name = "TYPE2";

  ColumnEntry entry3(key);
  entry3.extension_name = "ext3";
  entry3.extension_version = "3.0";
  entry3.type_name = "TYPE3";

  {
    auto guard = client_->get_write_lock();

    // INSERT, then UPDATE, then UPDATE again
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry1));
    client_->columns().MarkForUpdate(*fake_thd, std::move(entry2), key);
    client_->columns().MarkForUpdate(*fake_thd, std::move(entry3), key);

    // get() should return the most recent (entry3)
    const ColumnEntry *result = client_->columns().get(fake_thd, key.str());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->type_name, "TYPE3");
    EXPECT_EQ(result->extension_name, "ext3");
  }

  client_->clear_all_tables();
}

// Test get() returns nullptr after DELETE even if there were prior operations
TEST_F(VictionaryClientTest, GetDeleteAfterMultipleOperations) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x0030);

  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry1(key);
  entry1.extension_name = "ext1";
  entry1.extension_version = "1.0";
  entry1.type_name = "TYPE1";

  ColumnEntry entry2(key);
  entry2.extension_name = "ext2";
  entry2.extension_version = "2.0";
  entry2.type_name = "TYPE2";

  {
    auto guard = client_->get_write_lock();

    // INSERT, UPDATE, then DELETE
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry1));
    client_->columns().MarkForUpdate(*fake_thd, std::move(entry2), key);
    client_->columns().MarkForDeletion(*fake_thd, key);

    // get() should return nullptr (most recent operation is DELETE)
    EXPECT_EQ(client_->columns().get(fake_thd, key.str()), nullptr);
  }

  client_->clear_all_tables();
}

// Test get() with nullptr THD falls back to get_committed()
TEST_F(VictionaryClientTest, GetWithNullTHD) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x0040);

  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry(key);
  entry.extension_name = "ext1";
  entry.extension_version = "1.0";
  entry.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry));
  }
  client_->commit_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();

    // get() with nullptr THD should behave like get_committed()
    const ColumnEntry *result1 = client_->columns().get(nullptr, key.str());
    const ColumnEntry *result2 = client_->columns().get_committed(key.str());

    ASSERT_NE(result1, nullptr);
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result1->type_name, result2->type_name);
  }

  client_->clear_all_tables();
}

// Test get() from different THD doesn't see uncommitted operations
TEST_F(VictionaryClientTest, GetFromDifferentTHD) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd1 = reinterpret_cast<THD *>(0x0050);
  THD *fake_thd2 = reinterpret_cast<THD *>(0x0058);

  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry(key);
  entry.extension_name = "ext1";
  entry.extension_version = "1.0";
  entry.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();

    // fake_thd1 marks for insertion
    client_->columns().MarkForInsertion(*fake_thd1, std::move(entry));

    // fake_thd1 should see the uncommitted entry
    const ColumnEntry *result1 = client_->columns().get(fake_thd1, key.str());
    ASSERT_NE(result1, nullptr);
    EXPECT_EQ(result1->type_name, "TYPE1");

    // fake_thd2 should NOT see it (different transaction)
    const ColumnEntry *result2 = client_->columns().get(fake_thd2, key.str());
    EXPECT_EQ(result2, nullptr);
  }

  client_->clear_all_tables();
}

// Test get() after rollback returns committed state
TEST_F(VictionaryClientTest, GetAfterRollback) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd1 = reinterpret_cast<THD *>(0x0070);
  THD *fake_thd2 = reinterpret_cast<THD *>(0x0078);

  // Commit an entry
  ColumnKey key("db1", "table1", "col1");
  ColumnEntry entry1(key);
  entry1.extension_name = "ext1";
  entry1.extension_version = "1.0";
  entry1.type_name = "TYPE1";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd1, std::move(entry1));
  }
  client_->commit_all_tables(fake_thd1);

  // Start new transaction and update
  ColumnEntry entry2(key);
  entry2.extension_name = "ext2";
  entry2.extension_version = "2.0";
  entry2.type_name = "TYPE2";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForUpdate(*fake_thd2, std::move(entry2), key);

    // Before rollback, get() sees updated entry
    const ColumnEntry *result = client_->columns().get(fake_thd2, key.str());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->type_name, "TYPE2");
  }

  // Rollback the update
  client_->rollback_all_tables(fake_thd2);

  {
    auto guard = client_->get_read_lock();

    // After rollback, get() should see original committed entry
    const ColumnEntry *result = client_->columns().get(fake_thd2, key.str());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->type_name, "TYPE1");
  }

  client_->clear_all_tables();
}

// Test read_string_field clears output when field is NULL
TEST_F(VictionaryClientTest, ReadStringFieldClearsOnNull) {
  TABLE_SHARE share;

  // Create a nullable varchar field
  Mock_field_varstring field(&share, "test_field", 100, true /* is_nullable */);

  // Pre-populate the output string with a value
  std::string out = "previous_value";
  EXPECT_EQ(out, "previous_value");

  // Set the field to NULL
  field.set_null();
  EXPECT_TRUE(field.is_null());

  // Read from the null field - should clear the output
  read_string_field(&field, out);

  // The output should be empty (cleared) when field is NULL
  EXPECT_EQ(out, "");
}

// ===== Tests for PersistenceMode::MEMORY_ONLY =====

// Test fixture for MEMORY_ONLY SystemTableMap tests
class MemoryOnlySystemTableMapTest : public ::testing::Test {
 protected:
  void SetUp() override { mysql_rwlock_init(0, &lock_); }

  void TearDown() override { mysql_rwlock_destroy(&lock_); }

  mysql_rwlock_t lock_;
};

// Test that MEMORY_ONLY maps have the correct persistence_mode constant
TEST_F(MemoryOnlySystemTableMapTest, PersistenceModeConstant) {
  villagesql::SystemTableMap<villagesql::TypeDescriptor,
                             villagesql::PersistenceMode::MEMORY_ONLY>
      memory_only_map(&lock_);

  EXPECT_EQ(memory_only_map.persistence_mode,
            villagesql::PersistenceMode::MEMORY_ONLY);
}

// Test that MEMORY_ONLY maps support basic insert/commit operations
TEST_F(MemoryOnlySystemTableMapTest, InsertAndCommit) {
  villagesql::SystemTableMap<villagesql::TypeDescriptor,
                             villagesql::PersistenceMode::MEMORY_ONLY>
      map(&lock_);
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x1230);

  villagesql::TypeDescriptorKey key("MYTYPE", "myext", "1.0");
  villagesql::TypeDescriptor entry(key);

  {
    mysql_rwlock_wrlock(&lock_);

    // Mark for insertion
    EXPECT_FALSE(map.MarkForInsertion(*fake_thd, std::move(entry)));

    // Should be visible via get() but not get_committed()
    const villagesql::TypeDescriptor *uncommitted =
        map.get(fake_thd, key.str());
    ASSERT_NE(uncommitted, nullptr);
    EXPECT_EQ(uncommitted->type_name(), "MYTYPE");

    const villagesql::TypeDescriptor *committed = map.get_committed(key.str());
    EXPECT_EQ(committed, nullptr);

    // Commit
    map.commit(fake_thd);

    // Now should be visible via both
    const villagesql::TypeDescriptor *after_commit =
        map.get_committed(key.str());
    ASSERT_NE(after_commit, nullptr);
    EXPECT_EQ(after_commit->type_name(), "MYTYPE");

    mysql_rwlock_unlock(&lock_);
  }
}

// Test that MEMORY_ONLY maps support rollback
TEST_F(MemoryOnlySystemTableMapTest, InsertAndRollback) {
  villagesql::SystemTableMap<villagesql::TypeDescriptor,
                             villagesql::PersistenceMode::MEMORY_ONLY>
      map(&lock_);
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x5678);

  villagesql::TypeDescriptorKey key("ROLLBACK_TYPE", "ext", "1.0");
  villagesql::TypeDescriptor entry(key);

  {
    mysql_rwlock_wrlock(&lock_);

    // Mark for insertion
    EXPECT_FALSE(map.MarkForInsertion(*fake_thd, std::move(entry)));

    // Should be visible via get()
    const villagesql::TypeDescriptor *uncommitted =
        map.get(fake_thd, key.str());
    ASSERT_NE(uncommitted, nullptr);

    // Rollback
    map.rollback(fake_thd);

    // Should no longer be visible
    const villagesql::TypeDescriptor *after_rollback =
        map.get(fake_thd, key.str());
    EXPECT_EQ(after_rollback, nullptr);

    const villagesql::TypeDescriptor *committed = map.get_committed(key.str());
    EXPECT_EQ(committed, nullptr);

    mysql_rwlock_unlock(&lock_);
  }
}

// Test that MEMORY_ONLY maps support delete operations
TEST_F(MemoryOnlySystemTableMapTest, DeleteAndCommit) {
  villagesql::SystemTableMap<villagesql::TypeDescriptor,
                             villagesql::PersistenceMode::MEMORY_ONLY>
      map(&lock_);
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xDEF0);

  villagesql::TypeDescriptorKey key("DELETE_TYPE", "ext", "1.0");
  villagesql::TypeDescriptor entry(key);

  {
    mysql_rwlock_wrlock(&lock_);

    // Insert and commit
    map.MarkForInsertion(*fake_thd, std::move(entry));
    map.commit(fake_thd);

    // Verify it exists
    ASSERT_NE(map.get_committed(key.str()), nullptr);

    // Mark for deletion
    map.MarkForDeletion(*fake_thd, key);

    // get() should return nullptr (deleted in this txn)
    EXPECT_EQ(map.get(fake_thd, key.str()), nullptr);

    // get_committed() should still show it
    EXPECT_NE(map.get_committed(key.str()), nullptr);

    // Commit the deletion
    map.commit(fake_thd);

    // Now it should be gone from committed too
    EXPECT_EQ(map.get_committed(key.str()), nullptr);

    mysql_rwlock_unlock(&lock_);
  }
}

// Test transaction isolation between THDs for MEMORY_ONLY maps
TEST_F(MemoryOnlySystemTableMapTest, TransactionIsolation) {
  villagesql::SystemTableMap<villagesql::TypeDescriptor,
                             villagesql::PersistenceMode::MEMORY_ONLY>
      map(&lock_);
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *thd_A = reinterpret_cast<THD *>(0x2228);
  THD *thd_B = reinterpret_cast<THD *>(0x3338);

  villagesql::TypeDescriptorKey key("ISOLATED_TYPE", "ext", "1.0");
  villagesql::TypeDescriptor entry(key);

  {
    mysql_rwlock_wrlock(&lock_);

    // THD A inserts
    map.MarkForInsertion(*thd_A, std::move(entry));

    // THD A can see it
    EXPECT_NE(map.get(thd_A, key.str()), nullptr);

    // THD B cannot see it (different transaction)
    EXPECT_EQ(map.get(thd_B, key.str()), nullptr);

    // Commit from THD A
    map.commit(thd_A);

    // Now THD B can see it (committed)
    EXPECT_NE(map.get(thd_B, key.str()), nullptr);

    mysql_rwlock_unlock(&lock_);
  }
}

// Note: reload_from_table and write_uncommitted_to_table do not exist for
// MEMORY_ONLY maps (SFINAE removes them). Attempting to call them would be a
// compile error, which is the intended behavior - there's no backing table.

// ===== Tests for TypeDescriptor in VictionaryClient =====

// Dummy function pointers for TypeDescriptor tests
static bool test_encode(unsigned char *, size_t, const char *, size_t,
                        size_t *) {
  return false;
}
static bool test_decode(const unsigned char *, size_t, char *, size_t,
                        size_t *) {
  return false;
}
static int test_compare(const unsigned char *, size_t, const unsigned char *,
                        size_t) {
  return 0;
}

// Test TypeDescriptor operations through VictionaryClient
TEST_F(VictionaryClientTest, TypeDescriptorOperations) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xDE58);

  // Create a TypeDescriptor
  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("COMPLEX", "test_ext", "1.0.0"),
      VEF_PROTOCOL_1,
      1,                  // implementation_type
      16,                 // persisted_length
      256,                // max_decode_buffer_length
      0,                  // max_persisted_length
      LengthKind::Fixed,  // length_kind
      villagesql::EncodeFunction(test_encode),
      villagesql::DecodeFunction(test_decode),
      villagesql::CompareFunction(test_compare));

  {
    auto guard = client_->get_write_lock();

    // Mark for insertion
    EXPECT_FALSE(client_->type_descriptors().MarkForInsertion(*fake_thd,
                                                              std::move(desc)));

    // Create another descriptor to look up the key
    villagesql::TypeDescriptorKey key("COMPLEX", "test_ext", "1.0.0");

    // Should be visible via get() but not get_committed()
    const villagesql::TypeDescriptor *uncommitted =
        client_->type_descriptors().get(fake_thd, key.str());
    ASSERT_NE(uncommitted, nullptr);
    EXPECT_EQ(uncommitted->type_name(), "COMPLEX");
    EXPECT_EQ(uncommitted->extension_name(), "test_ext");
    EXPECT_EQ(uncommitted->persisted_length(), 16);

    const villagesql::TypeDescriptor *committed =
        client_->type_descriptors().get_committed(key.str());
    EXPECT_EQ(committed, nullptr);
  }

  // Commit
  client_->commit_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();

    villagesql::TypeDescriptorKey key("COMPLEX", "test_ext", "1.0.0");
    const villagesql::TypeDescriptor *committed =
        client_->type_descriptors().get_committed(key.str());
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->type_name(), "COMPLEX");
    EXPECT_EQ(committed->extension_name(), "test_ext");
    EXPECT_EQ(committed->implementation_type(), 1);
    EXPECT_EQ(committed->persisted_length(), 16);
    EXPECT_EQ(committed->max_decode_buffer_length(), 256);
  }
}

// Test TypeDescriptor rollback through VictionaryClient
TEST_F(VictionaryClientTest, TypeDescriptorRollback) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0x8018);

  villagesql::TypeDescriptor desc(
      villagesql::TypeDescriptorKey("ROLLBACK_TYPE", "ext", "1.0"),
      VEF_PROTOCOL_1, 0, 8, 64,
      0,                  // max_persisted_length
      LengthKind::Fixed,  // length_kind
      villagesql::EncodeFunction(test_encode),
      villagesql::DecodeFunction(test_decode),
      villagesql::CompareFunction(test_compare));

  {
    auto guard = client_->get_write_lock();
    client_->type_descriptors().MarkForInsertion(*fake_thd, std::move(desc));

    // Verify it's visible
    villagesql::TypeDescriptorKey key("ROLLBACK_TYPE", "ext", "1.0");
    EXPECT_NE(client_->type_descriptors().get(fake_thd, key.str()), nullptr);
  }

  // Rollback instead of commit
  client_->rollback_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();
    villagesql::TypeDescriptorKey key("ROLLBACK_TYPE", "ext", "1.0");
    EXPECT_EQ(client_->type_descriptors().get_committed(key.str()), nullptr);
  }
}

// ===== Tests for acquire() method with reference counting =====

// Test basic acquire functionality - returns valid pointer for existing entry
TEST_F(VictionaryClientTest, AcquireBasic) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xACE0);

  // Create and commit an entry
  ColumnKey key("testdb", "testtable", "col1");
  ColumnEntry entry(key);
  entry.extension_name = "test_ext";
  entry.type_name = "COMPLEX";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry));
  }
  client_->commit_all_tables(fake_thd);

  // Create a MEM_ROOT for the test
  MEM_ROOT mem_root(PSI_NOT_INSTRUMENTED, 1024);

  {
    auto guard = client_->get_read_lock();

    // Acquire the entry
    const ColumnEntry *acquired =
        client_->columns().acquire(key.str(), mem_root);

    ASSERT_NE(acquired, nullptr);
    EXPECT_EQ(acquired->db_name(), "testdb");
    EXPECT_EQ(acquired->table_name(), "testtable");
    EXPECT_EQ(acquired->column_name(), "col1");
    EXPECT_EQ(acquired->extension_name, "test_ext");
  }

  // MEM_ROOT cleanup happens when mem_root goes out of scope
}

// Test acquire on non-existent key returns nullptr
TEST_F(VictionaryClientTest, AcquireNonExistent) {
  MEM_ROOT mem_root(PSI_NOT_INSTRUMENTED, 1024);

  {
    auto guard = client_->get_read_lock();

    const ColumnEntry *acquired =
        client_->columns().acquire("nonexistent.key.here", mem_root);

    EXPECT_EQ(acquired, nullptr);
  }
}

// Test that acquire keeps entry alive via reference counting
TEST_F(VictionaryClientTest, AcquireKeepsEntryAlive) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xACE8);

  // Create and commit a TypeDescriptor entry
  TypeDescriptorKey key("REFCOUNT_TYPE", "ext", "1.0");
  TypeDescriptor entry(key, VEF_PROTOCOL_1, 0, 42, 0,
                       0,                  // max_persisted_length
                       LengthKind::Fixed,  // length_kind
                       EncodeFunction(test_encode), DecodeFunction(test_decode),
                       CompareFunction(test_compare));

  {
    auto guard = client_->get_write_lock();
    client_->type_descriptors().MarkForInsertion(*fake_thd, std::move(entry));
  }
  client_->commit_all_tables(fake_thd);

  // Create a MEM_ROOT that will outlive the lock scope
  MEM_ROOT mem_root(PSI_NOT_INSTRUMENTED, 1024);
  const TypeDescriptor *acquired = nullptr;

  {
    auto guard = client_->get_read_lock();

    // Before acquire, refcount should be 1 (only in the map)
    EXPECT_EQ(client_->type_descriptors().get_use_count(key.str()), 1);

    acquired = client_->type_descriptors().acquire(key.str(), mem_root);
    ASSERT_NE(acquired, nullptr);

    // After acquire, refcount should be 2 (map + cleanup callback copy)
    EXPECT_EQ(client_->type_descriptors().get_use_count(key.str()), 2);
  }
  // Lock is released here, but acquired pointer should still be valid
  // because the shared_ptr copy is keeping the entry alive

  // Access the entry outside the lock (safe because of refcount)
  EXPECT_EQ(acquired->type_name(), "REFCOUNT_TYPE");
  EXPECT_EQ(acquired->persisted_length(), 42);

  {
    auto guard = client_->get_read_lock();
    // Refcount should still be 2 before Clear
    EXPECT_EQ(client_->type_descriptors().get_use_count(key.str()), 2);
  }

  // Clear the MEM_ROOT - this triggers the cleanup callback
  mem_root.Clear();

  {
    auto guard = client_->get_read_lock();
    // After Clear(), refcount should be back to 1
    EXPECT_EQ(client_->type_descriptors().get_use_count(key.str()), 1);
  }
}

// Test acquire with key_type overload
TEST_F(VictionaryClientTest, AcquireWithKeyType) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xACF0);

  PropertyKey key("test_property");
  PropertyEntry entry(key);
  entry.value = "test_value";

  {
    auto guard = client_->get_write_lock();
    client_->properties().MarkForInsertion(*fake_thd, std::move(entry));
  }
  client_->commit_all_tables(fake_thd);

  MEM_ROOT mem_root(PSI_NOT_INSTRUMENTED, 1024);

  {
    auto guard = client_->get_read_lock();

    // Use the key_type overload
    const PropertyEntry *acquired =
        client_->properties().acquire(key, mem_root);

    ASSERT_NE(acquired, nullptr);
    EXPECT_EQ(acquired->name(), "test_property");
    EXPECT_EQ(acquired->value, "test_value");
  }
}

// Test multiple acquires on the same entry
TEST_F(VictionaryClientTest, AcquireMultipleTimes) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xACF8);

  ColumnKey key("db", "table", "col");
  ColumnEntry entry(key);
  entry.type_name = "TYPE";

  {
    auto guard = client_->get_write_lock();
    client_->columns().MarkForInsertion(*fake_thd, std::move(entry));
  }
  client_->commit_all_tables(fake_thd);

  MEM_ROOT mem_root1(PSI_NOT_INSTRUMENTED, 1024);
  MEM_ROOT mem_root2(PSI_NOT_INSTRUMENTED, 1024);

  const ColumnEntry *acquired1 = nullptr;
  const ColumnEntry *acquired2 = nullptr;

  {
    auto guard = client_->get_read_lock();

    // Acquire twice with different MEM_ROOTs
    acquired1 = client_->columns().acquire(key.str(), mem_root1);
    acquired2 = client_->columns().acquire(key.str(), mem_root2);

    ASSERT_NE(acquired1, nullptr);
    ASSERT_NE(acquired2, nullptr);
    // Both should point to the same entry
    EXPECT_EQ(acquired1, acquired2);
  }

  // Clear one MEM_ROOT - the other should still keep entry alive
  mem_root1.Clear();

  // acquired2 should still be valid (mem_root2 still holds a reference)
  EXPECT_EQ(acquired2->column_name(), "col");

  // Clear the second MEM_ROOT
  mem_root2.Clear();
}

// Test acquire_or_create creates a new entry when not present
TEST_F(VictionaryClientTest, AcquireOrCreateNew) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xA0C0);

  // Create a TypeDescriptor for the type
  TypeDescriptor type_desc(TypeDescriptorKey("AOTEST_TYPE", "test_ext", "1.0.0"));

  {
    auto guard = client_->get_write_lock();
    client_->type_descriptors().MarkForInsertion(*fake_thd,
                                                 std::move(type_desc));
  }
  client_->commit_all_tables(fake_thd);

  MEM_ROOT mem_root(PSI_NOT_INSTRUMENTED, 1024);

  // Create a TypeContextKey
  TypeContextKey ctx_key("AOTEST_TYPE", "test_ext", "1.0.0", TypeParameters());

  {
    auto guard = client_->get_write_lock();

    // Get the TypeDescriptor pointer
    const TypeDescriptor *desc_ptr =
        client_->type_descriptors().get_committed(ctx_key.descriptor_key());
    ASSERT_NE(desc_ptr, nullptr);

    // Verify entry doesn't exist yet
    EXPECT_EQ(client_->type_contexts().get_committed(ctx_key), nullptr);

    // acquire_or_create should create the entry
    const TypeContext *ctx =
        client_->type_contexts().acquire_or_create(ctx_key, mem_root, desc_ptr);

    ASSERT_NE(ctx, nullptr);
    EXPECT_EQ(ctx->type_name(), "AOTEST_TYPE");
    EXPECT_EQ(ctx->extension_name(), "test_ext");
    EXPECT_EQ(ctx->extension_version(), "1.0.0");
    EXPECT_EQ(ctx->descriptor(), desc_ptr);

    // Entry should now exist in the map
    EXPECT_NE(client_->type_contexts().get_committed(ctx_key), nullptr);

    // Reference count should be 2 (map + our acquire)
    EXPECT_EQ(client_->type_contexts().get_use_count(ctx_key.str()), 2);
  }
}

// Test acquire_or_create returns existing entry when present
TEST_F(VictionaryClientTest, AcquireOrCreateExisting) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xA0C8);

  // Create a TypeDescriptor for the type
  TypeDescriptor type_desc(TypeDescriptorKey("AOTEST_TYPE2", "test_ext", "1.0.0"));

  {
    auto guard = client_->get_write_lock();
    client_->type_descriptors().MarkForInsertion(*fake_thd,
                                                 std::move(type_desc));
  }
  client_->commit_all_tables(fake_thd);

  MEM_ROOT mem_root1(PSI_NOT_INSTRUMENTED, 1024);
  MEM_ROOT mem_root2(PSI_NOT_INSTRUMENTED, 1024);

  TypeContextKey ctx_key("AOTEST_TYPE2", "test_ext", "1.0.0", TypeParameters());

  const TypeContext *ctx1 = nullptr;
  const TypeContext *ctx2 = nullptr;

  {
    auto guard = client_->get_write_lock();

    const TypeDescriptor *desc_ptr =
        client_->type_descriptors().get_committed(ctx_key.descriptor_key());
    ASSERT_NE(desc_ptr, nullptr);

    // First call creates the entry
    ctx1 = client_->type_contexts().acquire_or_create(ctx_key, mem_root1,
                                                      desc_ptr);
    ASSERT_NE(ctx1, nullptr);

    // Refcount should be 2
    EXPECT_EQ(client_->type_contexts().get_use_count(ctx_key.str()), 2);

    // Second call should return the same entry
    ctx2 = client_->type_contexts().acquire_or_create(ctx_key, mem_root2,
                                                      desc_ptr);
    ASSERT_NE(ctx2, nullptr);

    // Both pointers should be the same
    EXPECT_EQ(ctx1, ctx2);

    // Refcount should now be 3 (map + two acquires)
    EXPECT_EQ(client_->type_contexts().get_use_count(ctx_key.str()), 3);
  }

  // Clear first MEM_ROOT
  mem_root1.Clear();

  {
    auto guard = client_->get_read_lock();
    // Refcount should be 2 now
    EXPECT_EQ(client_->type_contexts().get_use_count(ctx_key.str()), 2);
  }

  // Clear second MEM_ROOT
  mem_root2.Clear();

  {
    auto guard = client_->get_read_lock();
    // Refcount should be 1 now (only in map)
    EXPECT_EQ(client_->type_contexts().get_use_count(ctx_key.str()), 1);
  }
}

// Test acquire_or_create returns nullptr when create fails (null type_entry)
TEST_F(VictionaryClientTest, AcquireOrCreateFailure) {
  MEM_ROOT mem_root(PSI_NOT_INSTRUMENTED, 1024);

  TypeContextKey ctx_key("NONEXISTENT_TYPE", "test_ext", "1.0.0",
                         TypeParameters());

  {
    auto guard = client_->get_write_lock();

    // Pass nullptr for descriptor - create() should fail
    const TypeContext *ctx =
        client_->type_contexts().acquire_or_create(ctx_key, mem_root, nullptr);

    EXPECT_EQ(ctx, nullptr);

    // Entry should not be in the map
    EXPECT_EQ(client_->type_contexts().get_committed(ctx_key), nullptr);
  }
}

// Test acquire_or_create with TypeParameters
TEST_F(VictionaryClientTest, AcquireOrCreateWithParameters) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xA0D0);

  // Create a TypeDescriptor for the type
  TypeDescriptor type_desc(TypeDescriptorKey("VECTOR", "vector_ext", "2.0.0"));

  {
    auto guard = client_->get_write_lock();
    client_->type_descriptors().MarkForInsertion(*fake_thd,
                                                 std::move(type_desc));
  }
  client_->commit_all_tables(fake_thd);

  MEM_ROOT mem_root(PSI_NOT_INSTRUMENTED, 1024);

  // Create different TypeContextKeys with different parameters
  TypeParameters params1("dimension=1536");
  TypeParameters params2("dimension=3");

  TypeContextKey ctx_key1("VECTOR", "vector_ext", "2.0.0", params1);
  TypeContextKey ctx_key2("VECTOR", "vector_ext", "2.0.0", params2);

  {
    auto guard = client_->get_write_lock();

    TypeDescriptorKey desc_key("VECTOR", "vector_ext", "2.0.0");
    const TypeDescriptor *desc_ptr =
        client_->type_descriptors().get_committed(desc_key);
    ASSERT_NE(desc_ptr, nullptr);

    // Create two contexts with different parameters
    const TypeContext *ctx1 = client_->type_contexts().acquire_or_create(
        ctx_key1, mem_root, desc_ptr);
    const TypeContext *ctx2 = client_->type_contexts().acquire_or_create(
        ctx_key2, mem_root, desc_ptr);

    ASSERT_NE(ctx1, nullptr);
    ASSERT_NE(ctx2, nullptr);

    // They should be different entries
    EXPECT_NE(ctx1, ctx2);

    // Verify parameters are preserved
    EXPECT_EQ(ctx1->parameters().str(), "dimension=1536");
    EXPECT_EQ(ctx2->parameters().str(), "dimension=3");

    // Both should be in the map
    EXPECT_NE(client_->type_contexts().get_committed(ctx_key1), nullptr);
    EXPECT_NE(client_->type_contexts().get_committed(ctx_key2), nullptr);
  }
}

// ===== SpParamEntry / sp_params() Tests =====

// Test SpParamEntry basic construction and defaults
TEST_F(VictionaryClientTest, SpParamEntryBasics) {
  SpParamKey key("mydb", "my_proc", "param1");
  SpParamEntry entry(key, "test_ext", "1.0", "VECTOR", "{}");

  EXPECT_EQ(entry.db_name(), "mydb");
  EXPECT_EQ(entry.sp_name(), "my_proc");
  EXPECT_EQ(entry.param_name(), "param1");
  EXPECT_EQ(entry.extension_name, "test_ext");
  EXPECT_EQ(entry.extension_version, "1.0");
  EXPECT_EQ(entry.type_name, "VECTOR");
  EXPECT_EQ(entry.type_parameters, "{}");
}

// Test SpParamEntry default type_parameters is "{}"
TEST_F(VictionaryClientTest, SpParamEntryDefaultTypeParameters) {
  SpParamKey key("mydb", "my_proc", "param1");
  SpParamEntry entry(key, "test_ext", "1.0", "VECTOR");
  EXPECT_EQ(entry.type_parameters, "{}");

  // Default-constructed entry also has "{}"
  SpParamEntry default_entry;
  EXPECT_EQ(default_entry.type_parameters, "{}");
}

// Test SpParamKey normalization
TEST_F(VictionaryClientTest, SpParamKeyNormalization) {
  int original_setting = lower_case_table_names;
  test_set_lower_case_table_names(1);

  SpParamKey key1("MyDB", "MyProc", "MyParam");
  SpParamKey key2("mydb", "myproc", "myparam");

  // With lower_case_table_names=1 both should produce the same key
  EXPECT_EQ(key1.str(), key2.str());

  test_set_lower_case_table_names(original_setting);
}

// Test sp_params insert, commit, and lookup
TEST_F(VictionaryClientTest, SpParamInsertAndLookup) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xAA00);

  SpParamKey key("testdb", "test_proc", "p1");
  SpParamEntry entry(key, "test_ext", "1.0", "VECTOR", "{\"dim\":\"3\"}");

  {
    auto guard = client_->get_write_lock();
    EXPECT_FALSE(
        client_->sp_params().MarkForInsertion(*fake_thd, std::move(entry)));
  }
  client_->commit_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();
    const SpParamEntry *result = client_->sp_params().get_committed(key);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->extension_name, "test_ext");
    EXPECT_EQ(result->type_name, "VECTOR");
    EXPECT_EQ(result->type_parameters, "{\"dim\":\"3\"}");
  }
}

// Test sp_params prefix query finds all params for a stored procedure
TEST_F(VictionaryClientTest, SpParamPrefixQuery) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xAA08);

  SpParamEntry p1(SpParamKey("db1", "proc1", "param1"), "ext1", "1.0",
                  "VECTOR");
  SpParamEntry p2(SpParamKey("db1", "proc1", "param2"), "ext1", "1.0",
                  "VECTOR");
  SpParamEntry p3(SpParamKey("db1", "proc2", "param1"), "ext1", "1.0",
                  "VECTOR");

  {
    auto guard = client_->get_write_lock();
    client_->sp_params().MarkForInsertion(*fake_thd, std::move(p1));
    client_->sp_params().MarkForInsertion(*fake_thd, std::move(p2));
    client_->sp_params().MarkForInsertion(*fake_thd, std::move(p3));
  }
  client_->commit_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();
    auto results = client_->sp_params().get_prefix_committed(
        SpParamKeyPrefix("db1", "proc1"));
    EXPECT_EQ(results.size(), 2);

    auto all = client_->sp_params().get_prefix_committed(
        SpParamKeyPrefix("db1", "proc2"));
    EXPECT_EQ(all.size(), 1);
  }
}

// Test sp_params delete by key
TEST_F(VictionaryClientTest, SpParamDelete) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xAA10);

  SpParamKey key("db1", "proc1", "param1");
  SpParamEntry entry(key, "ext1", "1.0", "VECTOR");

  {
    auto guard = client_->get_write_lock();
    client_->sp_params().MarkForInsertion(*fake_thd, std::move(entry));
  }
  client_->commit_all_tables(fake_thd);

  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd2 = reinterpret_cast<THD *>(0xAA18);
  {
    auto guard = client_->get_write_lock();
    EXPECT_FALSE(client_->sp_params().MarkForDeletion(*fake_thd2, key));
  }
  client_->commit_all_tables(fake_thd2);

  {
    auto guard = client_->get_read_lock();
    EXPECT_EQ(client_->sp_params().get_committed(key), nullptr);
  }
}

// Test sp_params rollback discards pending entries
TEST_F(VictionaryClientTest, SpParamRollback) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xAA20);

  SpParamKey key("db1", "proc1", "param1");
  SpParamEntry entry(key, "ext1", "1.0", "VECTOR");

  {
    auto guard = client_->get_write_lock();
    client_->sp_params().MarkForInsertion(*fake_thd, std::move(entry));
  }
  client_->rollback_all_tables(fake_thd);

  {
    auto guard = client_->get_read_lock();
    EXPECT_EQ(client_->sp_params().get_committed(key), nullptr);
  }
}

// Test the invariant that persisting SP params does not leave uncommitted
// entries on other system tables. This mirrors the assert in
// PersistCustomSpParams() in metadata_modifier.cc.
TEST_F(VictionaryClientTest, SpParamOnlyHasUncommittedOnSpParams) {
  // Use a fake THD pointer for testing that is 8-byte aligned (for ubsan)
  THD *fake_thd = reinterpret_cast<THD *>(0xAA28);

  SpParamEntry entry(SpParamKey("db1", "proc1", "param1"), "ext1", "1.0",
                     "VECTOR");

  {
    auto guard = client_->get_write_lock();
    client_->sp_params().MarkForInsertion(*fake_thd, std::move(entry));

    // Verify only sp_params has uncommitted entries — the invariant relied upon
    // in PersistCustomSpParams() which only opens custom_sp_params.
    EXPECT_TRUE(client_->sp_params().has_uncommitted(fake_thd));
    EXPECT_FALSE(client_->columns().has_uncommitted(fake_thd));
    EXPECT_FALSE(client_->properties().has_uncommitted(fake_thd));
    EXPECT_FALSE(client_->extensions().has_uncommitted(fake_thd));
  }

  client_->rollback_all_tables(fake_thd);
}

}  // namespace villagesql_unittest
