# Adding New VillageSQL System Tables

This guide provides a checklist for adding new system tables to the VillageSQL victionary infrastructure.

## Complete Checklist

### Create Table Schema (villagesql/schema/villagesql_schema.sql.in)

Add the CREATE TABLE statement for your new table:

```sql
CREATE TABLE IF NOT EXISTS villagesql.your_table (
  key_field VARCHAR(64) NOT NULL COMMENT 'Primary key field',
  data_field VARCHAR(64) NOT NULL COMMENT 'Data field',
  PRIMARY KEY (key_field)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
ROW_FORMAT=DYNAMIC
COMMENT='Description of your table';
```

**Important**: Use VARCHAR(64) for string fields in keys to be consistent with other VillageSQL tables.
This is because there's a limit on key size that affects system tables.

### Create Entry and Key Structs (villagesql/schema/systable/your_table.h)

Create the Key/KeyPrefix struct and Entry struct following the pattern from properties.h or extensions.h:

Store identifier components as entered; build the normalized key with the
`canonical_*` functions from `villagesql/schema/identifier_names.h`, which is
the single home for identifier collation rules. Never pick a collation in the
key struct itself.

```cpp
// Key struct with normalization
struct YourTableKey {
 public:
  YourTableKey() = default;

  explicit YourTableKey(std::string name)
      : name_(std::move(name)),
        normalized_key_(canonical_your_field_name(name_)) {}

  const std::string &str() const { return normalized_key_; }

  // Component accessors (return original values)
  const std::string &name() const { return name_; }

  // Comparison operators for std::map (use normalized key)
  bool operator<(const YourTableKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const YourTableKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string name_;
  std::string normalized_key_;
};

// Only needed if prefix queries are supported for YourTableEntry.
struct YourTableKeyPrefix {
  // See an example like ColumnKeyPrefix.
  // These will construct a key prefix for searching by prefix.
  ...
}

// Entry struct
struct YourTableEntry {
 public:
  using key_type = YourTableKey;
  // Only if prefix queries are supported.
  using key_prefix_type = YourTableKeyPrefix;

  // Non-key public data fields
  std::string data_field;

  // Full constructor with all fields
  YourTableEntry(YourTableKey key, std::string data)
      : data_field(std::move(data)),
        key_(std::move(key)) {}

  // Construct with key only, other fields can be set separately (useful for
  // testing)
  explicit YourTableEntry(YourTableKey key) : key_(std::move(key)) {}

  const YourTableKey &key() const { return key_; }

  // Key parts - delegate to key_
  const std::string &name() const { return key_.name(); }

 protected:
  void set_key(YourTableKey key) { key_ = std::move(key); }
  friend struct TableTraits<YourTableEntry>;

 private:
  YourTableKey key_;
};
```

### 3. Implement TableTraits (villagesql/schema/<subdir>/your_table.{h,cc})

Where <subdir> is systable for a real table, or <descriptor> for memory-backed
extension objects (such as TypeDescriptor).

Specialize TableTraits with serialization methods:

**In .h file:**
```cpp
template <>
struct TableTraits<YourTableEntry> {
  static bool read_from_table(TABLE &table, YourTableEntry &entry);
  static bool write_to_table(TABLE &table, const YourTableEntry &entry);
  static bool update_in_table(TABLE &table, const YourTableEntry &entry,
                              const YourTableKey &old_key);
  static bool delete_from_table(TABLE &table, const YourTableEntry &entry);
};
```

If your entry type supports prefix queries, add a `key_prefix_type` alias to your Entry class and create a corresponding KeyPrefix type (see CustomKeyPrefix in custom_columns.h for an example).

**In .cc file**, implement following the pattern in custom_columns.cc.

### 4. Add to VictionaryClient (villagesql/schema/victionary_client.h)

Add member variable, accessor methods, and initializer:

Search for: `SystemTableMap<ExtensionEntry> m_extensions;` to find where to add your table

```cpp
class VictionaryClient {
 private:
  SystemTableMap<YourTableEntry> m_your_table;

 public:
  // Add accessor methods (follow pattern from extensions())
  SystemTableMap<YourTableEntry> &your_table() { return m_your_table; }
  const SystemTableMap<YourTableEntry> &your_table() const { return m_your_table; }
};
```

And in the constructor initializer list, add: `m_your_table(&m_lock)`

### 5. Add clear() Call (villagesql/schema/victionary_client.cc)

In `VictionaryClient::clear_all_tables()`, add the clear call:

Search for: `m_properties.clear();` and add your table's clear() call:

```cpp
m_properties.clear();
m_columns.clear();
m_types.clear();
m_extensions.clear();
m_your_table.clear();  // Add this
```

### 6. Add to Schema Manager (villagesql/schema/schema_manager.cc)

Add table field definitions for schema validation:

```cpp
static const TABLE_FIELD_TYPE your_table_fields[] = {
    {{STRING_WITH_LEN("key_field")},
     {STRING_WITH_LEN("varchar(64)")},
     {nullptr, 0}},
    {{STRING_WITH_LEN("data_field")},
     {STRING_WITH_LEN("varchar(64)")},
     {nullptr, 0}}};
static const TABLE_FIELD_DEF your_table_def = {2, your_table_fields};
```

Then add to `tables_to_check` array:

```cpp
static const VillageSQL_table tables_to_check[] = {
    {SchemaManager::PROPERTIES_TABLE_NAME, &properties_def},
    {SchemaManager::COLUMNS_TABLE_NAME, &columns_def},
    {SchemaManager::EXTENSIONS_TABLE_NAME, &extensions_def},
    {SchemaManager::YOUR_TABLE_NAME, &your_table_def}};  // Add this
```

And add the constant:

```cpp
constexpr const char *YOUR_TABLE_NAME = "your_table";
```

## Common Pitfalls

- **Forgetting clear() call** - Causes stale data to persist in cache
- **Wrong collation** - Use `&my_charset_utf8mb4_bin` in C++ code (matches runtime behavior), even though SQL schema uses `utf8mb4_0900_ai_ci`
- **Missing normalization** - Keys should normalize for case-insensitive comparison
- **Wrong initializer order** - Must match member declaration order
- **Missing update_key()** - Must call in read_from_table() after deserializing fields
- **Using public key fields** - Keep key components private with accessors to prevent stale keys
- **Hardcoded table names** - Use `SchemaManager::VILLAGESQL_SCHEMA_NAME` and `SchemaManager::YOUR_TABLE_NAME` instead of string literals in Table_ref and error messages. To check for system tables, use villagesql::is_system_schema() or villagesql::is_villagesql_schema().

## Usage in Code

When opening your table or referencing it in error messages, always use SchemaManager constants:

```cpp
// Opening table - use constants not string literals
Table_ref tables(villagesql::SchemaManager::VILLAGESQL_SCHEMA_NAME,
                 villagesql::SchemaManager::YOUR_TABLE_NAME, TL_WRITE);

// Error messages - use constants
villagesql_error("Cannot open %s.%s table", MYF(0),
                 villagesql::SchemaManager::VILLAGESQL_SCHEMA_NAME,
                 villagesql::SchemaManager::YOUR_TABLE_NAME);
```

## Validation

After adding a table:

1. **Compile**: Ensure everything builds
2. **Schema validation**: Server startup should validate table structure
3. **Test CRUD**: Write tests for create, read, update operations
4. **Run full test suite**: Many tests query system metadata and will need result updates
5. **Re-record affected tests**: Tests that query villagesql schema or system metadata will need re-recording

**Test result files that will likely require updates:**
- `r/mysqld--help-notwin.result` - Performance schema count updated
- `r/partition_innodb_tablespace.result`
- `suite/innodb/r/create_tablespace*.result` (9 files: base + 4k/8k/16k/32k/64k/debug/replication variants)
- `suite/innodb/r/default_row_format*.result` (3 files: base/compatibility/tablespace)
- `suite/innodb/r/*_tablespace.result` (4 files: discard/innodb/portability/portability_linux)
- `suite/innodb/r/*_directory*.result` (5 files: case_insensitive_fs/hidden_directory_dotfile/known_directory/known_dir_unique_undo/unknown_directory)
- `suite/innodb/r/partition_autoinc.result`
- `suite/innodb/r/innodb-index-online-fk.result`
- `suite/innodb/r/tablespace_per_table.result`
- `suite/innodb_fts/r/tablespace_location*.result` (2 files: base/error)
- `suite/innodb_zip/r/*.result` (4 files: 4k/8k/16k/restart)
- `suite/parts/r/partition_reorganize_innodb.result`
- `suite/perfschema/r/start_server_low_table_lock.result`
- `suite/perfschema/r/telemetry_metrics_server.result`
- `suite/rpl/r/default_row_format_01.result`

**VillageSQL-specific test files:**
- `suite/villagesql/r/init_file_schema_version.test` - Shows all system tables on startup
- Any tests that do `SHOW TABLES FROM villagesql` or similar

Note: `information_schema_keywords.result` was also updated because the EXTENSION keyword was added for the new SQL command - see `Docs/ADDING_SQL_COMMANDS.md` for keyword-related updates.
