# Adding New SQL Commands to VillageSQL

This guide provides a checklist for adding new SQL commands to the MySQL/VillageSQL server.

## Overview

Adding a new SQL command requires changes across multiple files to register the command, parse it, set its behavior flags, track metrics, and implement its execution logic.

## Complete Checklist

### 1. Add Command to Enum (include/my_sqlcommand.h)

Add your command to the `enum_sql_command` enum. VillageSQL commands go in the VSQL block (starting at SQLCOM_VSQL_FIRST), before `SQLCOM_END`:

```cpp
// include/my_sqlcommand.h
enum enum_sql_command {
  // ... MySQL commands (0-200+ (circa 2026), contiguous) ...
  SQLCOM_SHOW_PARSE_TREE,
  SQLCOM_MYSQL_COUNT,  // Sentinel — must stay at end of MySQL commands

  // VillageSQL commands start at 1024 to avoid conflicts with upstream MySQL
  SQLCOM_VSQL_FIRST = 1024,
  SQLCOM_INSTALL_EXTENSION = SQLCOM_VSQL_FIRST,
  SQLCOM_UNINSTALL_EXTENSION,
  SQLCOM_YOUR_NEW_COMMAND,     // Add here
  SQLCOM_END                    // Must be last
};
```

**Important**: VillageSQL commands use `sqlcom_compact_index()` for `com_stat` array access (see step 7).

### 2. Add Keyword Token (sql/lex.h)

If your command uses a new keyword, add it to the symbols array in alphabetical order. VillageSQL additions should be grouped together for easier maintenance during MySQL rebases.

Search for: `SYM("EXTENSION"` to find where VillageSQL symbols are located, and add your new symbol in alphabetical order within that group.

### 3. Add Token Declaration (sql/sql_yacc.yy)

Add the token with a unique number near the end of the token declarations:

```yacc
// sql/sql_yacc.yy - Token declarations section
// TODO(villagesql-rebase): Check if token number needs updating during MySQL rebase
%token<lexer.keyword> YOURNEWKEYWORD_SYM    XXXX  /* VILLAGESQL */
```

Choose a token number (XXXX) that doesn't conflict with existing tokens.

### 4. Add Grammar Rules (sql/sql_yacc.yy)

Add the parsing rules for your command. Place them near similar commands (e.g., INSTALL commands together):

```yacc
// sql/sql_yacc.yy - Grammar rules section
your_command_stmt:
        // TODO(villagesql-rebase): YOUR COMMAND grammar rule, check placement during MySQL rebase
        YOURNEWKEYWORD_SYM TEXT_STRING_sys
          {
            LEX *lex= Lex;
            lex->sql_command= SQLCOM_YOUR_NEW_COMMAND;
            lex->m_sql_cmd= new (YYMEM_ROOT) Sql_cmd_your_command(to_lex_cstring($2));
            $$ = nullptr;
          }
```

**Note**: Add `TODO(villagesql-rebase)` comments to help future maintainers during MySQL version rebases.

### 5. Add Command Flags (sql/sql_parse.cc - init_sql_command_flags())

Set appropriate behavior flags for your command in `init_sql_command_flags()`. There are typically **three** locations:

**Location A: Basic flags**
Search for: `sql_command_flags[SQLCOM_INSTALL_PLUGIN] =`
```cpp
sql_command_flags[SQLCOM_YOUR_NEW_COMMAND] =
    CF_CHANGES_DATA | CF_AUTO_COMMIT_TRANS;
```

**Location B: Read-only transaction restrictions**
Search for: `sql_command_flags[SQLCOM_INSTALL_PLUGIN] |= CF_DISALLOW_IN_RO_TRANS;`
```cpp
sql_command_flags[SQLCOM_YOUR_NEW_COMMAND] |= CF_DISALLOW_IN_RO_TRANS;
```

**Location C: Autocommit requirements**
Search for: `sql_command_flags[SQLCOM_INSTALL_PLUGIN] |= CF_NEEDS_AUTOCOMMIT_OFF;`
```cpp
sql_command_flags[SQLCOM_YOUR_NEW_COMMAND] |= CF_NEEDS_AUTOCOMMIT_OFF;
```

Common flag combinations:
- DDL commands: `CF_CHANGES_DATA | CF_AUTO_COMMIT_TRANS | CF_NEEDS_AUTOCOMMIT_OFF | CF_DISALLOW_IN_RO_TRANS`
- Plugin-like commands: `CF_CHANGES_DATA | CF_AUTO_COMMIT_TRANS | CF_NEEDS_AUTOCOMMIT_OFF | CF_DISALLOW_IN_RO_TRANS`

### 6. Add Switch Cases (sql/sql_parse.cc - mysql_execute_command())

Add cases to the main command dispatcher switch statement.

Search for: `case SQLCOM_INSTALL_PLUGIN:` in `mysql_execute_command()`

### 7. Add Status Metrics (sql/mysqld.cc)

Add entries to **TWO** arrays for command tracking:

**Array 1: com_status_vars**
Search for: `{"install_extension",` in `com_status_vars`

**Array 2: com_metrics**
Search for: `{"install_extension",` in `com_metrics`

**Important**: Keep alphabetical ordering within each command type group (INSTALL*, UNINSTALL*, CREATE*, etc.)

**Important for VillageSQL commands**: The `com_stat` arrays use compact indexing to avoid a gap. Use `sqlcom_compact_index()` in `offsetof` expressions:
```cpp
// com_status_vars entry:
{"your_new_command",
 (char *)offsetof(System_status_var,
                  com_stat[sqlcom_compact_index((uint)SQLCOM_YOUR_NEW_COMMAND)]),
 SHOW_LONG_STATUS, SHOW_SCOPE_ALL},

// com_metrics entry:
{"your_new_command", "", COM_COMMON_DESCRIPTION,
 MetricOTELType::ASYNC_COUNTER, MetricNumType::METRIC_INTEGER, 0, 0,
 get_metric_aggregated_integer,
 (void *)offsetof(aggregated_stats_buffer,
                  com_stat[sqlcom_compact_index((uint)SQLCOM_YOUR_NEW_COMMAND)])},
```
MySQL-native commands don't need `sqlcom_compact_index()` — their compact index equals their enum value.

### 8. Update ABI Check File (include/mysql/plugin_audit.h.pp)

Add your new commands to the enum_sql_command in the preprocessed header for audit logging:

Search for: `SQLCOM_END` in `include/mysql/plugin_audit.h.pp` and add your commands in the VSQL block:

```cpp
  SQLCOM_MYSQL_COUNT,
  SQLCOM_VSQL_FIRST = 1024,
  SQLCOM_INSTALL_EXTENSION = SQLCOM_VSQL_FIRST,
  SQLCOM_UNINSTALL_EXTENSION,
  SQLCOM_YOUR_NEW_COMMAND,      // Add here
  SQLCOM_END
```

**Important**: This prevents ABI check failures in CI builds.

### 9. Implement Command Class

Create the command execution class (e.g., in villagesql/veb/sql_extension.cc):

```cpp
class Sql_cmd_your_command : public Sql_cmd {
 public:
  explicit Sql_cmd_your_command(LEX_CSTRING name) : m_name(name) {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_YOUR_NEW_COMMAND;
  }

  bool execute(THD *thd) override {
    // Implementation here
    // Use transaction handling if needed:
    // const Disable_autocommit_guard autocommit_guard(thd);
    // const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
    // ... your logic ...
    // return end_transaction(thd, error_occurred);
  }

 private:
  LEX_CSTRING m_name;
};
```

## Common Pitfalls

1. **Forgetting command flags** - Commands without proper flags may work in normal mode but fail in read-only mode or with different autocommit settings
2. **Missing switch cases** - Can cause assertion failures in mysql_execute_command()
3. **Misaligned status arrays** - com_status_vars and com_metrics must have entries in the right alphabetical position
4. **Token number conflicts** - Each token needs a unique number in sql_yacc.yy
5. **Missing rebase TODOs** - Grammar changes need markers for MySQL version updates

## Validation

After adding a command:

1. **Compile**: Ensure mysqld builds without errors
2. **Static assert**: Check for com_status_vars array size assertions
3. **Run all tests and re-record broken tests**: Most likely test to break is `information_schema_keywords.result` if you added a new keyword
4. **Runtime test**: Create a .test file in mysql-test/suite/villagesql/t/
5. **Read-only test**: Verify command is blocked with `SET GLOBAL read_only = ON`
6. **Transaction test**: Verify autocommit handling works correctly

## Example: INSTALL EXTENSION

See the implementation of INSTALL/UNINSTALL EXTENSION commands as a reference:
- **Enum**: Search `enum_sql_command` for `SQLCOM_INSTALL_EXTENSION` in include/my_sqlcommand.h
- **Token**: Search `EXTENSION_SYM` in sql/lex.h
- **Bison token**: Search `EXTENSION_SYM` in sql/sql_yacc.yy token declarations
- **Grammar**: Search `INSTALL_SYM EXTENSION_SYM` and `UNINSTALL_SYM EXTENSION_SYM` in sql/sql_yacc.yy
- **Flags**: Search `SQLCOM_INSTALL_EXTENSION` in sql/sql_parse.cc (three locations)
- **Switch**: Search `SQLCOM_INSTALL_EXTENSION` in `mysql_execute_command()` in sql/sql_parse.cc
- **Status vars**: Search `install_extension` in sql/mysqld.cc (two arrays)
- **Implementation**: villagesql/veb/sql_extension.{h,cc}

## Tips

- **Follow existing patterns**: Find a similar command (INSTALL PLUGIN, CREATE TABLE, etc.) and mimic its structure
- **Use grep**: Search for an existing command across all files to find all the places that need updates
- **Alphabetical order**: Maintain alphabetical ordering in status vars and metrics arrays
- **Test early**: Write tests as soon as the command parses to catch issues quickly
