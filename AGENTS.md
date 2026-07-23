# AGENTS.md

This file provides guidance to ai agents when working with code in this repository.

## Repository Overview

This is VillageSQL Server, a fork of MySQL 8.4.6 LTS that adds VillageSQL Extension Framework (VEF), which includes support for custom types. The codebase generally follows Oracle/MySQL development practices, though with additional VillageSQL modifications.

**Terminology:**
- **VEF** (VillageSQL Extension Framework): The overall framework for building and loading extensions
- **VEB** (VillageSQL Extension Bundle): The file format for VillageSQL extension packages (`.veb` files)

**Development Guides:**
- [Adding SQL Commands](Docs/ADDING_SQL_COMMANDS.md) - Complete checklist for adding new SQL commands to the parser
- [Adding System Tables](villagesql/schema/ADDING_SYSTEM_TABLES.md) - Guide for adding new VillageSQL system tables to victionary
- [Error Handling](Docs/ERROR_HANDLING.md) - strategies for handling errors in VillageSQL and the boundary with MySQL
- [CI Build Cache](.github/CI_BUILD_CACHE.md) - How CI build caching works, known issues, and diagnostics
- [VEF SDK Overview](villagesql/sdk/README.md) - The extension SDK, protocol versions, and how to stabilize a protocol
- [VEF API vs ABI](villagesql/sdk/API_ABI.md) - The API/ABI distinction and the rules for evolving each compatibly

**When changing the VEF framework** (anything under `villagesql/sdk/`, especially
the ABI in `villagesql/sdk/include/villagesql/abi/types.h`), update the SDK
documentation in the same change:
- Keep the protocol-version table in `villagesql/sdk/README.md` in sync with the
  `vef_protocol_t` enum in `abi/types.h` (the enum is the source of truth).
- Update `villagesql/sdk/API_ABI.md` when the API/ABI contract or its evolution
  rules change, and the relevant `README.md` files under
  `villagesql/sdk/include/villagesql/` (`abi/`, `preview/`).

## Key Development Commands

### Environment Setup

Create an `AGENTS.local.md` file (ignored by git) with your local environment variables for AI agents to reference. You can also create `~/AGENTS.local.md` for settings that apply across all your projects.

**IMPORTANT**: Before running any build or test commands, always read `AGENTS.local.md` in the repository root to get the correct environment variable values for this machine. Both files are mentioned at the bottom to ensure they are read.

Example AGENTS.local.md:
```
HOME=/your/home/path
BUILD_HOME=$HOME/build
SOURCE_HOME=$HOME/code/villagesql-server
```

### Building
Assume environment variable definitions as per AGENTS.local.md

**Determining CPU cores for parallel builds:**
- `getconf _NPROCESSORS_ONLN` (works on both macOS and Linux)
- Typical machines have 8-16 cores, use with `-j` flag (e.g., `-j14` for 14 cores), but check what number to use. All examples here use -j14.

```bash
# Standard release build
cd $BUILD_HOME
rm CMakeCache.txt; cmake $SOURCE_HOME
make -j14

# Debug build
rm CMakeCache.txt; cmake $SOURCE_HOME -DWITH_DEBUG=1 -DWITH_SHOW_PARSE_TREE=1
make -j14

# Maintainer build (strict warnings)
rm CMakeCache.txt; cmake $SOURCE_HOME -DMYSQL_MAINTAINER_MODE=ON
make -j14

# macOS with specific SSL
rm CMakeCache.txt; cmake $SOURCE_HOME -DWITH_SSL=/opt/homebrew/opt/openssl@3
make -j14
```

### Testing
```bash
# Run all tests
cd $BUILD_HOME
./mysql-test/mysql-test-run.pl

# Run specific test
./mysql-test/mysql-test-run.pl test_name

# Run tests in parallel (faster)
./mysql-test/mysql-test-run.pl --parallel=auto

# Run ALL VillageSQL tests including sub-suites (alter_table, create_table, etc.)
./mysql-test/mysql-test-run.pl --do-suite=village --nounit-tests --parallel=auto

# Run VillageSQL tests including "big" tests (longer running)
./mysql-test/mysql-test-run.pl --do-suite=village --nounit-tests  --parallel=auto --big-test

# Run specific VillageSQL sub-suite
./mysql-test/mysql-test-run.pl --suite=villagesql/alter_table --parallel=auto

# Run specific test suite
./mysql-test/mysql-test-run.pl --suite=innodb --parallel=auto

# Run all available test suites
./mysql-test/mysql-test-run.pl --suite=all --parallel=auto

# Run multiple specific suites
./mysql-test/mysql-test-run.pl --suite=villagesql,innodb --parallel=auto

# Run tests with increased server logging verbosity (needed to see LogVSQL output)
./mysql-test/mysql-test-run.pl --suite=villagesql --parallel=auto --mysqld=--log-error-verbosity=3

# Create/update test results
./mysql-test/mysql-test-run.pl --record my_new_test

# Build and run villagesql unittests
make -j14 villagesql-unit-tests && ctest -L villagesql
```

#### Debugging Test Failures and Error Messages

When debugging failed tests or investigating server-side logging:

**Error Log Locations:**
- Main error log: `$BUILD_HOME/mysql-test/var/log/mysqld.1.err`
- Test-specific logs: `$BUILD_HOME/mysql-test/var/log/test_name/`

**Finding VillageSQL Debug Messages:**
VillageSQL uses `LogVSQL()` for debug logging, which appears in the MySQL error log but not in test output by default. To view these messages:

```bash
# View error log during/after test run
tail -f $BUILD_HOME/mysql-test/var/log/mysqld.1.err

# Search for VillageSQL debug messages
grep -i "villagesql" $BUILD_HOME/mysql-test/var/log/mysqld.1.err
```

**Including Error Log Messages in Test Output:**
If you need error log messages to appear in test output for debugging, add a perl section to read from the error log:

```perl
--perl
use strict;
my $error_log = $ENV{'MYSQLTEST_VARDIR'} . "/log/mysqld.1.err";
open(FILE, "$error_log") or die("Unable to open error log: $error_log");
while (<FILE>) {
  my $line = $_;
  if ($line =~ /VillageSQL.*DEBUG/) {
    # Remove timestamp/thread info, show just the message
    $line =~ s/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s+\d+\s+\[ERROR\]\s+\[MY-\d+\]\s+\[Server\]\s+//;
    print $line;
  }
}
close(FILE);
EOF
```

If you need to add your own temporary debugging, use sql_print_information to make it easier to know it is
temporary and needs to be removed.

#### Test Result Portability

**CRITICAL**: Test results can end up including paths that are tied to specific directories on the user's laptop, which will fail to match on others'. The following lines are examples of how
to redact these filenames. They must be inserted right before a command (e.g. exec) that will emit the log lines with the user's specific paths (replacing them with a placeholder like MYSQLTEST_VARDIR)
```sql
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
--replace_result $MYSQL_TEST_DIR MYSQL_TEST_DIR
```

### Code Quality

Use `./scripts/villint.sh` to lint changed files in your PR/branch. It runs clang-format on C/C++ files, adds/updates copyright headers, removes trailing whitespace, and ensures files end with newlines.

```bash
# Lint changed files (default usage)
./scripts/villint.sh
```

For additional options, see `./scripts/villint.sh --help`.

## Architecture Overview

TODO(villagesql-ga): Add VillageSQL-specific architecture documentation covering:
- Extension framework architecture
- Custom type system design
- Victionary caching layer
- System table organization

### Core MySQL Structure
- `/sql/`: Core SQL server implementation
- `/storage/`: Storage engines (InnoDB, MyISAM, etc.)
- `/mysql-test/`: Comprehensive test framework
- `/cmake/`: Build configuration
- `/include/`: Public headers
- `/client/`: MySQL client tools
- `/extra/`: Third-party dependencies

### Key Integration Points
When adding VillageSQL features:
1. Follow MySQL coding standards and include appropriate copyright headers
1. Run villint.sh to add copyright notices automatically
1. Comments should be // style, not /* */

## Important Notes

- This is a production database codebase - changes should maintain stability
- All new code must include tests in the mysql-test framework
  - Add unittests or unittest coverage when appropriate for the change
- For any SQL code, including .test files, remove any stray end-of-line whitespace
- **VillageSQL system tables**: Use `villagesql.*` database for system tables, NOT `mysql.*`
  - Columns storing custom types will be stored in 'villagesql.custom_columns'
  - In code we'll merge data from both both 'villagesql' and 'mysql' tables.
  - Don't change 'mysql.*' tables (to maintain backwards compatibility)

## Coding Guidelines

### Comments and Documentation
- Use `//` style comments for inline code comments and explanations
- For header guards, use `// HEADER_GUARD_NAME_H` at the `#endif`
- All todos in the code that we add should look like `// TODO(villagesql): <thing to do>`
  - Use subareas for categorization:
    - `TODO(villagesql-beta):` - Must handle before Beta Release (0.1.0)
    - `TODO(villagesql-preview):` - Related to preview capability system
    - `TODO(villagesql-ga):` - Must handle before General Availability (1.0.0)
    - `TODO(villagesql-performance):` - Performance optimization possibility
    - `TODO(villagesql-rebase):` - Check during MySQL version rebases
    - `TODO(villagesql-crash):` - Known crash
    - `TODO(villagesql-windows):` - Known problems with supporting Windows
    - `TODO(villagesql-indexing):` - Related to indexing work
    - `TODO(villagesql-back-to-mysql):` - Required for downgrade safety / vanilla MySQL compatibility
- Avoid `/* */` style comments except for copyright headers
- Do NOT use section separator comments (e.g., `// ===== Serialization =====`) - they're hard to maintain and add little value
- Do NOT add explanatory comments on #include lines (e.g., `#include "my_sys.h" // my_ok, my_printf_error`) - they're hard to maintain
- Do NOT reference line numbers in comments (e.g., `// see line 123`) - they become stale as code changes
- Class and method documentation can use doxygen format but with `//` style
- End files with a newline

### Filesystem Operations

When working with files and directories, prefer MySQL's platform-independent wrappers over POSIX functions:

**Use MySQL wrappers:**
MySQL wraps many syscalls; e.g. `my_stat()` instead of `stat()`. Check for wrappers when writing file operations.

**Path construction:**
- Use `fn_format(result, filename, directory, extension, flags)` from `my_sys.h` for joining paths.
Do not use literal "/", because that will fail on Windows.

### Shell Script Naming

Name shell (`/bin/bash`) scripts using `snake_case` — all lowercase letters with underscores as the word separator (e.g. `build_ci.sh`, `setup_dev_db.sh`).

- Do NOT use hyphens/dashes as word separators. The hyphen is also the minus operator in many contexts, which adds friction in common development practices (e.g. cut-and-paste) due to variant names and inconsistent word boundaries.
- Do NOT use capitalization schemes (e.g. `TitleName`, `camelCase`). All-lowercase is common practice for script names.
- Include a `.sh` suffix when appropriate.

This aligns with MySQL script practice: in the `scripts/` tree, only the Perl (`.pl`) files use hyphens. Existing VillageSQL scripts that use hyphens are grandfathered; apply this convention to new scripts.

### Include Order for RapidJSON Headers
When using RapidJSON headers, `my_rapidjson_size_t.h` must be included **before** other rapidjson headers for compilation to work correctly. Use separate include blocks (separated by blank lines) to have clang-format preserve this order.

```cpp
#include "my_rapidjson_size_t.h"  // IWYU pragma: keep

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
```

### Byte Order Functions

MySQL provides two sets of functions for reading/writing double values:

**1. float8store/float8get - Platform-independent (little-endian storage)**
- Use for: persistent storage, cross-platform data, serialization, indexes
- Always stores in little-endian format regardless of system architecture
- Required for data that will be stored to disk or transmitted over network

**2. doublestore/doubleget - Platform-dependent (native byte order)**
- Use for: in-memory operations within the same process
- Direct memory copy using system's native byte order
- Faster but not portable between different architectures

**CRITICAL**: Never mix these functions! Either use float8store/float8get throughout or doublestore/doubleget throughout. Mixing can cause data corruption on big-endian systems.

For VillageSQL custom types that need persistence, always use the platform-independent `float8store/float8get` functions.

Read the file: ~/AGENTS.local.md
Read the file: AGENTS.local.md
