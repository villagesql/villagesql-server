![VillageSQL Logo](https://villagesql.com/assets/logo-light.svg)

# VillageSQL Server

[![License: GPL v2](https://img.shields.io/badge/License-GPL%20v2-blue.svg)](./LICENSE)
[![Discord](https://img.shields.io/discord/1234567890?label=Discord&logo=discord)](https://discord.com/invite/KSr6whd3Fr)
[![GitHub Release](https://img.shields.io/github/v/release/villagesql/villagesql-server?include_prereleases)](https://github.com/villagesql/villagesql-server/releases)

VillageSQL is the innovation platform for MySQL and a new path for MySQL in the agentic AI era. VillageSQL Server is an open-source tracking fork of **MySQL 8.4.6 LTS** that introduces the **VillageSQL Extension Framework (VEF)**.

VEF enables custom data types and functions while maintaining MySQL 8.4 compatibility.

> [!WARNING]
> **Alpha Status:** VillageSQL is currently in alpha. It is intended for development and testing purposes only and is **not yet recommended for production use**.

## Table of Contents

- [Key Features](#key-features)
- [Installation (Building from Source)](#installation-building-from-source)
- [Quick Start: Using Extensions](#quick-start-using-extensions)
- [Developing Extensions](#developing-extensions)
- [Known Limitations](#known-limitations)
- [Roadmap](#roadmap)
- [Troubleshooting](#troubleshooting)
- [Reporting Bugs and Requesting Features](#reporting-bugs-and-requesting-features)
- [Contributing](#contributing)
- [Community & Support](#community--support)
- [License](#license)

## Key Features

- **VillageSQL Extension Framework (VEF):** Framework for building and loading extensions (`.veb` bundles).
- **Custom Data Types:** Define and use domain-specific data types directly in your SQL schema.
- **Custom Functions:** Implement high-performance logic within the database.
- **Drop-in Replacement:** Compatible with existing MySQL 8.4 applications and tools.

## Installation (Building from Source)

During the alpha phase, VillageSQL must be built from source. Docker and pre-built binary installations are coming soon. VillageSQL follows the same build requirements as standard MySQL 8.4.

### Prerequisites

- **CMake** (3.14.3 or higher)
- **C++17 Compiler** (GCC 11+, Clang 13+, or MSVC 2019+)
- **OpenSSL 3.0+**
- **Bison** (3.0 or higher)
- **pkg-config**
- **ncurses development libraries**
- **libtirpc and rpcsvc-proto**

#### Installing Dependencies

**Linux (Debian/Ubuntu):**
```bash
sudo apt install cmake libssl-dev libncurses5-dev pkg-config bison \
                 libtirpc-dev rpcsvc-proto build-essential
```

**macOS (Homebrew):**
```bash
brew install cmake openssl@3 pkgconf bison libtirpc rpcsvc-proto
```

### Build Steps (Linux & macOS)

1. **Clone the repository:**
   ```bash
   git clone https://github.com/villagesql/villagesql-server.git
   cd villagesql-server
   ```

2. **Create a build directory (outside the repository):**
   ```bash
   mkdir -p ~/build/villagesql
   cd ~/build/villagesql
   ```

3. **Configure with CMake:**
   ```bash
   # Standard build
   cmake ~/villagesql-server -DWITH_SSL=system

   # Or for a debug build (recommended for development)
   cmake ~/villagesql-server -DWITH_DEBUG=1 -DWITH_SSL=system
   ```

4. **Build:**
   ```bash
   make -j $(($(getconf _NPROCESSORS_ONLN) - 2))
   ```

5. **Initialize and Start the Server:**
   ```bash
   # Create the data directory
   mkdir -p ~/mysql-data

   # Initialize the data directory (insecure mode for development)
   bin/mysqld --initialize-insecure --datadir=~/mysql-data/data --basedir=~/build/villagesql

   # Start the server (runs in foreground, use Ctrl-C to stop)
   bin/mysqld --gdb --datadir=~/mysql-data/data --basedir=~/build/villagesql

   # In a new terminal, connect using the client
   ~/build/villagesql/bin/mysql -u root

   # Verify the installation
   ~/build/villagesql/bin/mysql -u root -e "SELECT VERSION()"
   ```

   *Note: `--initialize-insecure` creates a root user with no password for development. The `--gdb` flag installs a signal handler that allows you to Ctrl-C to quit the server. For production, use `--initialize` instead (generates a temporary password) and refer to [MySQL 8.4 initialization documentation](https://dev.mysql.com/doc/refman/8.4/en/data-directory-initialization.html) for secure setup.*

   **Setting up users and permissions:**
   ```sql
   -- Create a new user
   CREATE USER myuser IDENTIFIED BY 'password';

   -- Grant permissions
   GRANT ALL PRIVILEGES ON *.* TO myuser;
   ```

   **Verify the new user:**
   ```bash
   ~/build/villagesql/bin/mysql -u myuser -p -e "SELECT USER()"
   ```

### Running Tests

Verify your build with the test suite:

```bash
# From your build directory (~/build/villagesql)
cd ~/build/villagesql

# Run all VillageSQL tests including sub-suites
mysql-test/mysql-test-run.pl --do-suite=villagesql --parallel=auto

# Run a specific test
mysql-test/mysql-test-run.pl villagesql.my_test_name

# Update test results after making changes
mysql-test/mysql-test-run.pl --record villagesql.my_test_name

# Run VillageSQL unit tests
make -j $(($(getconf _NPROCESSORS_ONLN) - 2)) villagesql-unit-tests && ctest -L villagesql
```

## Quick Start: Using Extensions

VillageSQL Server ships with five built-in extensions:
- **`vsql_complex`**: Complex number data type and arithmetic
- **`vsql_uuid`**: UUID generation, validation, and comparison
- **`vsql_ai`**: AI prompting via SQL functions
- **`vsql_crypto`**: Cryptographic functions (hashing, encryption, random data)
- **`vsql_network_address`**: IPv4, IPv6, and MAC address types

Once the server is running, you can manage extensions using new SQL commands:

```sql
-- Install an extension bundle (e.g., vsql_complex)
INSTALL EXTENSION vsql_complex;

-- Verify the extension is loaded
SELECT extension_name, extension_version
FROM INFORMATION_SCHEMA.EXTENSIONS;

-- Create a database and use it
CREATE DATABASE demo;
USE demo;

-- Use a custom type provided by an extension
CREATE TABLE signals (
  id INT PRIMARY KEY,
  reading COMPLEX -- Example type from vsql_complex
);

-- Insert sample data
INSERT INTO signals VALUES (1, '(3,4)'), (2, '(5,12)'), (3, '(-1,2)');

-- Query using custom functions (note: functions require extension prefix)
SELECT
  id,
  reading,
  vsql_complex.complex_abs(reading) AS magnitude,
  vsql_complex.complex_real(reading) AS real_part,
  vsql_complex.complex_imag(reading) AS imag_part
FROM signals;

-- Clean up: Drop table first, then uninstall extension
DROP TABLE signals;
UNINSTALL EXTENSION vsql_complex;
```

## Developing Extensions

VillageSQL provides a C++ SDK for building high-performance extensions.

- **Example Code:**
  - `villagesql/examples/vsql-complex`: Reference implementation with arithmetic, custom hash handlers, and platform-independent serialization.
  - `villagesql/examples/vsql-simple`: A minimal "Hello World" implementation of a custom type and functions.
- **Header API:** Detailed extension API definitions can be found in `villagesql/include/villagesql/extension.h`.

## Known Limitations

- **Source-only Build:** No official Docker images or binary packages are available yet.
- **No Custom Indexes:** Custom data types cannot be indexed in this version (coming soon).
- **Alpha Stability:** Expect breaking changes and potential bugs as we progress towards Beta.
- **No Windows Support:** We don’t support compiling to .dll to Windows yet.

## Roadmap

Priority items are listed below. The full roadmap can be found at [villagesql.com/roadmap](https://villagesql.com/roadmap).

- [ ] **Custom Indexes:** Support for indexing custom data types.
- [ ] **ALTER/UPGRADE Extension:** Lifecycle management for installed extensions.
- [ ] **Docker & Shell Installer:** Official images and simplified installation.
- [ ] **Aggregate Functions:** Support for aggregations with custom types.
- [ ] **Analytical Capabilities:** Embedded analytical engines (e.g., DuckDB integration).
- [ ] **Fully-Managed Cloud Service:** VillageSQL as a managed database offering.

## Troubleshooting

### Build Failures

**OpenSSL not found:**
```bash
# macOS with Homebrew
brew install openssl@3
cmake ~/villagesql-server -DWITH_SSL=/opt/homebrew/opt/openssl@3

# Linux (Ubuntu/Debian)
sudo apt-get install libssl-dev
cmake ~/villagesql-server -DWITH_SSL=system
```

**Bison version too old:**
```bash
# macOS
brew install bison
export PATH="/opt/homebrew/opt/bison/bin:$PATH"

# Linux
sudo apt-get install bison
```

### Runtime Issues

**Can't connect to server:**
- Check that `mysqld` is running: `pgrep -a mysqld` or `ps aux | grep mysqld`
- Verify socket path matches between server and client
- Check error log in your data directory (e.g., `~/mysql-data/data/*.err`)

**Port already in use:**
If you see "Bind on TCP/IP port: Address already in use", either stop the existing MySQL instance or specify a different port:
```bash
bin/mysqld --gdb --datadir=~/mysql-data/data --basedir=~/build/villagesql --port=3307
```

For more help, visit our [Discord community](https://discord.gg/KSr6whd3Fr) or [file an issue](https://github.com/villagesql/villagesql-server/issues).

## Reporting Bugs and Requesting Features

If you encounter a bug or have a feature request, please open an [issue](https://github.com/villagesql/villagesql-server/issues) using GitHub Issues. Please provide:
- A clear title and detailed description.
- Steps to reproduce (if applicable).
- Environment details (OS, compiler, OpenSSL version).

## Contributing

We welcome contributions. Please see [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines on how to get involved.

## Community & Support

- 💬 [Discord](https://discord.gg/KSr6whd3Fr)
- 🏗️ [Discussions](https://github.com/villagesql/villagesql-server/discussions)
- 🌐 [Website](https://villagesql.com)

## License

VillageSQL Server is licensed under the [GPLv2](./LICENSE) (the same as MySQL).
