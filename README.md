<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://villagesql.com/assets/logo-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://villagesql.com/assets/logo-light.svg">
  <img alt="VillageSQL Logo" src="https://villagesql.com/assets/logo-light.svg">
</picture>

# VillageSQL Server

[![License: GPL v2](https://img.shields.io/badge/License-GPL%20v2-blue.svg)](./LICENSE)
[![Discord](https://img.shields.io/discord/1445037832707113043?logo=discord&label=discord)](https://discord.com/invite/KSr6whd3Fr)
[![GitHub Release](https://img.shields.io/github/v/release/villagesql/villagesql-server?include_prereleases)](https://github.com/villagesql/villagesql-server/releases)

VillageSQL is the innovation platform for MySQL and a new path for MySQL in the agentic AI era. VillageSQL Server is an open-source tracking fork of **MySQL** that introduces the **VillageSQL Extension Framework (VEF)**.

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

📚 **Full Documentation**: Visit [villagesql.com/docs](https://villagesql.com/docs) for comprehensive guides on building extensions, architecture details, and more.

## Installation (Building from Source)

For quick installation, visit [villagesql.com/install](https://villagesql.com/install) — the shell installer and Docker are both available. To build from source, VillageSQL follows the same build requirements as standard MySQL 8.4.

### Prerequisites

- **Git**
- **A supported platform**: Debian or Ubuntu Linux, or macOS with Homebrew

The build also needs a C++20 compiler, CMake 3.14.6 or newer (3.19 on macOS),
Bison 3.0.4 or newer, OpenSSL 3, and several development libraries. Step 2
below installs all of them.

### Build Steps (Linux & macOS)

> **Note:** These steps are the same on Linux and macOS. Paths use `$HOME`
> rather than `~` because the shell only expands `~` at the start of a word:
> `--datadir=~/mysql-data/data` reaches `mysqld` as a directory literally named
> `~`, and the server aborts. Quoting `"$HOME/..."` keeps the path in one piece
> if your home directory name contains a space.

1. **Clone the repository into `$HOME`:**

   The steps below assume the clone is at `$HOME/villagesql-server`. If you put
   it elsewhere, use that path in step 4.

   ```bash
   cd "$HOME"
   git clone --depth 1 https://github.com/villagesql/villagesql-server.git
   ```

2. **Install the build dependencies:**

   ```bash
   cd "$HOME/villagesql-server"
   villagesql/bld_tools/setup_build_env.sh
   ```

   The script detects the operating system and runs
   `villagesql/bld_tools/setup_linux_build_env.sh` or
   `villagesql/bld_tools/setup_macos_build_env.sh`. Those two scripts are the
   definitive dependency lists, and VillageSQL CI installs from the same ones.
   On Linux the script uses `apt-get` and asks for `sudo`; on macOS it uses
   Homebrew. On a Linux distribution that is not Debian or Ubuntu, read
   `villagesql/bld_tools/setup_linux_build_env.sh` and install the equivalent
   packages with your own package manager.

3. **Create a build directory (outside the repository):**

   ```bash
   mkdir -p "$HOME/build/villagesql"
   cd "$HOME/build/villagesql"
   ```

4. **Configure with CMake:**

   ```bash
   # Standard build
   cmake "$HOME/villagesql-server" -DWITH_SSL=system

   # Or for a debug build (recommended for development)
   cmake "$HOME/villagesql-server" -DWITH_DEBUG=1 -DWITH_SSL=system
   ```

5. **Build:**
   ```bash
   make -j $(getconf _NPROCESSORS_ONLN)
   ```

   This builds every target, including the `mysql` client that step 6 uses to
   connect. A build limited to the `mysqld` target leaves step 6 with no client.

6. **Initialize and Start the Server:**

   ```bash
   # Create the data directory
   mkdir -p "$HOME/mysql-data/data"

   # Initialize the data directory (insecure mode for development)
   bin/mysqld --initialize-insecure --datadir="$HOME/mysql-data/data" --basedir="$HOME/build/villagesql"

   # Start the server (runs in foreground, use Ctrl-C to stop)
   bin/mysqld --gdb --datadir="$HOME/mysql-data/data" --basedir="$HOME/build/villagesql"

   # In a new terminal, connect using the client
   "$HOME/build/villagesql/bin/mysql" -u root

   # Verify the installation
   "$HOME/build/villagesql/bin/mysql" -u root -e "SELECT VERSION()"
   ```

   **Running as root (Docker or sudo):**

   If running as root (e.g., in Docker), MySQL requires the `--user=root` flag:
   ```bash
   # Initialize as root
   bin/mysqld --user=root --initialize-insecure --datadir="$HOME/mysql-data/data" --basedir="$HOME/build/villagesql"

   # Start as root
   bin/mysqld --user=root --gdb --datadir="$HOME/mysql-data/data" --basedir="$HOME/build/villagesql"
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
   "$HOME/build/villagesql/bin/mysql" -u myuser -p -e "SELECT USER()"
   ```

### Running Tests

Verify your build with the test suite:

```bash
# From your build directory
cd "$HOME/build/villagesql"

# Run all VillageSQL tests including sub-suites
mysql-test/mysql-test-run.pl --do-suite=villagesql --parallel=auto

# Run a specific test
mysql-test/mysql-test-run.pl villagesql.my_test_name

# Update test results after making changes
mysql-test/mysql-test-run.pl --record villagesql.my_test_name

# Run VillageSQL unit tests
make -j $(getconf _NPROCESSORS_ONLN) villagesql-unit-tests && ctest -L villagesql
```

## Quick Start: Using Extensions

When building from source, VillageSQL Server includes two built-in extensions:
- **`vsql_complex`**: Complex number data type and arithmetic
- **`vsql_simple`**: A minimal "Hello World" demonstration of custom types and functions

> [!NOTE]
> Additional extensions are available in separate repositories:
> - [**`vsql_uuid`**](https://github.com/villagesql/vsql-uuid): UUID generation and validation
> - [**`vsql_ai`**](https://github.com/villagesql/vsql-ai): AI prompting via SQL functions
> - [**`vsql_crypto`**](https://github.com/villagesql/vsql-crypto): Cryptographic functions
> - [**`vsql_network_address`**](https://github.com/villagesql/vsql-network-address): IPv4, IPv6, and MAC address types
> - [**`vsql_http`**](https://github.com/villagesql/vsql-http): HTTP client functions (`http_get`, `http_post`, and more)
>
> These can be built from their repositories and installed by copying the `.veb` files to your VEF directory.

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

-- Query using custom functions
SELECT
  id,
  reading,
  complex_abs(reading) AS magnitude,
  complex_real(reading) AS real_part,
  complex_imag(reading) AS imag_part
FROM signals;

-- Clean up: Drop table first, then uninstall extension
DROP TABLE signals;
UNINSTALL EXTENSION vsql_complex;
```

## Developing Extensions

VillageSQL provides a C++ SDK and a Rust SDK for building high-performance extensions.

- **Example Code:**
  - `villagesql/examples/vsql-complex`: Reference implementation with arithmetic, custom hash handlers, and platform-independent serialization.
  - `villagesql/examples/vsql-simple`: A minimal "Hello World" implementation of a custom type and functions.
- **Templates:**
  - [`villagesql/vsql-extension-template`](https://github.com/villagesql/vsql-extension-template) (C++)
  - [`villagesql/vsql-extension-template-rust`](https://github.com/villagesql/vsql-extension-template-rust) (Rust)
- **Header API:** Detailed extension API definitions can be found in `villagesql/sdk/include/villagesql/vsql.h`.
- **Rust SDK:** [`villagesql/vsql-rust-sdk`](https://github.com/villagesql/vsql-rust-sdk) provides the `villagesql` crate and the `cargo-vsql` build tool, with runnable examples under `examples/`.

## Known Limitations

- **No Custom Index Types:** Custom-typed columns work with standard B-tree indexes via the type's compare function. Extension-defined custom index types are not available yet (coming soon).
- **Limited Built-in Aggregate Support:** COUNT(DISTINCT), MIN, and MAX work with custom types. Extensions can define custom aggregate functions via VDF Aggregates. Built-in aggregates like SUM and AVG are not yet supported for custom types.
- **Alpha Stability:** Expect breaking changes and potential bugs as we progress towards Beta.
- **No Windows Support:** We don't support compiling to .dll to Windows yet. ([#16](https://github.com/villagesql/villagesql-server/issues/16))

## Roadmap

The full, up-to-date roadmap is at [villagesql.com/roadmap](https://villagesql.com/roadmap).

## Troubleshooting

### Build Failures

**OpenSSL not found:**
```bash
# macOS with Homebrew
brew install openssl@3
cmake "$HOME/villagesql-server" -DWITH_SSL=/opt/homebrew/opt/openssl@3

# Linux (Ubuntu/Debian)
sudo apt-get install libssl-dev
cmake "$HOME/villagesql-server" -DWITH_SSL=system
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
- Check error log in your data directory (e.g., `$HOME/mysql-data/data/*.err`)

**Port already in use:**
If you see "Bind on TCP/IP port: Address already in use", either stop the existing MySQL instance or specify a different port:
```bash
bin/mysqld --gdb --datadir="$HOME/mysql-data/data" --basedir="$HOME/build/villagesql" --port=3307
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
