# Docker Development Environment for VillageSQL

This Docker setup provides a containerized build and test environment for VillageSQL Server that **matches the GitHub Actions CI environment**.

## Quick Start

```bash
# Build and run the Docker container (one command!)
./docker/dev/run.sh

# Inside the container: configure build
cmake /source -DWITH_SSL=system $CMAKE_EXTRA_FLAGS

# Build VillageSQL (ccache speeds up rebuilds)
make -j$(getconf _NPROCESSORS_ONLN)

# Run tests
./mysql-test/mysql-test-run.pl --suite=villagesql
```

## Environment Details

- **Base image**: Ubuntu 24.04 (matches GitHub Actions `ubuntu-latest`)
- **Dependencies**: Identical to `.github/actions/setup-villagesql-build`
- **Memory limit**: 16GB
- **CPU limit**: 8 cores
- **ccache**: Enabled for faster rebuilds (cached in Docker volume)
- **NUMA disabled**: `-DHAVE_LIBNUMA=no` (Docker containers don't support NUMA syscalls)

## File Organization

```
/
├── docker/
│   └── dev/
│       ├── Dockerfile      # Development container definition
│       ├── build.sh        # Build the Docker image
│       ├── run.sh          # Run the container
│       └── .dockerignore   # Files to exclude from build
└── DOCKER.md               # This file
```

## Architecture

- **Build context**: Repository root (allows access to all source files)
- **Source code**: Your local repository is mounted read-only at `/source`
- **Build artifacts**: Stored in a Docker volume at `/build` (persists between runs)
- **ccache**: Stored in a Docker volume at `/root/.ccache` (speeds up rebuilds)
- **Edit locally**: Use your IDE/editor on your host machine - changes are immediately visible in the container

## Common Commands

### Container Management

```bash
# Start container
./docker/dev/run.sh

# Exit and re-enter (build artifacts persist)
exit
./docker/dev/run.sh

# Rebuild Docker image (after updating Dockerfile)
./docker/dev/build.sh

# Start fresh (delete build artifacts and ccache)
docker volume rm villagesql-build villagesql-ccache
./docker/dev/run.sh
```

### Building (inside container)

```bash
# Use the CI build script (recommended - automatically includes CMAKE_EXTRA_FLAGS)
cd /source
PARALLEL_JOBS=4 ./villagesql/bld_tools/build_ci.sh

# Or manual build (standard release)
cd /build
cmake /source -DWITH_SSL=system $CMAKE_EXTRA_FLAGS
make -j4

# Debug build
cd /build
cmake /source -DWITH_DEBUG=1 -DWITH_SSL=system $CMAKE_EXTRA_FLAGS
make -j4

# Rebuild from scratch
rm CMakeCache.txt
cmake /source -DWITH_SSL=system $CMAKE_EXTRA_FLAGS
make -j4

# Check ccache statistics
ccache -s
```

**Note:** Adjust the number of parallel jobs based on available memory. With 8GB RAM, use `-j2` or `-j4`. With 16GB+, you can use `-j8` or more.

### Testing (inside container)

```bash
# Run all VillageSQL tests
./mysql-test/mysql-test-run.pl --do-suite=village

# Run specific test
./mysql-test/mysql-test-run.pl test_name

# Run tests in parallel
./mysql-test/mysql-test-run.pl --parallel=auto --suite=villagesql

# Create/update test results
./mysql-test/mysql-test-run.pl --record my_new_test
```

## Environment Variables

The following are pre-configured in the container:

- `SOURCE_HOME=/source` - Your source code
- `BUILD_HOME=/build` - Build directory
- `MYSQLTEST_VARDIR=/build/mysql-test/var` - Test artifacts
- `CMAKE_EXTRA_FLAGS=-DHAVE_LIBNUMA=no` - Additional cmake flags for Docker environment

## Workflow

1. **Edit code** on your host machine using your preferred editor
2. **Build** in the container: `make -j$(getconf _NPROCESSORS_ONLN)`
3. **Test** in the container: `./mysql-test/mysql-test-run.pl`
4. **Repeat** - exit and re-enter as needed, build artifacts persist

## Troubleshooting

### Build artifacts from previous runs
```bash
# Clean the build directory
cd /build
rm -rf *
cmake /source -DWITH_SSL=system $CMAKE_EXTRA_FLAGS
make -j$(getconf _NPROCESSORS_ONLN)
```

### NUMA-related errors during tests
NUMA (Non-Uniform Memory Access) syscalls are not available in Docker containers. The container automatically sets `CMAKE_EXTRA_FLAGS=-DHAVE_LIBNUMA=no` to disable NUMA support at compile time. Always include `$CMAKE_EXTRA_FLAGS` in your cmake commands, or use the CI build script which handles this automatically.

### Image updates not taking effect
```bash
./docker/dev/build.sh
```

### Port 3306 already in use
```bash
# Stop any running MySQL on your host
# Or edit docker/dev/run.sh to change the port mapping
```
