# VillageSQL Build Tools

Shell scripts that build, test, and package VillageSQL Server. The GitHub
Actions workflows in [../../.github/workflows/](../../.github/workflows/) are
thin wrappers around them.

## Stages

Workflows compose these stages as needed; no single workflow runs all of them.
The required pull-request gate builds and tests but never packages, the release
build runs the whole chain, and the sanitizer build exercises extensions
without producing a package.

- **Environment.** `setup_build_env.sh` installs the toolchain, dispatching to
  the per-OS script for the host it detects.
- **Build.** `build_ci.sh` configures with CMake, builds the default target set
  (`mysqld`, the `mysql` client, and the rest), then builds the
  `villagesql-unit-tests` target.
- **Extensions.** `checkout_bundled_extensions.sh` clones the extension repos,
  `build_bundled_extensions.sh` builds each into a `.veb`, and
  `test_extension_vebs.sh` runs the MTR suite that each extension repo carries.
  All three are driven by the manifest at
  [../dev_server/bundled_extensions.txt](../dev_server/bundled_extensions.txt),
  and all three take a **build channel** saying how much of it to act on:
  `release` (the `bundle=all` extensions), `dev` (those plus `bundle=dev`), or
  `test` (plus `bundle=none`, which no artifact ships but the sanitizer and
  compat suites still build). Which entries a channel selects is decided in one
  place, [../bld_matrix/json_bundle_extensions.sh](../bld_matrix/json_bundle_extensions.sh).
- **Test.** `test_ci.sh` runs the VillageSQL unit tests and the MTR
  integration suites.
- **Package.** `package_dev_server.sh` produces the dev-server tarball.

Two ordering constraints are not evident from the script names:

- The **SDK is a build output**, so `get_sdk.sh` resolves a real path only
  after `build_ci.sh` has run — and `build_bundled_extensions.sh` needs that
  path.
- `package_dev_server.sh` **packages, it does not build.** It runs `cpack`
  against a finished `BUILD_DIR`, and with `BUILD_BUNDLED_EXTENSIONS=1` it
  expects the VEBs to already be in `$BUILD_DIR/veb_output_directory`.

A third constraint follows from the channel: **the clone, build and package
stages must all be given the same one.** Packaging copies rather than builds, so
a channel wider than the build stage's fails on a `.veb` that was never
produced. `package_dev_server.sh` defaults its channel to `dev` when
`VSQL_PRE_RELEASE_VERSION` is set and `release` otherwise, which is why CI
resolves `BUNDLE_CHANNEL` once at job scope and passes the same value to each
stage.

## The scripts

| File                             | Purpose                                                                                                       |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `setup_build_env.sh`             | Install the build toolchain for the host OS by dispatching to the matching per-OS script below.                |
| `setup_linux_build_env.sh`       | Install the required apt packages on Debian/Ubuntu.                                                           |
| `setup_macos_build_env.sh`       | Install the required Homebrew packages on macOS.                                                              |
| `discover_build_env.sh`          | Print the host OS family (`linux`, `macos`, or `unknown`) on stdout.                                           |
| `build_ci.sh`                    | Configure with CMake and build the server and unit tests.                                                     |
| `get_sdk.sh`                     | Print the path to the extension SDK directory within a build tree.                                            |
| `checkout_bundled_extensions.sh` | Clone the bundled extension repos named in the manifest.                                                      |
| `build_bundled_extensions.sh`    | Build each cloned extension into a `.veb` file.                                                               |
| `test_extension_vebs.sh`         | Run each extension repo's MTR suite against a build.                                                          |
| `include_bundled_extensions.sh`  | Copy built `.veb` files into a package tree.                                                                  |
| `test_ci.sh`                     | Run the unit tests and MTR integration suites. Takes flags to select and skip suites; `--help` lists them.     |
| `package_dev_server.sh`          | Package a completed build as a dev-server tarball.                                                            |
| `workflow_skips_build.sh`        | Decide whether a pull request may skip the required build gate. Prints `skip` or `build`, failing safe to `build`. |
| `workflow_skip_paths.txt`        | The authoritative list of paths exempt from that gate.                                                        |
| `build_info.sh`                  | Version and platform helpers. Sourced, not executed.                                                          |

`build_info.sh` is the only file here meant to be sourced. The other shared
helper the scripts source, `die` and the colour codes, lives outside this
directory at [../scripts/vsql_script_utils.sh](../scripts/vsql_script_utils.sh).

Each script's header comment documents its arguments and environment
variables, and is the source of truth for them. See
[../../.github/workflows/](../../.github/workflows/) for how the stages are
composed into CI jobs.

## Examples

Build and package a dev-server tarball, running only the unit tests. Set
`SOURCE_DIR` and `BUILD_DIR` explicitly so every stage agrees on the same
trees:

```bash
export SOURCE_DIR=/path/to/villagesql-server
export BUILD_DIR=/path/to/build
export OUTPUT_DIR="$PWD/packages"
TOOLS="$SOURCE_DIR/villagesql/bld_tools"

"$TOOLS/setup_build_env.sh"                  # once per machine
"$TOOLS/build_ci.sh"
"$TOOLS/test_ci.sh" --no-integration-tests   # unit tests only
"$TOOLS/package_dev_server.sh"
```

The tarball lands in `$OUTPUT_DIR`.

To include the bundled extensions, clone and build them between the build and
package stages, and set `BUILD_BUNDLED_EXTENSIONS=1` so packaging folds the
VEBs in:

```bash
export EXTS_DIR=/path/to/extensions
mkdir -p "$EXTS_DIR"

"$TOOLS/checkout_bundled_extensions.sh" "$EXTS_DIR"
"$TOOLS/build_bundled_extensions.sh" \
    "$EXTS_DIR" \
    "$("$TOOLS/get_sdk.sh" "$BUILD_DIR")" \
    "$BUILD_DIR/veb_output_directory"

BUILD_BUNDLED_EXTENSIONS=1 "$TOOLS/package_dev_server.sh"
```

For a pre-release tarball that also carries the `bundle=dev` extensions, pass
the `dev` channel to every stage. The extension filter is the argument before
it, so pass `""` to keep them all:

```bash
"$TOOLS/checkout_bundled_extensions.sh" "$EXTS_DIR" "" dev
"$TOOLS/build_bundled_extensions.sh" \
    "$EXTS_DIR" \
    "$("$TOOLS/get_sdk.sh" "$BUILD_DIR")" \
    "$BUILD_DIR/veb_output_directory" \
    "" dev

BUILD_BUNDLED_EXTENSIONS=1 BUNDLE_CHANNEL=dev "$TOOLS/package_dev_server.sh"
```

Extensions have their own build dependencies, installed by
`scripts/setup_linux_ext_deps.sh` or `scripts/setup_macos_ext_deps.sh`.

