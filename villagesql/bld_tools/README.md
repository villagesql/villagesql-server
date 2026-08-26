# VillageSQL Build Tools

Shell scripts that build, test, and package VillageSQL Server. The GitHub
Actions workflows in [.github/workflows/](../../.github/workflows/) are
thin wrappers around them.

## Build Activities and Concerns

The build of a VillageSQL server involves multiple activites, some optional,
and several overlapping concerns.  Standard naming for these activities and
concerns helps avoid ambiguity and confusion.

- **Checkout.** The goal during checkout is to acquire all of the source assets
  before starting any build.  Server builds checkout their source with the
  GitHub directive `uses: actions/checkout`.  Builds that include the bundled
  extensions should use `checkout_bundled_extensions.sh` to clone the extension
  repos.
- **Environment.** `setup_build_env.sh` installs the toolchain, dispatching to
  the per-OS script for the host it detects.
- **Build.** `build_ci.sh` configures with CMake, builds the standard target
  set (`mysqld`, the `mysql` client, and the rest), and the
  `villagesql-unit-tests` target.  `build_server.sh` adds CMake flags for
  servers on each platform and hands off to `build_ci.sh`.
- **Extensions.** `checkout_bundled_extensions.sh` clones the extension repos,
  `build_bundled_extensions.sh` builds each into a `.veb`, and
  `test_extension_vebs.sh` runs the MTR suite that each extension repo carries.
  All three read the manifest at
  [../dev_server/bundled_extensions.txt](../dev_server/bundled_extensions.txt)
  and take a **build channel** saying how much of it to act on: `release` (the
  `bundle=all` extensions), `dev` (those plus `bundle=dev`), or `test` (plus
  `bundle=none`, which no artifact ships but the sanitizer and compat suites
  still build). Give every stage the same channel: packaging copies rather than
  builds, so a wider one fails on a `.veb` that was never produced. What a
  channel selects is decided in one place,
  [../bld_matrix/json_bundle_extensions.sh](../bld_matrix/json_bundle_extensions.sh).
- **Test.** `test_ci.sh` runs the VillageSQL unit tests and the MTR
  integration suites.
- **Package.** `package_dev_server.sh` produces the dev-server tarball.

## The scripts

| File                             | Purpose                                                                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `build_bundled_extensions.sh`    | Build each cloned extension into a `.veb` file.                                                                    |
| `build_ci.sh`                    | Configure with CMake and build the server and unit tests.                                                          |
| `build_info.sh`                  | Version and platform helpers. Sourced, not executed.                                                               |
| `build_server.sh`                | Run `build_ci.sh` with server platform options.                                                                    |
| `checkout_bundled_extensions.sh` | Clone the bundled extension repos.  |
| `discover_build_env.sh`          | Print the host OS family (`linux`, `macos`, or `unknown`) on stdout.                                               |
| `get_sdk.sh`                     | Print the path to the extension SDK directory within a build tree.                                                 |
| `include_bundled_extensions.sh`  | Copy built `.veb` files into a package tree.                                                                       |
| `package_dev_server.sh`          | Package a completed build as a dev-server tarball.                                                                 |
| `setup_build_env.sh`             | Install the build host's toolchain by dispatching to the matching per-OS script.                     |
| `setup_linux_build_env.sh`       | Install the required apt packages on Debian/Ubuntu.                                                                |
| `setup_macos_build_env.sh`       | Install the required Homebrew packages on macOS.                                                                   |
| `test_ci.sh`                     | Run the unit tests and MTR integration suites. Takes flags to select and skip suites; `--help` lists them.         |
| `test_extension_vebs.sh`         | Run each extension repo's MTR suite against a build.                                                               |
| `workflow_skip_paths.txt`        | The authoritative list of paths exempt from that gate.                                                             |
| `workflow_skips_build.sh`        | Decide whether a pull request may skip the required build gate. Prints `skip` or `build`, failing safe to `build`. |

`build_info.sh` is the only file here meant to be sourced. The other shared
helper the scripts source, with `die` and the colour codes, lives in the
VillageSQL script directory at
[`villagsql/scripts/vsql_script_utils.sh`](../scripts/vsql_script_utils.sh)
and should normally be references via that path.

See each script's header comment for a description of its arguments and
environment variables.  Most scripts also have good usage instructions.

## Examples

Various workflows use the available scripts as needed.  The `build.yml`
workflow, which controls merges, does builds and simple tests, but never
packages.  The sanitizer builds and exercises extensions.  The release build
workflows do run the whole chain of release processes.

*Build and package a dev-server tarball, running only the unit tests.* Set
`SOURCE_DIR` and `BUILD_DIR` explicitly so every stage agrees on the same
trees:

```bash
export SOURCE_DIR=/path/to/villagesql-server
export BUILD_DIR=/path/to/build
export OUTPUT_DIR="$PWD/packages"
TOOLS="$SOURCE_DIR/villagesql/bld_tools"

"$TOOLS/setup_build_env.sh"                  # once per machine
"$TOOLS/build_server.sh"
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

