# VillageSQL Build Matrix

These scripts provide JSON data tables. They may be widely used.

The output is compact — a single line — because Actions step outputs cannot
span lines. Pipe through `jq .` to read it.

## The scripts

| File                 | Prints                                                       |
| -------------------- | ------------------------------------------------------------ |
| `json_extensions.sh` | The bundled-extension manifest, parsed into an array of objects. |
| `json_platforms.sh`  | The platforms built and tested on, as an array of objects.     |
| `json_version.sh`    | The VSQL_VERSION file, parsed into a single object.            |
| `json_docker.sh`     | The Docker images published, as an array of objects.           |
| `json_build_matrix.sh` | The build-server matrix, one row per platform.                |
| `json_test_extensions.sh` | The extension tests, one row per (platform, extension, abi). |
| `json_bundle_extensions.sh` | The extensions one build channel ships.                      |

Each script's header comment documents its arguments and environment
variables, and is the source of truth for them.

## Scope

`json_extensions.sh`, `json_platforms.sh`, `json_version.sh`, and
`json_docker.sh` are the data tables. They parse and shape but never select,
and each prints its whole set.

The rest select and combine. They call the tables they need and take filters
as positional arguments; pass "" for a filter to keep everything.

`json_bundle_extensions.sh` is the one to reach for when the question is "which
extensions does this build ship?" — the answer depends on a build channel
(release, dev, all), not on the manifest alone, and every consumer that copies
that logic by hand is a chance for the image and the tarball to disagree.

A `_matrix` suffix means the output is a GitHub Actions strategy matrix —
`{"include": [...]}`, ready for `matrix: ${{ fromJson(...) }}`. Everything
else prints a bare array, which a caller can wrap with `jq '{include: .}'`.

## Examples

```bash
MATRIX="$PWD/villagesql/bld_matrix"

"$MATRIX/json_extensions.sh" | jq .

# The extensions shipped in a release tarball, and in a pre-release one.
"$MATRIX/json_bundle_extensions.sh" release | jq -r '.[].extension'
"$MATRIX/json_bundle_extensions.sh" dev | jq -r '.[].extension'

# Everything the sanitizer and compat suites build, bundle=none included.
"$MATRIX/json_bundle_extensions.sh" test | jq -r '.[].extension'

# What a channel adds over the release set.
diff <("$MATRIX/json_bundle_extensions.sh" release | jq -r '.[].extension') \
     <("$MATRIX/json_bundle_extensions.sh" dev | jq -r '.[].extension')

# A manifest from somewhere else.
EXTENSIONS_FILE=/tmp/exts.txt "$MATRIX/json_extensions.sh"

# The version, then the semver string built from its fields.
"$MATRIX/json_version.sh" | jq .
"$MATRIX/json_version.sh" | jq -r '"\(.major).\(.minor).\(.patch)"'

# The Docker images published, and the same wrapped as an Actions matrix.
"$MATRIX/json_docker.sh" | jq .
"$MATRIX/json_docker.sh" | jq -c '{include: .}'

# The build-server matrix, every platform and then just one.
"$MATRIX/json_build_matrix.sh"
"$MATRIX/json_build_matrix.sh" macos-arm64

# Every extension test, then one extension on one platform against one ABI.
"$MATRIX/json_test_extensions.sh" | jq length
"$MATRIX/json_test_extensions.sh" macos-arm64 vsql-ai stable

# Wrapped as an Actions matrix, which json_test_extensions.sh leaves to the caller.
"$MATRIX/json_test_extensions.sh" | jq -c '{include: .}'
```

## Engineering Notes

Errors go to stderr and exit non-zero. Nothing else may write to stdout, which
rules out `log_info` from
[../scripts/vsql_script_utils.sh](../scripts/vsql_script_utils.sh); `die` is
safe.
