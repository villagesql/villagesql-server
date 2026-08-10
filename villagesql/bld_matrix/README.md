# VillageSQL Build Matrix

These scripts provide JSON data tables. They may be widely used.

The output is compact — a single line — because Actions step outputs cannot
span lines. Pipe through `jq .` to read it.

## The scripts

| File                 | Prints                                                       |
| -------------------- | ------------------------------------------------------------ |
| `json_extensions.sh` | The bundled-extension manifest, parsed into an array of objects. |
| `json_platforms.sh`  | The platforms built and tested on, as an array of objects.     |

Each script's header comment documents its arguments and environment
variables, and is the source of truth for them.

## Scope

These scripts parse and shape; they do not select. Each prints its whole set.
Filtering by platform, extension, or ABI belongs to the caller assembling a
particular matrix —
today [../../scripts/villagesql_build-compat-matrix.sh](../../scripts/villagesql_build-compat-matrix.sh),
which [extension-compat-suite.yml](../../.github/workflows/extension-compat-suite.yml)
calls to build its `build-matrix` and `test-matrix`.

## Examples

```bash
MATRIX="$PWD/villagesql/bld_matrix"

"$MATRIX/json_extensions.sh" | jq .

# The extensions shipped in the dev-server tarball.
"$MATRIX/json_extensions.sh" | jq -r '.[] | select(.bundle) | .extension'

# A manifest from somewhere else.
EXTENSIONS_FILE=/tmp/exts.txt "$MATRIX/json_extensions.sh"

# One platform, selected by the caller.
"$MATRIX/json_platforms.sh" | jq -c '[.[] | select(.platform == "macos-arm64")]'
```

## Engineering Notes

Errors go to stderr and exit non-zero. Nothing else may write to stdout, which
rules out `log_info` from
[../scripts/vsql_script_utils.sh](../scripts/vsql_script_utils.sh); `die` is
safe.
