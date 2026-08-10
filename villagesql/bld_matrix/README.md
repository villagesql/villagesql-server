# VillageSQL Build Matrix

Scripts that turn build configuration into the JSON that GitHub Actions
consumes. Each script prints one JSON value on stdout and nothing else, so the
pieces compose in a shell and each can be run and inspected on its own.

The output is compact — a single line — because Actions step outputs cannot
span lines. Pipe through `jq .` to read it.

Errors go to stderr and exit non-zero. Nothing else may write to stdout, which
rules out `log_info` from
[../scripts/vsql_script_utils.sh](../scripts/vsql_script_utils.sh); `die` is
safe.

## The scripts

| File                 | Prints                                                       |
| -------------------- | ------------------------------------------------------------ |
| `json_extensions.sh` | The bundled-extension manifest, parsed into an array of objects. |

Each script's header comment documents its arguments and environment
variables, and is the source of truth for them.

## Scope

These scripts parse and shape; they do not select. Filtering by platform,
extension, or ABI belongs to the caller assembling a particular matrix —
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
```
