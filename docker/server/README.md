# VillageSQL Server Docker Image

## Building

Build a Docker image for the VillageSQL server locally, tagging it as
dev-server:

```bash
docker build -f docker/server/Dockerfile -t villagesql/server:dev-server .
```

The build compiles VillageSQL from source and bundles several extensions:
- `vsql_complex` (in-tree example)
Plus the bundled extensions defined by
`villagesql/dev_server/bundled_extensions.txt`

The first build takes ~25 minutes (server compilation). Subsequent builds are fast
(~35 seconds) as long as source files outside `docker/` haven't changed.

### Build Args

- `VSQL_DEV_ABI` — build extensions against the dev (unstable) ABI headers instead
  of the stable ABI. Default: `ON`. Set to `OFF` for release builds once extensions
  support the stable ABI.

```bash
docker build -f docker/server/Dockerfile --build-arg VSQL_DEV_ABI=OFF -t villagesql/server:dev-server .
```

## Running

```bash
docker run -d --name vsql -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -p 3306:3306 villagesql/server:dev-server
```

### Environment Variables

Standard MySQL Docker environment variables are supported:

- `MYSQL_ROOT_PASSWORD` — set the root password
- `MYSQL_ALLOW_EMPTY_PASSWORD=1` — allow root login with no password
- `MYSQL_RANDOM_ROOT_PASSWORD=1` — generate a random root password (printed to logs)
- `MYSQL_DATABASE` — create a database on first startup
- `MYSQL_USER` / `MYSQL_PASSWORD` — create a non-root user on first startup

Additionally, VillageSQL adds support for:
- `MYSQL_ROOT_PASSWORD_CACHING_SHA2_HASH_HEX` — set the root caching_sha2_password password hash
  directly (hexadecimal encoded)

At least one of the password options is required.

Wait for the server to be ready:

```bash
until mysqladmin ping -h 127.0.0.1 --silent 2>/dev/null; do sleep 1; done
```

## Connecting

```bash
mysql -h 127.0.0.1 -u root
```

## Installing Extensions

The bundled `.veb` files are pre-installed at `/usr/lib/veb/`. To load them:

```sql
INSTALL EXTENSION vsql_complex;
INSTALL EXTENSION vsql_uuid;
INSTALL EXTENSION vsql_network_address;
INSTALL EXTENSION vsql_ai;
INSTALL EXTENSION vsql_crypto;
```

## Testing

Smoke test an image to verify the server starts and all extensions install correctly:

```bash
docker/server/test-image.sh villagesql/server:dev-server
```

This starts a container, installs each extension, runs a basic CRUD test with the
COMPLEX type, and cleans up.

## Publishing Releases

Various scripts publish multi-arch images to a registry (Docker Hub by default).
The option `--dry-run` prints the docker commands without running them.

| Script | Does |
| --- | --- |
| `publish_image.sh` | Builds one platform into one arch-specific tag. Local only unless given `--push`. |
| `publish_manifest.sh` | Stitches the arch images already in the registry into the multi-arch tags. |

A release is identified by `CODEBASE_VERSION` (e.g. `mysql-8.4_0.0.6`), and that
is the `TAG` both scripts take. Per platform, images are tagged `REPO:TAG-ARCH`
(e.g. `villagesql/server:mysql-8.4_0.0.6-arm64`).

The manifest list is published under `TAG`, plus one tag per `--version-labels`
label, with the version replaced by the label. So `--version-labels latest,stable`
on `mysql-8.4_0.0.6` also publishes `mysql-8.4_latest` and `mysql-8.4_stable`.
An empty list publishes `TAG` alone.

`--define-tags` adds tags that are published as given, with no codebase prefix,
so `--define-tags latest,edge` publishes `REPO:latest` and `REPO:edge`. It is
optional and defaults to none.

## Release Build Args

Both build args are read from the environment and forwarded to `docker build`:

```bash
VSQL_PRE_RELEASE_VERSION="" VSQL_DEV_ABI=OFF \
    docker/server/publish_image.sh --tag mysql-8.4_0.0.6 --platform linux/arm64 --push
```

Defaults are an empty `VSQL_PRE_RELEASE_VERSION` (a release build) and `VSQL_DEV_ABI=ON`.

## Building Additional Extensions

The image includes a build toolchain (cmake, g++, libssl-dev) and the VillageSQL
Extension SDK. To build an extension from source at runtime:

```bash
docker exec -it vsql vsql-build-extension.sh /path/to/extension-source
```

The extension source directory should contain a `CMakeLists.txt`. The script builds
the extension and installs the resulting `.veb` file to `/usr/lib/veb/`.
