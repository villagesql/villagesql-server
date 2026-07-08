# VillageSQL 0.0.5

Release commit: `16509322376` (#737)

## Highlights

### Custom Types

- **Variable-length custom types** — Types can now declare `persisted_length = -1` to store variadic-size values backed by a `VARBINARY`, keeping only the bytes each value needs while coexisting with fixed-size values in one column. (`077d3a52b6d`, #638; #535)
- **VEF protocol v4** — Variable-length types require `VEF_PROTOCOL_4`; new `veb_register_type_v4` registration entry point and VEB file version bump. (`6ec2c78a271`, #664; `b320b92d921`, #647; `bdd55eb16c1`, #606)
- **VDF result buffers** — Result buffer now grows to fit VDF output, and the charset is set correctly on aggregate results. (`76416fa8ac0`, #728; `74f43784fce`, #719)
- **Parameter resolution fixes** — Correct handling when a parameterized type's `persisted_length` is still `-1` before `resolve_params` runs, plus resolve-params refinements. (`a80de3b94d5`, #683; `0b40842d30d`, #693; `b3dc7428e05`, #677)
- **DROP TABLE with custom VDF generated columns** — Fixed a failure when dropping a table with a generated column backed by a custom VDF after a server restart. (`7580cde82cd`, #761)

### Extension Framework

- **ALTER EXTENSION** — New `ALTER EXTENSION` syntax with pre-check validation, plus supporting refactors. (`8bb2f1c190e`, #667; `99aa8d2c354`, #686; `78b47849186`, #689)
- **Pending extension actions** — Alterations that cannot complete immediately are persisted and resumed, including across server restart; multiple stacked alterations apply in order and independently. (`ebb1aec2220`, #717; `376969b31f7`, #718; `a5502be85e6`, #727; `9014577d139`, #738)
- **UNINSTALL EXTENSION** — Uninstalling an extension is no longer blocked by VDF dependencies. (`2b8a80aeb4b`, #680)
- **INSTALL EXTENSION ... VERSION** — Optional `VERSION` clause asserts the VEB manifest version on install and fails before writing any rows on mismatch. (`e1c01fbd279`, #569)
- **VEF statement events** — Read-only query hooks; the after-execution hook is implemented in this release. (`ebb782cceb7`, #645)
- **INFORMATION_SCHEMA.EXTENSIONS** — Gains four pending-action columns: `PENDING_VERSION`, `PENDING_REQUESTED_AT`, `PENDING_LAST_ERROR`, and `PENDING_LAST_ERROR_AT`. (`de93ebb9d10`, #714; `34dc18bda0a`, #715)
- **Chained capability registration fix** — Fixed registration of chained capabilities. (`7b75f2b7aae`, #614)

### Build & Compatibility

- **MySQL 8.4.10** — Merged upstream `mysql-8.4.10`, which includes two upstream security fixes: an unauthenticated repeated X Protocol TLS upgrade crash (Bug#39204635) and an out-of-bounds read (Bug#39116965). (`2acd413be74`; `874935c05dd`; `6adc159923b`; `413d194f649`)
- **Fixed-width ABI** — Standardized the extension ABI on fixed-width integer types (`uint32_t`, `int64_t`). (`ba56076ba7d`, #755)

## Community

Thanks to Piyush Chauhan ([@pyshx](https://github.com/pyshx)), an external
contributor, for hardening the GitHub Actions workflows (#568) and adding the
`INSTALL EXTENSION ... VERSION` clause (#569).
