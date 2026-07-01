# VillageSQL 0.0.5

Release commit: `2acd413be74` (merge of `mysql-8.4.10` into `main`)

## Highlights

### Custom Index

A new custom-index framework lets extensions register and back their own index
types through the SDK, with full DDL support on InnoDB.

- **Registration pipeline & capability services** — SDK plumbing for extensions to register custom index types and advertise their capabilities. (`ea256f4398a`, #559; `067a4dd83d1`, #560; `35a58046bc3`, #557)
- **Descriptors & SDK capability** — Descriptor model for custom indexes exposed through the SDK. (`8cf84694679`, #558)
- **DDL on InnoDB** — `CREATE`/`DROP` of custom indexes now works against the InnoDB storage engine. (`07b16f2dda9`, #650)
- **Bulk DDL fix** — Fixed custom storage bulk DDL handling in InnoDB. (`a671501ec7e`, #694)
- **TABLE_SHARE metadata & extension qualification** — Custom index metadata is carried on the table share and can be qualified by extension. (`37fd3a7a7d2`, #631; `d027e3d71e7`, #630)
- **Helper profile functions & test coverage** — Profiling helpers plus reorganized and corrected index tests. (`227a60117b4`, #632; `d062695c7d9`, #561; `a197b2ac089`, #651)

### Custom Types

- **Variable-length custom types** — Types can now declare `persisted_length = -1` to store variadic-size values backed by a `VARBINARY`, keeping only the bytes each value needs while coexisting with fixed-size values in one column. (`077d3a52b6d`, #638; #535)
- **VEF protocol v4** — Variable-length types require `VEF_PROTOCOL_4`; new `veb_register_type_v4` registration entry point and VEB file version bump. (`6ec2c78a271`, #664; `b320b92d921`, #647; `bdd55eb16c1`, #606)
- **VDF result buffers** — Result buffer now grows to fit VDF output, and the charset is set correctly on aggregate results. (`76416fa8ac0`, #728; `74f43784fce`, #719)
- **Parameter resolution fixes** — Correct handling when a parameterized type's `persisted_length` is still `-1` before `resolve_params` runs, plus resolve-params refinements. (`a80de3b94d5`, #683; `0b40842d30d`, #693; `b3dc7428e05`, #677)

### Extensions

- **ALTER EXTENSION** — New `ALTER EXTENSION` syntax with pre-check validation, plus supporting refactors. (`8bb2f1c190e`, #667; `99aa8d2c354`, #686; `78b47849186`, #689)
- **INSTALL EXTENSION ... VERSION** — Optional `VERSION` clause asserts the VEB manifest version on install and fails before writing any rows on mismatch. (`e1c01fbd279`, #569)
- **VEF statement events** — Read-only query hooks; the after-execution hook is implemented in this release. (`ebb782cceb7`, #645)
- **Extensions information_schema** — Extended extension I_S views with a pending-action column in the schema. (`de93ebb9d10`, #714; `34dc18bda0a`, #715)
- **Chained capability registration fix** — Fixed registration of chained capabilities. (`7b75f2b7aae`, #614)

### Build & Compatibility

- **MySQL 8.4.10** — Merged upstream `mysql-8.4.10`, including two upstream security fixes: an unauthenticated repeated X Protocol TLS upgrade crash (Bug#39204635) and an out-of-bounds read (Bug#39116965). (`2acd413be74`; `874935c05dd`; `6adc159923b`; `413d194f649`)
- **Version & build metadata** — Codebase identifier added to the VillageSQL version, Semver revised to make adding it easier, and build-time information embedded in the binary. (`00241ea0a25`, #713; `54e43ed0a96`, #671; `2b76a0c2369`, #684)
- **Dev server & tooling** — Extracted the dev-server packager, allowed arbitrary dev-server flags, added `villint` support and a version VEB target, and bundled `vsql-boolean` / `vsql-rest`. (`070039a30d1`, #707; `beb08aa3421`, #712; `05eecb9dec2`, #726; `d13748926ef`, #675; `17d097300ee`, #635)

## Community

This release incorporates upstream MySQL 8.4.10, including security fixes from
the Oracle MySQL team.

<!-- REVIEW: PRs #568 (harden GitHub Actions workflows) and #569 (INSTALL
EXTENSION VERSION clause) are by Piyush Chauhan (GitHub noreply email) — confirm
whether this is an external contributor to thank here. -->
