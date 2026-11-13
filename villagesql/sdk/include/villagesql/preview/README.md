# Preview Capabilities

Headers in this directory expose **preview capabilities** — VEF features that
are available for early use and feedback but are not yet part of the stable
SDK surface.

## What "preview" means

A preview capability is an SDK feature that we want extension authors to be
able to try, but that we have not committed to keeping stable. Concretely:

- **API may change.** Function names, class shapes, method signatures, and
  header locations can change between server versions, with no deprecation
  window.
- **ABI may change.** A preview capability's ABI version is independent of
  the VEF protocol version; an extension built against one server's preview
  ABI is not guaranteed to load on a newer server.
- **The capability may be removed.** A preview capability can be withdrawn
  entirely if the design doesn't pan out.
- **No backwards-compatibility guarantee.** Unlike code that uses only the
  stable SDK (`villagesql/sdk/include/villagesql/*.h` outside this directory),
  extensions that use preview capabilities should expect to be recompiled,
  and possibly rewritten, against future server versions.
- **Individual capabilities may make stronger guarantees.** A specific
  preview capability's own header may document narrower commitments (for
  example, an ABI that is stable even while the API still evolves). When
  those exist, they override the defaults above for that capability only.

This is intentional. Preview is how we ship a capability early enough to
get real usage and feedback while we still have room to change its shape.

## Enabling preview capabilities on the server

Extensions that use any preview capability will not load unless the server
has preview extensions enabled. The relevant system variable is:

```
vsql_allow_preview_extensions
```

It defaults to `OFF`. To turn it on, use `SET PERSIST` so the setting
survives restart (the server rejects plain `SET GLOBAL = ON`):

```sql
SET PERSIST vsql_allow_preview_extensions = ON;
```

Once any preview extension is installed, the server will refuse to set this
variable back to `OFF` until those extensions are uninstalled.

## How to use a preview capability

- Treat preview headers as **opt-in**: an extension that touches a preview
  capability is opting out of the normal cross-version compatibility story
  for that capability.
- **Send us feedback.** Preview exists so we can learn what works before
  freezing the API; if something is awkward, please tell us.

## Graduation

When a preview capability's shape has settled and we are ready to commit to
its stability, it will move out of `villagesql/preview/` into the stable
SDK surface, with appropriate protocol/version guarantees. Until then,
assume nothing in this directory is stable.
