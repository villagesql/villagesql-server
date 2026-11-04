# Vendored Boost subset

This directory contains a partial vendoring of [Boost](https://www.boost.org/),
not the full distribution.  The VillageSQL build only uses the headers actually
included; the directory is *not* a complete or self-consistent Boost install.

## What's here

- **`boost/pfr/`** and **`boost/pfr.hpp`** — [Boost.PFR](https://github.com/boostorg/pfr)
  ("Precise and Flat Reflection"), header-only library for compile-time
  enumeration of aggregate struct fields.  Copyright © 2016-2023 Antony
  Polukhin (and contributors).  Used by
  `villagesql/services/abi_signature_compute.h` to compute
  structural ABI fingerprints.  Version: 1.84.0 (matching the larger Boost
  release tag the path uses; the PFR tree itself was pulled from
  `github.com/boostorg/pfr` at the `boost-1.84.0` tag).

## License

All vendored Boost components are distributed under the
[Boost Software License, Version 1.0](LICENSE_1_0.txt) — see that file for the
full text.  Per-file copyright notices are preserved in the individual headers.

## Adding more Boost

If you need another Boost component, vendor the smallest subtree that compiles
and add a note about it above.  Keep the license file at the root of
`extra/boost/<version>/` covering everything beneath.
