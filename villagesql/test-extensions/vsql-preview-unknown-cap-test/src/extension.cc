// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// vsql_preview_unknown_cap_test extension: exercises the preview capability
// system when a requested capability is not registered on the server.
//
// Requests a capability named "vsql::nonexistent" which the server will never
// register. INSTALL EXTENSION fails with a "required capability not
// registered" error before any user-visible code runs, so the extension
// exposes no VDFs.

#include <villagesql/vsql.h>
#include <villagesql/vsql/capability_base.h>

using namespace vsql;

// Made-up abi struct + capability name that the server never registers.
struct NonexistentAbi {
  uint32_t version;
  void (*fn)();
};

struct NonexistentCap : public ::vsql::detail::CapabilityBase<NonexistentCap> {
  NonexistentCap() {}

  const NonexistentAbi *abi = nullptr;
};

namespace vsql::detail {

template <>
struct CapabilityTraits<NonexistentCap> {
  static constexpr const char *kName = "vsql::nonexistent";
  static constexpr uint32_t kAbiVersion = 1;
  using AbiType = NonexistentAbi;

  static constexpr void *vtable_destination(NonexistentCap *p) noexcept {
    return static_cast<void *>(&p->abi);
  }
};

}  // namespace vsql::detail

static NonexistentCap g_nonexistent;

VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_nonexistent))
