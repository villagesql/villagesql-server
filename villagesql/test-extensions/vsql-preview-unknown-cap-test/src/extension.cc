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
// populate. The cap_available() VDF returns whether the capability was
// populated — it should always return 0.
//
// VDFs provided:
//   cap_available() -> INT   Returns 1 if the unknown cap was populated, else
//   0.

#include <villagesql/vsql.h>

using namespace vsql;

// Minimal ABI struct for the nonexistent capability — one function pointer.
// The server will never populate this, so fn stays null after registration.
struct NonexistentAbi {
  void (*fn)();
};

struct UnknownCapability {
  static constexpr const char *kName = "vsql::nonexistent";
  static constexpr uint32_t kVersion = 1;
  NonexistentAbi abi_;
};

static UnknownCapability g_cap{};

static void cap_available_impl(IntResult out) {
  out.set(g_cap.abi_.fn != nullptr ? 1 : 0);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&cap_available_impl>("cap_available")
                  .returns(INT)
                  .build())
        .preview_require<g_cap>())
