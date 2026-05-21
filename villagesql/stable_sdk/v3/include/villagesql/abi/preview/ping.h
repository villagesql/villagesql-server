// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// =============================================================================
// VEF PREVIEW ABI HEADER — UNSTABLE BINARY INTERFACE
// =============================================================================
// This header is both:
//   - an ABI header — extension authors should use the C++ API in
//     <villagesql/vsql.h>, not these raw types. See villagesql/abi/README.md.
//   - a preview capability — API and ABI may change or be removed without
//     notice. See villagesql/preview/README.md.
// =============================================================================

#ifndef VILLAGESQL_ABI_PREVIEW_PING_H
#define VILLAGESQL_ABI_PREVIEW_PING_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::preview::ping"
//
// A trivial capability used to exercise and test the preview capability
// registration system. The server provides a single ping() function that
// returns a monotonically incrementing counter.
//
// Capability name: VEF_PREVIEW_PING_NAME

#define VEF_PREVIEW_PING_NAME "vsql::preview::ping"

// Capability ABI version compiled into this SDK snapshot.
// Extensions can compare this against abi_->version at runtime to determine
// which fields the server supports.
#define VEF_PREVIEW_PING_ABI_VERSION 1

// Returns a monotonically incrementing counter. Used to verify that the
// capability system is wired up correctly end-to-end.
typedef uint64_t (*vef_ping_fn)(void);

typedef struct {
  // Capability ABI version. Always the first field in every capability vtable.
  // Extensions must check this before accessing fields added in later versions.
  uint32_t version;

  // version >= 1
  vef_ping_fn ping;
} vef_preview_ping_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_PING_H
