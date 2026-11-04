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

#ifndef VILLAGESQL_DETAIL_ABI_PIN_H
#define VILLAGESQL_DETAIL_ABI_PIN_H

// Selects the structural ABI fingerprint ("pin") of a capability's vtable or
// capability_config struct for the target being compiled.
//
// Each preview ABI header defines one pin literal per target next to the
// struct it describes, e.g. in abi/preview/ping.h:
//
//   #define VEF_PREVIEW_PING_VTABLE_HASH_MAC       "hash-..."
//   #define VEF_PREVIEW_PING_VTABLE_HASH_LINUX_X86 "hash-..."
//   #define VEF_PREVIEW_PING_VTABLE_HASH_LINUX_ARM "hash-..."
//
// A literal has the form "hash-XXXXXXXXXXXXXXXX": a scheme tag and 16 hex
// digits of a structural hash of the C type (every field, pointee, function
// signature and enum underlying type it reaches).  The extension sends the
// literal for its target as vef_required_capability_t::vtable_hash (or
// capability_config_hash) and the server matches it by strcmp, so an extension
// only loads into a server that serves exactly the shape it was compiled
// against.  The hash is the version: there is no number to bump, and the
// shapes a server serves form a set, not a sequence.
//
// Extensions trust the literal in the header.  The server recomputes the hash
// from the C type at compile time and fails its own build if a literal is
// stale (VEF_PIN_VERIFY in villagesql/services/abi_signature_compute.h), so a
// shape change cannot ship without a new literal.  Because the wire contract
// is just a string, an SDK in another language only needs to copy the
// literals; it never recomputes the hash.
//
// The literals differ per target because the hash follows C++ type identity:
// uint64_t is `unsigned long long` on macOS but `unsigned long` on Linux.
// Whether to hash integers by width instead, which would make the targets
// agree, is an open question; see abi_signature_compute.h.
//
// An empty literal ("") is a placeholder for a target whose pin has not been
// recorded yet.  The server accepts it without verification, and on the wire
// it matches only another empty pin.  The abi_pin_literals unit test fails on
// placeholders and prints the value to paste in.
//
// On a target with no slot the selector expands to "", the same placeholder.
//
// `linux` (bare) is avoided in every name: glibc-targeted GCC predefines
// `#define linux 1`.
//
// TODO(villagesql-windows): add windows_x86 / windows_arm slots.
#if defined(__APPLE__)
#define VEF_PIN(mac_lit, x86_lit, arm_lit) (mac_lit)
#elif defined(__linux__) && defined(__x86_64__)
#define VEF_PIN(mac_lit, x86_lit, arm_lit) (x86_lit)
#elif defined(__linux__) && defined(__aarch64__)
#define VEF_PIN(mac_lit, x86_lit, arm_lit) (arm_lit)
#else
#define VEF_PIN(mac_lit, x86_lit, arm_lit) ""
#endif

// VEF_PIN_OF(VEF_PREVIEW_PING_VTABLE_HASH) selects among the _MAC,
// _LINUX_X86 and _LINUX_ARM literals that share that prefix.
#define VEF_PIN_OF(prefix) \
  VEF_PIN(prefix##_MAC, prefix##_LINUX_X86, prefix##_LINUX_ARM)

#endif  // VILLAGESQL_DETAIL_ABI_PIN_H
