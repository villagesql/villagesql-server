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

#ifndef VILLAGESQL_DETAIL_CAPABILITY_TRAITS_H
#define VILLAGESQL_DETAIL_CAPABILITY_TRAITS_H

namespace vsql::detail {

// Primary template for capability traits. Each capability provides a
// specialization that defines:
//   static constexpr const char* kName;
//       The capability name the server looks up.
//   static constexpr const char* kCppTypeName;
//       The fully-qualified C++ type spelling of the wrapper class. Used
//       only for diagnostic messages from the per-.so CapabilityBase
//       registry (verify-with check), so authors see the same name they
//       wrote in source rather than the ABI string.
//   static constexpr const char* kVtableHash;
//       Version tag for the capability's C ABI vtable struct ("ver-1",
//       "ver-2", ...).  Sent on the wire and matched by strcmp against
//       the server's registered version.  Bump the literal whenever the
//       struct's layout changes in a way the server cannot tolerate.
//   using CapabilityConfigType = ...;  (optional, present iff the
//       capability carries a configuration struct)
//       The C ABI struct type the extension passes as capability_config.
//       Its presence serves as a SFINAE tag in vef_register.h for
//       "this capability has a capability_config."
//   static constexpr const char* kCapabilityConfigHash;  (required if
//       CapabilityConfigType is defined)
//       Version tag for the capability_config struct, same form as
//       kVtableHash.
//   static constexpr void* vtable_destination(Capability* p);
//       The address inside *p where the server should write the
//       capability's vtable pointer at registration time. Returned as
//       void*; vef_register_impl casts it to void** for the wire entry.
//
// TODO(villagesql): kVtableHash and kCapabilityConfigHash are manually
// maintained version strings ("ver-1", "ver-2", ...), not actual hashes of
// the struct layout. Either compute a real hash of the ABI struct at compile
// time (e.g. a constexpr CRC over field sizes) so mismatches are caught
// automatically, or rename these to kVtableVersion / kCapabilityConfigVersion
// to reflect what they actually are.
//
// .with(Capability&) on ExtensionBuilder reads these from the
// specialization keyed by the user's wrapper type.
template <typename Capability>
struct CapabilityTraits;

}  // namespace vsql::detail

#endif  // VILLAGESQL_DETAIL_CAPABILITY_TRAITS_H
