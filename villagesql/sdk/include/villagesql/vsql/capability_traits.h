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

#ifndef VILLAGESQL_VSQL_CAPABILITY_TRAITS_H
#define VILLAGESQL_VSQL_CAPABILITY_TRAITS_H

namespace vsql::detail {

// Primary template for capability traits. Each capability provides a
// specialization that defines:
//   static constexpr const char* kName;
//       The capability name the server looks up.
//   static constexpr uint32_t kAbiVersion;
//       The ABI version this extension was compiled against. Used as
//       min_version in the wire entry and for the exact-match check in
//       CapReceiveSlot::receive.
//   using AbiType = ...;
//       The C ABI struct type (e.g. vef_preview_ping_t). Used to
//       compute villagesql::detail::abi_type_hash<AbiType>() for the
//       wire format's mismatch check.
//   static constexpr void* vtable_destination(Capability* p);
//       The address inside *p where the server should write the
//       capability's vtable pointer at registration time.
//
// .with(Capability&) on ExtensionBuilder reads these from the
// specialization keyed by the user's wrapper type.
template <typename Capability>
struct CapabilityTraits;

}  // namespace vsql::detail

#endif  // VILLAGESQL_VSQL_CAPABILITY_TRAITS_H
