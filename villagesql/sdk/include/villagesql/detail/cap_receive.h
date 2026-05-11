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

#ifndef VILLAGESQL_DETAIL_CAP_RECEIVE_H
#define VILLAGESQL_DETAIL_CAP_RECEIVE_H

#include <cstdio>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/capability_traits.h>

namespace vsql::detail {

// Glue between the wire format's single-arg receive signature and the
// extension's wrapper object.
//
// The wire format carries `bool (*receive)(vef_capability_receive_arg_t*)` —
// no per-call user_data — so the receive callback needs another way to
// know which wrapper instance to store into. CapReceiveSlot<Capability>
// provides that: a per-type static `instance` is set at registration
// time (in vef_register_impl, which runs at .so load and is NOT
// constexpr), and `receive` reads it.
//
// One Capability per extension is fine because each extension's .so
// has its own static storage. Within an extension, register at most
// one instance per Capability type.
template <typename Capability>
struct CapReceiveSlot {
  // Set at registration time to point at the extension's wrapper.
  // Read by `receive` to locate the abi-pointer slot via the trait.
  static Capability *instance;

  // Captureless — convertible to a plain function pointer for the
  // wire format. Performs a version check before storing the vtable.
  static bool receive(vef_capability_receive_arg_t *arg) noexcept {
    Capability *p = instance;
    if (p == nullptr) {
      // Should not happen if registration ran correctly.
      snprintf(arg->error_buf, arg->error_buf_len,
               "capability slot not initialized");
      return false;
    }
    using Traits = CapabilityTraits<Capability>;
    using AbiType = typename Traits::AbiType;
    auto *v = static_cast<const AbiType *>(arg->vtable);
    if (v->version != Traits::kAbiVersion) {
      snprintf(arg->error_buf, arg->error_buf_len,
               "version mismatch: server=%u, compiled=%u",
               static_cast<unsigned>(v->version),
               static_cast<unsigned>(Traits::kAbiVersion));
      return false;
    }
    void *dest = Traits::vtable_destination(p);
    *static_cast<const AbiType **>(dest) = v;
    return true;
  }
};

template <typename Capability>
Capability *CapReceiveSlot<Capability>::instance = nullptr;

}  // namespace vsql::detail

#endif  // VILLAGESQL_DETAIL_CAP_RECEIVE_H
