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

#ifndef VILLAGESQL_DETAIL_CAPABILITY_BASE_H
#define VILLAGESQL_DETAIL_CAPABILITY_BASE_H

#include <cstddef>

#include <villagesql/vsql/capability_traits.h>

// Lifecycle enforcement for extension capability wrappers.
//
// Every capability wrapper (PingCapability, SqlQueryCapability, etc.) that
// inherits CapabilityBase<Self> enrolls its `this` pointer into a per-.so
// pending list at static-init time. ExtensionBuilder::with(cap) records the
// pointer it was passed. At extension load time, vef_register_impl
// cross-checks the two lists: every enrolled capability must appear in
// exactly one .with() call, and every pointer passed to .with() must
// correspond to an enrolled capability. Anything else fails the load with
// a named diagnostic, instead of silently producing a null vtable that
// would crash later inside a VDF or worker callback.

namespace vsql::detail {

// Linked list node: one per declared capability in this .so. Allocated
// in-place inside CapabilityBase (no heap, no std::vector), so static-init
// order has no global initializers to fight with.
struct PendingCapability {
  const void *cap_ptr;      // identity: address of the capability object
  const char *name;         // CapabilityTraits<Derived>::kName for diagnostics
  bool consumed;            // flipped to true by ExtensionBuilder::with()
  PendingCapability *next;  // intrusive list link
};

// TU-local registry head. Each .so gets its own copy because this is a
// function-local static; no cross-.so coupling.
inline PendingCapability *&pending_capabilities_head() {
  static PendingCapability *head = nullptr;
  return head;
}

// Push a node onto the registry. Called from CapabilityBase's constructor.
inline void enroll_capability(PendingCapability *node) {
  PendingCapability *&head = pending_capabilities_head();
  node->next = head;
  head = node;
}

// Reset all consumed flags. Called at the top of vef_register_impl so that
// extension reload (a second call to vef_register) re-validates against the
// existing static objects, which are not reconstructed on reload.
inline void reset_pending_consumed() {
  for (PendingCapability *p = pending_capabilities_head(); p != nullptr;
       p = p->next) {
    p->consumed = false;
  }
}

// Look up the registry entry for a capability pointer. Returns nullptr if
// not enrolled (i.e. the caller passed a non-CapabilityBase object to
// .with(), or some object from another .so).
inline PendingCapability *find_pending(const void *cap_ptr) {
  for (PendingCapability *p = pending_capabilities_head(); p != nullptr;
       p = p->next) {
    if (p->cap_ptr == cap_ptr) return p;
  }
  return nullptr;
}

// CRTP base for capability wrappers. Adding ": public CapabilityBase<Self>"
// to a capability class is the entire opt-in.
template <typename Derived>
class CapabilityBase {
 protected:
  CapabilityBase() {
    node_.cap_ptr = static_cast<const void *>(this);
    node_.name = CapabilityTraits<Derived>::kName;
    node_.consumed = false;
    node_.next = nullptr;
    enroll_capability(&node_);
  }

  CapabilityBase(const CapabilityBase &) = delete;
  CapabilityBase &operator=(const CapabilityBase &) = delete;
  CapabilityBase(CapabilityBase &&) = delete;
  CapabilityBase &operator=(CapabilityBase &&) = delete;

 private:
  PendingCapability node_;
};

}  // namespace vsql::detail

#endif  // VILLAGESQL_DETAIL_CAPABILITY_BASE_H
