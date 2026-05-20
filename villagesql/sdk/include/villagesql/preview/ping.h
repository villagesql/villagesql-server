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

#ifndef VILLAGESQL_PREVIEW_PING_H
#define VILLAGESQL_PREVIEW_PING_H

#include <villagesql/abi/preview/ping.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

namespace vsql::preview_ping {

// Declare a PingCapability by value in your extension and pass it to
// .with(). VEF populates the capability during registration. Inheriting
// CapabilityBase enrolls each declared instance in a per-.so registry; the
// extension fails to load if any instance is not registered via .with()
// or if the same instance is passed to .with() more than once.
class PingCapability : public ::vsql::detail::CapabilityBase<PingCapability> {
 public:
  long long ping() const;

 private:
  template <typename Capability>
  friend struct ::vsql::detail::CapabilityTraits;

  const vef_preview_ping_t *abi_ = nullptr;
};

}  // namespace vsql::preview_ping

#include <villagesql/preview/detail/ping_register.h>
#include <villagesql/preview/ping_impl.h>

#endif  // VILLAGESQL_PREVIEW_PING_H
