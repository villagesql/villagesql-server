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
// PREVIEW CAPABILITY — UNSTABLE API
// =============================================================================
// This header is part of the VEF preview surface. Its API and ABI may change
// or be removed without notice. See villagesql/preview/README.md for details.
// =============================================================================

#ifndef VILLAGESQL_PREVIEW_TABLE_STORAGE_H
#define VILLAGESQL_PREVIEW_TABLE_STORAGE_H

#include <villagesql/abi/preview/table_storage.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

namespace vsql::preview_table_storage {

class TableStorageCapability
    : public ::vsql::detail::CapabilityBase<TableStorageCapability> {
 public:
  const vef_preview_table_storage_t *abi() const { return abi_; }
  explicit operator bool() const { return abi_ != nullptr; }

 private:
  template <typename Capability>
  friend struct ::vsql::detail::CapabilityTraits;

  const vef_preview_table_storage_t *abi_ = nullptr;
};

}  // namespace vsql::preview_table_storage

#include <villagesql/preview/detail/table_storage_register.h>

#endif  // VILLAGESQL_PREVIEW_TABLE_STORAGE_H
