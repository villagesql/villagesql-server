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

#ifndef VILLAGESQL_PREVIEW_KEYRING_IMPL_H
#define VILLAGESQL_PREVIEW_KEYRING_IMPL_H

#include <villagesql/abi/preview/keyring.h>
#include <villagesql/preview/keyring.h>

namespace vsql::preview_keyring {

inline KeyringCapability::ReadResult KeyringCapability::read(
    std::string_view data_id, std::string_view auth_id) const {
  std::string value;
  value.resize(4096);
  size_t out_len = 0;
  vef_keyring_result_t result = abi_->read(
      data_id.data(), auth_id.empty() ? nullptr : auth_id.data(),
      reinterpret_cast<unsigned char *>(value.data()), value.size(), &out_len);
  if (result == VEF_KEYRING_OK) {
    value.resize(out_len);
    return {Status::OK, std::move(value)};
  }
  return {static_cast<Status>(result), {}};
}

inline KeyringCapability::Status KeyringCapability::write(
    std::string_view data_id, std::string_view auth_id,
    std::string_view data) const {
  return static_cast<Status>(abi_->write(
      data_id.data(), auth_id.empty() ? nullptr : auth_id.data(),
      reinterpret_cast<const unsigned char *>(data.data()), data.size()));
}

}  // namespace vsql::preview_keyring

#endif  // VILLAGESQL_PREVIEW_KEYRING_IMPL_H
