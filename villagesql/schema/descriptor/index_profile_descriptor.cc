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

#include "villagesql/schema/descriptor/index_profile_descriptor.h"

#include "villagesql/schema/systable/helpers.h"

namespace villagesql {

IndexProfileDescriptorKey::IndexProfileDescriptorKey(
    std::string profile_name, std::string extension_name,
    std::string extension_version)
    : profile_name_(std::move(profile_name)),
      extension_name_(std::move(extension_name)),
      extension_version_(std::move(extension_version)),
      normalized_key_(normalize_extension_name(profile_name_) + "." +
                      normalize_extension_name(extension_name_) + "." +
                      normalize_extension_name(extension_version_)) {}

IndexProfileDescriptor::IndexProfileDescriptor(
    IndexProfileDescriptorKey key, TypeDescriptorKeyPrefix type_ref,
    IndexTypeDescriptorKeyPrefix index_type_ref,
    std::vector<vef_index_profile_fn_binding_t> functions, bool ordering_asc,
    bool default_for_type)
    : key_(std::move(key)),
      type_ref_(std::move(type_ref)),
      index_type_ref_(std::move(index_type_ref)),
      functions_(std::move(functions)),
      ordering_asc_(ordering_asc),
      default_for_type_(default_for_type) {}

}  // namespace villagesql
