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

#include "villagesql/schema/descriptor/index_type_descriptor.h"

#include "villagesql/schema/systable/helpers.h"

namespace villagesql {

IndexTypeDescriptorKey::IndexTypeDescriptorKey(std::string index_type_name,
                                               std::string extension_name,
                                               std::string extension_version)
    : index_type_name_(std::move(index_type_name)),
      extension_name_(std::move(extension_name)),
      extension_version_(std::move(extension_version)),
      normalized_key_(normalize_type_name(index_type_name_) + "." +
                      normalize_extension_name(extension_name_) + "." +
                      normalize_extension_name(extension_version_)) {}

IndexTypeDescriptor::IndexTypeDescriptor(IndexTypeDescriptorKey key,
                                         vef_type_index_intf_t intf)
    : key_(std::move(key)), intf_(intf) {}

}  // namespace villagesql
