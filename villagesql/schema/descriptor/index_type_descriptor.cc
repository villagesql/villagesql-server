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

#include <cstdio>

#include "villagesql/schema/identifier_names.h"

namespace villagesql {

IndexTypeDescriptorKey::IndexTypeDescriptorKey(std::string index_type_name,
                                               std::string extension_name,
                                               std::string extension_version)
    : index_type_name_(std::move(index_type_name)),
      extension_name_(std::move(extension_name)),
      extension_version_(std::move(extension_version)),
      normalized_key_(canonical_type_name(index_type_name_) + "." +
                      canonical_extension_name(extension_name_) + "." +
                      canonical_extension_name(extension_version_)) {}

IndexTypeDescriptor::IndexTypeDescriptor(IndexTypeDescriptorKey key,
                                         vef_type_index_intf_t intf)
    : key_(std::move(key)), intf_(intf) {}

bool IndexTypeDescriptor::parse(const TypeParameters &in,
                                TypeParameters *params_out, void *options_out,
                                char *error_msg) const {
  if (intf_.parse == nullptr) {
    *params_out = in;
    return false;
  }

  const vef_type_params_t params = {in.count(), in.key_data(), in.value_data()};

  char buf[VEF_MAX_TYPE_PARAMS_STRING_LEN];
  vef_inferred_type_params_t out = {buf, sizeof(buf), 0, false};
  if (intf_.parse(&params, &out, options_out, error_msg, VEF_MAX_ERROR_LEN))
    return true;

  if (out.overflow) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "parse produced %zu bytes of parameters, more than the %d byte "
             "limit",
             out.actual_len, VEF_MAX_TYPE_PARAMS_STRING_LEN);
    return true;
  }

  *params_out = TypeParameters::from_raw(std::string(out.buf, out.actual_len));
  return false;
}

}  // namespace villagesql
