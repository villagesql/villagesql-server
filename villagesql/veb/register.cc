/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#include "villagesql/veb/register.h"

#include "sql/sql_class.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/victionary_client.h"

namespace villagesql {
namespace veb {

bool register_validated_extension(THD &thd, ValidatedRegistration validated,
                                  std::string &error_out) {
  auto &victionary = VictionaryClient::instance();
  victionary.assert_write_lock_held();

  for (auto &descriptor : validated.types) {
    std::string type_name = descriptor.type_name();
    std::string ext_name = descriptor.extension_name();

    LogVSQL(INFORMATION_LEVEL, "Registering type '%s' from extension '%s'",
            type_name.c_str(), ext_name.c_str());

    const TypeDescriptor *existing =
        victionary.type_descriptors().get_committed(descriptor.key());
    if (existing) {
      error_out = "type '" + type_name + "' already exists";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    if (victionary.type_descriptors().MarkForInsertion(thd,
                                                       std::move(descriptor))) {
      error_out = "failed to register type '" + type_name + "'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Successfully registered type '%s'",
            type_name.c_str());
  }

  for (auto &descriptor : validated.funcs) {
    std::string func_name = descriptor.function_name();
    std::string ext_name = descriptor.extension_name();

    LogVSQL(INFORMATION_LEVEL, "Registering VDF '%s' from extension '%s'",
            func_name.c_str(), ext_name.c_str());

    const FuncDescriptor *existing =
        victionary.funcs().get_committed(descriptor.key());
    if (existing) {
      error_out = "VDF '" + func_name + "' already exists";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    if (victionary.funcs().MarkForInsertion(thd, std::move(descriptor))) {
      error_out = "failed to register VDF '" + func_name + "'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Successfully registered VDF '%s'",
            func_name.c_str());
  }

  return false;
}

}  // namespace veb
}  // namespace villagesql
