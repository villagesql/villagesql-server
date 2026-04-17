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

// Registers a ValidatedRegistration into the victionary.
//
// register_validated_extension() inserts pre-validated TypeDescriptors and
// FuncDescriptors into the victionary.  The caller must hold the victionary
// write lock and supply a ValidatedRegistration produced by
// validate_extension_registration().

#ifndef VILLAGESQL_VEB_REGISTER_H_
#define VILLAGESQL_VEB_REGISTER_H_

#include <string>

#include "villagesql/veb/validate.h"

class THD;

namespace villagesql {
namespace veb {

// Registers the validated types and funcs into the victionary.
// REQUIRES: Caller must hold victionary write lock.
// Returns false on success, true on error; error_out is set to a descriptive
// message.
bool register_validated_extension(THD &thd, ValidatedRegistration validated,
                                  std::string &error_out);

}  // namespace veb
}  // namespace villagesql

#endif  // VILLAGESQL_VEB_REGISTER_H_
