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

#include "custom_index_helper_fn_name.h"

#include <cstdio>

#include "storage/innobase/include/dict0mem.h"
#include "storage/innobase/villagesql/custom_index.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"

namespace villagesql {
namespace innodb {

// Report the registered name of the helper bound at (key_pos, fn_id), so the
// extension can resolve a native fast-path once at index open. Reuses the same
// profile/binding lookup as dispatch_profile_call.
bool vef_index_helper_fn_name_impl(vef_index_ref_t index_ref, uint32_t key_pos,
                                   uint32_t fn_id, char *name_buf,
                                   uint32_t name_buf_len, char *error_msg,
                                   uint32_t error_msg_len) {
  const auto *index = static_cast<const dict_index_t *>(index_ref);
  const IndexProfileDescriptor *profile =
      index->custom_index->profile_for_key(key_pos);
  if (profile == nullptr) {
    snprintf(error_msg, error_msg_len,
             "helper_fn_name: no profile bound for key column %u", key_pos);
    return true;
  }
  const vef_index_profile_fn_binding_t *binding = nullptr;
  for (const auto &b : profile->helpers()) {
    if (b.fn_id == fn_id) {
      binding = &b;
      break;
    }
  }
  if (binding == nullptr || binding->name == nullptr) {
    snprintf(error_msg, error_msg_len,
             "helper_fn_name: no helper bound at fn_id %u for key column %u",
             fn_id, key_pos);
    return true;
  }
  snprintf(name_buf, name_buf_len, "%s", binding->name);
  return false;
}

}  // namespace innodb
}  // namespace villagesql
