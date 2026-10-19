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

#ifndef STORAGE_INNOBASE_VILLAGESQL_CUSTOM_INDEX_HELPER_FN_NAME_H_
#define STORAGE_INNOBASE_VILLAGESQL_CUSTOM_INDEX_HELPER_FN_NAME_H_

#include "villagesql/sdk/include/villagesql/abi/preview/index.h"

// Server-side implementation of the vef_index_ctx_t::helper_fn_name_fn ABI
// callback. A self-contained feature: an extension calls it at index open to
// look up the registered name of the helper bound at (key_pos, fn_id), so it
// can resolve a native fast-path once instead of dispatching through the
// profile on every call. Installed in init_index_ctx (custom_index.cc); the
// logic lives here so it can be reviewed and evolved independently of the index
// engine.

namespace villagesql {
namespace innodb {

// Matches the vef_index_helper_fn_name_fn signature. `index_ref` is the
// dict_index_t*. Writes the bound helper's registered name into name_buf.
// Returns false on success, true on error (with error_msg populated).
bool vef_index_helper_fn_name_impl(vef_index_ref_t index_ref, uint32_t key_pos,
                                   uint32_t fn_id, char *name_buf,
                                   uint32_t name_buf_len, char *error_msg,
                                   uint32_t error_msg_len);

}  // namespace innodb
}  // namespace villagesql

#endif  // STORAGE_INNOBASE_VILLAGESQL_CUSTOM_INDEX_HELPER_FN_NAME_H_
