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

#ifndef VILLAGESQL_SQL_CUSTOM_INDEX_HANDLE_H_
#define VILLAGESQL_SQL_CUSTOM_INDEX_HANDLE_H_

#include "villagesql/sdk/include/villagesql/abi/preview/index.h"
#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"

namespace villagesql {

/**
  Handle to a custom index instance the storage engine has already loaded (via
  the VEF extension's intf.load) and keeps for the lifetime of the open index.
  Returned by handler::get_custom_index_handle() so the SQL layer can drive the
  extension's scan callbacks against the engine's live, correctly-loaded storage
  context.

  The SQL layer holds these but does not own them; the storage engine owns their
  lifetime (they are valid while the index is open). Kept out of sql/handler.h
  (which only forward-declares this type) so that widely-included header stays
  free of the preview VEF ABI.
*/
struct CustomIndexHandle {
  vef_index_ctx_t *index_ctx{nullptr};
  vef_storage_ctx_t *storage_ctx{nullptr};
  const vef_type_index_intf_t *intf{nullptr};
};

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_HANDLE_H_
