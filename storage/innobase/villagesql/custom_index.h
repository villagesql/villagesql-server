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

#ifndef STORAGE_INNOBASE_VILLAGESQL_CUSTOM_INDEX_H_
#define STORAGE_INNOBASE_VILLAGESQL_CUSTOM_INDEX_H_

#include <memory>
#include <utility>

#include "db0err.h"
#include "trx0types.h"

#include "villagesql/sdk/include/villagesql/abi/preview/index.h"

// Forward declarations
struct dict_index_t;
struct dict_table_t;

namespace villagesql {

class IndexContext;

namespace innodb {

// Per-index runtime state for an index backed by a custom index type
// extension. Analogous to villagesql::innodb::Custom_column for columns.
//
// Owns a client-managed shared_ptr to the IndexContext: a cache-resident
// dict_index_t can outlive the TABLE_SHARE that resolved the context, so a raw
// pointer would dangle. The object is allocated on the (cache-internal) index
// heap and destroyed from dict_mem_index_free.
class Custom_index {
 public:
  using StorageCtx = vef_storage_ctx_t;

  static constexpr uint32_t ERROR_MSG_SIZE = 512;

  explicit Custom_index(std::shared_ptr<const IndexContext> index_context)
      : index_context_(std::move(index_context)) {}

  const std::shared_ptr<const IndexContext> &index_context() const {
    return index_context_;
  }

  // ABI index context handed to every extension index function. The pointer
  // remains valid for the lifetime of this Custom_index (the index heap).
  vef_index_ctx_t *index_ctx() { return &index_ctx_; }

  // Storage context for this custom index's own storage implementation,
  // populated during the index create/load lifecycle. Unrelated to column
  // storage; Custom_index only manages the storage used by the index itself.
  StorageCtx *storage_ctx() const { return storage_ctx_; }
  void set_storage_ctx(StorageCtx *ctx) { storage_ctx_ = ctx; }

  // Creates the Custom_index runtime state on index->heap and sets
  // index->custom_index. No-op when ctx is null.
  // TODO(villagesql-indexing): load() is currently incomplete and only supports
  // the create path. Once the implementation is complete, revisit whether this
  // should return a dberr_t so callers can propagate load failures.
  static void load(dict_index_t *index, const IndexContext *ctx);

  // Returns true if index has Custom_index runtime state.
  static bool is_custom(const dict_index_t *index);

  // Creates the extension-managed storage for a custom index, invoking the
  // registered index-type create function. No-op (DB_SUCCESS) when the index
  // is not backed by a custom index type.
  static dberr_t create(dict_index_t *index, trx_id_t trx_id);

  // Drops the extension-managed storage for a custom index.
  static dberr_t drop(dict_index_t *index, trx_id_t trx_id);

  // Frees the Custom_index runtime state for an index. Called from
  // dict_mem_index_free() before the index heap is released.
  static void free_all(dict_index_t *index);

 private:
  std::shared_ptr<const IndexContext> index_context_;
  vef_index_ctx_t index_ctx_{};
  StorageCtx *storage_ctx_{nullptr};
};

}  // namespace innodb
}  // namespace villagesql

#endif  // STORAGE_INNOBASE_VILLAGESQL_CUSTOM_INDEX_H_
