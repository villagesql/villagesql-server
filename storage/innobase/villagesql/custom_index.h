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
struct dtuple_t;
namespace dd {
class Index;
}

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
  using StorageRef = vef_storage_ref_t;
  using KeyRef = vef_storage_col_ref_t;

  static constexpr uint32_t ERROR_MSG_SIZE = 512;

  explicit Custom_index(std::shared_ptr<const IndexContext> index_metadata)
      : index_metadata_(std::move(index_metadata)) {}

  // Custom index metadata from victionary.
  const std::shared_ptr<const IndexContext> &index_meta() const {
    return index_metadata_;
  }

  // Custom index extension interface.
  const vef_type_index_intf_t &interface() const;

  // ABI index context handed to every extension index function. The pointer
  // remains valid for the lifetime of this Custom_index (the index heap).
  vef_index_ctx_t *index_ctx() { return &index_ctx_; }

  // Storage context for this custom index's own storage implementation,
  // populated during the index create/load lifecycle. Unrelated to column
  // storage; Custom_index only manages the storage used by the index itself.
  StorageCtx *storage_ctx() const { return storage_ctx_; }
  void set_storage_ctx(StorageCtx *ctx) { storage_ctx_ = ctx; }

  // Persistent storage reference
  StorageRef storage_ref() const {
    ut_ad(storage_ref_initialized_);
    return storage_ref_;
  }

  // Copy storage reference from a reference index, if initialized.
  // @return true if a reference could be set, false otherwise
  bool copy_storage_ref(Custom_index *ref_index) {
    if (!ref_index->storage_ref_initialized_) return false;
    storage_ref_initialized_ = true;
    storage_ref_ = ref_index->storage_ref_;
    return true;
  }

  // Set storage reference
  void set_storage_ref(StorageRef ref) {
    storage_ref_ = ref;
    storage_ref_initialized_ = true;
  }

  // Sets up index->custom_index and restores its persistent storage
  // reference from the DD, if available.
  static dberr_t attach(dict_index_t *index, const IndexContext *meta,
                        const dd::Index *dd_index);

  // Loads index from custom index storage. Carries the Custom_index runtime
  // state from old_index onto new_index's heap.
  static dberr_t load(dict_index_t *new_index, const dict_index_t *old_index);

  // Returns true if index has Custom_index runtime state.
  static bool is_custom(const dict_index_t *index);

  // Creates the extension-managed storage for a custom index, invoking the
  // registered index-type create function.
  static dberr_t create(dict_index_t *index, trx_id_t trx_id);

  // Drops the extension-managed storage for a custom index.
  static dberr_t drop(dict_index_t *index, trx_id_t trx_id);

  // Inserts a key into the extension-managed storage for a custom index.
  static dberr_t insert(dict_index_t *index, trx_id_t trx_id,
                        const dtuple_t *entry, bool dup_chk_only);

  // Persists the custom index storage reference into dd::Index se_private_data.
  // Called from dd_write_index() after standard index metadata is written.
  template <typename Index>
  static void save_ref(const dict_index_t *index, Index *dd_index);

  // Frees the Custom_index runtime state for an index. Called from
  // dict_mem_index_free() before the index heap is released.
  static void free_all(dict_index_t *index);

 private:
  std::shared_ptr<const IndexContext> index_metadata_;
  vef_index_ctx_t index_ctx_{};
  StorageCtx *storage_ctx_ = nullptr;
  StorageRef storage_ref_{};
  bool storage_ref_initialized_ = false;
};

}  // namespace innodb
}  // namespace villagesql

#endif  // STORAGE_INNOBASE_VILLAGESQL_CUSTOM_INDEX_H_
