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
#include "custom_index.h"

#include <memory>
#include <new>

#include "storage/innobase/include/dict0dict.h"
#include "storage/innobase/include/dict0mem.h"
#include "storage/innobase/include/ha_prototypes.h"
#include "storage/innobase/include/mem0mem.h"
#include "storage/innobase/include/univ.i"
#include "villagesql/schema/descriptor/index_context.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/types/util.h"

namespace villagesql {
namespace innodb {

// Returns the extension's registered index interface for this index, or nullptr
// if the index is not backed by a custom index type.
static const vef_type_index_intf_t *index_interface(const dict_index_t *index) {
  if (index == nullptr || index->custom_index == nullptr) {
    return nullptr;
  }
  return &index->custom_index->index_context()->descriptor()->intf();
}

// Number of primary key columns for the table owning this index. The clustered
// index's unique-prefix length is the primary key column count.
static uint32_t primary_key_columns(const dict_index_t *index) {
  const dict_table_t *table = index->table;
  if (table == nullptr) return 0;
  const dict_index_t *clust = table->first_index();
  return clust != nullptr ? clust->n_uniq : 0;
}

// TODO(villagesql-indexing): Replace these stubs with the real server-side
// dispatchers (a dedicated index_abi.cc, analogous to storage_abi.cc) that
// route profile/helper calls to the registered VDFs and report key lengths
// from the index field metadata. They are wired here so the ABI contract
// (these callbacks are non-NULL) holds for the create() call.
static void vef_index_profile_stub(vef_index_ref_t, uint32_t fn_id, void *,
                                   uint32_t, void *) {
  ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
      << "InnoDB: custom index profile_fn invoked before implemented, fn_id="
      << fn_id;
}

static uint32_t vef_index_max_key_len_stub(vef_index_ref_t, uint32_t, bool) {
  return 0;
}

// Calls intf.parse to validate the WITH (...) parameters. Allocates the
// options struct on index->heap, writes the parsed result into it, and sets
// *options_out to point to it. No-op (leaves *options_out unchanged) when
// intf.parse is nullptr.
static dberr_t parse_index_options(dict_index_t *index,
                                   const vef_type_index_intf_t &intf,
                                   const void **options_out) {
  if (intf.parse == nullptr) return DB_SUCCESS;

  ut_ad(intf.options_size > 0);
  const TypeParameters &params =
      index->custom_index->index_context()->parameters();
  const uint32_t param_count = params.count();
  vef_index_param_t *param_arr = nullptr;
  if (param_count > 0) {
    param_arr = static_cast<vef_index_param_t *>(
        mem_heap_alloc(index->heap, param_count * sizeof(vef_index_param_t)));
    const char *const *keys = params.key_data();
    const char *const *vals = params.value_data();
    for (uint32_t i = 0; i < param_count; i++) {
      param_arr[i].key = keys[i];
      param_arr[i].value = vals[i];
    }
  }

  void *parsed = mem_heap_zalloc(index->heap, intf.options_size);
  char error_msg[Custom_index::ERROR_MSG_SIZE] = {};
  if (intf.parse(param_arr, param_count, parsed, error_msg,
                 sizeof(error_msg))) {
    error_msg[sizeof(error_msg) - 1] = '\0';
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Error parsing custom index options for index "
        << index->name << ": " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }

  *options_out = parsed;
  return DB_SUCCESS;
}

bool Custom_index::is_custom(const dict_index_t *index) {
  return index != nullptr && index->custom_index != nullptr;
}

void Custom_index::load(dict_index_t *index, const IndexContext *ctx) {
  if (ctx == nullptr) return;

  auto ic = AcquireIndexContextClientManaged(ctx);
  if (ic == nullptr) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Failed to acquire custom index context for index "
        << index->name;
    return;
  }

  // mem_heap_alloc guarantees UNIV_MEM_ALIGNMENT-byte alignment; ensure that is
  // enough for placement-new of Custom_index.
  static_assert(alignof(Custom_index) <= UNIV_MEM_ALIGNMENT,
                "Custom_index over-aligned for mem_heap_alloc");
  void *mem = mem_heap_alloc(index->heap, sizeof(Custom_index));
  index->custom_index = new (mem) Custom_index(std::move(ic));
  index->disable_ahi = true;
  // TODO(villagesql-indexing): Call parse_index_options() here to populate
  // ctx->options when opening an already-created index. Requires load() to
  // also invoke the extension's load function to reconnect to persisted storage
  // before options are meaningful.
}

dberr_t Custom_index::create(dict_index_t *index, trx_id_t trx_id) {
  if (index->custom_index == nullptr) {
    // Not a custom index.
    return DB_SUCCESS;
  }
  index->page = FIL_NULL;
  index->trx_id = trx_id;

  const vef_type_index_intf_t &intf =
      index->custom_index->index_context()->descriptor()->intf();

  vef_index_ctx_t *ctx = index->custom_index->index_ctx();
  ctx->version = VEF_INDEX_TYPE_INTF_VERSION;
  ctx->index_ref = static_cast<vef_index_ref_t>(index);
  ctx->num_key_columns = index->n_user_defined_cols;
  ctx->num_primary_key_columns = primary_key_columns(index);
  ctx->profile_fn = vef_index_profile_stub;
  ctx->helper_fn = vef_index_profile_stub;
  ctx->key_len_fn = vef_index_max_key_len_stub;
  ctx->options = nullptr;

  dberr_t parse_err = parse_index_options(index, intf, &ctx->options);
  if (parse_err != DB_SUCCESS) return parse_err;

  auto arena_alloc = [](vef_storage_arena_t *actx, uint32_t sz) -> void * {
    return mem_heap_zalloc(reinterpret_cast<mem_heap_t *>(actx), sz);
  };

  StorageCtx *storage = nullptr;
  char error_msg[ERROR_MSG_SIZE] = {};

  ut_ad(intf.create);
  bool failed =
      intf.create == nullptr ||
      intf.create(ctx, static_cast<vef_storage_space_ref_t>(index->space),
                  static_cast<vef_storage_trx_ref_t>(trx_id),
                  reinterpret_cast<vef_storage_arena_t *>(index->heap),
                  arena_alloc, &storage, error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Error creating custom index storage: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }

  index->custom_index->set_storage_ctx(storage);
  return DB_SUCCESS;
}

dberr_t Custom_index::drop(dict_index_t *index, trx_id_t trx_id) {
  const vef_type_index_intf_t *intf = index_interface(index);
  if (intf == nullptr) {
    return DB_SUCCESS;
  }

  Custom_index *custom_index = index->custom_index;
  if (custom_index->storage_ctx() == nullptr) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Drop: Uninitialized custom index storage";
    return DB_VILLAGESQL_ERROR;
  }

  char error_msg[ERROR_MSG_SIZE] = {};

  ut_ad(intf->drop);
  bool failed =
      intf->drop == nullptr ||
      intf->drop(custom_index->index_ctx(), custom_index->storage_ctx(),
                 static_cast<vef_storage_trx_ref_t>(trx_id), error_msg,
                 sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Error dropping custom index storage: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }
  return DB_SUCCESS;
}

void Custom_index::free_all(dict_index_t *index) {
  if (index->custom_index != nullptr) {
    index->custom_index->~Custom_index();
    index->custom_index = nullptr;
  }
}

}  // namespace innodb
}  // namespace villagesql
