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

#include <sql_const.h>

#include <array>
#include <memory>
#include <new>
#include <type_traits>
#include <vector>

#include "storage/innobase/include/data0data.h"
#include "storage/innobase/include/dict0dd.h"
#include "storage/innobase/include/dict0dict.h"
#include "storage/innobase/include/dict0mem.h"
#include "storage/innobase/include/ha_prototypes.h"
#include "storage/innobase/include/mem0mem.h"
#include "storage/innobase/include/univ.i"
#include "villagesql/schema/descriptor/index_context.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/types/util.h"

namespace villagesql {
namespace innodb {

// Number of primary key columns for the table owning this index. The clustered
// index's unique-prefix length is the primary key column count.
static uint32_t primary_key_columns(const dict_index_t *index) {
  const dict_table_t *table = index->table;
  const dict_index_t *clust = table->first_index();
  return clust->n_uniq;
}

// Upper bound on number of arguments for profile/helper function.
constexpr uint32_t MAX_PROFILE_FN_ARGS = 8;

static const vef_index_profile_fn_binding_t *find_fn_binding(
    const std::vector<vef_index_profile_fn_binding_t> &bindings,
    uint32_t fn_id) {
  for (const auto &binding : bindings) {
    if (binding.fn_id == fn_id) return &binding;
  }
  return nullptr;
}

template <typename InvalueType>
static void fill_custom_invalue(const vef_storage_col_data_t &col_data,
                                const TypeContext *tc, InvalueType &v) {
  v.type = VEF_TYPE_CUSTOM;
  v.is_null = col_data.data == nullptr;
  v.bin_value = col_data.data;
  v.bin_len = col_data.length;
  if constexpr (!std::is_same_v<InvalueType, vef_invalue_v1_t>) {
    if (tc != nullptr) {
      const TypeParameters &params = tc->parameters();
      v.type_params = {params.count(), params.key_data(), params.value_data()};
    } else {
      v.type_params = {0, nullptr, nullptr};
    }
  }
}

template <typename InvalueType>
static bool call_vdf(const vef_index_profile_fn_binding_t &binding,
                     const TypeContext *tc, const void *const *args,
                     uint32_t nargs, vef_vdf_result_t *vdf_result) {
  std::array<InvalueType, MAX_PROFILE_FN_ARGS> invalues{};
  for (uint32_t i = 0; i < nargs; i++) {
    const auto *col_data = static_cast<const vef_storage_col_data_t *>(args[i]);
    fill_custom_invalue(*col_data, tc, invalues[i]);
  }

  vef_context_t ctx{};
  ctx.protocol = binding.protocol;

  vef_vdf_args_t vdf_args{};
  vdf_args.value_count = nargs;

  if constexpr (std::is_same_v<InvalueType, vef_invalue_v1_t>) {
    vdf_args.values_v1 = invalues.data();
    binding.vdf(&ctx, &vdf_args, vdf_result);
  } else {
    std::array<vef_invalue_t *, MAX_PROFILE_FN_ARGS> invalue_ptrs{};
    for (uint32_t i = 0; i < nargs; i++) invalue_ptrs[i] = &invalues[i];
    vdf_args.values = invalue_ptrs.data();
    binding.vdf(&ctx, &vdf_args, vdf_result);
  }
  return vdf_result->type == VEF_RESULT_VALUE;
}

static void call_profile_binding(const dict_index_t *index, uint32_t key_pos,
                                 const vef_index_profile_fn_binding_t &binding,
                                 const void *const *args, uint32_t nargs,
                                 void *result) {
  ut_a(binding.vdf != nullptr);
  ut_a(nargs == binding.signature.param_count);
  ut_a(nargs <= MAX_PROFILE_FN_ARGS);
  ut_a(binding.signature.return_type.id == VEF_TYPE_REAL);

  const dict_col_t *col = index->get_field(key_pos)->col;
  ut_a(col->custom_column != nullptr);

  const TypeContext *tc = col->custom_column->type_context();

  char error_msg[Custom_index::ERROR_MSG_SIZE] = {};
  vef_vdf_result_t vdf_result{};
  vdf_result.error_msg = error_msg;

  ut_a(binding.protocol >= VEF_PROTOCOL_3);
  bool ok = call_vdf<vef_invalue_t>(binding, tc, args, nargs, &vdf_result);
  if (!ok) {
    error_msg[sizeof(error_msg) - 1] = '\0';
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: custom index function '" << binding.name
        << "' failed: " << error_msg;
  }
  *static_cast<double *>(result) = vdf_result.real_value;
}

static void dispatch_profile_call(
    vef_index_ref_t index_ref, uint32_t key_pos, uint32_t fn_id,
    const std::vector<vef_index_profile_fn_binding_t> &(
        IndexProfileDescriptor::*bindings)() const,
    const void *const *args, uint32_t nargs, void *result) {
  const auto *index = static_cast<const dict_index_t *>(index_ref);
  const IndexProfileDescriptor *profile =
      index->custom_index->profile_for_key(key_pos);

  const vef_index_profile_fn_binding_t *binding =
      find_fn_binding((profile->*bindings)(), fn_id);

  call_profile_binding(index, key_pos, *binding, args, nargs, result);
}

static void vef_index_profile_fn_impl(vef_index_ref_t index_ref,
                                      uint32_t key_pos, uint32_t fn_id,
                                      const void *const *args, uint32_t nargs,
                                      void *result) {
  dispatch_profile_call(index_ref, key_pos, fn_id,
                        &IndexProfileDescriptor::functions, args, nargs,
                        result);
}

static void vef_index_helper_fn_impl(vef_index_ref_t index_ref,
                                     uint32_t key_pos, uint32_t fn_id,
                                     const void *const *args, uint32_t nargs,
                                     void *result) {
  dispatch_profile_call(index_ref, key_pos, fn_id,
                        &IndexProfileDescriptor::helpers, args, nargs, result);
}

// Maximum storage length in bytes for the key_pos'th column.
static uint32_t vef_index_max_key_len_impl(vef_index_ref_t index_ref,
                                           uint32_t key_pos, bool is_primary) {
  const auto *index = static_cast<const dict_index_t *>(index_ref);
  const dict_index_t *field_index =
      is_primary ? index->table->first_index() : index;
  ut_a(field_index != nullptr);
  ut_a(is_primary || key_pos < field_index->n_user_defined_cols);
  ut_a(!is_primary || key_pos < field_index->n_uniq);

  const dict_field_t *field = field_index->get_field(key_pos);
  const dict_col_t *col = field->col;

  uint32_t max_size = col->len;
  if (field->prefix_len != 0 && field->prefix_len < max_size) {
    max_size = field->prefix_len;
  }
  return max_size;
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

  const auto &params = index->custom_index->index_meta()->parameters();
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
        << "Error parsing custom index options for index " << index->name
        << ": " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }

  *options_out = parsed;
  return DB_SUCCESS;
}

static dberr_t init_index_ctx(dict_index_t *index) {
  vef_index_ctx_t *ctx = index->custom_index->index_ctx();
  ctx->version = VEF_INDEX_TYPE_INTF_VERSION;
  ctx->index_ref = static_cast<vef_index_ref_t>(index);
  ctx->num_key_columns = index->n_user_defined_cols;
  ctx->num_primary_key_columns = primary_key_columns(index);
  ctx->profile_fn = vef_index_profile_fn_impl;
  ctx->helper_fn = vef_index_helper_fn_impl;
  ctx->key_len_fn = vef_index_max_key_len_impl;
  ctx->options = nullptr;

  const auto &intf = index->custom_index->interface();
  return parse_index_options(index, intf, &ctx->options);
}

bool Custom_index::is_custom(const dict_index_t *index) {
  return index != nullptr && index->custom_index != nullptr;
}

const vef_type_index_intf_t &Custom_index::interface() const {
  return index_meta()->descriptor()->intf();
}

dberr_t Custom_index::attach(dict_index_t *index, const IndexContext *meta,
                             const dd::Index *dd_index) {
  if (meta == nullptr) return DB_SUCCESS;

  auto ic = AcquireIndexContextClientManaged(meta);
  if (ic == nullptr) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Failed to acquire custom index context for index " << index->name;
    ut_ad(false);
    return DB_VILLAGESQL_ERROR;
  }

  // mem_heap_alloc guarantees UNIV_MEM_ALIGNMENT-byte alignment; ensure that is
  // enough for placement-new of Custom_index.
  static_assert(alignof(Custom_index) <= UNIV_MEM_ALIGNMENT,
                "Custom_index over-aligned for mem_heap_alloc");
  void *mem = mem_heap_alloc(index->heap, sizeof(Custom_index));
  index->custom_index = new (mem) Custom_index(std::move(ic));
  // AHI (Adaptive Hash Index) is tightly coupled to B-tree. Custom index types
  // do not currently support AHI.
  index->disable_ahi = true;

  if (dd_index == nullptr) return DB_SUCCESS;

  const char *store_key = dd_index_key_strings[DD_INDEX_EXTENDED_STORAGE_REF];
  if (!dd_index->se_private_data().exists(store_key)) {
    return DB_SUCCESS;
  }

  StorageRef storage_ref;
  dd_index->se_private_data().get(store_key, &storage_ref);

  index->custom_index->set_storage_ref(storage_ref);
  return DB_SUCCESS;
}

dberr_t Custom_index::add_profile(dict_index_t *index,
                                  const IndexProfileDescriptor *profile,
                                  uint32_t key_pos) {
  if (profile == nullptr || !is_custom(index)) return DB_SUCCESS;

  auto ipd = AcquireIndexProfileDescriptorClientManaged(profile);
  if (ipd == nullptr) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Failed to acquire custom index profile for index " << index->name
        << ", key position " << key_pos;
    ut_ad(false);
    return DB_VILLAGESQL_ERROR;
  }

  Custom_index *custom_index = index->custom_index;
  ut_ad(key_pos == custom_index->key_profiles_.size());
  if (key_pos != custom_index->key_profiles_.size()) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Out-of-order custom index profile for index " << index->name
        << ", key position " << key_pos << ", expected "
        << custom_index->key_profiles_.size();
    return DB_VILLAGESQL_ERROR;
  }
  custom_index->key_profiles_.push_back(std::move(ipd));

  return DB_SUCCESS;
}

dberr_t Custom_index::load(dict_index_t *new_index,
                           const dict_index_t *old_index) {
  if (!is_custom(old_index)) return DB_SUCCESS;
  Custom_index *old_custom_index = old_index->custom_index;

  dberr_t err =
      attach(new_index, old_custom_index->index_meta().get(), nullptr);
  if (err != DB_SUCCESS) return err;

  const auto &key_profiles = old_index->custom_index->key_profiles_;
  for (uint32_t i = 0; i < key_profiles.size(); i++) {
    dberr_t perr = add_profile(new_index, key_profiles[i].get(), i);
    if (perr != DB_SUCCESS) return perr;
  }

  dberr_t init_err = init_index_ctx(new_index);
  if (init_err != DB_SUCCESS) return init_err;

  Custom_index *custom_index = new_index->custom_index;

  // We proceed to load only if we have a storage reference from DD. It
  // means the index was already created and needs to be loaded.
  if (!custom_index->copy_storage_ref(old_custom_index)) return DB_SUCCESS;

  auto arena_alloc = [](vef_storage_arena_t *actx, uint32_t sz) -> void * {
    return mem_heap_zalloc(reinterpret_cast<mem_heap_t *>(actx), sz);
  };

  const auto &intf = custom_index->interface();
  StorageCtx *storage = nullptr;
  char error_msg[ERROR_MSG_SIZE] = {};

  bool failed =
      intf.load(custom_index->index_ctx(), custom_index->storage_ref(),
                reinterpret_cast<vef_storage_arena_t *>(new_index->heap),
                arena_alloc, &storage, error_msg, sizeof(error_msg));

  // No dict_sys->size adjustment here: the index is not yet in the dictionary
  // cache, so dict_index_add_to_cache() will account for the heap growth.

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Error loading custom index storage: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }

  storage->ref = custom_index->storage_ref();
  custom_index->set_storage_ctx(storage);
  return DB_SUCCESS;
}

dberr_t Custom_index::create(dict_index_t *index, trx_id_t trx_id) {
  ut_a(is_custom(index));

  index->page = FIL_NULL;
  index->trx_id = trx_id;

  dberr_t init_err = init_index_ctx(index);
  if (init_err != DB_SUCCESS) return init_err;

  auto arena_alloc = [](vef_storage_arena_t *actx, uint32_t sz) -> void * {
    return mem_heap_zalloc(reinterpret_cast<mem_heap_t *>(actx), sz);
  };

  const auto &intf = index->custom_index->interface();
  StorageCtx *storage = nullptr;
  char error_msg[ERROR_MSG_SIZE] = {};
  auto old_size = mem_heap_get_size(index->heap);

  bool failed =
      intf.create(index->custom_index->index_ctx(),
                  static_cast<vef_storage_space_ref_t>(index->space),
                  static_cast<vef_storage_trx_ref_t>(trx_id),
                  reinterpret_cast<vef_storage_arena_t *>(index->heap),
                  arena_alloc, &storage, error_msg, sizeof(error_msg));

  // dict_sys->size tracks the total memory occupied by dictionary heaps.
  auto new_size = mem_heap_get_size(index->heap);
  ut_a(new_size >= old_size);
  ut_ad(!dict_sys_mutex_own());
  dict_sys_mutex_enter();
  dict_sys->size += new_size - old_size;
  dict_sys_mutex_exit();

  if (failed) {
    error_msg[sizeof(error_msg) - 1] = '\0';
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Error creating custom index storage: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }

  index->custom_index->set_storage_ctx(storage);
  return DB_SUCCESS;
}

dberr_t Custom_index::drop(dict_index_t *index, trx_id_t trx_id) {
  ut_a(is_custom(index));

  Custom_index *custom_index = index->custom_index;
  const auto &intf = custom_index->interface();
  char error_msg[ERROR_MSG_SIZE] = {};

  bool failed = intf.drop(
      custom_index->index_ctx(), custom_index->storage_ctx(),
      static_cast<vef_storage_trx_ref_t>(trx_id), error_msg, sizeof(error_msg));
  custom_index->set_storage_ctx(nullptr);

  if (failed) {
    error_msg[sizeof(error_msg) - 1] = '\0';
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Error dropping custom index storage: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }
  return DB_SUCCESS;
}

// Extracts a key column's data for the extension.
static vef_storage_col_data_t to_col_data(const dfield_t *field) {
  if (dfield_is_null(field)) {
    return {nullptr, 0};
  }
  return {static_cast<const unsigned char *>(dfield_get_data(field)),
          static_cast<uint32_t>(dfield_get_len(field))};
}

dberr_t Custom_index::insert(dict_index_t *index, trx_id_t trx_id,
                             const dtuple_t *entry, bool dup_chk_only) {
  // Custom indexes are not supported on intrinsic tables, and dup_chk_only is
  // only used for intrinsic tables because they cannot be rolled back.
  ut_ad(!dup_chk_only);

  Custom_index *custom_index = index->custom_index;
  const auto &intf = custom_index->interface();
  auto *storage_ctx = custom_index->storage_ctx();

  if (!storage_ctx) {
    ut_ad(false);
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Custom index storage context is not initialized.";
    return DB_VILLAGESQL_ERROR;
  }

  vef_index_ctx_t *index_ctx = custom_index->index_ctx();
  uint32_t num_key_columns = index_ctx->num_key_columns;
  uint32_t num_pk_columns = index_ctx->num_primary_key_columns;
  // Both are bounded by MAX_REF_PARTS: num_key_columns is the user-defined
  // key part count, and num_pk_columns is the clustered index's n_uniq
  // (either the primary key's part count, or 1 for the hidden DB_ROW_ID).
  ut_a(num_key_columns <= MAX_REF_PARTS);
  ut_a(num_pk_columns <= MAX_REF_PARTS);

  // entry holds the num_key_columns user-defined key columns at positions
  // [0, num_key_columns), followed by whichever PK columns dict_index_build_
  // internal_non_clust() didn't already find there (a PK column already
  // present among the key columns is not duplicated). So entry->n_fields is
  // between num_key_columns (all PK columns overlap) and num_key_columns +
  // num_pk_columns (no overlap).
  ut_a(entry->n_fields >= num_key_columns);
  ut_a(entry->n_fields <= num_key_columns + num_pk_columns);

  const dict_index_t *clust_index = index->table->first_index();
  ut_a(clust_index != nullptr);

  std::array<vef_storage_col_data_t, MAX_REF_PARTS> key_columns;
  std::array<vef_storage_col_data_t, MAX_REF_PARTS> pkey_columns{};

  if (intf.storage_props & VEF_INDEX_STORAGE_HAS_ROW_REF) {
    // A PK column may already be one of the key columns above; find its actual
    // position in entry the same way row_build_row_ref() does for ordinary
    // secondary indexes.
    for (uint32_t i = 0; i < num_pk_columns; i++) {
      ulint pos = dict_index_get_nth_field_pos(index, clust_index, i);
      ut_a(pos != ULINT_UNDEFINED);
      pkey_columns[i] = to_col_data(dtuple_get_nth_field(entry, pos));
    }
  } else {
    ut_ad(intf.storage_props & VEF_INDEX_STORAGE_HAS_COLUMN_REF);
  }

  for (uint32_t i = 0; i < num_key_columns; i++) {
    key_columns[i] = to_col_data(dtuple_get_nth_field(entry, i));
  }

  KeyRef key_ref{};
  char error_msg[ERROR_MSG_SIZE] = {};

  bool failed = intf.insert(index_ctx, storage_ctx,
                            static_cast<vef_storage_trx_ref_t>(trx_id),
                            key_columns.data(), pkey_columns.data(), &key_ref,
                            error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Error inserting into custom index storage: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }

  if (intf.storage_props & VEF_INDEX_STORAGE_REF_LOOKUP) {
    // TODO(villagesql-indexing): Persist key_ref once mark_delete()/purge()
    // are implemented for Custom_index and need it to relocate this entry.
  }
  return DB_SUCCESS;
}

void Custom_index::free_all(dict_index_t *index) {
  if (index->custom_index != nullptr) {
    Custom_index *custom_index = index->custom_index;
    if (custom_index->storage_ctx()) {
      const auto &intf = custom_index->interface();
      char error_msg[ERROR_MSG_SIZE] = {};
      if (intf.unload != nullptr &&
          intf.unload(custom_index->index_ctx(), custom_index->storage_ctx(),
                      error_msg, sizeof(error_msg))) {
        error_msg[sizeof(error_msg) - 1] = '\0';
        ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
            << "InnoDB: Error unloading custom index storage: " << error_msg;
      }
    }
    custom_index->~Custom_index();
    index->custom_index = nullptr;
  }
}

template <typename Index>
void Custom_index::save_ref(const dict_index_t *index, Index *dd_index) {
  if (!is_custom(index)) return;

  const char *store_key = dd_index_key_strings[DD_INDEX_EXTENDED_STORAGE_REF];
  Custom_index *custom_index = index->custom_index;
  dd_index->se_private_data().set(store_key, custom_index->storage_ctx_->ref);
}

template void Custom_index::save_ref<dd::Index>(const dict_index_t *,
                                                dd::Index *);
template void Custom_index::save_ref<dd::Partition_index>(
    const dict_index_t *, dd::Partition_index *);

}  // namespace innodb
}  // namespace villagesql
