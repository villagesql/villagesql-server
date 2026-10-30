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
#include <cstdio>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "custom_column.h"
#include "sql/dd/types/index.h"
#include "sql/dd/types/table.h"
#include "sql/handler.h"
#include "sql/table.h"
#include "storage/innobase/include/data0data.h"
#include "storage/innobase/include/dict0dd.h"
#include "storage/innobase/include/dict0dict.h"
#include "storage/innobase/include/dict0mem.h"
#include "storage/innobase/include/ha_prototypes.h"
#include "storage/innobase/include/mach0data.h"
#include "storage/innobase/include/mem0mem.h"
#include "storage/innobase/include/que0que.h"
#include "storage/innobase/include/row0mysql.h"
#include "storage/innobase/include/univ.i"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/index_context.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/types/util.h"
#include "villagesql/veb/veb_file.h"

namespace villagesql {
namespace innodb {

class Mtr_guard {
 public:
  Mtr_guard() { mtr_start(&m_mtr); }
  ~Mtr_guard() {
    if (m_mtr.is_active()) mtr_commit(&m_mtr);
  }
  Mtr_guard(const Mtr_guard &) = delete;
  Mtr_guard &operator=(const Mtr_guard &) = delete;

  mtr_t *get() { return &m_mtr; }

 private:
  mtr_t m_mtr;
};

// Number of primary key columns for the table owning this index. The clustered
// index's unique-prefix length is the primary key column count.
static uint32_t primary_key_columns(const dict_index_t *index) {
  const dict_table_t *table = index->table;
  const dict_index_t *clust = table->first_index();
  return clust->n_uniq;
}

// Upper bound on number of arguments for profile/helper function. Matches
// VEF_INDEX_PROFILE_FN_MAX_ARGS, which villagesql/veb/validate.cc enforces at
// extension registration time.
constexpr uint32_t MAX_PROFILE_FN_ARGS = VEF_INDEX_PROFILE_FN_MAX_ARGS;

static const vef_index_profile_fn_binding_t *find_fn_binding(
    const std::vector<vef_index_profile_fn_binding_t> &bindings,
    uint32_t fn_id) {
  for (const auto &binding : bindings) {
    if (binding.fn_id == fn_id) return &binding;
  }
  return nullptr;
}

static void fill_custom_invalue(const vef_storage_col_data_t &col_data,
                                const TypeContext *tc, vef_invalue_t &v) {
  v.type = VEF_TYPE_CUSTOM;
  v.is_null = col_data.data == nullptr;
  v.bin_value = col_data.data;
  v.bin_len = col_data.length;
  if (tc != nullptr) {
    const TypeParameters &params = tc->parameters();
    v.type_params = {params.count(), params.key_data(), params.value_data()};
  } else {
    v.type_params = {0, nullptr, nullptr};
  }
}

static bool call_vdf(const vef_index_profile_fn_binding_t &binding,
                     const TypeContext *tc, const void *const *args,
                     uint32_t nargs, vef_vdf_result_t *vdf_result) {
  std::array<vef_invalue_t, MAX_PROFILE_FN_ARGS> invalues{};
  std::array<vef_invalue_t *, MAX_PROFILE_FN_ARGS> invalue_ptrs{};
  for (uint32_t i = 0; i < nargs; i++) {
    const auto *col_data = static_cast<const vef_storage_col_data_t *>(args[i]);
    fill_custom_invalue(*col_data, tc, invalues[i]);
    invalue_ptrs[i] = &invalues[i];
  }

  vef_context_t ctx{};
  ctx.protocol = veb::vef_server_protocol_version;

  vef_vdf_args_t vdf_args{};
  vdf_args.value_count = nargs;
  vdf_args.values = invalue_ptrs.data();

  binding.vdf(&ctx, &vdf_args, vdf_result);
  return vdf_result->type == VEF_RESULT_VALUE;
}

static void call_profile_binding(const dict_index_t *index, uint32_t key_pos,
                                 const vef_index_profile_fn_binding_t &binding,
                                 const void *const *args, uint32_t nargs,
                                 void *result) {
  ut_a(binding.vdf != nullptr);
  ut_a(nargs == binding.signature.param_count);
  ut_a(nargs <= MAX_PROFILE_FN_ARGS);
  // Only REAL is supported below; villagesql/veb/validate.cc rejects
  // extension registrations with any other return type, so this holds for
  // any binding that reached the dictionary cache.
  ut_a(binding.signature.return_type.id == VEF_TYPE_REAL);

  const dict_col_t *col = index->get_field(key_pos)->col;
  ut_a(col->custom_column != nullptr);

  const TypeContext *tc = col->custom_column->type_context();

  char error_msg[Custom_index::ERROR_MSG_SIZE] = {};
  vef_vdf_result_t vdf_result{};
  vdf_result.error_msg = error_msg;

  // binding.protocol is the minimum protocol the binding can be called at, so
  // this server must be new enough to meet it. veb/validate.cc has no
  // equivalent check, but every binding that reaches here came from an
  // extension the server already accepted at load time.
  ut_a(binding.protocol <= veb::vef_server_protocol_version);
  bool ok = call_vdf(binding, tc, args, nargs, &vdf_result);
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

  uint32_t max_size =
      col->custom_column ? col->custom_column->get_len() : col->len;
  if (field->prefix_len != 0 && field->prefix_len < max_size) {
    max_size = field->prefix_len;
  }
  return max_size;
}

// The key_pos'th key column of the index. Column references only address
// columns held in extension-managed column storage; returns nullptr and fills
// error_msg for any other column.
static const dict_col_t *ref_column(const dict_index_t *index, uint32_t key_pos,
                                    char *error_msg, uint32_t error_msg_len) {
  if (key_pos >= index->n_user_defined_cols) {
    ut_ad(false);
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len,
               "Key position %u is out of range for index %s with %u key "
               "columns",
               key_pos, index->name(),
               static_cast<uint32_t>(index->n_user_defined_cols));
    }
    return nullptr;
  }

  const dict_col_t *col = index->get_field(key_pos)->col;
  if (!col->stored_by_extn()) {
    ut_ad(false);
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len,
               "Column %s at key position %u of index %s is not in extension "
               "managed column storage",
               index->table->get_col_name(col->ind), key_pos, index->name());
    }
    return nullptr;
  }
  return col;
}

// Reads the value a column reference points to out of the column store into the
// caller's buffer. The extension gets the same layout as any other value of the
// custom type: the reference header followed by the data held by the store.
// Runs its own mini-transaction, so the extension must not call this while
// holding page latches in a mini-transaction of its own.
static bool vef_index_col_ref_to_data_impl(
    vef_index_ref_t index_ref, uint32_t key_pos, vef_storage_col_ref_t col_ref,
    vef_storage_col_data_t *col_data, vef_storage_trx_ref_t *trx_ref,
    char *error_msg, uint32_t error_msg_len) {
  const auto *index = static_cast<const dict_index_t *>(index_ref);
  const dict_col_t *col = ref_column(index, key_pos, error_msg, error_msg_len);
  if (col == nullptr) return true;

  if (col_ref == Custom_column::EMPTY_REF) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len, "Empty column reference for column %s",
               index->table->get_col_name(col->ind));
    }
    return true;
  }

  constexpr uint32_t ref_size =
      static_cast<uint32_t>(dfield_t::extended_ref_size());
  const uint32_t value_len = col->custom_column->get_len();
  ut_a(value_len > ref_size);

  if (col_data->data == nullptr || col_data->length < value_len) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len,
               "Buffer of %u bytes is too small for column %s, %u bytes needed",
               col_data->length, index->table->get_col_name(col->ind),
               value_len);
    }
    return true;
  }

  auto *value = const_cast<byte *>(col_data->data);
  mach_write_to_8(value, col_ref);

  dberr_t err =
      Custom_column::fetch(index->table, col, value + ref_size,
                           value_len - ref_size, value, ref_size, trx_ref);
  if (err != DB_SUCCESS) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len,
               "Failed to fetch column %s from column storage",
               index->table->get_col_name(col->ind));
    }
    return true;
  }

  col_data->length = value_len;
  return false;
}

// Extracts the column reference from a stored value of an extension-managed
// column. Every such value carries its reference in the header, so this needs
// no column store lookup.
static bool vef_index_col_data_to_ref_impl(vef_index_ref_t index_ref,
                                           uint32_t key_pos,
                                           vef_storage_col_data_t col_data,
                                           vef_storage_col_ref_t *col_ref,
                                           char *error_msg,
                                           uint32_t error_msg_len) {
  const auto *index = static_cast<const dict_index_t *>(index_ref);
  const dict_col_t *col = ref_column(index, key_pos, error_msg, error_msg_len);
  if (col == nullptr) return true;

  constexpr uint32_t ref_size =
      static_cast<uint32_t>(dfield_t::extended_ref_size());
  if (col_data.data == nullptr || col_data.length < ref_size) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len,
               "Column %s value of %u bytes carries no reference header",
               index->table->get_col_name(col->ind), col_data.length);
    }
    return true;
  }

  *col_ref = mach_read_from_8(col_data.data);

  if (*col_ref == Custom_column::EMPTY_REF) {
    if (error_msg != nullptr && error_msg_len > 0) {
      snprintf(error_msg, error_msg_len,
               "Column %s value is not in column storage yet",
               index->table->get_col_name(col->ind));
    }
    return true;
  }
  return false;
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
  ctx->is_unique = dict_index_is_unique(index) ? 1 : 0;

  const auto &intf = index->custom_index->interface();
  if ((intf.storage_props & VEF_INDEX_STORAGE_HAS_COLUMN_REF) != 0 ||
      (intf.storage_props & VEF_INDEX_STORAGE_REF_LOOKUP) != 0) {
    // Map the index key to the primary key and index record reference.
    index->set_mapping_btree();
  }

  if ((intf.storage_props & VEF_INDEX_STORAGE_HAS_COLUMN_REF) != 0) {
    // Column references uniquely identify the index entry. Mark the mapping
    // B-tree as unique.
    ut_ad(index->has_mapping_btree());
    index->type |= DICT_UNIQUE;
    index->n_uniq = index->n_user_defined_cols;
  }

  // Column reference conversion is only meaningful for an index type that
  // stores column references in its entries.
  if (intf.storage_props & VEF_INDEX_STORAGE_HAS_COLUMN_REF) {
    ctx->col_ref_to_data_fn = vef_index_col_ref_to_data_impl;
    ctx->col_data_to_ref_fn = vef_index_col_data_to_ref_impl;
  } else {
    ctx->col_ref_to_data_fn = nullptr;
    ctx->col_data_to_ref_fn = nullptr;
  }

  return parse_index_options(index, intf, &ctx->options);
}

bool Custom_index::is_custom(const dict_index_t *index) {
  return index != nullptr && index->custom_index != nullptr;
}

bool Custom_index::is_knn(const dict_index_t *index) {
  if (!is_custom(index)) return false;
  const Custom_index *custom_index = index->custom_index;
  return (flags(custom_index->index_meta().get()) & HA_SUPPORT_KNN) != 0;
}

const vef_type_index_intf_t &Custom_index::interface() const {
  return index_meta()->descriptor()->intf();
}

uint32_t Custom_index::n_extra_fields(const dict_index_t *index) {
  if (!is_custom(index)) return 0;

  const vef_type_index_intf_t &intf = index->custom_index->interface();
  return (intf.storage_props & VEF_INDEX_STORAGE_REF_LOOKUP) ? 1 : 0;
}

bool Custom_index::is_extra_field(const dict_index_t *index, ulint pos) {
  const ulint n_fields = dict_index_get_n_fields(index);
  if (pos >= n_fields) return false;

  const ulint n_extra = n_extra_fields(index);
  if (n_extra == 0 || pos + n_extra < n_fields) return false;

  // Extra fields are appended after all other fields of the index.
  ut_ad(index->has_mapping_btree());
  return true;
}

bool Custom_index::set_extra_field(const dict_index_t *index, ulint pos,
                                   dfield_t *dfield, mem_heap_t *heap) {
  if (!is_extra_field(index, pos)) return false;

  dfield->reset();
  index->get_field(pos)->col->copy_type(dfield_get_type(dfield));

  byte *data = static_cast<byte *>(mem_heap_zalloc(heap, sizeof(KeyRef)));
  dfield_set_data(dfield, data, sizeof(KeyRef));

  return true;
}

void Custom_index::add_extra_fields(dict_index_t *new_index,
                                    const dict_index_t *index) {
  const uint32_t n_extra = n_extra_fields(index);
  ut_a(n_extra <= MAX_EXTRA_FIELDS);
  for (uint32_t i = 0; i < n_extra; i++) {
    const std::string name = "INDEX_KEY_REF_" + std::to_string(i + 1);
    new_index->add_field(mem_heap_strdup(new_index->heap, name.c_str()), 0,
                         true);
    dict_field_t *field = new_index->get_field(new_index->n_def - 1);
    field->fixed_len = sizeof(vef_storage_col_ref_t);

    dict_col_t *col = static_cast<dict_col_t *>(
        mem_heap_alloc(new_index->heap, sizeof(dict_col_t)));
    dict_mem_fill_column_struct(col, 0, DATA_INT, DATA_NOT_NULL | DATA_UNSIGNED,
                                sizeof(vef_storage_col_ref_t), true,
                                UINT32_UNDEFINED, UINT8_UNDEFINED,
                                UINT8_UNDEFINED);
    field->col = col;
  }
}

ulong Custom_index::flags(const IndexContext *meta) {
  ut_a(meta != nullptr);
  const vef_index_cap_t caps = meta->descriptor()->intf().capabilities;

  // TODO(villagesql-indexing):
  // 1. Custom index support:
  //    A. Predicate push down: HA_DO_INDEX_COND_PUSHDOWN
  //    B. For part key search: HA_ONLY_WHOLE_INDEX
  //    C. For storing NULL: HA_TABLE_SCAN_ON_NULL
  //    D. For row ID ordered scan: HA_KEY_SCAN_NOT_ROR
  //    E. For covered scan: HA_KEYREAD_ONLY
  ulong flags = HA_READ_NEXT | HA_ONLY_WHOLE_INDEX | HA_TABLE_SCAN_ON_NULL |
                HA_KEY_SCAN_NOT_ROR;

  if (caps & VEF_INDEX_CAP_RANGE_SCAN) flags |= HA_READ_RANGE;
  if (caps & VEF_INDEX_CAP_REVERSE_SCAN) flags |= HA_READ_PREV;
  if (caps & VEF_INDEX_CAP_ORDER_BY) flags |= HA_READ_ORDER;
  if (caps & VEF_INDEX_CAP_KNN) flags |= HA_SUPPORT_KNN;

  return flags;
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

  return load_storage(new_index);
}

dberr_t Custom_index::load_storage(dict_index_t *new_index) {
  Custom_index *custom_index = new_index->custom_index;

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
  ut_ad(new_index->page == FIL_NULL || new_index->has_mapping_btree());

  storage->ref = custom_index->storage_ref();
  custom_index->set_storage_ctx(storage);
  return DB_SUCCESS;
}

// Recomputes fixed_len of custom column fields, as dict_index_add_col() does.
static void fix_index_fields(dict_index_t *index) {
  const dict_table_t *table = index->table;
  for (uint i = 0; i < index->n_def; i++) {
    dict_field_t *field = index->get_field(i);
    if (field->col->custom_column == nullptr) continue;

    field->fixed_len = static_cast<unsigned int>(
        field->col->get_fixed_size(dict_table_is_comp(table)));

    if (field->prefix_len && field->fixed_len > field->prefix_len) {
      field->fixed_len = (unsigned int)field->prefix_len;
    }

    if (field->fixed_len > DICT_MAX_FIXED_COL_LEN) {
      field->fixed_len = 0;
    }
  }
}

void Custom_index::load_all(dict_table_t *table, const dd::Table *dd_table,
                            const TABLE_SHARE *ts) {
  fix_index_fields(table->first_index());

  // Same key_info[] to DD index mapping as dd_fill_dict_index().
  const uint dd_offset = (ts->primary_key == MAX_KEY) ? 1 : 0;
  for (uint k = 0; k < ts->keys; k++) {
    const KEY *key = &ts->key_info[k];
    if (key->custom_index_context == nullptr) continue;

    const dd::Index *dd_index = dd_table->indexes()[k + dd_offset];
    dict_index_t *index = const_cast<dict_index_t *>(
        dd_find_index(table, const_cast<dd::Index *>(dd_index)));
    if (index == nullptr || is_custom(index)) continue;

    auto old_size = mem_heap_get_size(index->heap);

    fix_index_fields(index);
    dberr_t err = attach(index, key->custom_index_context, dd_index);
    for (uint i = 0; err == DB_SUCCESS && i < key->user_defined_key_parts;
         i++) {
      err = add_profile(index, key->key_part[i].custom_index_profile, i);
    }
    if (err == DB_SUCCESS) err = init_index_ctx(index);
    // Load only if the index storage was already created.
    if (err == DB_SUCCESS && index->custom_index->storage_ref_initialized_) {
      err = load_storage(index);
    }

    // Heap grew after the index was added to the dictionary cache.
    auto new_size = mem_heap_get_size(index->heap);
    dict_sys_mutex_enter();
    dict_sys->size += new_size - old_size;
    dict_sys_mutex_exit();

    if (err != DB_SUCCESS) {
      LogVSQL(WARNING_LEVEL,
              "Cannot load custom index '%s' for table '%s' during recovery",
              index->name(), table->name.m_name);
      ut_ad(false);
    }
  }
}

dberr_t Custom_index::create(dict_index_t *index, trx_id_t trx_id) {
  ut_a(is_custom(index));

  index->page = FIL_NULL;
  index->trx_id = trx_id;

  // Custom index must already have been initialized by ::load
  ut_ad(index->custom_index->index_ctx());

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
  // Pass ref_length=false to use total length for extended storage columns
  // while setting data length for index entries.
  return {static_cast<const unsigned char *>(dfield_get_data(field)),
          static_cast<uint32_t>(dfield_get_len(field, false))};
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
  ut_a(entry->n_fields <= num_key_columns + num_pk_columns + MAX_EXTRA_FIELDS);

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

  const uint32_t n_extra = n_extra_fields(index);
  if (n_extra > 0) {
    ut_a(entry->n_fields > n_extra);
    uint32_t key_ref_pos = entry->n_fields - n_extra;
    dfield_t *key_ref_field = dtuple_get_nth_field(entry, key_ref_pos);

    ut_a(!dfield_is_null(key_ref_field));
    ut_a(dfield_get_len(key_ref_field) == sizeof(KeyRef));
    mach_write_to_8(static_cast<byte *>(dfield_get_data(key_ref_field)),
                    key_ref);
  }
  return DB_SUCCESS;
}

dberr_t Custom_index::fill_profile(const row_prebuilt_t *prebuilt,
                                   vef_index_scan_desc_t *scan_desc,
                                   page_cur_mode_t) {
  // TODO(villagesql-indexing): Support scans for other types based on mode.
  if (!prebuilt->is_knn_query()) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Custom index scan is supported only for KNN queries.";
    return DB_VILLAGESQL_ERROR;
  }

  const IndexProfileDescriptor *profile = profile_for_key(0);
  const vef_index_profile_fn_binding_t *order_fn = nullptr;
  for (const auto &fn : profile->functions()) {
    if (fn.kind == VEF_INDEX_PROFILE_FN_KIND_ORDER) {
      order_fn = &fn;
      break;
    }
  }
  if (order_fn == nullptr) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Custom index profile has no ORDER function.";
    return DB_VILLAGESQL_ERROR;
  }

  scan_desc->keys[0].profile_function_id = order_fn->fn_id;
  scan_desc->limit = static_cast<uint32_t>(prebuilt->get_knn_limit());
  scan_desc->limit_is_exact = prebuilt->knn_limit_is_exact();
  return DB_SUCCESS;
}

dberr_t Custom_index::open_cursor(row_prebuilt_t *prebuilt, mtr_t *mtr,
                                  page_cur_mode_t mode, bool reverse,
                                  bool *eof) {
  const auto &intf = interface();
  const dtuple_t *search_tuple = prebuilt->search_tuple;

  const ulint n_fields = dtuple_get_n_fields(search_tuple);
  ut_a(n_fields <= MAX_REF_PARTS);

  std::array<vef_storage_col_data_t, MAX_REF_PARTS> key_columns;
  for (ulint i = 0; i < n_fields; i++) {
    key_columns[i] = to_col_data(dtuple_get_nth_field(search_tuple, i));
  }

  vef_index_scan_key_t scan_key{};
  scan_key.num_key_columns = static_cast<uint32_t>(n_fields);
  scan_key.key_columns = key_columns.data();

  // Without a search tuple the scan covers the whole index and has no keys.
  vef_index_scan_desc_t scan_desc{};
  scan_desc.reverse = reverse;
  scan_desc.num_keys = n_fields > 0 ? 1 : 0;
  scan_desc.keys = n_fields > 0 ? &scan_key : nullptr;
  scan_desc.readview = static_cast<vef_readview_ref_t>(
      const_cast<ReadView *>(trx_get_read_view(prebuilt->trx)));
  if (n_fields > 0) {
    dberr_t err = fill_profile(prebuilt, &scan_desc, mode);
    if (err != DB_SUCCESS) return err;
  }

  CursorRef cursor = nullptr;
  char error_msg[ERROR_MSG_SIZE] = {};
  bool failed = intf.scan_begin(
      index_ctx(), storage_ctx(), static_cast<vef_storage_mtr_ref_t>(mtr),
      &scan_desc, &cursor, eof, error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Error opening custom index scan: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }

  prebuilt->pcur->custom_cursor = cursor;
  return DB_SUCCESS;
}

dberr_t Custom_index::restore_cursor(row_prebuilt_t *prebuilt, mtr_t *mtr,
                                     bool *eof) {
  const auto &intf = interface();
  CursorRef cursor = prebuilt->pcur->custom_cursor;
  ut_a(cursor != nullptr);

  char error_msg[ERROR_MSG_SIZE] = {};
  bool failed =
      intf.scan_restore(cursor, static_cast<vef_storage_mtr_ref_t>(mtr), eof,
                        error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Error restoring custom index scan: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }
  return DB_SUCCESS;
}

void Custom_index::close_cursor(row_prebuilt_t *prebuilt) {
  CursorRef cursor = prebuilt->pcur->custom_cursor;
  if (cursor == nullptr) return;
  interface().scan_end(&cursor);
  prebuilt->pcur->custom_cursor = nullptr;
}

dberr_t Custom_index::row_search(byte *buf, page_cur_mode_t mode,
                                 row_prebuilt_t *prebuilt, ulint match_mode,
                                 ulint direction, bool range_lock,
                                 bool scan_forward, bool is_mvcc) {
  ut_ad(is_mvcc);
  if (!is_mvcc) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Custom index is not supported for intrinsic tables.";
    return DB_VILLAGESQL_ERROR;
  }

  if (range_lock && prebuilt->select_lock_type != LOCK_NONE) {
    ut_ad(false);
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "Custom index doesn't support range lock.";
    return DB_VILLAGESQL_ERROR;
  }
  // We come here from row_search_mvcc after PHASE 1, PHASE 2, and part of
  // PHASE 3, before opening the cursor. Keep this functionally aligned with
  // PHASE 3, 4 nd 5 of row_search_mvcc. We cannot reuse the code directly
  // because it depends on the B-tree record format.

  que_thr_t *thr = que_fork_get_first_thr(prebuilt->sel_graph);
  dict_index_t *index = prebuilt->index;

  ut_ad(Custom_index::is_custom(index));
  Custom_index *custom_index = index->custom_index;

  dict_index_t *clust_index = index->table->first_index();
  ut_ad(!index->is_clustered());
  ut_ad(!dict_index_is_spatial(index));

  dtuple_t *search_tuple = prebuilt->search_tuple;
  trx_t *trx = prebuilt->trx;
  bool clust_templ_for_sec = prebuilt->need_to_access_clustered;

  (void)thr;
  (void)clust_index;
  (void)trx;
  (void)clust_templ_for_sec;

  // PHASE 3: Open or restore index cursor position
  Mtr_guard mtr_guard;
  mtr_t *mtr = mtr_guard.get();
  bool eof = false;

  if (direction != 0) {
    // Restore Cursor.
    ut_ad(prebuilt->row_read_type != ROW_READ_TRY_SEMI_CONSISTENT);
    ut_ad(prebuilt->row_read_type != ROW_READ_DID_SEMI_CONSISTENT);
    dberr_t err = custom_index->restore_cursor(prebuilt, mtr, &eof);
    if (err != DB_SUCCESS) return err;
  } else if (dtuple_get_n_fields(search_tuple) > 0) {
    // Open Cursor using search tuple and mode.
    dberr_t err =
        custom_index->open_cursor(prebuilt, mtr, mode, !scan_forward, &eof);
    if (err != DB_SUCCESS) return err;
  } else {
    // Open cursor to read all.
    ut_ad(mode == PAGE_CUR_G || mode == PAGE_CUR_L);
    dberr_t err =
        custom_index->open_cursor(prebuilt, mtr, mode, !scan_forward, &eof);
    if (err != DB_SUCCESS) return err;
  }
  // PHASE 4: Look for matching records in a loop

  // PHASE 5: Move the cursor to the next index record

  if (eof) custom_index->close_cursor(prebuilt);
  ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
      << "Row search on a custom index is not supported yet.";
  return DB_VILLAGESQL_ERROR;
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
