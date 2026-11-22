/*
   Copyright (c) 2026 VillageSQL Contributors

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "sql/sql_returning.h"

#include <memory>

#include "mem_root_deque.h"
#include "mysqld_error.h"
#include "sql-common/json_dom.h"
#include "sql/auth/auth_acls.h"
#include "sql/auth/auth_common.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/item_strfunc.h"
#include "sql/item_json_func.h"
#include "sql/parse_tree_nodes.h"
#include "sql/protocol.h"
#include "sql/query_result.h"
#include "sql/sp_rcontext.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_insert.h"
#include "sql/sql_lex.h"
#include "sql/sql_udf.h"
#include "sql/table.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/sql/func_lookup.h"
#include "villagesql/vdf/vdf_handler.h"

// One collect aggregate (the target type's collect_aggregate VDF) for a
// RETURNING ... INTO <collect-type variable> target. It is not an Item_sum:
// it is driven directly by Query_result_returning (clear, accumulate per
// returned row, evaluate once at the end) and never takes part in grouping.
// Its result TypeContext is the target variable's, so the VDF sees the
// target's parameters (e.g. element type and max_size of an ARRAY).
class Item_returning_collect final : public Item_str_func {
 public:
  Item_returning_collect(Item *arg, udf_func *udf)
      : Item_str_func(arg), m_udf(udf), m_vdf(udf) {}

  const char *func_name() const override { return m_udf->name.str; }

  bool resolve_type(THD *) override {
    set_data_type(MYSQL_TYPE_VARCHAR);
    collation.set(&my_charset_bin);
    return false;
  }

  // Bind the VDF to the RETURNING item, then override its result TypeContext
  // with the target variable's.
  bool setup(THD *thd, const villagesql::TypeContext *target_tc) {
    if (resolve_type(thd)) return true;
    fixed = true;
    // A collector is set up again at every execution, so its VDF does not
    // make a stored routine parse the statement again each time.
    m_vdf.set_resolved_at_each_execution();
    if (m_vdf.fix_fields(thd, this, arg_count, args, &m_arg_buffer))
      return true;
    set_type_context(nullptr);
    set_type_context(target_tc);
    if (m_vdf.set_return_type_context(target_tc)) return true;
    m_active = true;
    m_vdf.clear();
    return false;
  }

  bool accumulate(THD *thd) {
    bool is_null = false;
    m_vdf.accumulate(&is_null);
    return thd->is_error();
  }

  // Run the aggregate's result function once and keep the value; val_str()
  // then returns it to the variable assignment.
  bool evaluate(THD *thd) {
    String *res = m_vdf.val_str(&m_value, &m_save, m_udf->name.str,
                                &my_charset_bin);
    if (thd->is_error()) return true;
    if (res == nullptr) {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0),
               "a NULL result from a collect aggregate in RETURNING ... INTO");
      return true;
    }
    if (res != &m_value && m_value.copy(*res)) return true;
    null_value = false;
    m_evaluated = true;
    return false;
  }

  String *val_str(String *) override {
    assert(m_evaluated);
    null_value = !m_evaluated;
    return m_evaluated ? &m_value : nullptr;
  }

  void cleanup() override {
    if (m_active) m_vdf.cleanup();
    m_active = false;
    m_evaluated = false;
    Item_str_func::cleanup();
  }

 private:
  udf_func *m_udf;
  villagesql::vdf::vdf_handler m_vdf;
  String m_arg_buffer;
  String m_value;
  String m_save;
  bool m_active{false};
  bool m_evaluated{false};
};

bool prepare_returning_fields(THD *thd, Query_block *query_block,
                              mem_root_deque<Item *> *returning_fields,
                              Query_result_returning **returning_result,
                              PT_returning_into *into) {
  // DELETE resolves its RETURNING items directly into query_block->fields
  // (i.e. returning_fields aliases &query_block->fields); INSERT and UPDATE own
  // a separate deque and expect query_block->fields to be left untouched.
  // TODO(villagesql): remove this special-case once DELETE also uses a separate
  // returning deque (see the aliasing call site in
  // Sql_cmd_delete::prepare_inner).
  const bool fields_alias_query_block =
      returning_fields == &query_block->fields;

  mem_root_deque<Item *> saved_fields = query_block->fields;
  query_block->fields = *returning_fields;
  query_block->resolve_place = Query_block::RESOLVE_SELECT_LIST;
  if (query_block->with_wild && query_block->setup_wild(thd)) return true;
  *returning_fields = query_block->fields;
  if (query_block->setup_base_ref_items(thd))
    return true; /* purecov: inspected */
  if (setup_fields(thd, SELECT_ACL, /*allow_sum_func=*/true,
                   /*split_sum_funcs=*/true, /*column_update=*/false,
                   /*typed_items=*/nullptr, returning_fields,
                   query_block->base_ref_items))
    return true;
  query_block->resolve_place = Query_block::RESOLVE_NONE;
  if (!fields_alias_query_block) query_block->fields = saved_fields;

  *returning_result = new (thd->mem_root) Query_result_returning;
  if (*returning_result == nullptr) return true; /* purecov: inspected */
  (*returning_result)->set_fields(returning_fields);
  if (into != nullptr) {
    (*returning_result)->set_into(into, query_block);
    // INTO v1, v2 assigns positionally, so the counts must agree (same rule and
    // error as SELECT ... INTO).
    if ((*returning_result)->into_scalar() &&
        (*returning_result)->into_scalar_count() !=
            CountVisibleFields(*returning_fields)) {
      my_error(ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, MYF(0));
      return true;
    }
    // INTO TABLE: the target was opened with the statement's tables; report
    // an unusable target at prepare time rather than at the first row.
    if ((*returning_result)->into_table() &&
        (*returning_result)->check_into_table(thd))
      return true;
  }
  return false;
}

void Query_result_returning::set_into(PT_returning_into *into,
                                      Query_block *query_block) {
  m_into = into->json_var;
  m_into_vars =
      into->scalar_vars != nullptr ? &into->scalar_vars->value : nullptr;
  m_into_table_ref = into->table_ref;
  m_query_block = query_block;
}

void Query_result_returning::cleanup() {
  Query_result_send::cleanup();
  reset_collect();
  m_into_rows = 0;
  m_into_array.reset();
  m_into_table_checked = false;
}

bool Query_result_returning::check_into_table(THD *thd) {
  if (m_into_table_checked) return false;
  Table_ref *const tr = m_into_table_ref;
  TABLE *const table = tr->table;
  if (table == nullptr) {
    // open_tables() reports a missing table or a view (required_type is
    // BASE_TABLE); guard against any other unopened reference.
    my_error(ER_WRONG_OBJECT, MYF(0), tr->db, tr->table_name, "BASE TABLE");
    return true;
  }

  // The INTO table must not be one of the tables the statement modifies or
  // reads: the rows would be written to the table being scanned.
  for (Table_ref *leaf = m_query_block != nullptr ? m_query_block->leaf_tables
                                                  : nullptr;
       leaf != nullptr; leaf = leaf->next_leaf) {
    if (leaf->table != nullptr && leaf->table->s == table->s) {
      my_error(ER_UPDATE_TABLE_USED, MYF(0), tr->table_name);
      return true;
    }
  }

  // Triggers on the target are not supported: they were not pre-locked with
  // the statement, and firing INSERT triggers from inside a DELETE/UPDATE is
  // not a defined behaviour.
  if (table->triggers != nullptr) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "RETURNING ... INTO TABLE into a table with triggers");
    return true;
  }

  // Columns are assigned positionally to every column of the target except
  // system-hidden ones (e.g. functional index columns), as INSERT without a
  // column list does.
  uint target_columns = 0;
  for (Field **field = table->field; *field != nullptr; ++field) {
    if (!(*field)->is_hidden_by_system()) ++target_columns;
  }
  if (target_columns != CountVisibleFields(*m_fields)) {
    my_error(ER_WRONG_VALUE_COUNT, MYF(0));
    return true;
  }

  // INSERT privilege on the target (temporary tables pass, as for INSERT).
  if (check_table_access(thd, INSERT_ACL, tr, false, 1, false)) return true;

  m_into_table_checked = true;
  return false;
}

bool Query_result_returning::write_into_table(THD *thd) {
  if (check_into_table(thd)) return true;
  TABLE *const table = m_into_table_ref->table;

  // Every column is written (positional, full-row), so read/write all.
  table->use_all_columns();
  restore_record(table, s->default_values);
  table->next_number_field = table->found_next_number_field;

  // Same row-write path as INSERT ... SELECT (Query_result_insert::send_data):
  // store the values with truncation warnings, enforce NOT NULL and CHECK
  // constraints, then write_record() for the handler write, error reporting
  // and auto-increment bookkeeping. The target has no triggers
  // (check_into_table), so no BEFORE trigger runs. Duplicates are errors (no
  // REPLACE/ON DUPLICATE semantics; a statement-level IGNORE downgrades them
  // as for any write).
  const enum_check_fields save_check_for_truncated_fields =
      thd->check_for_truncated_fields;
  thd->check_for_truncated_fields = CHECK_FIELD_WARN;
  bool error = fill_record_n_invoke_before_triggers(
      thd, table->field, *m_fields, table, TRG_EVENT_INSERT, table->s->fields);
  thd->check_for_truncated_fields = save_check_for_truncated_fields;
  if (!error && !thd->is_error() &&
      !invoke_table_check_constraints(thd, table) && !thd->is_error()) {
    COPY_INFO info(COPY_INFO::INSERT_OPERATION, nullptr,
                   /*manage_defaults=*/false, DUP_ERROR);
    error = write_record(thd, table, &info, nullptr);
  }
  table->next_number_field = nullptr;
  table->autoinc_field_has_explicit_non_null_value = false;
  table->file->ha_release_auto_increment();
  return error || thd->is_error();
}

bool Query_result_returning::send_metadata(THD *thd) {
  // In the INTO cases there is no client result set: rows are captured into
  // variables and the terminator is a plain OK. Suppress metadata entirely.
  if (has_into()) return false;
  return send_result_set_metadata(thd, *m_fields,
                                  Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);
}

bool Query_result_returning::send_count_eof(THD *thd, longlong row_count) {
  if (into_scalar() && decide_into_collect(thd)) return true;
  if (into_collect()) {
    // Zero rows still assigns: each aggregate's result for no input (an empty
    // ARRAY for vsql_array).
    m_into_rows = 0;
    const bool error = assign_collected(thd);
    reset_collect();
    if (error || thd->is_error()) return true;
    my_ok(thd, row_count);
    return false;
  }
  if (into_scalar()) {
    m_into_mode_decided = false;
    // Same contract as SELECT ... INTO v1, v2: no row is a NOT FOUND warning
    // (it triggers a CONTINUE/EXIT HANDLER FOR NOT FOUND); the variables keep
    // their previous values.
    const bool no_row = m_into_rows == 0;
    m_into_rows = 0;
    if (no_row)
      push_warning(thd, Sql_condition::SL_WARNING, ER_SP_FETCH_NO_DATA,
                   ER_THD(thd, ER_SP_FETCH_NO_DATA));
    if (thd->is_error()) return true;
    my_ok(thd, row_count);
    return false;
  }
  if (into_json()) {
    if (assign_into(thd)) return true;
    my_ok(thd, row_count);
    return false;
  }
  if (into_table()) {
    // Validate even when no row was returned, so a bad target is reported
    // consistently; then end the statement with a plain OK like the other
    // INTO forms.
    const bool error = check_into_table(thd);
    m_into_table_checked = false;
    if (error || thd->is_error()) return true;
    my_ok(thd, row_count);
    return false;
  }
  thd->set_row_count_func(row_count);
  return send_eof(thd);
}

// Encode one RETURNING item as a JSON value for the row object. SP-local JSON
// variables are stored as binary JSON (Field_json), so native typed encoding
// round-trips DECIMAL/temporal exactly and is used directly. A @user_var stores
// JSON as text, and text serialization renders a native decimal as a bare
// number that reparses as a double (losing precision) and a native datetime as
// an already-quoted string; so for that target we encode
// DECIMAL/temporal/binary as JSON strings, which reparse exactly.
static bool item_to_json_dom(THD *, Item *item, bool target_is_text,
                             Json_dom_ptr *out) {
  const enum_field_types t = item->data_type();
  const bool stringify =
      target_is_text &&
      (t == MYSQL_TYPE_NEWDECIMAL || t == MYSQL_TYPE_DECIMAL ||
       t == MYSQL_TYPE_DATE || t == MYSQL_TYPE_DATETIME ||
       t == MYSQL_TYPE_TIMESTAMP || t == MYSQL_TYPE_TIME ||
       ((t == MYSQL_TYPE_STRING || t == MYSQL_TYPE_VAR_STRING ||
         t == MYSQL_TYPE_VARCHAR || t == MYSQL_TYPE_BLOB ||
         t == MYSQL_TYPE_TINY_BLOB || t == MYSQL_TYPE_MEDIUM_BLOB ||
         t == MYSQL_TYPE_LONG_BLOB) &&
        item->collation.collation == &my_charset_bin));

  if (stringify) {
    StringBuffer<STRING_BUFFER_USUAL_SIZE> buf;
    String *s = item->val_str(&buf);
    if (item->null_value) {
      *out = create_dom_ptr<Json_null>();
      return *out == nullptr;
    }
    if (item->collation.collation == &my_charset_bin) {
      // Binary data: hex-encode so the text round-trip is lossless (the
      // consumer recovers the bytes with UNHEX in the JSON_TABLE projection).
      const size_t hex_len = s->length() * 2;
      String hex;
      if (hex.alloc(hex_len)) return true; /* purecov: inspected */
      octet2hex(hex.ptr(), s->ptr(), s->length());
      *out = create_dom_ptr<Json_string>(hex.ptr(), hex_len);
    } else {
      *out = create_dom_ptr<Json_string>(s->ptr(), s->length());
    }
    return *out == nullptr;
  }

  // Convert an arbitrary SQL scalar to typed JSON. Item::val_json() only works
  // for items that are already JSON-typed; get_atom_null_as_null() is the
  // general path (used by JSON_ARRAY/JSON_OBJECT) and preserves Json_decimal /
  // Json_datetime opcodes, so the SP-local (binary Field_json) target keeps
  // full precision.
  StringBuffer<STRING_BUFFER_USUAL_SIZE> value_buf;
  StringBuffer<STRING_BUFFER_USUAL_SIZE> tmp_buf;
  Json_wrapper wr;
  if (get_atom_null_as_null(&item, 0, "RETURNING INTO", &value_buf, &tmp_buf,
                            &wr))
    return true;
  *out = wr.clone_dom();
  return *out == nullptr;
}

bool Query_result_returning::assign_scalars(THD *thd) {
  // Mirrors Query_dumpvar::send_data: one row only, columns assigned in order
  // to SP-local variables or @user_vars.
  if (m_into_rows++ > 0) {
    my_error(ER_TOO_MANY_ROWS, MYF(0));
    return true;
  }
  List_iterator_fast<PT_select_var> var_li(*m_into_vars);
  auto it = VisibleFields(*m_fields).begin();
  const auto end = VisibleFields(*m_fields).end();
  PT_select_var *mv;
  while ((mv = var_li++) && it != end) {
    Item *item = *it++;
    if (mv->is_local()) {
      if (thd->sp_runtime_ctx->set_variable(thd, false, mv->get_offset(),
                                            &item))
        return true;
    } else {
      Item_func_set_user_var *suv =
          new (thd->mem_root) Item_func_set_user_var(mv->name, item);
      if (suv == nullptr || suv->fix_fields(thd, nullptr)) return true;
      suv->save_item_result(item);
      if (suv->update()) return true;
    }
  }
  return thd->is_error();
}

bool Query_result_returning::decide_into_collect(THD *thd) {
  if (m_into_mode_decided) return false;
  m_into_mode_decided = true;
  const uint n = m_into_vars->elements;
  const villagesql::TypeContext **tcs =
      thd->mem_root->ArrayAlloc<const villagesql::TypeContext *>(n);
  if (tcs == nullptr) return true; /* purecov: inspected */
  uint n_collect = 0;
  uint i = 0;
  List_iterator_fast<PT_select_var> var_li(*m_into_vars);
  for (PT_select_var *mv; (mv = var_li++) != nullptr;) {
    tcs[i] = nullptr;
    if (mv->is_local() && thd->sp_runtime_ctx != nullptr) {
      const villagesql::TypeContext *tc =
          thd->sp_runtime_ctx->get_item(mv->get_offset())->get_type_context();
      if (tc != nullptr && tc->descriptor() != nullptr &&
          !tc->descriptor()->collect_aggregate_name().empty()) {
        tcs[i] = tc;
        ++n_collect;
      }
    }
    ++i;
  }
  if (n_collect == 0) return false;  // plain scalar INTO
  if (n_collect != n) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "mixing collect-type (e.g. ARRAY) and other variables as "
             "RETURNING ... INTO targets");
    return true;
  }

  Item_returning_collect **collectors =
      thd->mem_root->ArrayAlloc<Item_returning_collect *>(n);
  if (collectors == nullptr) return true; /* purecov: inspected */
  for (uint k = 0; k < n; ++k) collectors[k] = nullptr;
  m_collectors = collectors;
  i = 0;
  for (Item *item : VisibleFields(*m_fields)) {
    if (i >= n) break;
    const villagesql::TypeContext *tc = tcs[i];
    const std::string &agg = tc->descriptor()->collect_aggregate_name();
    // The reference taken by find_func() lives until thd->mem_root (this
    // execution) is cleared.
    const villagesql::FuncDescriptor *fd =
        villagesql::find_func(tc->extension_name(), agg, *thd->mem_root);
    udf_func *udf = fd != nullptr
                        ? villagesql::make_udf_func_from_vdf(fd, *thd->mem_root)
                        : nullptr;
    if (udf == nullptr) {
      my_printf_error(ER_UDF_ERROR,
                      "collect aggregate '%s.%s' of type '%s' is not available",
                      MYF(0), tc->extension_name().c_str(), agg.c_str(),
                      tc->type_name().c_str());
      return true;
    }
    collectors[i] = new (thd->mem_root) Item_returning_collect(item, udf);
    if (collectors[i] == nullptr) return true; /* purecov: inspected */
    if (collectors[i]->setup(thd, tc)) return true;
    ++i;
  }
  return false;
}

bool Query_result_returning::assign_collected(THD *thd) {
  const uint n = m_into_vars->elements;
  // Evaluate everything first: procedure variables are not transactional, so
  // on any error (NULL value, overflow, range) no target is changed.
  for (uint i = 0; i < n; ++i) {
    if (m_collectors[i]->evaluate(thd)) return true;
  }
  uint i = 0;
  List_iterator_fast<PT_select_var> var_li(*m_into_vars);
  for (PT_select_var *mv; (mv = var_li++) != nullptr;) {
    Item *value = m_collectors[i++];
    if (thd->sp_runtime_ctx->set_variable(thd, false, mv->get_offset(),
                                          &value))
      return true;
  }
  return thd->is_error();
}

void Query_result_returning::reset_collect() {
  // The collectors live on the execution mem_root and are on thd->free_list,
  // so their cleanup() (which runs the VDF's postrun) is done by the normal
  // end-of-statement item cleanup; only forget them here.
  m_collectors = nullptr;
  m_into_mode_decided = false;
}

bool Query_result_returning::append_row(THD *thd) {
  if (into_scalar() && decide_into_collect(thd)) return true;
  if (into_collect()) {
    for (uint i = 0; i < m_into_vars->elements; ++i)
      if (m_collectors[i]->accumulate(thd)) return true;
    return false;
  }
  if (into_scalar()) return assign_scalars(thd);
  if (into_table()) return write_into_table(thd);
  const bool target_is_text = !m_into->is_local();
  if (m_into_array == nullptr) {
    m_into_array = create_dom_ptr<Json_array>();
    if (m_into_array == nullptr) return true; /* purecov: inspected */
  }

  Json_object_ptr row(new (std::nothrow) Json_object());
  if (row == nullptr) return true; /* purecov: inspected */

  size_t pos = 0;
  for (Item *item : VisibleFields(*m_fields)) {
    Json_dom_ptr val;
    if (item_to_json_dom(thd, item, target_is_text, &val)) return true;
    // item_name is the RETURNING column alias (or the column name for a bare
    // column); it is the JSON key the JSON_TABLE consumer paths against. Fall
    // back to a positional key for an unnamed expression.
    std::string key;
    if (item->item_name.is_set() && item->item_name.ptr() != nullptr)
      key.assign(item->item_name.ptr(), item->item_name.length());
    else
      key = "col" + std::to_string(pos + 1);
    ++pos;
    if (row->add_alias(key, std::move(val)))
      return true; /* purecov: inspected */
  }
  return m_into_array->append_alias(std::move(row));
}

bool Query_result_returning::assign_into(THD *thd) {
  // No row matched: assign an empty array so the JSON_TABLE consumer yields
  // zero rows without a NULL guard.
  if (m_into_array == nullptr) {
    m_into_array = create_dom_ptr<Json_array>();
    if (m_into_array == nullptr) return true; /* purecov: inspected */
  }
  auto wrapper = make_unique_destroy_only<Json_wrapper>(
      thd->mem_root, std::move(m_into_array));
  if (wrapper == nullptr) return true; /* purecov: inspected */

  Item *value =
      new (thd->mem_root) Item_json(std::move(wrapper), Item_name_string());
  if (value == nullptr) return true; /* purecov: inspected */

  if (m_into->is_local()) {
    return thd->sp_runtime_ctx->set_variable(thd, false, m_into->get_offset(),
                                             &value);
  }
  Item_func_set_user_var *suv =
      new (thd->mem_root) Item_func_set_user_var(m_into->name, value);
  if (suv == nullptr || suv->fix_fields(thd, nullptr)) return true;
  suv->save_item_result(value);
  return suv->update();
}

bool Query_result_returning::send_row_if_changed(
    THD *thd, const COPY_INFO &info, const COPY_INFO::Statistics &before) {
  // TODO(villagesql): this re-derives "did the row change" by diffing COPY_INFO
  // counters around write_record(). fill_record_n_invoke_before_triggers()
  // already computes an is_row_changed flag (see sql_base.cc); thread that
  // value through so callers can drop the before-snapshot and this diff
  // entirely.
  const bool row_changed = info.stats.copied != before.copied ||
                           info.stats.deleted != before.deleted ||
                           info.stats.updated != before.updated ||
                           info.stats.touched != before.touched;
  return row_changed && send_row(thd);
}

bool finish_returning_or_ok(THD *thd, Query_result_returning *returning,
                            ulonglong row_count, ulonglong id,
                            const char *message) {
  if (returning != nullptr) return returning->send_count_eof(thd, row_count);
  my_ok(thd, row_count, id, message);
  return false;
}
