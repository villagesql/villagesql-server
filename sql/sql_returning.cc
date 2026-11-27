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
#include "sql-common/json_dom.h"
#include "sql/auth/auth_acls.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/item_json_func.h"
#include "sql/parse_tree_nodes.h"
#include "sql/protocol.h"
#include "sql/query_result.h"
#include "sql/sp_rcontext.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"

bool prepare_returning_fields(THD *thd, Query_block *query_block,
                              mem_root_deque<Item *> *returning_fields,
                              Query_result_returning **returning_result,
                              PT_select_var *into) {
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
  if (into != nullptr) (*returning_result)->set_into(into);
  return false;
}

bool Query_result_returning::send_metadata(THD *thd) {
  // In the INTO-JSON case there is no client result set: rows are captured into
  // a variable and the terminator is a plain OK. Suppress metadata entirely.
  if (into_json()) return false;
  return send_result_set_metadata(thd, *m_fields,
                                  Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);
}

bool Query_result_returning::send_count_eof(THD *thd, longlong row_count) {
  if (into_json()) {
    if (assign_into(thd)) return true;
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

bool Query_result_returning::append_row(THD *thd) {
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
