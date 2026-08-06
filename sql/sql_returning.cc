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

#include "mem_root_deque.h"
#include "sql/auth/auth_acls.h"
#include "sql/item.h"
#include "sql/protocol.h"
#include "sql/query_result.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"

bool prepare_returning_fields(THD *thd, Query_block *query_block,
                              mem_root_deque<Item *> *returning_fields,
                              Query_result_returning **returning_result) {
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
  return false;
}

bool Query_result_returning::send_metadata(THD *thd) {
  return send_result_set_metadata(thd, *m_fields,
                                  Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);
}

bool Query_result_returning::send_count_eof(THD *thd, longlong row_count) {
  thd->set_row_count_func(row_count);
  return send_eof(thd);
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
