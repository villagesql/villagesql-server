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

#ifndef SQL_RETURNING_INCLUDED
#define SQL_RETURNING_INCLUDED

#include "mem_root_deque.h"
#include "my_inttypes.h"
#include "sql/query_result.h"
#include "sql/sql_data_change.h"

class Item;
class Query_block;
class THD;

// A Query_result_send that owns RETURNING's send sequencing. The DML paths only
// ever talk to this object through the three Query_result virtuals plus the
// helpers below; the row-count and empty-result handling live here rather than
// being open-coded into each delete/update/insert loop.
class Query_result_returning final : public Query_result_send {
 public:
  void set_fields(mem_root_deque<Item *> *fields) { m_fields = fields; }
  const mem_root_deque<Item *> &fields() const { return *m_fields; }

  bool send_metadata(THD *thd);

  // Emit the row currently in the table's record buffer.
  bool send_row(THD *thd) { return send_data(thd, *m_fields); }

  // Emit the row only if write_record() actually changed the table, as
  // determined by comparing info's counters against the snapshot taken before
  // the write. Lets INSERT/REPLACE keep a single RETURNING call site next to
  // write_record() instead of an inline stats diff.
  bool send_row_if_changed(THD *thd, const COPY_INFO &info,
                           const COPY_INFO::Statistics &before);

  // Metadata + immediate EOF, for the "nothing matched" short-circuits.
  bool send_empty(THD *thd) { return send_metadata(thd) || send_eof(thd); }

  // Terminates a RETURNING result set, reporting row_count as the row count.
  bool send_count_eof(THD *thd, longlong row_count);

 private:
  mem_root_deque<Item *> *m_fields{nullptr};
};

// Resolve the RETURNING select-list of query_block and allocate a
// Query_result_returning bound to those fields.
bool prepare_returning_fields(THD *thd, Query_block *query_block,
                              mem_root_deque<Item *> *returning_fields,
                              Query_result_returning **returning_result);

// Terminate a data-change statement: when returning is non-null, close the
// RETURNING result set reporting row_count; otherwise send a plain OK packet
// carrying row_count/id/message. A statement sends exactly one terminator, and
// only the OK packet has slots for id and message, so those are ignored in the
// RETURNING case. Keeps DELETE/UPDATE/INSERT terminal sites to a single call.
bool finish_returning_or_ok(THD *thd, Query_result_returning *returning,
                            ulonglong row_count, ulonglong id = 0,
                            const char *message = nullptr);

#endif  // SQL_RETURNING_INCLUDED
