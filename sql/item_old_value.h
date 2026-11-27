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

#ifndef SQL_ITEM_OLD_VALUE_INCLUDED
#define SQL_ITEM_OLD_VALUE_INCLUDED

#include "lex_string.h"
#include "sql/field.h"
#include "sql/item.h"

class Item_old_field final : public Item_field {
  typedef Item_field super;

  // Shifts the field to read its pre-update image in record[1] for the
  // duration of one value fetch, then shifts it back. record[0] (new) and
  // record[1] (old) are parallel buffers of identical layout, so the old image
  // of any field is a constant offset away; move_field_offset() moves both the
  // data and null pointers by that offset.
  class Old_value_guard {
    Field *m_field;
    ptrdiff_t m_diff;

   public:
    explicit Old_value_guard(Field *field_arg)
        : m_field(field_arg),
          m_diff(field_arg->table->record[1] - field_arg->table->record[0]) {
      m_field->move_field_offset(m_diff);
    }
    ~Old_value_guard() { m_field->move_field_offset(-m_diff); }
  };

 public:
  Item_old_field(THD *, Name_resolution_context *context_arg,
                 const LEX_STRING &field_name_arg)
      : Item_field(context_arg, nullptr, nullptr, field_name_arg.str) {
    item_name.copy(field_name_arg.str, field_name_arg.length,
                   system_charset_info, false);
  }

  enum Type type() const override { return FIELD_ITEM; }
  bool fix_fields(THD *thd, Item **reference) override;
  double val_real() override;
  longlong val_int() override;
  longlong val_time_temporal() override;
  longlong val_date_temporal() override;
  longlong val_time_temporal_at_utc() override;
  longlong val_date_temporal_at_utc() override;
  my_decimal *val_decimal(my_decimal *) override;
  String *val_str(String *) override;
  bool val_json(Json_wrapper *result) override;
  bool send(Protocol *protocol, String *str_arg) override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override;
  bool get_time(MYSQL_TIME *ltime) override;
  bool get_timeval(my_timeval *tm, int *warnings) override;
  bool is_null() override;
};

#endif  // SQL_ITEM_OLD_VALUE_INCLUDED
