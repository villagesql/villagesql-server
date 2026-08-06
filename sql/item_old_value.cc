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

#include "sql/item_old_value.h"

#include "my_sqlcommand.h"
#include "mysqld_error.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_lex.h"
#include "sql/table.h"
#include "sql_string.h"

bool Item_old_field::fix_fields(THD *thd, Item **reference) {
  if (thd->lex->sql_command != SQLCOM_UPDATE &&
      thd->lex->sql_command != SQLCOM_UPDATE_MULTI) {
    my_error(ER_WRONG_USAGE, MYF(0), "OLD_VALUE", "non-UPDATE");
    return true;
  }

  if (super::fix_fields(thd, reference)) return true;

  if (field == nullptr) {
    Item *real_item = (*reference)->real_item();
    if (real_item->type() == FIELD_ITEM)
      field = down_cast<Item_field *>(real_item)->field;
  }

  return false;
}

double Item_old_field::val_real() {
  Old_value_guard guard(field);
  return super::val_real();
}

longlong Item_old_field::val_int() {
  Old_value_guard guard(field);
  return super::val_int();
}

longlong Item_old_field::val_time_temporal() {
  Old_value_guard guard(field);
  return super::val_time_temporal();
}

longlong Item_old_field::val_date_temporal() {
  Old_value_guard guard(field);
  return super::val_date_temporal();
}

longlong Item_old_field::val_time_temporal_at_utc() {
  Old_value_guard guard(field);
  return super::val_time_temporal_at_utc();
}

longlong Item_old_field::val_date_temporal_at_utc() {
  Old_value_guard guard(field);
  return super::val_date_temporal_at_utc();
}

my_decimal *Item_old_field::val_decimal(my_decimal *decimal_value) {
  Old_value_guard guard(field);
  return super::val_decimal(decimal_value);
}

String *Item_old_field::val_str(String *str) {
  Old_value_guard guard(field);
  return super::val_str(str);
}

bool Item_old_field::val_json(Json_wrapper *result) {
  Old_value_guard guard(field);
  return super::val_json(result);
}

bool Item_old_field::send(Protocol *protocol, String *str_arg) {
  Old_value_guard guard(field);
  return super::send(protocol, str_arg);
}

bool Item_old_field::get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) {
  Old_value_guard guard(field);
  return super::get_date(ltime, fuzzydate);
}

bool Item_old_field::get_time(MYSQL_TIME *ltime) {
  Old_value_guard guard(field);
  return super::get_time(ltime);
}

bool Item_old_field::get_timeval(my_timeval *tm, int *warnings) {
  Old_value_guard guard(field);
  return super::get_timeval(tm, warnings);
}

bool Item_old_field::is_null() {
  Old_value_guard guard(field);
  return super::is_null();
}
