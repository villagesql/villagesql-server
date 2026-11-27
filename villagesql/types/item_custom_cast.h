// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_TYPES_ITEM_CUSTOM_CAST_H_
#define VILLAGESQL_TYPES_ITEM_CUSTOM_CAST_H_

#include "sql/enum_query_type.h"
#include "sql/item_strfunc.h"
#include "sql/parse_location.h"

class PT_type;
class String;
class THD;
class udf_handler;

namespace villagesql {

class FuncDescriptor;
class TypeContext;

// CAST(expr AS TYPE[(params)]) for a custom type. It converts a value only
// through a cast function the extension declares; otherwise it tells the
// server the parameters of a value of the same custom type:
//   - expr is TYPE::from_string(...) with unknown parameters: the parameters
//     are handed to from_string, which then parses each row with them.
//   - expr is another TYPE value with unknown parameters: the value is
//     relabeled; a row whose length does not fit the target is NULL with a
//     warning.
//   - expr is a TYPE value with the same parameters: nothing to do.
//   - TYPE's extension has a cast function from expr's type to TYPE: CAST
//     calls it. Its bind_and_check_types hook decides the result's
//     parameters, which must match the target's.
// Anything else (other parameters, another type, a string) is an error.
class Item_func_custom_cast final : public Item_str_func {
  typedef Item_str_func super;

 public:
  Item_func_custom_cast(const POS &pos, Item *a, const TypeContext *target)
      : Item_str_func(pos, a), m_target(target) {}

  const char *func_name() const override { return "cast_as_custom"; }
  enum Functype functype() const override { return TYPECAST_FUNC; }
  bool fix_fields(THD *thd, Item **ref) override;
  bool resolve_type(THD *thd) override;
  String *val_str(String *str) override;
  void cleanup() override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  const CHARSET_INFO *charset_for_protocol() override;

 private:
  const TypeContext *m_target;
  // True when the argument's parameters were unknown and this cast relabels
  // its value, so each row's length is checked against the target.
  bool m_relabel{false};
  // The call of the extension's cast function, when CAST converts through
  // one. It runs with this item's own arguments and result.
  udf_handler *m_cast_call{nullptr};

  bool fix_cast_function_call(THD *thd, const FuncDescriptor *cast_function);
};

// Build the item for CAST(expr AS custom_type). Returns nullptr on error,
// which the type has already reported.
Item *MakeCustomCast(THD *thd, const POS &pos, Item *expr, PT_type *type);

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_ITEM_CUSTOM_CAST_H_
