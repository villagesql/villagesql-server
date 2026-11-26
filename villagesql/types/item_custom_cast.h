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

namespace villagesql {

class TypeContext;

// CAST(expr AS TYPE[(params)]) for a custom type. It never converts a value;
// it tells the server the parameters of a value of the same custom type:
//   - expr is TYPE::from_string(...) with unknown parameters: the parameters
//     are handed to from_string, which then parses each row with them.
//   - expr is another TYPE value with unknown parameters: the value is
//     relabeled; a row whose length does not fit the target is NULL with a
//     warning.
//   - expr is a TYPE value with the same parameters: nothing to do.
// Anything else (other parameters, another type, a string) is an error.
class Item_func_custom_cast final : public Item_str_func {
  typedef Item_str_func super;

 public:
  Item_func_custom_cast(const POS &pos, Item *a, const TypeContext *target)
      : Item_str_func(pos, a), m_target(target) {}

  const char *func_name() const override { return "cast_as_custom"; }
  enum Functype functype() const override { return TYPECAST_FUNC; }
  bool resolve_type(THD *thd) override;
  String *val_str(String *str) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  const CHARSET_INFO *charset_for_protocol() override;

 private:
  const TypeContext *m_target;
  // True when the argument's parameters were unknown and this cast relabels
  // its value, so each row's length is checked against the target.
  bool m_relabel{false};
};

// Build the item for CAST(expr AS custom_type). Returns nullptr on error,
// which the type has already reported.
Item *MakeCustomCast(THD *thd, const POS &pos, Item *expr, PT_type *type);

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_ITEM_CUSTOM_CAST_H_
