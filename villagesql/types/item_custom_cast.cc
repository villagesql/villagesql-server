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

#include "villagesql/types/item_custom_cast.h"

#include <cstdint>

#include "mysql/strings/m_ctype.h"
#include "mysqld_error.h"
#include "sql/item_func.h"
#include "sql/parse_tree_column_attrs.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_show.h"
#include "sql_string.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/vdf/vdf_handler.h"

namespace villagesql {

namespace {

// Returns the VDF call if item is target's own from_string, nullptr otherwise.
Item_udf_func *AsTypeFromString(Item *item, const TypeContext &target) {
  if (item->type() != Item::FUNC_ITEM) return nullptr;
  auto *func = down_cast<Item_func *>(item);
  if (func->functype() != Item_func::UDF_FUNC) return nullptr;
  auto *udf = down_cast<Item_udf_func *>(func);
  const TypeDescriptor *descriptor = target.descriptor();
  if (!udf->is_vdf() || !descriptor->has_encode_fn() ||
      descriptor->encode_fn().vdf() == nullptr ||
      udf->vdf_func_desc() != descriptor->encode_fn().vdf()) {
    return nullptr;
  }
  return udf;
}

// True if a value of `length` bytes can be a value of type tc.
bool LengthFits(const TypeContext &tc, size_t length) {
  if (tc.is_variable_length()) {
    return length <= static_cast<size_t>(tc.field_buffer_length());
  }
  return length == static_cast<size_t>(tc.persisted_length());
}

}  // namespace

bool Item_func_custom_cast::resolve_type(THD *) {
  Item *arg = args[0];
  const TypeContext *source = arg->get_type_context();
  if (source == nullptr) {
    villagesql_error("Cannot CAST a value that is not of a custom type AS %s",
                     MYF(0), m_target->qualified_name().c_str());
    return true;
  }

  if (!source->is_compatible_with(*m_target)) {
    // Only a source of the same type whose parameters are unknown can take
    // the target's.
    if (!source->is_assignable_with(*m_target)) {
      villagesql_error("Cannot CAST %s AS %s", MYF(0),
                       source->qualified_name().c_str(),
                       m_target->qualified_name().c_str());
      return true;
    }
    Item_udf_func *from_string = AsTypeFromString(arg, *m_target);
    if (from_string != nullptr) {
      // from_string parses each row with the target's parameters.
      if (from_string->vdf()->adopt_return_type_context(from_string,
                                                        m_target)) {
        return true;
      }
    } else {
      m_relabel = true;
    }
  }

  set_data_type_string(static_cast<uint32>(m_target->field_buffer_length()),
                       &my_charset_bin);
  set_type_context(m_target);
  // A relabeled row that does not fit the target is NULL.
  set_nullable(m_relabel || arg->is_nullable());
  return false;
}

String *Item_func_custom_cast::val_str(String *str) {
  assert(fixed);
  String *res = args[0]->val_str(str);
  if (res == nullptr) {
    null_value = true;
    return nullptr;
  }
  if (m_relabel && !LengthFits(*m_target, res->length())) {
    THD *thd = current_thd;
    push_warning_printf(
        thd, Sql_condition::SL_WARNING, ER_VILLAGESQL_GENERIC_ERROR,
        "VillageSQL: Cannot CAST a %zu-byte value AS %s at row %ld",
        res->length(), m_target->qualified_name().c_str(),
        thd->get_stmt_da()->current_row_for_condition());
    null_value = true;
    return nullptr;
  }
  null_value = false;
  return res;
}

void Item_func_custom_cast::print(const THD *thd, String *str,
                                  enum_query_type query_type) const {
  str->append(STRING_WITH_LEN("cast("));
  args[0]->print(thd, str, query_type);
  str->append(STRING_WITH_LEN(" as "));
  const std::string &extension = m_target->extension_name();
  const std::string &type = m_target->type_name();
  append_identifier(thd, str, extension.c_str(), extension.length());
  str->append('.');
  append_identifier(thd, str, type.c_str(), type.length());
  const std::string &params = m_target->parameters().str();
  if (!params.empty()) {
    str->append(STRING_WITH_LEN("('"));
    str->append(params.c_str(), params.length());
    str->append(STRING_WITH_LEN("')"));
  }
  str->append(')');
}

const CHARSET_INFO *Item_func_custom_cast::charset_for_protocol() {
  // Sent as text, like a column of a custom type.
  return &my_charset_utf8mb4_bin;
}

Item *MakeCustomCast(THD *thd, const POS &pos, Item *expr, PT_type *type) {
  // An unknown type name has reported a syntax error and has no type.
  if (type == nullptr || thd->is_error()) return nullptr;
  return new (thd->mem_root)
      Item_func_custom_cast(pos, expr, type->get_type_context());
}

}  // namespace villagesql
