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
#include <string>

#include "mysql/strings/m_ctype.h"
#include "mysqld_error.h"
#include "sql/item_func.h"
#include "sql/parse_tree_column_attrs.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_show.h"
#include "sql/sql_udf.h"
#include "sql_string.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/sql/func_lookup.h"
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

// Returns the extension's cast function that converts arg to target, or
// nullptr. CAST's own cases come first: a value of target's type that has, or
// can take, target's parameters needs none. A cast function takes only a type
// of its own extension, or an INT or REAL; a string goes through
// T::from_string.
const FuncDescriptor *FindCastFunction(THD *thd, Item *arg,
                                       const TypeContext &target) {
  const TypeContext *source = arg->get_type_context();
  if (source != nullptr) {
    if (source->is_assignable_with(target) ||
        source->extension_name() != target.extension_name()) {
      return nullptr;
    }
    return find_cast_function(target.extension_name(), target.type_name(),
                              source->type_name(), VEF_TYPE_CUSTOM,
                              *thd->mem_root);
  }
  int source_id;
  switch (arg->result_type()) {
    case INT_RESULT:
      source_id = VEF_TYPE_INT;
      break;
    case REAL_RESULT:
    case DECIMAL_RESULT:
      source_id = VEF_TYPE_REAL;
      break;
    default:
      return nullptr;
  }
  return find_cast_function(target.extension_name(), target.type_name(), {},
                            source_id, *thd->mem_root);
}

// The type of arg, for error messages.
std::string SourceName(const Item *arg) {
  if (const TypeContext *source = arg->get_type_context()) {
    return source->qualified_name();
  }
  switch (arg->result_type()) {
    case INT_RESULT:
      return "INT";
    case REAL_RESULT:
      return "REAL";
    case DECIMAL_RESULT:
      return "DECIMAL";
    default:
      return "STRING";
  }
}

}  // namespace

bool Item_func_custom_cast::fix_fields(THD *thd, Item **ref) {
  assert(!fixed);
  // The argument's type decides whether a cast function converts it.
  if (!args[0]->fixed && args[0]->fix_fields(thd, &args[0])) return true;
  const FuncDescriptor *cast_function =
      FindCastFunction(thd, args[0], *m_target);
  if (cast_function == nullptr) return super::fix_fields(thd, ref);
  return fix_cast_function_call(thd, cast_function);
}

bool Item_func_custom_cast::fix_cast_function_call(
    THD *thd, const FuncDescriptor *cast_function) {
  udf_func *udf = make_udf_func_from_vdf(cast_function, *thd->mem_root);
  if (udf == nullptr) return true;
  m_cast_call = new (thd->mem_root) udf_handler(udf);
  if (m_cast_call == nullptr) return true;
  // As in Item_udf_func::fix_fields(): the handler calls resolve_type(), then
  // runs the hook, which sets this item's type context to the result's.
  if (m_cast_call->fix_fields(thd, this, arg_count, args) || thd->is_error()) {
    return true;
  }
  used_tables_cache = m_cast_call->used_tables_cache;

  const TypeContext *result = get_type_context();
  if (result == nullptr || result->is_unknown()) {
    villagesql_error(
        "Cannot CAST %s AS %s: cast function '%s' did not decide the "
        "parameters of its result",
        MYF(0), SourceName(args[0]).c_str(), m_target->qualified_name().c_str(),
        cast_function->function_name().c_str());
    return true;
  }
  if (!result->is_compatible_with(*m_target)) {
    villagesql_error("Cannot CAST %s AS %s: cast function '%s' returns %s",
                     MYF(0), SourceName(args[0]).c_str(),
                     m_target->qualified_name().c_str(),
                     cast_function->function_name().c_str(),
                     result->qualified_name().c_str());
    return true;
  }
  set_data_type_string(static_cast<uint32>(m_target->field_buffer_length()),
                       &my_charset_bin);
  set_type_context(m_target);
  fixed = true;
  return false;
}

bool Item_func_custom_cast::resolve_type(THD *) {
  if (m_cast_call != nullptr) {
    // Called by m_cast_call's fix_fields before its hook runs; the hook's
    // result sets the type in fix_cast_function_call().
    set_data_type_string(args[0]->max_length, &my_charset_bin);
    return false;
  }
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
  if (m_cast_call != nullptr) {
    String *res = m_cast_call->val_str(str, &str_value);
    null_value = res == nullptr;
    return res;
  }
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

void Item_func_custom_cast::cleanup() {
  if (m_cast_call != nullptr) m_cast_call->cleanup();
  super::cleanup();
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
