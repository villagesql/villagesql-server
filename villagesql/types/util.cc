/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#include "villagesql/types/util.h"

#include <cinttypes>
#include <optional>

#include "lex_string.h"
#include "my_alloc.h"
#include "my_base.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/create_field.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/types/column.h"
#include "sql/dd/types/table.h"
#include "sql/derror.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/item_sum.h"
#include "sql/key.h"
#include "sql/parse_tree_column_attrs.h"
#include "sql/sql_class.h"
#include "sql/sql_list.h"
#include "sql/sql_udf.h"
#include "sql/table.h"
#include "sql/visible_fields.h"
#include "template_utils.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/systable/custom_columns.h"
#include "villagesql/schema/util.h"
#include "villagesql/schema/victionary_client.h"

namespace villagesql {

static const char *ER_INCOMPARABLE_TYPES =
    "Cannot compare values of custom and non-custom types in %s";
static const char *ER_INCOMPATIBLE_TYPES =
    "Cannot compare values of incompatible types '%s' and '%s'";

bool MaybeInjectCustomType(THD *thd, TABLE_SHARE &share, Field *field) {
  if (should_assert_if_null(thd)) {
    LogVSQL(ERROR_LEVEL, "thd is null in MaybeInjectCustomType");
    return true;
  }
  if (should_assert_if_null(field)) {
    LogVSQL(ERROR_LEVEL, "field is null in MaybeInjectCustomType");
    return true;
  }

  // Extract identifiers directly
  std::string db_name = std::string(share.db.str, share.db.length);

  // Skip special databases
  if (::villagesql::is_system_schema(db_name.c_str())) {
    return false;
  }

  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    // Too early to perform a lookup. We must be starting up.
    return false;
  }

  std::string table_name(share.table_name.str, share.table_name.length);
  std::string column_name(field->field_name);

  // Create ColumnEntry to get normalized key
  ColumnEntry lookup_entry(ColumnKey(db_name, table_name, column_name));
  auto key = lookup_entry.key();

  auto guard = vclient.get_write_lock();
  const auto &columns = vclient.columns();
  const ColumnEntry *column_entry = columns.get(thd, key.str());
  if (!column_entry) return false;

  // This is a custom type - cross-reference with TypeDescriptor.
  TypeDescriptorKey type_descriptor_key(column_entry->type_name,
                                        column_entry->extension_name,
                                        column_entry->extension_version);
  // Note: the TypeDescriptor must already be committed.
  const TypeDescriptor *type_descriptor =
      vclient.type_descriptors().get_committed(type_descriptor_key);
  if (should_assert_if_null(type_descriptor)) {
    LogVSQL(ERROR_LEVEL,
            "Failed to find type %s in extension %s, version %s when looking "
            "up column %s in table %s.%s",
            column_entry->type_name.c_str(),
            column_entry->extension_name.c_str(),
            column_entry->extension_version.c_str(), column_name.c_str(),
            db_name.c_str(), table_name.c_str());
    return true;
  }

  TypeParameters parameters =
      TypeParameters::from_json(column_entry->type_parameters);
  TypeContextKey type_context_key(type_descriptor_key, parameters);

  const TypeContext *tc = vclient.type_contexts().acquire_or_create(
      type_context_key, share.mem_root, type_descriptor);
  if (should_assert_if_null(tc)) {
    // The size required (3rd param) could be wrong, but we have no way of
    // knowing because acquire_or_create performed other allocations and thus we
    // can't be sure of what the shortfall is (e.g. TypeContext or shared_ptr).
    // However, these allocations are small.
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), sizeof(TypeContext));
    return true;
  }

  field->set_type_context(tc);

  // Verify that the Field's storage length matches the TypeContext's
  // persisted_length. A mismatch here would mean InnoDB allocated a different
  // amount of storage than the type expects, which leads to silent data
  // corruption.
  if (should_assert_if_false(static_cast<int64_t>(field->field_length) ==
                             tc->persisted_length())) {
    LogVSQL(ERROR_LEVEL,
            "field_length (%u) != persisted_length (%" PRId64
            ") for column %s in table %s.%s (type %s)",
            field->field_length, tc->persisted_length(), column_name.c_str(),
            db_name.c_str(), table_name.c_str(), tc->qualified_name().c_str());
    return true;
  }

  return false;
}

bool ResolveTypeToContext(const LEX_STRING &extension_name,
                          const LEX_STRING &type_name,
                          const TypeParameters &parameters, MEM_ROOT &mem_root,
                          const TypeContext *&result) {
  result = nullptr;

  auto &vclient = VictionaryClient::instance();
  if (should_assert_if_false(vclient.is_initialized())) {
    LogVSQL(ERROR_LEVEL,
            "Failed to resolve type %.*s; VictionaryClient not initialized",
            static_cast<int>(type_name.length), type_name.str);
    return true;
  }

  TypeDescriptorKeyPrefix prefix(
      std::string(type_name.str, type_name.length),
      std::string(extension_name.str, extension_name.length));

  auto guard = vclient.get_write_lock();
  std::vector<const TypeDescriptor *> results =
      vclient.type_descriptors().get_prefix_committed(prefix);

  if (should_assert_if_true(results.size() > 1)) {
    LogVSQL(ERROR_LEVEL, "Found more than one entry for type %.*s",
            static_cast<int>(type_name.length), type_name.str);
    return true;
  }

  // The type didn't resolve, which isn't a failure here. It probably is to the
  // caller, but they will see result is nullptr.
  if (results.empty()) return false;

  const TypeDescriptor *type_descriptor = results[0];
  TypeDescriptorKey type_descriptor_key(type_descriptor->type_name(),
                                        type_descriptor->extension_name(),
                                        type_descriptor->extension_version());
  TypeContextKey type_context_key(type_descriptor_key, parameters);

  result = vclient.type_contexts().acquire_or_create(type_context_key, mem_root,
                                                     type_descriptor);
  if (should_assert_if_null(result)) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), sizeof(TypeContext));
    return true;
  }

  return false;
}

bool HasCustomTypeColumns(const List<Create_field> &create_list) {
  for (const Create_field &field : create_list) {
    if (field.custom_type_context != nullptr) {
      return true;
    }
  }
  return false;
}

bool HandleCustomColumnsForTableRename(THD &thd, const char *old_db,
                                       const char *old_table,
                                       const char *new_db,
                                       const char *new_table) {
  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    return false;  // VictionaryClient not initialized, skip
  }

  vclient.assert_write_lock_held();

  // Get all custom columns for the old table
  auto custom_columns = vclient.GetCustomColumnsForTable(old_db, old_table);

  if (custom_columns.empty()) {
    return false;  // No custom columns to rename
  }

  // Mark each column for update with new db/table names
  for (const ColumnEntry *old_col : custom_columns) {
    ColumnEntry new_entry(ColumnKey(new_db, new_table, old_col->column_name()),
                          old_col->extension_name, old_col->extension_version,
                          old_col->type_name, old_col->type_parameters);

    if (should_assert_if_true(vclient.columns().MarkForUpdate(
            thd, std::move(new_entry), old_col->key()))) {
      LogVSQL(ERROR_LEVEL, "Failed to mark custom column for rename: %s.%s.%s",
              old_db, old_table, old_col->column_name().c_str());
      return true;
    }
  }

  return false;
}

String *EncodeString(const TypeContext &tc, const String &from,
                     MEM_ROOT &mem_root, bool &is_valid) {
  assert(tc.descriptor()->encode());
  is_valid = true;

  const auto length = tc.persisted_length();
  assert(length > 0);
  char *buffer = new (&mem_root) char[length];
  if (should_assert_if_null(buffer)) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), length);
    return nullptr;
  }

  size_t actual_length = 0;
  if (tc.descriptor()->encode()(pointer_cast<uchar *>(buffer), length,
                                from.ptr(), from.length(), &actual_length)) {
    is_valid = false;
    return nullptr;
  }
  auto *ret = new (&mem_root) String(buffer, actual_length, &my_charset_bin);
  if (should_assert_if_null(ret)) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), sizeof(String));
    return nullptr;
  }
  return ret;
}

String *EncodeStringForField(const TypeContext &tc, const String &from,
                             MEM_ROOT &mem_root, const char *field_name,
                             bool &is_valid) {
  String *encoded = EncodeString(tc, from, mem_root, is_valid);
  if (encoded == nullptr) {
    if (is_valid) {
      // OOM - my_error already called by EncodeString
      return nullptr;
    }
    // Encoding failed - invalid value for custom type
    // Always push a warning (consistent with MySQL built-in types)
    // Caller decides whether to also promote to error
    THD *thd = current_thd;
    const ErrConvString errmsg(from.ptr(), from.length(), from.charset());
    const Diagnostics_area *da = thd->get_stmt_da();
    push_warning_printf(
        thd, Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
        ER_THD(thd, ER_TRUNCATED_WRONG_VALUE_FOR_FIELD), tc.type_name().c_str(),
        errmsg.ptr(), field_name, da->current_row_for_condition());
    return nullptr;
  }
  return encoded;
}

bool DecodeString(const TypeContext &tc, const uchar *encoded_data,
                  size_t encoded_length, MEM_ROOT &mem_root,
                  String *output_buffer, bool &is_valid) {
  assert(tc.descriptor()->decode());
  assert(output_buffer);
  is_valid = true;

  const auto max_length = tc.max_decode_buffer_length();
  char *buffer = new (&mem_root) char[max_length];
  if (should_assert_if_null(buffer)) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), max_length);
    return true;
  }

  size_t decoded_length = 0;
  if (tc.descriptor()->decode()(encoded_data, encoded_length, buffer,
                                max_length, &decoded_length)) {
    is_valid = false;
    return true;
  }

  output_buffer->set(buffer, decoded_length, &my_charset_utf8mb4_bin);
  return false;
}

void AppendFullyQualifiedName(const TypeContext &tc, String *out) {
  out->append(tc.extension_name().c_str(), tc.extension_name().length());
  out->append('.');
  out->append(tc.type_name().c_str(), tc.type_name().length());
}

bool InjectAndEncodeCustomType(Item *item, const TypeContext &tc) {
  if (item->has_type_context()) {
    // We already got one, you see?
    // Make sure they are compatible!
    return !AreTypesCompatible(*item->get_type_context(), tc);
  }

  // Set the type context
  item->set_type_context(const_cast<TypeContext *>(&tc));

  // The rest of the function is for strings only, since we may need to
  // re-encode it to the custom type's representation.
  if (item->type() != Item::STRING_ITEM) return false;

  // Encode the string value
  Item_string *str_item = down_cast<Item_string *>(item);
  String tmp_buf;
  String *str_val = str_item->val_str(&tmp_buf);
  if (str_val == nullptr) {
    return false;  // NULL value, nothing to encode
  }

  bool is_valid = true;
  String *encoded =
      EncodeString(tc, *str_val, *current_thd->mem_root, is_valid);
  if (encoded == nullptr) {
    if (!is_valid) {
      const ErrConvString errmsg(str_val->ptr(), str_val->length(),
                                 str_item->collation.collation);
      my_error(ER_WRONG_VALUE, MYF(0), tc.type_name().c_str(), errmsg.ptr());
    }
    return true;
  }

  // Replace the string value with the encoded value
  str_item->set_str_value(encoded);
  return false;
}

int CustomMemCompare(const Item *item, const uchar *data1, size_t len1,
                     const uchar *data2, size_t len2, size_t min_len,
                     bool reverse) {
  int res;

  // Use custom comparison for custom types
  if (item != nullptr && item->has_type_context()) {
    auto *tc = item->get_type_context();
    auto cmp_func = GetCompareFunc(*tc);
    if (cmp_func != nullptr) {
      res = cmp_func(data1, len1, data2, len2);
      // Handle reverse sort direction in this path only since the comparison
      // function assumes ASC but memcmp relies on bits being flipped for DESC.
      if (reverse) res = -res;
    } else {
      res = memcmp(data1, data2, min_len);
    }
  } else {
    res = memcmp(data1, data2, min_len);
  }

  return res;
}

bool AreTypesCompatible(const TypeContext &tc1, const TypeContext &tc2) {
  // Types are compatible if they have the same key (type name, extension,
  // version, and parameters). This ensures e.g. TVECTOR(3) != TVECTOR(4).
  return tc1.key() == tc2.key();
}

bool MaybeValidateUnionTypeCompatibility(Item *accumulator, Item *item) {
  const TypeContext *accumulator_tc = accumulator->get_type_context();
  const TypeContext *item_tc = item->get_type_context();

  // If neither has a custom type, no validation needed
  if (accumulator_tc == nullptr && item_tc == nullptr) {
    return false;
  }

  if (accumulator_tc == nullptr && item_tc != nullptr) {
    // First join: accumulator has no custom type yet, but incoming item does
    // Copy type context to accumulator for next join
    accumulator->set_type_context(item_tc);
  } else if (item_tc == nullptr && accumulator_tc != nullptr) {
    // Accumulated type has custom, but incoming doesn't
    // Allow NULL items (they are compatible with any type including custom)
    if (item->type() == Item::NULL_ITEM) {
      return false;
    }
    // Other non-custom types are incompatible
    villagesql_error("Cannot use UNION with mixed custom and non-custom types",
                     MYF(0));
    return true;
  } else if (accumulator_tc != nullptr && item_tc != nullptr &&
             !AreTypesCompatible(*accumulator_tc, *item_tc)) {
    // Both have custom types but they're incompatible
    villagesql_error(
        "Cannot use UNION with different custom types '%s' and '%s'", MYF(0),
        accumulator_tc->qualified_name().c_str(),
        item_tc->qualified_name().c_str());
    return true;
  }
  // Otherwise: both have same custom type - OK

  return false;  // success
}

bool MaybeValidateAndCastCustomTypeComparison(Item &left, Item &right,
                                              const char *operation_name) {
  const TypeContext *lhs_tc = left.get_type_context();
  const TypeContext *rhs_tc = right.get_type_context();

  // If neither has a custom type, no validation needed
  if (lhs_tc == nullptr && rhs_tc == nullptr) {
    return false;
  }

  // Case 1: Both sides have custom types
  if (lhs_tc != nullptr && rhs_tc != nullptr) {
    if (!AreTypesCompatible(*lhs_tc, *rhs_tc)) {
      villagesql_error("Cannot compare types %s and %s in %s", MYF(0),
                       lhs_tc->qualified_name().c_str(),
                       rhs_tc->qualified_name().c_str(), operation_name);
      return true;
    }
    return false;
  }

  // Case 2: One side is custom, the other is not
  const TypeContext *tc = (lhs_tc != nullptr) ? lhs_tc : rhs_tc;
  Item *non_custom_arg = (lhs_tc != nullptr) ? &right : &left;

  // Try to cast string/null literals to custom type
  if (TryImplicitCastToCustom(non_custom_arg, *tc)) {
    return true;  // Error during cast
  }

  // If still not custom after trying to cast, it's incompatible
  if (!non_custom_arg->has_type_context()) {
    villagesql_error(
        "Unable to implicitly cast a non-custom type during compare with a "
        "custom type in %s",
        MYF(0), operation_name);
    return true;
  }

  return false;
}

const TypeContext *GetCompatibleCustomType(const Item &item1,
                                           const Item &item2) {
  bool has_custom1 = item1.has_type_context();
  bool has_custom2 = item2.has_type_context();

  // If neither has custom types, no custom comparison needed
  if (!has_custom1 && !has_custom2) {
    return nullptr;
  }

  // If only one has custom type, return that one's context for comparison
  if (has_custom1 && !has_custom2) {
    return item1.get_type_context();
  }
  if (!has_custom1 && has_custom2) {
    return item2.get_type_context();
  }

  // Both have custom types - check compatibility
  auto *tc1 = item1.get_type_context();
  auto *tc2 = item2.get_type_context();

  if (AreTypesCompatible(*tc1, *tc2)) {
    return tc1;  // Compatible - return either one
  }

  return nullptr;  // Incompatible custom types
}

bool CanStoreInCustomField(const Item *item, const Field *field) {
  assert(field->has_type_context());

  // If item also has custom type context, check compatibility
  if (item->has_type_context()) {
    return AreTypesCompatible(*item->get_type_context(),
                              *field->get_type_context());
  }

  // For non-custom items storing into custom fields:
  // Allow string literals and simple values - let the custom type's encoding
  // function validate the actual format
  switch (item->type()) {
    case Item::STRING_ITEM:
    case Item::NULL_ITEM:
    case Item::DEFAULT_VALUE_ITEM:
    case Item::PARAM_ITEM:
      return true;
    case Item::FUNC_ITEM: {
      // Block functions, mostly, but let some through.
      auto *func = down_cast<const Item_func *>(item);
      switch (func->functype()) {
        case Item_func::GUSERVAR_FUNC:
          // Allow user variables (@var) to be converted from string to custom
          // Note: they are treated as FUNC_ITEMs
          // Sample: INSERT INTO t1 (complex_val) VALUES (@var)
          return true;
        default:
          return false;
      }
      return false;
    }
    case Item::FIELD_ITEM: {
      // Allow string fields without custom type context to be implicitly cast.
      // This enables CTEs and subqueries with string literals to work:
      // INSERT INTO t1 WITH cte AS (SELECT '(1,2)' AS val) SELECT * FROM cte
      if (!item->has_type_context() && item->result_type() == STRING_RESULT) {
        return true;
      }
      return false;
    }
    default:
      // Block expressions, and other complex item types
      // These should be caught and result in ER_WRONG_VALUE_FOR_TYPE
      return false;
  }

  return false;
}

bool ValidateAndReportCustomFieldStore(const Item *item, const Field *field) {
  assert(field->has_type_context());

  // Check if the item can be stored in the custom field
  if (CanStoreInCustomField(item, field)) {
    return false;  // Success
  }

  // Validation failed - generate appropriate error message
  // Use val_external_str() to get a human-readable value: for custom type items
  // this decodes the binary representation; for others it behaves like val_str.
  String str_value;
  String *item_str = const_cast<Item *>(item)->val_external_str(&str_value);
  const char *value_str = item_str ? item_str->c_ptr_safe() : "<null>";
  const THD *thd = current_thd;
  const Diagnostics_area *da = thd->get_stmt_da();

  // Check if this is a string-returning function (not a VDF)
  bool is_string_function = false;
  if (item->type() == Item::FUNC_ITEM || item->type() == Item::COND_ITEM ||
      item->type() == Item::SUM_FUNC_ITEM) {
    if (item->result_type() == STRING_RESULT) {
      // Check if it's a VDF/UDF (which are allowed)
      bool is_vdf = false;
      if (item->type() == Item::FUNC_ITEM) {
        const Item_func *func = down_cast<const Item_func *>(item);
        is_vdf = (func->functype() == Item_func::UDF_FUNC);
      }
      if (!is_vdf) {
        is_string_function = true;
      }
    }
  }

  if (is_string_function) {
    // Better error for string expressions like CONCAT
    villagesql_error(
        "Incorrect %s value: cannot implicitly cast string expression. Use "
        "explicit conversion for column '%s' at row %ld",
        MYF(0), field->get_type_context()->type_name().c_str(),
        field->field_name, da->current_row_for_condition());
  } else if (item->has_type_context()) {
    villagesql_error(
        "Cannot implicitly cast from %s to %s for column '%s' at row %ld",
        MYF(0), item->get_type_context()->qualified_name().c_str(),
        field->get_type_context()->qualified_name().c_str(), field->field_name,
        da->current_row_for_condition());
  } else {
    // Generic error for other cases (invalid format, etc.)
    villagesql_error("Incorrect %s value: '%s' for column '%s' at row %ld",
                     MYF(0), field->get_type_context()->type_name().c_str(),
                     value_str, field->field_name,
                     da->current_row_for_condition());
  }

  return true;  // Error
}

bool TryCopyCustomTypeField(const Field *from, Field *to) {
  assert(from->has_type_context());

  // If target doesn't have a custom type, or custom types do not match,
  // this is an incompatible conversion.
  if (!to->has_type_context() ||
      !AreTypesCompatible(*from->get_type_context(), *to->get_type_context())) {
    StringBuffer<MAX_FIELD_WIDTH> result(from->charset());
    result.length(0U);
    from->val_external_str(&result);

    THD *thd = current_thd;
    villagesql_error(
        "Type mismatch. Use explicit conversion of %s value '%s' for column "
        "'%s' at row %ld",
        MYF(0), from->get_type_context()->type_name().c_str(),
        result.c_ptr_safe(), to->field_name,
        thd->get_stmt_da()->current_row_for_condition());
    return false;
  }

  // Both fields have the same custom type. Copy binary data directly.
  // Handle potential length_bytes differences (VARCHAR(255) vs VARCHAR(65535)).
  // Note: from->data_length() decodes the length and from->data_ptr() returns
  // a pointer to the data (skipping the length prefix). But to->field_ptr()
  // returns the start of the field including the length prefix area, so we must
  // manually encode the length prefix.
  const size_t data_len = from->data_length();
  const uint32 to_length_bytes =
      down_cast<const Field_varstring *>(to)->get_length_bytes();
  const uchar *from_data = from->data_ptr();
  uchar *to_ptr = to->field_ptr();

  // Write length prefix (1 or 2 bytes) to destination
  if (to_length_bytes == 1) {
    to_ptr[0] = static_cast<uchar>(data_len);
  } else {
    int2store(to_ptr, static_cast<uint16>(data_len));
  }

  // Ensure data fits in destination field
  assert(data_len <= to->field_length);
  // Copy the binary data
  memcpy(to_ptr + to_length_bytes, from_data, data_len);
  return false;
}

void CopyCustomToStringField(const Field *from, Field *to) {
  assert(from->has_type_context());
  // Custom → non-custom string: decode to string representation.
  // NULL is handled outside this function
  // TODO(villagesql-performance): evaluate something more performant
  StringBuffer<MAX_FIELD_WIDTH> res(from->charset());
  res.length(0U);
  from->val_external_str(&res);
  to->store(res.ptr(), res.length(), res.charset());
}

type_conversion_status TryEncodeStringFieldToCustom(Field *from_field,
                                                    Field *to_field) {
  assert(!from_field->has_type_context());
  assert(to_field->has_type_context());

  String str_buffer;
  String *str = from_field->val_str(&str_buffer);
  if (str == nullptr) {
    return set_field_to_null_with_conversions(to_field, false);
  }

  bool is_oom = false;
  String *encoded = EncodeStringForField(*to_field->get_type_context(), *str,
                                         *current_thd->mem_root,
                                         to_field->field_name, is_oom);
  if (encoded == nullptr) {
    return is_oom ? TYPE_ERR_OOM : TYPE_ERR_BAD_VALUE;
  }

  to_field->set_notnull();
  return to_field->store(encoded->ptr(), encoded->length(), &my_charset_bin);
}

type_conversion_status EncodeAndStoreStringToCustomField(
    const TypeContext &tc, const String &str_value, Field *field) {
  bool is_valid = true;
  String *encoded = EncodeStringForField(tc, str_value, *current_thd->mem_root,
                                         field->field_name, is_valid);
  if (encoded == nullptr) {
    return is_valid ? TYPE_ERR_OOM : TYPE_WARN_OUT_OF_RANGE;
  }
  return field->store(encoded->ptr(), encoded->length(), &my_charset_bin);
}

String *EncodeStringForCustomParam(const TypeContext &tc,
                                   const String &str_value,
                                   const char *item_name, bool &null_value) {
  bool is_valid = true;
  String *encoded =
      EncodeString(tc, str_value, *current_thd->mem_root, is_valid);
  if (encoded == nullptr) {
    if (!is_valid) {
      // Invalid value for custom type
      const ErrConvString errmsg(str_value.ptr(), str_value.length(),
                                 str_value.charset());
      villagesql_error("Incorrect %s value: '%s' for parameter '%s'", MYF(0),
                       tc.qualified_name().c_str(), errmsg.ptr(), item_name);
    }
    // else OOM - my_error already called by EncodeString
    null_value = true;
    return nullptr;
  }
  return encoded;
}

bool CanImplicitlyCastToCustom(const Item *item) {
  if (item->has_type_context()) return false;

  // Allow string and null literals
  if (item->type() == Item::STRING_ITEM || item->type() == Item::NULL_ITEM) {
    return true;
  }

  // Allow prepared statement parameters (needed for PS support)
  if (item->type() == Item::PARAM_ITEM) {
    return true;
  }

  // Block everything else
  return false;
}

bool TryImplicitCastToCustom(Item *item, const TypeContext &tc) {
  if (CanImplicitlyCastToCustom(item)) {
    return InjectAndEncodeCustomType(item, tc);
  }
  return false;
}

static bool AllArgsCompatible(Item_func *func) {
  // All args need to be custom, and they need to be compatible.
  if (!func->get_arg(func->arg_count - 1)->has_type_context()) {
    villagesql_error(ER_INCOMPARABLE_TYPES, MYF(0), func->func_name());
    return false;
  }
  for (int i = func->arg_count - 1; i >= 1; --i) {
    auto *tc0 = func->get_arg(i - 1)->get_type_context();
    auto *tc1 = func->get_arg(i)->get_type_context();
    if (!tc0) {
      villagesql_error(ER_INCOMPARABLE_TYPES, MYF(0), func->func_name());
      return false;
    }
    // Look for an incompatibility in types (since we know both are custom).
    if (!AreTypesCompatible(*tc0, *tc1)) {
      villagesql_error(ER_INCOMPATIBLE_TYPES, MYF(0), tc0->type_name().c_str(),
                       tc1->type_name().c_str());
      return false;
    }
  }
  return true;
}

// Check that THEN/ELSE args of CASE are compatible custom types.
// CASE args are: WHEN at even indices (0, 2, ...), THEN at odd indices (1, 3,
// ...) plus optional first_expr and else_expr. We only check THEN/ELSE result
// values.
static bool CaseArgsCompatible(Item_func *func) {
  auto *case_func = down_cast<Item_func_case *>(func);
  uint ncases = case_func->get_ncases();
  int else_expr_num = case_func->get_else_expr_num();

  // Collect THEN args (odd indices) and ELSE arg
  std::vector<Item *> result_args;
  for (uint i = 1; i < ncases; i += 2) {
    result_args.push_back(func->get_arg(i));
  }
  if (else_expr_num != -1) {
    result_args.push_back(func->get_arg(else_expr_num));
  }

  if (result_args.empty()) {
    return true;
  }

  // Check if any THEN/ELSE has a custom type. If none do, we're just
  // switching on a custom type (simple CASE) with non-custom results - allowed.
  bool has_custom_result = false;
  for (Item *arg : result_args) {
    if (arg->has_type_context()) {
      has_custom_result = true;
      break;
    }
  }
  if (!has_custom_result) {
    return true;  // No custom types in results, allow
  }

  // If returning custom type, all THEN/ELSE must be compatible custom types
  if (!result_args.back()->has_type_context()) {
    villagesql_error(ER_INCOMPARABLE_TYPES, MYF(0), func->func_name());
    return false;
  }
  for (size_t i = result_args.size() - 1; i >= 1; --i) {
    auto *tc0 = result_args[i - 1]->get_type_context();
    auto *tc1 = result_args[i]->get_type_context();
    if (!tc0) {
      villagesql_error(ER_INCOMPARABLE_TYPES, MYF(0), func->func_name());
      return false;
    }
    if (!AreTypesCompatible(*tc0, *tc1)) {
      villagesql_error(ER_INCOMPATIBLE_TYPES, MYF(0), tc0->type_name().c_str(),
                       tc1->type_name().c_str());
      return false;
    }
  }
  return true;
}

bool IsFuncAllowedWithCustom(THD *thd [[maybe_unused]], Item_func *func,
                             const TypeContext &tc) {
  // Allow comparison between same custom types
  switch (func->functype()) {
    case Item_func::EQ_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GE_FUNC:
    case Item_func::GT_FUNC:
    case Item_func::EQUAL_FUNC:
      assert(2 == func->arg_count);
      return AllArgsCompatible(func);
    case Item_func::BETWEEN:
      assert(3 == func->arg_count);
      return AllArgsCompatible(func);
    case Item_func::CASE_FUNC:
      return CaseArgsCompatible(func);
    case Item_func::IN_FUNC:
      return AllArgsCompatible(func);
    case Item_func::UDF_FUNC: {
      // VDFs (VillageSQL Defined Functions) explicitly declare their parameter
      // types in the function signature, so we allow mixed custom/non-custom
      // types. The VDF handler validates types at invocation time.
      auto *udf_func_item = down_cast<Item_udf_func *>(func);
      if (udf_func_item->is_vdf()) {
        return true;  // VDFs handle their own type validation
      }
      // Regular UDFs cannot handle custom types - only VDFs can
      my_error(ER_WRONG_USAGE, MYF(0), func->func_name(),
               tc.type_name().c_str());
      return false;
    }
    // Allow explicit CAST
    case Item_func::ISNULL_FUNC:
    case Item_func::ISNOTNULL_FUNC:
      return true;
    default:
      // Block everything else for now
      my_error(ER_WRONG_USAGE, MYF(0), func->func_name(),
               tc.type_name().c_str());
      return false;
  }

  return false;
}

bool CheckCustomTypeUsage(Item *item, THD *thd) {
  // Check functions operating on custom types
  if (item->type() == Item::FUNC_ITEM) {
    Item_func *func = down_cast<Item_func *>(item);

    // Check if any arg is custom type
    for (uint i = 0; i < func->arg_count; i++) {
      if (func->get_arg(i)->has_type_context()) {
        if (!IsFuncAllowedWithCustom(thd, func,
                                     *func->get_arg(i)->get_type_context())) {
          return true;  // Abort walk
        }
        break;  // Already checked this function
      }
    }
  } else if (item->type() == Item::SUM_FUNC_ITEM) {
    // Check aggregate functions operating on custom types
    Item_sum *sum_func = down_cast<Item_sum *>(item);

    // Check if any arg is custom type
    for (uint i = 0; i < sum_func->arg_count; i++) {
      if (sum_func->get_arg(i)->has_type_context()) {
        // Some aggregates work automatically via the type's compare/decode
        // functions
        switch (sum_func->sum_func()) {
          case Item_sum::MIN_FUNC:
          case Item_sum::MAX_FUNC:
          case Item_sum::COUNT_DISTINCT_FUNC:
          case Item_sum::FIRST_LAST_VALUE_FUNC:
          case Item_sum::NTH_VALUE_FUNC:
          case Item_sum::LEAD_LAG_FUNC:
          case Item_sum::GROUP_CONCAT_FUNC:
            continue;  // Allow these aggregates on custom types
          default:
            // Block all other aggregate functions on custom types
            my_error(
                ER_WRONG_USAGE, MYF(0), sum_func->func_name(),
                sum_func->get_arg(i)->get_type_context()->type_name().c_str());
            return true;  // Abort walk
        }
      }
    }
  }

  return false;  // Continue walking
}

bool WalkQueryBlockForCustomTypeValidation(THD *thd,
                                           const mem_root_deque<Item *> &fields,
                                           const SQL_I_List<ORDER> &group_list,
                                           const SQL_I_List<ORDER> &order_list,
                                           Item *where_condition,
                                           Item *having_condition) {
  // Validate custom type usage in SELECT fields
  // Note: VisibleFields() iterates over visible result columns (output of
  // SELECT), not table columns. INVISIBLE table columns are still validated
  // when they appear in result column expressions because the walker
  // recursively descends the expression tree.
  for (Item *item : VisibleFields(fields)) {
    if (item->walk(&Item::check_custom_type_usage_processor, enum_walk::POSTFIX,
                   (uchar *)thd)) {
      return true;
    }
  }

  // Check WHERE clause
  if (where_condition &&
      where_condition->walk(&Item::check_custom_type_usage_processor,
                            enum_walk::POSTFIX, (uchar *)thd)) {
    return true;
  }

  // Check GROUP BY clause
  for (ORDER *order = group_list.first; order; order = order->next) {
    if ((*order->item)
            ->walk(&Item::check_custom_type_usage_processor, enum_walk::POSTFIX,
                   (uchar *)thd)) {
      return true;
    }
  }

  // Check ORDER BY clause
  for (ORDER *order = order_list.first; order; order = order->next) {
    if ((*order->item)
            ->walk(&Item::check_custom_type_usage_processor, enum_walk::POSTFIX,
                   (uchar *)thd)) {
      return true;
    }
  }

  // Check HAVING clause
  if (having_condition &&
      having_condition->walk(&Item::check_custom_type_usage_processor,
                             enum_walk::POSTFIX, (uchar *)thd)) {
    return true;
  }

  return false;
}

void AnnotateCustomColumnsInTmpTable(TABLE *table,
                                     List<Create_field> &create_fields) {
  List_iterator_fast<Create_field> it(create_fields);
  Create_field *cdef;
  auto &vclient = VictionaryClient::instance();
  for (uint i = 0; i < table->s->fields && (cdef = it++); i++) {
    if (cdef->custom_type_context != nullptr) {
      // Acquire our own reference to this TypeContext.
      auto guard = vclient.get_read_lock();
      const TypeContext *tc = vclient.type_contexts().acquire(
          cdef->custom_type_context->key(), table->s->mem_root);
      table->field[i]->set_type_context(tc);
      assert(static_cast<int64_t>(table->field[i]->field_length) ==
             tc->persisted_length());
    }
  }
}

bool ValidateCustomTypeContext(THD *thd) {
  // TODO(villagesql-beta): Remove these restrictions once custom types are
  // fully supported in these contexts.

  // Check for stored procedures/functions
  // Tested by:
  // mysql-test/suite/villagesql/stored_procedure/t/stored_procedure_call_complex.test
  if (thd->sp_runtime_ctx != nullptr) {
    villagesql_error(
        "Custom types are not yet supported in stored procedures/functions",
        MYF(0));
    return true;
  }

  return false;  // Context is supported
}

bool ValidateAndConvertVDFArguments(THD *thd, const char *func_name,
                                    const LEX_STRING &extension_name,
                                    uint arg_count, Item **args,
                                    const vef_signature_t *signature) {
  // Validate argument count matches signature
  if (arg_count != signature->param_count) {
    villagesql_error(
        "Cannot initialize function '%s': wrong number of arguments "
        "(expected %u, got %u)",
        MYF(0), func_name, signature->param_count, arg_count);
    return true;
  }

  // Process each argument against expected parameter type
  for (uint i = 0; i < arg_count; i++) {
    const vef_type_t &expected_type = signature->params[i];

    // Only validate custom type parameters
    if (expected_type.id != VEF_TYPE_CUSTOM) {
      continue;
    }

    // NULL is allowed for any parameter type - the VDF will handle it
    if (args[i]->type() == Item::NULL_ITEM) {
      continue;
    }

    // Build the expected qualified base name ("extension.TYPE") once for use
    // in Cases 1 and 3. This ensures a TYPE from extension A is not accepted
    // by a VDF from extension B that also declares the same TYPE.
    // expected_type.custom_type is always non-null for VEF_TYPE_CUSTOM params
    // (enforced by the SDK).
    assert(expected_type.custom_type != nullptr);
    const std::string expected_qbn = make_qualified_base_name(
        std::string(extension_name.str, extension_name.length),
        expected_type.custom_type);

    // Case 1: Argument already has custom type - validate compatibility.
    auto *tc = args[i]->get_type_context();
    if (tc != nullptr) {
      if (tc->qualified_base_name() != expected_qbn) {
        villagesql_error(
            "Cannot initialize function '%s': argument %u type mismatch "
            "(expected %s, got %s)",
            MYF(0), func_name, i + 1, expected_qbn.c_str(),
            tc->qualified_base_name().c_str());
        return true;
      }
      continue;
    }

    // Case 2: Argument is a constant string - try implicit conversion
    if (args[i]->type() == Item::STRING_ITEM &&
        args[i]->const_for_execution()) {
      // Resolve the custom type by extension and type name
      LEX_STRING lex_type_name;
      lex_type_name.str = const_cast<char *>(expected_type.custom_type);
      lex_type_name.length = strlen(expected_type.custom_type);

      const TypeContext *type_ctx = nullptr;
      // TODO(villagesql-beta): pass real TypeParameters for parameterized types
      TypeParameters empty_params;
      if (ResolveTypeToContext(extension_name, lex_type_name, empty_params,
                               *thd->mem_root, type_ctx)) {
        return true;  // Error already reported
      }

      if (type_ctx == nullptr) {
        villagesql_error(
            "Cannot initialize function '%s': custom type '%s' not found for "
            "argument %u",
            MYF(0), func_name, expected_type.custom_type, i + 1);
        return true;
      }

      // Inject and encode the custom type
      if (InjectAndEncodeCustomType(args[i], *type_ctx)) {
        return true;  // Error already reported
      }
      continue;
    }

    // Case 3: Argument is a column reference (Item_field) without type context
    // yet. This happens during functional index creation: the table is being
    // created, so MaybeInjectCustomType hasn't run yet. The field's underlying
    // Create_field_wrapper (set by replace_field_processor) lets us check
    // whether the column will actually be a custom type.
    // We match on qualified_base_name() (extension.type, no parameters),
    // consistent with Case 1, because VDF signatures cannot express type
    // parameterization - a VDF declared with param("TVECTOR") must accept any
    // TVECTOR(N) column.
    if (args[i]->type() == Item::FIELD_ITEM) {
      auto *item_field = down_cast<Item_field *>(args[i]);
      if (item_field->field != nullptr &&
          item_field->field->is_wrapper_field()) {
        auto *wrapper =
            down_cast<const Create_field_wrapper *>(item_field->field);
        const Create_field *cf = wrapper->get_create_field();
        if (cf->custom_type_context != nullptr) {
          if (cf->custom_type_context->qualified_base_name() != expected_qbn) {
            villagesql_error(
                "Cannot initialize function '%s': argument %u type mismatch "
                "(expected %s, got %s)",
                MYF(0), func_name, i + 1, expected_qbn.c_str(),
                cf->custom_type_context->qualified_base_name().c_str());
            return true;
          }
          continue;
        }
      }
      // Column has no custom type context - fall through
    }

    // Case 4: Argument is not a custom type and not a constant string
    villagesql_error(
        "Cannot initialize function '%s': argument %u must be a custom type "
        "or string constant",
        MYF(0), func_name, i + 1);
    return true;
  }

  return false;
}

void SetVDFReturnTypeContext(THD *thd, const LEX_STRING &extension_name,
                             const vef_signature_t *signature,
                             Item *result_item) {
  const char *return_type_name = signature->return_type.custom_type;
  if (return_type_name == nullptr) {
    return;
  }

  LEX_STRING lex_return_type;
  lex_return_type.str = const_cast<char *>(return_type_name);
  lex_return_type.length = strlen(return_type_name);

  const TypeContext *return_type_ctx = nullptr;
  // TODO(villagesql-beta): pass real TypeParameters for parameterized types
  TypeParameters empty_params;
  if (!ResolveTypeToContext(extension_name, lex_return_type, empty_params,
                            *thd->mem_root, return_type_ctx) &&
      return_type_ctx != nullptr) {
    result_item->set_type_context(return_type_ctx);
  }
}

std::shared_ptr<const TypeContext> AcquireTypeContextClientManaged(
    const TypeContext *source_tc) {
  if (source_tc == nullptr) {
    return std::shared_ptr<const TypeContext>();
  }

  auto &vclient = VictionaryClient::instance();
  auto guard = vclient.get_read_lock();
  return vclient.type_contexts().acquire_client_managed(source_tc->key());
}

}  // namespace villagesql
