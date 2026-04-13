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
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/systable/custom_columns.h"
#include "villagesql/schema/tmp_metadata.h"
#include "villagesql/schema/util.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/types/type_decoder.h"
#include "villagesql/types/type_encoder.h"

namespace villagesql {

// Returns true if this is an ALTER TABLE #sql-xxx rebuild table. Requires both
// the INTERNAL_TMP_TABLE flag AND the #sql name prefix — the flag alone is too
// broad (DD system tables are also opened as INTERNAL_TMP_TABLE), and the name
// prefix alone is too broad (users can create tables named #sql... with
// backtick quoting).
static bool is_alter_rebuild_table(const TABLE_SHARE &share) {
  return share.tmp_table == INTERNAL_TMP_TABLE &&
         share.table_name.str != nullptr &&
         strncmp(share.table_name.str, tmp_file_prefix,
                 tmp_file_prefix_length) == 0;
}

static const char *ER_INCOMPARABLE_TYPES =
    "Cannot compare values of custom and non-custom types in %s";
static const char *ER_INCOMPATIBLE_TYPES =
    "Cannot compare values of incompatible types '%s' and '%s'";

// Verify that the Field's storage length matches the TypeContext's
// persisted_length.
static bool CheckFieldLengthMatchesType(const Field *field,
                                        const TypeContext *tc) {
  if (should_assert_if_false(static_cast<int64_t>(field->field_length) ==
                             tc->persisted_length())) {
    LogVSQL(ERROR_LEVEL,
            "field_length (%u) != persisted_length (%" PRId64
            ") for column %s (type %s)",
            field->field_length, tc->persisted_length(), field->field_name,
            tc->qualified_name().c_str());
    villagesql_error(
        "Internal error: field length mismatch for column %s (type %s); "
        "check server log for details",
        MYF(0), field->field_name, tc->qualified_name().c_str());
    return true;
  }
  return false;
}

bool MaybeInjectCustomType(THD *thd, TABLE_SHARE &share, Field *field) {
  if (should_assert_if_null(thd)) {
    LogVSQL(ERROR_LEVEL, "thd is null in MaybeInjectCustomType");
    return true;
  }
  if (should_assert_if_null(field)) {
    LogVSQL(ERROR_LEVEL, "field is null in MaybeInjectCustomType");
    return true;
  }

  // For ALTER TABLE #sql-xxx rebuild tables, inject from the session map
  // instead of the victionary.
  if (is_alter_rebuild_table(share)) {
    if (!thd->villagesql_alter_custom_fields.empty()) {
      auto it = thd->villagesql_alter_custom_fields.find(field->field_name);
      if (it != thd->villagesql_alter_custom_fields.end()) {
        field->set_type_context(it->second);
      }
    }
    return false;
  }

  // Extract identifiers directly
  std::string db_name = std::string(share.db.str, share.db.length);

  // Skip special databases
  if (::villagesql::is_system_schema(db_name.c_str())) {
    return false;
  }

  std::string table_name(share.table_name.str, share.table_name.length);
  std::string column_name(field->field_name);
  ColumnKey col_key(db_name, table_name, column_name);

  // User-created temporary tables are never in the victionary. If session
  // metadata has an entry for this column, inject it and return.
  if (share.tmp_table != NO_TMP_TABLE &&
      thd->villagesql_tmp_metadata != nullptr) {
    const TypeContext *tc = thd->villagesql_tmp_metadata->get(col_key.str());
    if (tc != nullptr) {
      field->set_type_context(tc);
      return CheckFieldLengthMatchesType(field, tc);
    }
    // Fall through to victionary for type injection and field length
    // validation.
  }

  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    // Too early to perform a lookup. We must be starting up.
    return false;
  }

  auto guard = vclient.get_write_lock();
  const auto &columns = vclient.columns();
  const ColumnEntry *column_entry = columns.get(thd, col_key.str());
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
  return CheckFieldLengthMatchesType(field, tc);
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
  if (result == nullptr) {
    // nullptr means OOM (SQL error already set) or TypeContext initialization
    // failure (only logged to error log). Set the SQL error if not already set.
    if (!current_thd->is_error()) {
      villagesql_error(
          "Type '%s' failed to initialize; check the error log for details",
          MYF(0), type_descriptor->qualified_base_name().c_str());
    }
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

// Lazily allocate a TypeEncoder for field, reused for all subsequent encodes
// within the table's lifetime.
//
// For regular tables (NO_TMP_TABLE), TABLE::mem_root has the same lifetime as
// the Field clone that caches the encoder pointer, so both are freed together
// when the TABLE is evicted from the table open cache.
//
// For any kind of tmp table use TABLE_SHARE::mem_root, this is where the
// Fields are allocated for most uses of tmp tables. The one exception is
// INTERNAL_TMP_TABLE where the initial Table object's fields are allocated
// from the THD::mem_root, but for these tables the share is destroyed at the
// end of the statement, so the lifetime is correct.
static TypeEncoder *GetTypeEncoderFor(Field *field) {
  TypeEncoder *encoder = field->get_type_encoder();
  if (encoder == nullptr) {
    MEM_ROOT &mem_root = (field->table->s->tmp_table == NO_TMP_TABLE)
                             ? field->table->mem_root
                             : field->table->s->mem_root;
    encoder = new (&mem_root) TypeEncoder(field->get_type_context(), mem_root);
    if (encoder == nullptr) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), sizeof(TypeEncoder));
      return nullptr;
    }
    if (encoder->Init()) return nullptr;
    field->set_type_encoder(encoder);
  }
  return encoder;
}

// Lazily allocate an TypeEncoder for an Item from thd->mem_root. The encoder
// is reused for all subsequent encodes on the same Item within a query.
// Item::cleanup() nulls type_encoder_ between executions (e.g. PS re-exec),
// so the next call after cleanup will allocate a fresh encoder on the new
// thd->mem_root.
static TypeEncoder *GetTypeEncoderFor(Item *item) {
  TypeEncoder *encoder = item->get_type_encoder();
  if (encoder == nullptr) {
    encoder = new (current_thd->mem_root)
        TypeEncoder(item->get_type_context(), *current_thd->mem_root);
    if (encoder == nullptr) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), sizeof(TypeEncoder));
      return nullptr;
    }
    if (encoder->Init()) return nullptr;
    item->set_type_encoder(encoder);
  }
  return encoder;
}

// Encode from for item's custom type. Returns the encoded String* on success.
// On failure returns nullptr; is_valid=true means OOM (my_error already
// called), is_valid=false means invalid value for the type.
static String *EncodeForItem(Item *item, const String &from, bool &is_valid) {
  TypeEncoder *encoder = GetTypeEncoderFor(item);
  if (encoder == nullptr) {
    is_valid = true;  // OOM, my_error already called
    return nullptr;
  }
  return encoder->encode(from, is_valid);
}

String *EncodeStringForField(Field *field, const String &from, bool &is_valid) {
  assert(field->has_type_context());

  TypeEncoder *encoder = GetTypeEncoderFor(field);
  if (encoder == nullptr) {
    is_valid = true;  // OOM, my_error already called
    return nullptr;
  }
  String *encoded = encoder->encode(from, is_valid);
  if (encoded == nullptr) {
    if (is_valid) return nullptr;  // OOM, my_error already called
    // Encoding failed - invalid value for custom type
    // Always push a warning (consistent with MySQL built-in types)
    // Caller decides whether to also promote to error
    const TypeContext *tc = field->get_type_context();
    THD *thd = current_thd;
    const ErrConvString errmsg(from.ptr(), from.length(), from.charset());
    const Diagnostics_area *da = thd->get_stmt_da();
    push_warning_printf(thd, Sql_condition::SL_WARNING,
                        ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                        ER_THD(thd, ER_TRUNCATED_WRONG_VALUE_FOR_FIELD),
                        tc->type_name().c_str(), errmsg.ptr(),
                        field->field_name, da->current_row_for_condition());
    return nullptr;
  }
  return encoded;
}

// Lazily allocate a TypeDecoder for field, reused for all subsequent decodes
// within the table's lifetime.
//
// For regular tables (NO_TMP_TABLE), TABLE::mem_root has the same lifetime as
// the Field clone that caches the decoder pointer, so both are freed together
// when the TABLE is evicted from the table open cache.
//
// For any kind of tmp table use TABLE_SHARE::mem_root, this is where the
// Fields are allocated for most uses of tmp tables. The one exception is
// INTERNAL_TMP_TABLE where the initial Table object's fields are allocated
// from the THD::mem_root, but for these tables the share is destroyed at the
// end of the statement, so the lifetime is correct.
static TypeDecoder *GetTypeDecoderFor(const Field *field) {
  TypeDecoder *decoder = field->get_type_decoder();
  if (decoder == nullptr) {
    MEM_ROOT &mem_root = (field->table->s->tmp_table == NO_TMP_TABLE)
                             ? field->table->mem_root
                             : field->table->s->mem_root;
    decoder = new (&mem_root) TypeDecoder(*field->get_type_context(), mem_root);
    if (decoder == nullptr) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), sizeof(TypeDecoder));
      return nullptr;
    }
    if (decoder->Init()) return nullptr;
    field->set_type_decoder(decoder);
  }
  return decoder;
}

// Lazily allocate a TypeDecoder for an Item from thd->mem_root. The context
// is reused for all subsequent decodes on the same Item within a query.
// Item::cleanup() nulls type_decoder_ between executions (e.g. PS re-exec),
// so the next call after cleanup will allocate a fresh context on the new
// thd->mem_root.
static TypeDecoder *GetTypeDecoderFor(Item *item) {
  TypeDecoder *decoder = item->get_type_decoder();
  if (decoder == nullptr) {
    decoder = new (current_thd->mem_root)
        TypeDecoder(*item->get_type_context(), *current_thd->mem_root);
    if (decoder == nullptr) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), sizeof(TypeDecoder));
      return nullptr;
    }
    if (decoder->Init()) return nullptr;
    item->set_type_decoder(decoder);
  }
  return decoder;
}

bool DecodeStringUncached(const TypeContext &tc, const String &from,
                          String *out) {
  TypeDecoder decoder(tc, *current_thd->mem_root);
  if (decoder.Init()) return true;
  return decoder.decode(pointer_cast<const uchar *>(from.ptr()), from.length(),
                        out) != DecodeResult::kSuccess;
}

// Logs a decode failure and reports an error (or warning in IGNORE mode) to
// the client. result must be kInvalidData or kExtensionError.
static void ReportDecodeError(DecodeResult result, const char *column_name,
                              const char *type_name, const uchar *data,
                              size_t len, const char *decoder_error_msg) {
  const ErrConvString value_prefix(pointer_cast<const char *>(data), len,
                                   &my_charset_bin);
  ha_rows row = current_thd->get_stmt_da()->current_row_for_condition();
  const char *label = (result == DecodeResult::kInvalidData)
                          ? "Invalid value in column"
                          : "Extension error decoding column";
  LogVSQL(WARNING_LEVEL, "%s '%s' of type '%s' at row %llu (value: %s): %s",
          label, column_name, type_name, static_cast<unsigned long long>(row),
          value_prefix.ptr(), decoder_error_msg);
  villagesql_error(
      "%s '%s' of type '%s' at row %llu: check server log for details", MYF(0),
      label, column_name, type_name, static_cast<unsigned long long>(row));
}

bool DecodeStringForField(const Field *field, String *out) {
  if (should_assert_if_false(field->has_type_context())) {
    return true;
  }
  TypeDecoder *decoder = GetTypeDecoderFor(field);
  if (decoder == nullptr) {
    return true;  // OOM, my_error already called
  }
  DecodeResult result =
      decoder->decode(field->data_ptr(), field->data_length(), out);
  if (result == DecodeResult::kSuccess) return false;
  ReportDecodeError(result, field->field_name,
                    field->get_type_context()->qualified_name().c_str(),
                    field->data_ptr(), field->data_length(),
                    decoder->last_error_msg());
  return true;
}

bool DecodeStringForItem(Item *item, const String &from, String *out) {
  if (should_assert_if_false(item->has_type_context())) {
    return true;
  }
  TypeDecoder *decoder = GetTypeDecoderFor(item);
  if (decoder == nullptr) {
    return true;  // OOM, my_error already called
  }
  // Extract ptr/len before decode() writes to out: from and *out may alias.
  const uchar *data = pointer_cast<const uchar *>(from.ptr());
  size_t len = from.length();
  DecodeResult result = decoder->decode(data, len, out);
  if (result == DecodeResult::kSuccess) return false;
  ReportDecodeError(result, item->full_name(),
                    item->get_type_context()->qualified_name().c_str(), data,
                    len, decoder->last_error_msg());
  return true;
}

void AppendFullyQualifiedName(const TypeContext &tc, String *out) {
  const std::string &name = tc.qualified_name();
  out->append(name.c_str(), name.length());
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
  String *encoded = EncodeForItem(item, *str_val, is_valid);
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
    auto result = CompareCustomType(*tc, data1, len1, data2, len2);
    res = reverse ? -result : result;
  } else {
    res = memcmp(data1, data2, min_len);
  }

  return res;
}

std::optional<size_t> TryComputeHash(const TypeContext &tc, const uchar *data,
                                     size_t len) {
  const auto &hash_op = tc.hash_op();
  if (!hash_op.has_value()) return std::nullopt;
  return hash_op->invoke(data, len);
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
  } else if (item->has_type_context() ||
             item->type() == Item::INSERT_VALUE_ITEM) {
    StringBuffer<SchemaManager::kMaxQualifiedTypeNameLen> src_type_str;
    if (item->has_type_context()) {
      src_type_str.append(item->get_type_context()->qualified_name().c_str());
    } else {
      down_cast<const Item_field *>(item)->field->sql_type(src_type_str);
    }
    villagesql_error(
        "Cannot implicitly cast from %s to %s for column '%s' at row %ld",
        MYF(0), src_type_str.c_ptr(),
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

void CopyCustomToCustomField(const Field *from, Field *to) {
  // Both fields have the same custom type. Copy binary data directly.
  // Handle potential length_bytes differences (VARCHAR(255) vs VARCHAR(65535)).
  // Note: from->data_length() decodes the length and from->data_ptr() returns
  // a pointer to the data (skipping the length prefix). But to->field_ptr()
  // returns the start of the field including the length prefix area, so we must
  // manually encode the length prefix.
  // TODO(villagesql-blob): needs update for blob implementation.
  assert(from->get_type_context()->descriptor()->implementation_type() ==
         MYSQL_TYPE_VARCHAR);
  assert(to->get_type_context()->descriptor()->implementation_type() ==
         MYSQL_TYPE_VARCHAR);
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
  // TODO(villagesql-beta): review this for variable length types
  assert(data_len <= to->field_length);
  // Copy the binary data
  memcpy(to_ptr + to_length_bytes, from_data, data_len);
}

static void do_field_custom_to_custom(Copy_field *, const Field *from,
                                      Field *to) {
  CopyCustomToCustomField(from, to);
}

static void do_field_custom_to_string(Copy_field *, const Field *from,
                                      Field *to) {
  assert(from->has_type_context());
  // Custom → non-custom string: decode to string representation.
  // NULL is handled outside this function
  // TODO(villagesql-performance): evaluate something more performant
  String buf;
  from->val_external_str(&buf);
  to->store(buf.ptr(), buf.length(), buf.charset());
}

FieldCopyFunc *GetCopyFunc(const Field *from, const Field *to) {
  assert(from->has_type_context());
  if (to->has_type_context()) {
    // TODO(villagesql-performance): split to targeted copy functions
    return do_field_custom_to_custom;
  }
  return do_field_custom_to_string;
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
  CopyCustomToCustomField(from, to);
  return false;
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
  String *encoded = EncodeStringForField(to_field, *str, is_oom);
  if (encoded == nullptr) {
    return is_oom ? TYPE_ERR_OOM : TYPE_ERR_BAD_VALUE;
  }

  to_field->set_notnull();
  return to_field->store(encoded->ptr(), encoded->length(), &my_charset_bin);
}

type_conversion_status EncodeAndStoreStringToCustomField(
    const String &str_value, Field *field) {
  bool is_valid = true;
  String *encoded = EncodeStringForField(field, str_value, is_valid);
  if (encoded == nullptr) {
    return is_valid ? TYPE_ERR_OOM : TYPE_WARN_OUT_OF_RANGE;
  }
  return field->store(encoded->ptr(), encoded->length(), &my_charset_bin);
}

String *EncodeStringForCustomParam(Item *item, const String &str_value,
                                   bool &null_value) {
  bool is_valid = true;
  String *encoded = EncodeForItem(item, str_value, is_valid);
  if (encoded == nullptr) {
    if (!is_valid) {
      // Invalid value for custom type
      const TypeContext *tc = item->get_type_context();
      const ErrConvString errmsg(str_value.ptr(), str_value.length(),
                                 str_value.charset());
      villagesql_error("Incorrect %s value: '%s' for parameter '%s'", MYF(0),
                       tc->qualified_name().c_str(), errmsg.ptr(),
                       item->item_name.ptr());
    }

    // else OOM - my_error already called
    null_value = true;
    return nullptr;
  }
  return encoded;
}

type_conversion_status StoreCustomFieldIntrinsicDefault(Field *field) {
  assert(field->has_type_context());
  const TypeContext &tc = *field->get_type_context();
  const unsigned char *cached_buffer = tc.intrinsic_default_buffer();
  if (cached_buffer == nullptr) {
    // Protocol-1 types have no intrinsic default; callers should not reach
    // here.
    villagesql_error("Type '%s' has no intrinsic default", MYF(0),
                     tc.qualified_name().c_str());
    return TYPE_ERR_BAD_VALUE;
  }
  const size_t cached_size = tc.intrinsic_default_size();
  assert(cached_size == static_cast<size_t>(tc.persisted_length()));

  field->set_notnull();
  return field->store(reinterpret_cast<const char *>(cached_buffer),
                      cached_size, &my_charset_bin);
}

bool LoadEncodeAndStoreCustomField(THD *thd, Field *field,
                                   const String &input_str) {
  bool is_valid = false;
  String *encoded = EncodeStringForField(field, input_str, is_valid);
  if (encoded != nullptr) {
    field->store(encoded->ptr(), encoded->length(), &my_charset_bin);
    return false;
  }
  if (is_valid) return true;  // OOM case
  if (!thd->lex->is_ignore() && thd->is_strict_mode()) {
    // EncodeStringForField already pushed a warning; promote to error
    my_error(ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, MYF(0),
             field->get_type_context()->type_name().c_str(),
             ErrConvString(&input_str).ptr(), field->field_name,
             thd->get_stmt_da()->current_row_for_condition());
    return true;
  }
  // EncodeStringForField already pushed a warning. Store the intrinsic default,
  // mirroring MySQL built-in behavior of storing 0, '', etc. for bad values in
  // IGNORE or non-strict mode.
  return StoreCustomFieldIntrinsicDefault(field) != TYPE_OK;
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
  // Only check that all custom-typed args are mutually compatible. Non-custom
  // args (string literals, integers, etc.) are left for the comparison
  // function's own fix_fields to cast or reject with the proper error message.
  const TypeContext *any_tc = nullptr;
  for (uint i = 0; i < func->arg_count; i++) {
    if (func->get_arg(i)->has_type_context()) {
      any_tc = func->get_arg(i)->get_type_context();
      break;
    }
  }
  for (uint i = 1; any_tc && i < func->arg_count; i++) {
    auto *tc = func->get_arg(i)->get_type_context();
    if (!tc) continue;  // not yet custom-typed, skip
    if (!AreTypesCompatible(*any_tc, *tc)) {
      villagesql_error("Cannot compare types %s and %s in %s", MYF(0),
                       any_tc->qualified_name().c_str(),
                       tc->qualified_name().c_str(), func->func_name());
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
    // Allow pass-through functions that don't interpret the custom value
    case Item_func::ISNULL_FUNC:
    case Item_func::ISNOTNULL_FUNC:
    case Item_func::ISNOTNULLTEST_FUNC:  // internal, used in ALL/ANY subqueries
    case Item_func::SUSERVAR_FUNC:       // SET @var := custom_value
    case Item_func::GUSERVAR_FUNC:       // GET @var
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

// Returns true on error (acquire failed, meaning the extension was uninstalled
// concurrently). Reports the error before returning.
static bool insert_tmp_metadata_for_thd(THD *thd, const ColumnKey &key,
                                        const TypeContextKey &source_key,
                                        Field *field = nullptr) {
  auto &vclient = VictionaryClient::instance();
  auto guard = vclient.get_read_lock();
  auto tc_owner = vclient.type_contexts().acquire_client_managed(source_key);
  if (tc_owner == nullptr) {
    villagesql_error(
        "Custom type '%s.%s' is no longer available; the extension may have "
        "been uninstalled",
        MYF(0), source_key.descriptor_key().extension_name().c_str(),
        source_key.descriptor_key().type_name().c_str());
    return true;
  }
  if (field != nullptr) {
    const TypeContext *tc = tc_owner.get();
    field->set_type_context(tc);
    if (CheckFieldLengthMatchesType(field, tc)) return true;
  }
  if (!thd) return false;
  if (!thd->villagesql_tmp_metadata) {
    thd->villagesql_tmp_metadata = std::make_unique<TmpMetadata>();
  }
  thd->villagesql_tmp_metadata->insert(key, std::move(tc_owner));
  return false;
}

bool PrepareTmpTableCustomColumns(
    THD *thd, const char *db, const char *table_name,
    List<Create_field> &create_fields,
    [[maybe_unused]] const HA_CREATE_INFO *create_info) {
  assert(create_info->options & HA_LEX_CREATE_TMP_TABLE);
  List_iterator_fast<Create_field> it(create_fields);
  Create_field *cdef;
  while ((cdef = it++) != nullptr) {
    if (cdef->custom_type_context == nullptr) continue;
    ColumnKey key(db, table_name, cdef->field_name);
    if (insert_tmp_metadata_for_thd(thd, key, cdef->custom_type_context->key()))
      return true;
  }
  return false;
}

bool AnnotateCustomColumnsInTmpTable(THD *thd, TABLE *table,
                                     List<Create_field> &create_fields) {
  assert(table->s->tmp_table != NO_TMP_TABLE);
  List_iterator_fast<Create_field> it(create_fields);
  Create_field *cdef;
  for (uint i = 0; i < table->s->fields && (cdef = it++); i++) {
    if (cdef->custom_type_context == nullptr) continue;
    ColumnKey key(table->s->db.str, table->s->table_name.str,
                  table->field[i]->field_name);
    if (insert_tmp_metadata_for_thd(thd, key, cdef->custom_type_context->key(),
                                    table->field[i]))
      return true;
  }
  return false;
}

void RemoveTmpTableMetadata(THD *thd, const std::string &db,
                            const std::string &table_name) {
  if (!thd || thd->villagesql_tmp_metadata == nullptr) return;
  thd->villagesql_tmp_metadata->delete_table(db, table_name);
}

void RemoveTmpTableMetadata(THD *thd, TABLE *table) {
  if (!table) return;
  RemoveTmpTableMetadata(thd, table->s->db.str, table->s->table_name.str);
}


void PrepareAlterCustomFields(THD *thd, const List<Create_field> &create_list) {
  thd->villagesql_alter_custom_fields.clear();
  for (const Create_field &cdef : create_list) {
    if (cdef.custom_type_context != nullptr)
      thd->villagesql_alter_custom_fields.emplace(cdef.field_name,
                                                  cdef.custom_type_context);
  }
}

void ClearAlterCustomFields(THD *thd) {
  thd->villagesql_alter_custom_fields.clear();
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

    // Case 3: Argument is not a custom type and not a constant string
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
