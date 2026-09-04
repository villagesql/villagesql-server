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

#ifndef VILLAGESQL_TYPES_UTIL_H_
#define VILLAGESQL_TYPES_UTIL_H_

#include <stddef.h>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "lex_string.h"
#include "my_inttypes.h"
#include "sql/dd/string_type.h"
#include "sql/field.h"
#include "sql/sql_array.h"
#include "sql_string.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

class Create_field;
class Field;
class Item;
class Item_func;
enum class enum_sp_type;
struct MEM_ROOT;
struct ORDER;
class sp_pcontext;
template <typename T>
class SQL_I_List;
class THD;
struct TABLE_SHARE;
struct TABLE;
class Table_ref;
template <typename T>
class List;
template <typename T>
class mem_root_deque;
class String;

namespace villagesql {

class IndexContext;
class IndexProfileDescriptor;

// Check if Field should be marked as a custom type by checking the
// VictionaryClient. If the Field is supposed to be a custom type, then fill the
// internal TypeContext appropriately to refer to the type information and the
// given mem_root. Checks uncommitted column metadata for the THD first before
// falling back to committed data.
extern bool MaybeInjectCustomType(THD *thd, TABLE_SHARE &share, Field *field);

// Check if a KEY represents a custom index by looking up the VictionaryClient.
// If found, sets keyinfo->custom_index_context (an IndexContext scoped to
// share.mem_root) and sets custom_index_profile on each KEY_PART_INFO from
// the per-column IndexColumnEntry profile bindings.
// Only operates on committed entries (existing tables opened from DD).
// Returns true on error, false on success (including when not a custom index).
extern bool MaybeInjectCustomIndex(THD *thd, TABLE_SHARE &share, KEY *keyinfo);

// Injects custom TypeContexts into SP variable fields for a stored procedure
// by looking up villagesql.custom_sp_params. Restores custom type information
// that was lost when MySQL normalized SP variable types (e.g. COMPLEX ->
// varbinary(16)) in the data dictionary. Also syncs the TypeContext into the
// Item wrappers so SP body statements see the correct type.
// pctx provides the variable definitions; fields is the TABLE field array;
// var_items is the Item wrapper array. type_refs receives a shared_ptr for each
// injected TypeContext and must remain alive as long as the fields reference
// it. had_custom_params is set to true when the SP has at least one
// custom-typed param, false when it has none. Returns false on success, true
// on error.
extern bool InjectCustomSpParams(
    const char *db_name, const char *sp_name, const sp_pcontext *pctx,
    Field **fields, Bounds_checked_array<Item *> var_items,
    std::vector<std::shared_ptr<const TypeContext>> &type_refs,
    bool *had_custom_params);

// Builds a parameter string with fully qualified custom type names for all
// parameters in root_ctx. If any parameter has a custom type, writes the result
// into *out using thd->strmake for the backing memory. Leaves *out unchanged if
// no custom types are present.
extern void BuildQualifiedParamsString(THD *thd, enum_sp_type sp_type,
                                       const sp_pcontext *root_ctx,
                                       LEX_STRING *out);

// Looks up just the TypeDescriptor for a (possibly extension-qualified) type
// name. Returns false on success and true on internal failure.
// If the type isn't known the function returns false (success) with result
// nullptr.
extern bool ResolveTypeDescriptor(std::string_view extension_name,
                                  std::string_view type_name,
                                  const TypeDescriptor *&result);

// Acquires (create if necessary) the cached TypeContext for an already
// resolved TypeDescriptor and its concrete parameters. The descriptor must be
// non-null (typically obtained from ResolveTypeDescriptor). The mem_root scopes
// the cleanup of the TypeContext. Returns false on success and true on failure.
extern bool AcquireOrCreateTypeContext(const TypeDescriptor *descriptor,
                                       const TypeParameters &parameters,
                                       MEM_ROOT &mem_root,
                                       const TypeContext *&result);

// Pre-populate session-local temporary metadata from Create_field list so that
// MaybeInjectCustomType can inject type contexts during open_table_from_share
// (which calls unpack_value_generator before AnnotateCustomColumnsInTmpTable
// runs). Must be called before open_table_uncached for temporary tables.
// Returns true on error (e.g. extension uninstalled concurrently).
extern bool PrepareTmpTableCustomColumns(THD *thd, const char *db,
                                         const char *table_name,
                                         List<Create_field> &create_fields,
                                         const HA_CREATE_INFO *create_info);

// Copy custom type contexts from Create_field list to corresponding Fields in
// a temporary TABLE, and update session metadata with shared_ptr-owned entries
// so subsequent opens of the same table can find the type context. Replaces
// PrepareTmpTableCustomColumns entries (net-zero refcount change).
// Returns true on error (e.g. extension uninstalled concurrently).
extern bool AnnotateCustomColumnsInTmpTable(THD *thd, TABLE *table,
                                            List<Create_field> &create_fields);

// Remove session metadata entries for a user-created temporary table,
// dropping shared_ptr refcounts to allow extension uninstall.
extern void RemoveTmpTableMetadata(THD *thd, const std::string &db,
                                   const std::string &table_name);
extern void RemoveTmpTableMetadata(THD *thd, TABLE *table);

// Populate thd->villagesql_alter_custom_fields with pointers to custom-typed
// Create_fields from create_list. Called from create_table_impl after
// mysql_prepare_alter_table has finalized the create_list.
extern void PrepareAlterCustomFields(THD *thd,
                                     const List<Create_field> &create_list);

// Clear thd->villagesql_alter_custom_fields.
extern void ClearAlterCustomFields(THD *thd);

// Check if any column in a create_list has a custom type.
// Used to determine if we need to regenerate the CREATE TABLE statement
// for binlogging (to ensure fully qualified type names are used).
extern bool HasCustomTypeColumns(const List<Create_field> &create_list);

// Handle custom column metadata updates when renaming a table
// Marks all custom columns for the table for UPDATE with new db/table names
// Queries VictionaryClient cache and marks MarkForUpdate operations
// Requires write lock to be held for the VictionaryClient before calling
// Returns false on success, true on error
extern bool HandleCustomColumnsForTableRename(THD &thd, const char *old_db,
                                              const char *old_table,
                                              const char *new_db,
                                              const char *new_table);

// Encodes a string for storage in a custom type field, with error reporting.
// The TypeContext and field name are taken from the field itself. On error,
// returns nullptr; if is_valid is false the value was invalid (warning
// pushed), otherwise OOM.
extern String *EncodeStringForField(Field *field, const String &from,
                                    bool &is_valid);

// Decode [data, data+len) for the given TypeContext without caching any state.
// Use this for one-shot decodes outside the row-reading hot path (e.g. query
// printing). Returns false on success, true on failure. On OOM, my_error is
// called; decode failures are silent (caller gets true but no error is set).
extern bool DecodeStringUncached(const TypeContext &tc, const String &from,
                                 String *out);

// Decode the field's current value into out. Returns false on success. On
// failure returns true; error already reported via villagesql_error (or
// my_error for OOM).
extern bool DecodeStringForField(const Field *field, String *out);

// Decode binary data from [data, data+len) into out for a custom type item.
// Returns false on success. On failure returns true; error already reported
// via villagesql_error (or my_error for OOM).
extern bool DecodeStringForItem(Item *item, const String &from, String *out);

// Appends "extension_name.type_name" to the given String.
extern void AppendFullyQualifiedName(const TypeContext &tc, String *out);

// Appends the custom index profile bound to a key part, as
// " extension_name.profile_name", for SHOW CREATE TABLE.
extern void AppendCustomIndexProfile(const THD *thd,
                                     const IndexProfileDescriptor *profile,
                                     String *out);

// Appends " USING EXTENDED(extension_name.index_type_name)" for a custom
// index, followed by " WITH (key = value, ...)" when it was instantiated with
// parameters. Appends nothing when index_ctx is nullptr (a regular key).
extern void AppendCustomIndexType(const THD *thd, const IndexContext *index_ctx,
                                  String *out);

// Compute hash for a custom type value. Returns nullopt if no custom hash is
// registered for this type (binary hash is safe in that case).
extern std::optional<size_t> TryComputeHash(const TypeContext &tc,
                                            const uchar *data, size_t len);

template <typename T>
inline std::optional<size_t> TryComputeHash(const T &obj, const uchar *data,
                                            size_t len) {
  if (!obj.has_type_context()) return std::nullopt;
  return TryComputeHash(*obj.get_type_context(), data, len);
}

// Inject custom type context into a string Item and encode it for comparison
// This is used to "poison" string literals with custom type information when
// they are compared against custom type columns (e.g., WHERE col = '(1,2)')
// Only affects STRING_ITEM types without existing type context
// Returns false on success, true on error (my_error already called if needed)
extern bool InjectAndEncodeCustomType(Item *item, const TypeContext &tc);

// Template version that works with both Item and Field types
// Both Item and Field have has_type_context() and get_type_context() methods
// Overload for TypeContext with binary data
inline int CompareCustomType(const TypeContext &tc, const uchar *data1,
                             size_t len1, const uchar *data2, size_t len2) {
  return tc.compare_op().invoke(data1, len1, data2, len2);
}

template <typename T>
std::optional<int> TryCompareCustomType(const T *obj, const uchar *data1,
                                        size_t len1, const uchar *data2,
                                        size_t len2) {
  if (obj == nullptr || !obj->has_type_context()) {
    return std::nullopt;
  }

  return CompareCustomType(*obj->get_type_context(), data1, len1, data2, len2);
}

// Template overload for comparing two String objects with custom type context
template <typename T>
std::optional<int> TryCompareCustomType(const T *obj, const String &str1,
                                        const String &str2) {
  if (obj == nullptr || !obj->has_type_context()) {
    return std::nullopt;
  }

  return CompareCustomType(
      *obj->get_type_context(), pointer_cast<const uchar *>(str1.ptr()),
      str1.length(), pointer_cast<const uchar *>(str2.ptr()), str2.length());
}

// Overload for comparing two String objects with TypeContext directly
inline std::optional<int> TryCompareCustomType(const TypeContext *tc,
                                               const String &str1,
                                               const String &str2) {
  if (tc == nullptr) {
    return std::nullopt;
  }

  return CompareCustomType(
      *tc, pointer_cast<const uchar *>(str1.ptr()), str1.length(),
      pointer_cast<const uchar *>(str2.ptr()), str2.length());
}

// Compare two binary buffers using custom comparison if item has custom type,
// otherwise falls back to memcmp. Handles sort direction (reverse) internally
// because direct mem comparison lays out the memory taking reverse order into
// account.
// Returns comparison result (<0, 0, >0) suitable for sorting
int CustomMemCompare(const Item *item, const uchar *data1, size_t len1,
                     const uchar *data2, size_t len2, size_t min_len,
                     bool reverse);

// Validate custom type compatibility for UNION operations.
// Checks if the accumulator (Item_aggregate_type) and incoming item have
// compatible custom types. Propagates type context to the accumulator as
// needed. Allows NULL items to be compatible with custom types.
// If neither item has a custom type, returns false immediately (no validation
// needed). Returns false on success, true on error (and reports the error).
bool MaybeValidateUnionTypeCompatibility(Item *accumulator, Item *item);

// Validate and cast custom type comparisons for comparison operators (=, <, >,
// etc). If neither item has a custom type, returns false immediately (no
// validation needed). Otherwise:
// 1. Both have custom types -> check compatibility, error if incompatible
// 2. One has custom type, other can be implicitly cast -> cast to custom type
// 3. One has custom type, other cannot be cast -> error
// Whether an item can be implicitly cast is determined by
// CanImplicitlyCastToCustom. May modify items (inject type context and encode
// string literals). Returns false on success, true on error (and reports the
// error).
bool MaybeValidateAndCastCustomTypeComparison(Item &left, Item &right,
                                              const char *operation_name);

// Get custom type context if Items have compatible custom types
// Returns TypeContext* if one or both Items have compatible custom types
// Returns nullptr if no custom types or if custom types are incompatible
// For Arg_comparator::set_compare_func - sets up custom comparison if possible
const TypeContext *GetCompatibleCustomType(const Item &item1,
                                           const Item &item2);

// Check if an Item value can be stored in a custom type field
// The field param must already be a custom type (i.e. field->has_type_context()
// returns true)
// Used by Item::save_in_field() to validate type conversions for
// INSERT/UPDATE
// Returns true if storage is allowed, false if it should be blocked
// It only allows:
// 1. Items with compatible custom type context (same type)
// 2. String literals that can be encoded by the custom type
extern bool CanStoreInCustomField(const Item *item, const Field *field);

// Validates storing an Item into a custom type Field and generates appropriate
// error message if validation fails.
// Returns false on success (validation passes), true on error (validation
// fails, error already reported).
extern bool ValidateAndReportCustomFieldStore(const Item *item,
                                              const Field *field);

// Copy custom type field data directly between fields with the same type.
// REQUIRES: from must have a custom type (has_type_context() == true).
// Returns false if copy was performed successfully or an error was generated,
// true if the types are incompatible and caller should fall back to normal
// copy logic. Custom types stored as binary data in VARCHAR fields need special
// handling to avoid charset conversion which would corrupt the data.
// If to does not have a custom type, generates an error with readable format.
extern bool TryCopyCustomTypeField(const Field *from, Field *to);

// Function pointer type for field copy operations (matches
// Copy_field::Copy_func).
using FieldCopyFunc = void(Copy_field *, const Field *, Field *);

// Returns the appropriate copy function for copying from a custom type field.
// from must have a custom type context.
extern FieldCopyFunc *GetCopyFunc(const Field *from, const Field *to);

// Encode a string field value and store it in a custom type field.
// This enables CTEs and subqueries with string values to work with custom
// types:
//   INSERT INTO t1 WITH cte AS (SELECT '(1,2)' AS val) SELECT * FROM cte
// REQUIRES: from_field has no custom type context, to_field has custom type
// context, and the source produces a string result.
extern type_conversion_status TryEncodeStringFieldToCustom(Field *from_field,
                                                           Field *to_field);

// Encode and store a string value into a custom type field.
// Returns appropriate type_conversion_status for the result.
extern type_conversion_status EncodeAndStoreStringToCustomField(
    const String &str_value, Field *field);

// Encode a string value for a custom type item (e.g. Item_param), reporting
// errors appropriately. The TypeContext and item name are taken from the item
// itself. Returns encoded String* on success, nullptr on error (with error
// reported). Sets null_value to true on error.
extern String *EncodeStringForCustomParam(Item *item, const String &str_value,
                                          bool &null_value);

// Store intrinsic default value for a NOT NULL custom field receiving NULL via
// INSERT/UPDATE IGNORE or LOAD DATA IGNORE.
// Returns TYPE_OK on success. On failure (no intrinsic default registered),
// sets a user-visible error and returns TYPE_ERR_BAD_VALUE.
extern type_conversion_status StoreCustomFieldIntrinsicDefault(Field *field);

// Encode and store a string value into a custom type field during LOAD DATA /
// LOAD XML, applying strict/non-strict/IGNORE semantics. Returns true on error.
extern bool LoadEncodeAndStoreCustomField(THD *thd, Field *field,
                                          const String &input_str);

// Check if a function is allowed to operate on custom types for the given type
// context
// Returns true if function is allowed, false if it should be blocked
extern bool IsFuncAllowedWithCustom(THD *thd, Item_func *func,
                                    const TypeContext &tc);

// Returns true if an Item can be implicitly cast to a custom type field, and
// false otherwise. We only allow implicit casts for Items that fall into an
// allowlisted category.
extern bool CanImplicitlyCastToCustom(const Item *item);

// Injects the TypeContext tc into the item if it can be implicitly cast to the
// type represented by tc. If item is already custom, then it is checked for
// compatibility with tc.
// Returns true if there is an error (e.g. incompatible types) as a consequence,
// or false on success (including the implicit cast is skipped).
extern bool TryImplicitCastToCustom(Item *item, const TypeContext &tc);

// Check custom type usage for a single Item
// Validates that incompatible custom types aren't used together
// Returns false on success, true on error (with my_error already called)
extern bool CheckCustomTypeUsage(Item *item, THD *thd);

// Validate VDF arguments against function signature and convert string
// constants to custom types where appropriate. Applies type disambiguation
// rules to the arguments (args of the same custom type share params) to resolve
// unknown type parameters. If out_return_params is non-null and the return type
// is a parameterized custom type, writes the resolved TypeParameters for the
// return type (inferred from args via the rule that the return parameters are
// the same as those matching arguements of the same abstract type). Returns
// false on success, true on error.
extern bool ValidateAndConvertVDFArguments(THD *thd, const char *func_name,
                                           std::string_view extension_name,
                                           uint arg_count, Item **args,
                                           const vef_signature_t *signature,
                                           TypeParameters *out_return_params);

// Set the return type_context on a VDF result Item if it returns a custom type.
// If return_params is non-null, uses those params instead of empty ones.
extern void SetVDFReturnTypeContext(THD *thd, std::string_view extension_name,
                                    const vef_signature_t *signature,
                                    Item *result_item,
                                    const TypeParameters *return_params);

// Acquire a client-managed reference to a TypeContext.
// Returns a shared_ptr that the caller is responsible for releasing.
// If source_tc is null, returns an empty shared_ptr.
extern std::shared_ptr<const TypeContext> AcquireTypeContextClientManaged(
    const TypeContext *source_tc);

// Acquire a client-managed reference to an IndexContext.
// Returns a shared_ptr that the caller is responsible for releasing.
// If source_ic is null, returns an empty shared_ptr.
extern std::shared_ptr<const IndexContext> AcquireIndexContextClientManaged(
    const IndexContext *source_ic);

// Acquire a client-managed reference to an IndexProfileDescriptor.
// Returns a shared_ptr that the caller is responsible for releasing.
// If source_ipd is null, returns an empty shared_ptr.
extern std::shared_ptr<const IndexProfileDescriptor>
AcquireIndexProfileDescriptorClientManaged(
    const IndexProfileDescriptor *source_ipd);

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_UTIL_H_
