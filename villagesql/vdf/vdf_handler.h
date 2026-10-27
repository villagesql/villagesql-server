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

#ifndef VILLAGESQL_VDF_VDF_HANDLER_H
#define VILLAGESQL_VDF_VDF_HANDLER_H

#include <cstddef>

#include "my_inttypes.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

class Item;
class Item_result_field;
class String;
class THD;
struct CHARSET_INFO;
struct MEM_ROOT;
struct udf_func;

namespace villagesql {
class TypeContext;
namespace vdf {

// Handler for VDF (VillageSQL Defined Function) calling convention.
// This class encapsulates all VDF-specific state and logic, keeping the
// core udf_handler class focused on classic MySQL UDF support.
class vdf_handler {
 public:
  explicit vdf_handler(udf_func *u_d);

  // Returns true if this VDF returns a string type
  bool returns_string() const;

  // Returns true if this VDF returns a real type
  bool returns_real() const;

  // Lifecycle methods
  // fix_fields: Allocate buffers and call prerun. Returns true on error.
  bool fix_fields(THD *thd, Item_result_field *func, uint arg_count,
                  Item **arguments, String *buffers);
  // cleanup: Call postrun and reset state
  void cleanup();

  // Returns true if the return type's parameters must be inferred from
  // a prepared-statement parameters
  bool needs_return_type_params_from_ps_param() const;

  // Resolves the return type's params from a prepared-statement parameter,
  // the way fix_fields does for a string constant. Called once per EXECUTE,
  // after the PS parameters are bound and before the statement uses the return
  // type. Returns true on error.
  bool ResolveReturnTypeParamsFromPsParam(THD *thd);

  // Aggregate methods - called by udf_handler for aggregate VDFs
  void clear();
  void accumulate(bool *null_value);

  // Value accessors - called by udf_handler when is_vdf() is true
  double val_real(bool *null_value);
  longlong val_int(bool *null_value);
  String *val_str(String *str, String *save_str, const char *func_name,
                  const CHARSET_INFO *charset);

 private:
  udf_func *m_udf;
  Item **m_args{nullptr};
  String *m_buffers{nullptr};
  uint m_arg_count{0};

  vef_context_t m_context{};
  vef_vdf_args_t m_vdf_args{};
  vef_invalue_v1_t *m_invalues_v1{nullptr};
  vef_invalue_t *m_invalues{nullptr};
  vef_invalue_t **m_invalues_ptrs{nullptr};
  char *m_result_buffer{nullptr};
  size_t m_result_buffer_size{0};
  char *m_error_msg{nullptr};
  bool m_active{false};
  const villagesql::TypeContext *m_return_type_context{nullptr};

  // Set by fix_fields when the return type's params must be inferred from a
  // prepared-statement parameter, which holds no value until EXECUTE.
  bool m_infer_return_type_params_from_ps_param{false};
  int64_t m_max_persisted_length{0};
  // The function's item, recorded by fix_fields so the item's type can be
  // updated at EXECUTE.
  Item_result_field *m_func{nullptr};
  // The unresolved return type as fix_fields left it, saved while
  // ResolveReturnTypeParamsFromPsParam has replaced it with a resolved one for
  // the current execution; cleanup() puts it back. Null otherwise.
  const villagesql::TypeContext *m_saved_return_type_context{nullptr};

  // Marshal arguments into m_invalues array based on declared parameter types
  void marshal_args();

  // Helper for VDF functions that return numeric types (double or longlong)
  // Returns true on success with value in out_value, false otherwise
  template <typename T>
  bool invoke_numeric(T *out_value, bool *null_value);

  // Process result string and return appropriate String pointer
  String *result_string(const char *res, size_t res_length, String *str,
                        String *save_str, const CHARSET_INFO *charset);

  // Grow m_result_buffer to hold at least `needed` bytes, rounding up to a
  // fixed quantum so a result that grows by a few bytes row-over-row does not
  // trigger a fresh allocation each time. No-op if already large enough.
  // Allocates on `mem_root`, or on THR_MALLOC when it is null. Returns true on
  // allocation failure.
  bool MaybeResizeBuffer(size_t needed, MEM_ROOT *mem_root = nullptr);

  // Call the VDF's postrun function, if any, and clear user_data.
  void RunPostrun();
};

// Calls ResolveReturnTypeParamsFromPsParam on every VDF in `item_list`, a
// prepared statement's item list. Returns true on error.
bool MaybeResolveReturnTypeParamsFromPsParams(THD *thd, Item *item_list);

}  // namespace vdf
}  // namespace villagesql

#endif  // VILLAGESQL_VDF_VDF_HANDLER_H
