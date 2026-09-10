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

// TypeFunction classes store the raw callable for a type operation — either a
// simple function pointer or a VDF descriptor. They live in TypeDescriptor and
// serve as templates for building bound Ops in TypeContext.
//
// These classes don't have an invoke method if they are intended to run with
// bound type parameters; instead, invocation is the responsibility of the
// corresponding Op classes (CompareOp, HashOp, etc.) which bind a TypeFunction
// with TypeParameters. IntToParamsFunction, ResolveParamsFunction, and
// IntrinsicDefaultFunction are all invoked during the construction of
// TypeContext, without TypeParameters, and thus support a direct invoke call.

#ifndef VILLAGESQL_TYPES_TYPE_FUNCTION_H_
#define VILLAGESQL_TYPES_TYPE_FUNCTION_H_

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {

// Storage characteristics resolved from type parameters.
// Defined identically in func_builder.h for extension authors.
struct ResolvedTypeParams {
  int64_t persisted_length;
  int64_t max_decode_buffer_length;
};

class EncodeFunction {
 public:
  explicit EncodeFunction(vef_encode_func_t fn) : fn_(fn) {
    assert(fn != nullptr);
  }
  explicit EncodeFunction(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  vef_encode_func_t fn() const { return fn_; }
  const vef_func_desc_t *vdf() const { return vdf_; }

 private:
  vef_encode_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
};

class DecodeFunction {
 public:
  explicit DecodeFunction(vef_decode_func_t fn) : fn_(fn) {
    assert(fn != nullptr);
  }
  explicit DecodeFunction(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  vef_decode_func_t fn() const { return fn_; }
  const vef_func_desc_t *vdf() const { return vdf_; }

 private:
  vef_decode_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
};

class CompareFunction {
 public:
  explicit CompareFunction(vef_compare_func_t fn) : fn_(fn) {
    assert(fn != nullptr);
  }
  explicit CompareFunction(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  vef_compare_func_t fn() const { return fn_; }
  const vef_func_desc_t *vdf() const { return vdf_; }

 private:
  vef_compare_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
};

class HashFunction {
 public:
  explicit HashFunction(vef_hash_func_t fn) : fn_(fn) { assert(fn != nullptr); }
  explicit HashFunction(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  vef_hash_func_t fn() const { return fn_; }
  const vef_func_desc_t *vdf() const { return vdf_; }

 private:
  vef_hash_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
};

class RealValueFunction {
 public:
  explicit RealValueFunction(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  const vef_func_desc_t *vdf() const { return vdf_; }

 private:
  const vef_func_desc_t *vdf_{nullptr};
};

class IntToParamsFunction {
 public:
  explicit IntToParamsFunction(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  // Converts TYPE(N) integer to a canonical parameter string.
  // On success, writes "key1=value1,key2=value2,..." to *result.
  // Returns false on success, true on error (writes to error_msg).
  bool invoke(int64_t value, std::string *result, char *error_msg) const;

 private:
  const vef_func_desc_t *vdf_{nullptr};
};

class ResolveParamsFunction {
 public:
  explicit ResolveParamsFunction(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  // Validates parameters and computes storage characteristics.
  // Takes a canonical "key=value,..." string.
  //
  // If the type's resolve_params uses the mutating overload, it may rewrite the
  // parameters (e.g. fill in defaults). When rewritten_params is non-null and
  // the VDF emitted a rewritten parameter string, it is written there in
  // canonical "key=value,..." form; callers should treat it as the new
  // canonical parameter string. When the VDF emits no rewrite, rewritten_params
  // is left untouched.
  //
  // Returns false on success, true on error (writes to error_msg).
  bool invoke(const std::string &params_str, ResolvedTypeParams *result,
              char *error_msg, std::string *rewritten_params = nullptr) const;

 private:
  const vef_func_desc_t *vdf_{nullptr};
};

class IntrinsicDefaultFunction {
 public:
  explicit IntrinsicDefaultFunction(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  // Returns the string representation of the type's intrinsic default value.
  // type_params provides resolved type parameters for variable-size types.
  // On success, writes the string to *result and returns false.
  // On failure, writes to error_msg and returns true.
  bool invoke(const vef_type_params_t &type_params, std::string *result,
              char *error_msg) const;

 private:
  const vef_func_desc_t *vdf_{nullptr};
};

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_TYPE_FUNCTION_H_
