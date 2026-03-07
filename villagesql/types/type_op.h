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

// TODO(villagesql-beta): should these ops own a reference count?
// TODO(villagesql-beta): use stateful ops so that setup including buffer
// allocation happens once per expression.

// TypeOp classes wrap a type operation (encode, decode, compare, hash) and
// dispatch to either a simple function pointer or a VDF, hiding the difference
// from callers. Each class requires exactly one non-null implementation at
// construction time (asserted).

#ifndef VILLAGESQL_TYPES_TYPE_OP_H_
#define VILLAGESQL_TYPES_TYPE_OP_H_

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

class EncodeOp {
 public:
  explicit EncodeOp(vef_encode_func_t fn) : fn_(fn) { assert(fn != nullptr); }
  explicit EncodeOp(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  vef_encode_func_t fn() const { return fn_; }
  const vef_func_desc_t *vdf() const { return vdf_; }

 private:
  vef_encode_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
};

class DecodeOp {
 public:
  explicit DecodeOp(vef_decode_func_t fn) : fn_(fn) { assert(fn != nullptr); }
  explicit DecodeOp(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  vef_decode_func_t fn() const { return fn_; }
  const vef_func_desc_t *vdf() const { return vdf_; }

 private:
  vef_decode_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
};

class CompareOp {
 public:
  explicit CompareOp(vef_compare_func_t fn) : fn_(fn) { assert(fn != nullptr); }
  explicit CompareOp(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  // Compares two binary values. Returns <0, 0, or >0 like strcmp.
  int invoke(const unsigned char *data1, size_t len1,
             const unsigned char *data2, size_t len2) const;

 private:
  vef_compare_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
};

class HashOp {
 public:
  explicit HashOp(vef_hash_func_t fn) : fn_(fn) { assert(fn != nullptr); }
  explicit HashOp(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  // Computes a hash of the binary value.
  size_t invoke(const unsigned char *data, size_t len) const;

 private:
  vef_hash_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
};

class IntToParamsOp {
 public:
  explicit IntToParamsOp(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  // Converts TYPE(N) integer to a canonical parameter string.
  // On success, writes "key1=value1,key2=value2,..." to *result.
  // Returns false on success, true on error (writes to error_msg).
  bool invoke(int64_t value, std::string *result, char *error_msg) const;

 private:
  const vef_func_desc_t *vdf_{nullptr};
};

class ResolveParamsOp {
 public:
  explicit ResolveParamsOp(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  // Validates parameters and computes storage characteristics.
  // Takes a canonical "key=value,..." string.
  // Returns false on success, true on error (writes to error_msg).
  bool invoke(const std::string &params_str, ResolvedTypeParams *result,
              char *error_msg) const;

 private:
  const vef_func_desc_t *vdf_{nullptr};
};

class IntrinsicDefaultOp {
 public:
  explicit IntrinsicDefaultOp(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  // Produces the binary encoding of the type's intrinsic default value.
  // type_params provides resolved type parameters so variable-size types can
  // compute the correct output. buffer_size is the allocated buffer size.
  // Returns false on success, true on error (writes to error_msg).
  bool invoke(const vef_type_params_t &type_params, unsigned char *buffer,
              size_t buffer_size, size_t *length, char *error_msg) const;

 private:
  const vef_func_desc_t *vdf_{nullptr};
};

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_TYPE_OP_H_
