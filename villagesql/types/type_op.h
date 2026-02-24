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

#include "villagesql/sdk/include/villagesql/abi/types.h"

class String;
struct MEM_ROOT;

namespace villagesql {

class EncodeOp {
 public:
  explicit EncodeOp(vef_encode_func_t fn) : fn_(fn) { assert(fn != nullptr); }
  explicit EncodeOp(const vef_func_desc_t *vdf) : vdf_(vdf) {
    assert(vdf != nullptr);
  }

  // Encodes 'from' into binary representation.
  // buffer_hint provides the initial allocation size (from persisted_length()).
  // Returns nullptr on OOM (my_error already called) or on encoding error.
  // Sets is_valid=false on encoding error, true otherwise.
  String *invoke(const String &from, int64_t buffer_hint, MEM_ROOT &mem_root,
                 bool &is_valid) const;

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

  // Decodes binary data into string representation in output_buffer.
  // buffer_hint provides the initial allocation size (from
  // max_decode_buffer_length()). Returns false on success, true on error. Sets
  // is_valid=false on invalid data; on OOM sets is_valid=true and calls
  // my_error.
  bool invoke(const unsigned char *data, size_t len, int64_t buffer_hint,
              MEM_ROOT &mem_root, String *output_buffer, bool &is_valid) const;

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

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_TYPE_OP_H_
