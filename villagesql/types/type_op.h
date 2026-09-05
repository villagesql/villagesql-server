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

// Op classes are bound type operations that live in TypeContext. Each Op is
// constructed from a TypeFunction (the raw callable from TypeDescriptor) plus
// TypeParameters, so callers can invoke them without any per-call setup.
//
// CompareOp and HashOp capture the function pointer or VDF descriptor plus
// pre-parsed TypeParameters at construction time. invoke() is thread-safe:
// the function pointer path is stateless, and the VDF path creates a
// stack-local SpecialVdfCall per call (the TypeParameterSlice data is shared
// read-only).
//
// EncodeOp and DecodeOp store the TypeFunction and TypeParameters for
// TypeEncoder/TypeDecoder to pull from — those classes own additional per-call
// state (buffers) that cannot be shared.

#ifndef VILLAGESQL_TYPES_TYPE_OP_H_
#define VILLAGESQL_TYPES_TYPE_OP_H_

#include <cassert>
#include <cstddef>
#include <cstdint>

#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/types/type_function.h"

namespace villagesql {

class TypeParameters;

class EncodeOp {
 public:
  EncodeOp(const EncodeFunction &func, const TypeParameters &params);

  vef_encode_func_t fn() const { return fn_; }
  const vef_func_desc_t *vdf() const { return vdf_; }
  const TypeParameters &parameters() const { return params_; }

 private:
  vef_encode_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
  const TypeParameters &params_;
};

class DecodeOp {
 public:
  DecodeOp(const DecodeFunction &func, const TypeParameters &params);

  vef_decode_func_t fn() const { return fn_; }
  const vef_func_desc_t *vdf() const { return vdf_; }
  const TypeParameters &parameters() const { return params_; }

 private:
  vef_decode_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
  const TypeParameters &params_;
};

class CompareOp {
 public:
  CompareOp(const CompareFunction &func, const TypeParameters &params);

  // Compares two binary values. Returns <0, 0, or >0 like strcmp.
  // Thread-safe: function pointer path is stateless; VDF path creates a
  // stack-local SpecialVdfCall per call.
  int invoke(const unsigned char *data1, size_t len1,
             const unsigned char *data2, size_t len2) const;

 private:
  vef_compare_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
  const TypeParameters &params_;
};

class HashOp {
 public:
  HashOp(const HashFunction &func, const TypeParameters &params);

  // Computes a hash of the binary value. Thread-safe.
  size_t invoke(const unsigned char *data, size_t len) const;

 private:
  vef_hash_func_t fn_{nullptr};
  const vef_func_desc_t *vdf_{nullptr};
  const TypeParameters &params_;
};

class RealValueOp {
 public:
  RealValueOp(const RealValueFunction &func, const TypeParameters &params);

  double invoke(const unsigned char *data, size_t len) const;

 private:
  const vef_func_desc_t *vdf_{nullptr};
  const TypeParameters &params_;
};

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_TYPE_OP_H_
