// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

#include "native_vector.h"

#include <cstring>

namespace svector::native {

// Get native representation length and alignment for a given dimension
Length length(uint32_t dimension) {
  // Clamp to maximum dimension
  assert(dimension <= MAX_VECTOR_DIMENSION);
  if (dimension > MAX_VECTOR_DIMENSION) {
    dimension = MAX_VECTOR_DIMENSION;
  }
  // Calculate size: sizeof(Data) gives us the header size,
  // then add space for the flexible array member
  size_t total_size = sizeof(Data) + dimension * sizeof(float);

  // alignof(Data) would be functionally sufficient, but use higher
  // alignment to be SIMD-friendly for vector operations
  return Length{total_size, alignof(max_align_t)};
}

// Convert float array to native representation.
// encoded_data points directly to the float array (no storage ref prefix).
bool from_encoded(const unsigned char *encoded_data, size_t encoded_len,
                  void *native_buffer, size_t native_buf_len) {
  if (!encoded_data || !native_buffer) return true;

  if (encoded_len % sizeof(float) != 0) return true;  // Invalid encoding

  uint32_t dim = static_cast<uint32_t>(encoded_len / sizeof(float));

  // Check dimension boundaries
  if (dim == 0 || dim > MAX_VECTOR_DIMENSION) return true;

  // Get required native length
  Length native_len = length(dim);

  // Verify buffer is large enough
  if (native_buf_len < native_len.length) return true;

  // Check buffer alignment (must match alignment from length())
  if (!is_aligned(native_buffer, native_len.alignment)) return true;

  // Cast buffer to native type and populate
  Data *native = static_cast<Data *>(native_buffer);
  native->dim = dim;

  for (uint32_t i = 0; i < dim; i++) {
    native->data[i] = float4get(encoded_data + i * sizeof(float));
  }
  return false;
}

// Convert native representation to float array.
// encoded_buffer receives the float array only (no storage ref prefix).
bool to_encoded(const void *native_data, unsigned char *encoded_buffer,
                size_t encoded_buf_len, size_t *encoded_len) {
  if (!native_data || !encoded_buffer || !encoded_len) return true;

  const Data *native = static_cast<const Data *>(native_data);
  uint32_t dim = native->dim;

  // Check dimension boundaries
  if (dim == 0 || dim > MAX_VECTOR_DIMENSION) return true;

  size_t required_size = dim * sizeof(float);
  if (encoded_buf_len < required_size) return true;

  for (uint32_t i = 0; i < dim; i++) {
    float4store(encoded_buffer + i * sizeof(float), native->data[i]);
  }
  *encoded_len = required_size;
  return false;
}

}  // namespace svector::native
