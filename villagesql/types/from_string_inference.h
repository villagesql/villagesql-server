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

#ifndef VILLAGESQL_TYPES_FROM_STRING_INFERENCE_H
#define VILLAGESQL_TYPES_FROM_STRING_INFERENCE_H

#include <cstdint>
#include <string>
#include <string_view>

#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

class THD;

namespace villagesql {

// Snapshot of the TypeDescriptor fields the inference path needs. Copied out
// of the victionary under a read lock so callers don't retain a pointer into
// the registry. Populated by LookupTypeForInference().
struct TypeInferenceSnapshot {
  bool is_parameterized;
  int64_t max_persisted_length;
};

// Reads the (extension, type) entry from the victionary under a read lock,
// copies the inference-relevant fields into *out, releases the lock, and
// returns false. Returns true if the registry is not initialized or the
// type is not registered (and *out is untouched).
bool LookupTypeForInference(std::string_view extension_name,
                            std::string_view type_name,
                            TypeInferenceSnapshot *out);

// Pre-executes a parameterized type's from_string VDF on a constant string
// literal at fix_fields time, and returns the inferred TypeParameters along
// with the encoded bytes the VDF produced.
//
// The extension's from_string is called with MaybeParams<P> in the unknown
// state. The SDK wrapper writes the inferred canonical "k=v,k=v" params
// string back through the vef_inferred_type_params_t channel on
// vef_vdf_result_t.
//
// Returns false on success; out_inferred holds the inferred TypeParameters
// and out_encoded_bytes holds the encoded binary value (length-trimmed).
//
// Returns true if inference is not possible (e.g., max_persisted_length <= 0,
// the type didn't register params_to_strings, the extension didn't infer, or
// the input was NULL). Caller should fall through to its existing ambiguity
// error path.
//
// Returns true and calls my_error() if the extension's from_string signaled
// a warning or error (malformed literal, etc.) — that's a fix_fields-time
// failure for the constant-string inference case.
bool InferFromStringConstant(THD *thd, int64_t max_persisted_length,
                             const vef_func_desc_t *encode_vdf,
                             std::string_view input_string,
                             TypeParameters *out_inferred,
                             std::string *out_encoded_bytes);

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_FROM_STRING_INFERENCE_H
