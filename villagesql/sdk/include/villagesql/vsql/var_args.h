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

#ifndef VILLAGESQL_VSQL_VAR_ARGS_H
#define VILLAGESQL_VSQL_VAR_ARGS_H

// Typed wrappers for varargs VDFs.
//
// A varargs VDF is declared at registration time with .varargs() on the
// func builder and takes a vsql::VarArgs argument in place of the usual
// fixed-arity IntArg/StringArg/CustomArg parameters. The body inspects
// each argument's runtime type via AnyArg and dispatches accordingly.
//
// Example:
//
//   void concat_all(vsql::VarArgs args, vsql::StringResult out) {
//     auto dst = out.buffer();
//     size_t off = 0;
//     for (auto a : args) {
//       if (a.is_null()) { out.set_null(); return; }
//       auto bytes = a.as_custom();
//       memcpy(dst.data() + off, bytes.data(), bytes.size());
//       off += bytes.size();
//     }
//     out.set_length(off);
//   }
//
//   make_func<&concat_all>("concat_all")
//       .returns(STRING)
//       .varargs()
//       .prerun<&concat_all_prerun>()
//       .build();
//
// The prerun is responsible for validating argument count and types
// (the framework cannot validate either for varargs).
//
// Varargs registration requires VEF_PROTOCOL_2 — older servers do not
// recognise the VEF_PARAM_VARARGS sentinel.

#include <cstddef>
#include <string_view>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/func_types.h>
#include <villagesql/vsql/type_params.h>

namespace vsql {

// AnyArg: view over one vef_invalue_t whose type is not known at compile
// time. Used inside a varargs VDF body where each argument may be a
// different SQL type. Always check type() (or is_int/is_real/is_str/
// is_custom) before reading the typed value.
class AnyArg {
 public:
  explicit AnyArg(const vef_invalue_t *v) : v_(v) {}

  bool is_null() const { return v_->is_null; }

  vef_type_id type() const { return v_->type; }

  bool is_int() const { return v_->type == VEF_TYPE_INT; }
  bool is_real() const { return v_->type == VEF_TYPE_REAL; }
  bool is_str() const { return v_->type == VEF_TYPE_STRING; }
  bool is_custom() const { return v_->type == VEF_TYPE_CUSTOM; }

  // Precondition: is_int().
  long long as_int() const { return v_->int_value; }
  // Precondition: is_real().
  double as_real() const { return v_->real_value; }
  // Precondition: is_str().
  std::string_view as_str() const { return {v_->str_value, v_->str_len}; }
  // Precondition: is_custom().
  Span<const unsigned char> as_custom() const {
    return {v_->bin_value, v_->bin_len};
  }

  // Precondition: is_custom() and the type was registered with
  // .params<P, &parse_fn>(). Returns the parsed type parameters from the
  // per-process cache.
  template <typename P>
  const P &custom_params() const {
    return type_params_cache_for<P>().get(v_->type_params);
  }

 private:
  const vef_invalue_t *v_;
};

// VarArgs: view over vef_vdf_args_t for varargs VDFs. Provides size,
// indexed access, and range-for iteration. The values array is always
// the protocol-2 pointer layout (args->values) because varargs registration
// requires VEF_PROTOCOL_2.
class VarArgs {
 public:
  explicit VarArgs(vef_vdf_args_t *a) : a_(a) {}

  size_t size() const { return a_->value_count; }

  AnyArg operator[](size_t i) const { return AnyArg(a_->values[i]); }

  // Read user_data stashed by prerun. Returns nullptr if prerun did not
  // set user_data (or no prerun is registered).
  template <typename T>
  T *state() const {
    return static_cast<T *>(a_->user_data);
  }

  class Iter {
   public:
    Iter(const VarArgs *v, size_t i) : v_(v), i_(i) {}
    AnyArg operator*() const { return (*v_)[i_]; }
    Iter &operator++() {
      ++i_;
      return *this;
    }
    bool operator!=(const Iter &o) const { return i_ != o.i_; }

   private:
    const VarArgs *v_;
    size_t i_;
  };

  Iter begin() const { return Iter(this, 0); }
  Iter end() const { return Iter(this, a_->value_count); }

 private:
  vef_vdf_args_t *a_;
};

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_VAR_ARGS_H
