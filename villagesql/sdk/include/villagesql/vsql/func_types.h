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

#ifndef VILLAGESQL_VSQL_FUNC_TYPES_H
#define VILLAGESQL_VSQL_FUNC_TYPES_H

// Typed wrappers for VDF function parameters and results.
//
// These types let extension authors write VDF functions in idiomatic C++
// without accessing the raw ABI structs (vef_invalue_t, vef_vdf_result_t)
// directly. The func_builder framework automatically converts to/from the
// underlying ABI types based on the function's declared parameter types.
//
// Input args:   IntArg, RealArg, StringArg
//               CustomArg            - custom type, binary data only
//               CustomArgWith<P>     - custom type + cached parsed params
//
// Result types: IntResult, RealResult, StringResult
//               CustomResult         - custom type, binary data only
//               CustomResultWith<P>  - custom type + cached parsed params
//
// Example - integer add:
//   void add(IntArg a, IntArg b, IntResult out) {
//     if (a.is_null() || b.is_null()) { out.set_null(); return; }
//     out.set(a.value() + b.value());
//   }
//
// Example - binary transform with no params (ROT13 on a fixed-size BYTEARRAY):
//   void rot13(CustomArg in, CustomResult out) {
//     if (in.is_null()) { out.set_null(); return; }
//     auto src = in.value();          // vsql::Span<const unsigned char>
//     auto dst = out.buffer();        // vsql::Span<unsigned char>
//     for (size_t i = 0; i < src.size(); i++) {
//       dst[i] = transform(src[i]);
//     }
//     out.set_length(src.size());
//   }
//
// Example - VDF with parameterized type (TVECTOR(N)):
//   void tvector_dot(CustomArgWith<TVectorParams> a,
//                    CustomArgWith<TVectorParams> b,
//                    RealResult out) {
//     int64_t dim = a.params().dimension;  // TVectorParams::parse called once
//     double sum = 0;
//     for (int64_t i = 0; i < dim; i++) {
//       sum += load_float(a.value().data() + i*4) *
//              load_float(b.value().data() + i*4);
//     }
//     out.set(sum);
//   }

#include <cstddef>
#include <cstring>
#include <string_view>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/type_params_cache.h>

// In C++20, Span<T> is std::span<T>. In C++17, it is a minimal compatible
// implementation. User code written against vsql::Span<T> works in
// either standard.
#if __cplusplus >= 202002L
#include <span>
#endif

namespace vsql {

// =============================================================================
// Span<T>
// =============================================================================

#if __cplusplus >= 202002L

template <typename T>
using Span = std::span<T>;

#else

// Non-owning view over a contiguous sequence of T, compatible with C++17.
// Mirrors the std::span interface so code written against vsql::Span<T>
// compiles unchanged under C++20 (where Span<T> aliases std::span<T>).
template <typename T>
class Span {
 public:
  Span() noexcept : data_(nullptr), size_(0) {}
  Span(T *data, size_t size) noexcept : data_(data), size_(size) {}

  T *data() const noexcept { return data_; }
  size_t size() const noexcept { return size_; }
  bool empty() const noexcept { return size_ == 0; }

  T &operator[](size_t i) const noexcept { return data_[i]; }

  T *begin() const noexcept { return data_; }
  T *end() const noexcept { return data_ + size_; }

 private:
  T *data_;
  size_t size_;
};

#endif  // __cplusplus >= 202002L

// =============================================================================
// Input argument wrappers
// =============================================================================

class IntArg {
 public:
  explicit IntArg(const vef_invalue_t *v) : v_(v) {}
  bool is_null() const { return v_->is_null; }
  long long value() const { return v_->int_value; }

 private:
  const vef_invalue_t *v_;
};

class RealArg {
 public:
  explicit RealArg(const vef_invalue_t *v) : v_(v) {}
  bool is_null() const { return v_->is_null; }
  double value() const { return v_->real_value; }

 private:
  const vef_invalue_t *v_;
};

class StringArg {
 public:
  explicit StringArg(const vef_invalue_t *v) : v_(v) {}
  bool is_null() const { return v_->is_null; }
  std::string_view value() const { return {v_->str_value, v_->str_len}; }

 private:
  const vef_invalue_t *v_;
};

// CustomArg: input wrapper for non-parameterized custom type arguments.
// For parameterized types (e.g., TVECTOR(N)), use CustomArgWith<P> instead.
class CustomArg {
 public:
  explicit CustomArg(const vef_invalue_t *v) : v_(v) {}
  bool is_null() const { return v_->is_null; }
  Span<const unsigned char> value() const {
    return {v_->bin_value, v_->bin_len};
  }

 private:
  const vef_invalue_t *v_;
};

// CustomArgWith<P>: input wrapper for parameterized custom type arguments.
// Use instead of CustomArg when the VDF needs to access the type parameters
// (e.g., the dimension of a TVECTOR(N) column). Requires the parse function
// to be registered via .params<P, &parse_fn>() in the type builder.
//
// The parse result is memoized per unique canonical params string, so parsing
// runs at most once per type instantiation.
//
// NOTE: Do not call params() when is_null() is true.
// TODO(villagesql-beta): remove this restriction.
template <typename P>
class CustomArgWith {
 public:
  explicit CustomArgWith(const vef_invalue_t *v) : v_(v) {}
  bool is_null() const { return v_->is_null; }
  Span<const unsigned char> value() const {
    return {v_->bin_value, v_->bin_len};
  }
  const P &params() const {
    return type_params_cache_for<P>().get(v_->type_params);
  }

 private:
  const vef_invalue_t *v_;
};

// =============================================================================
// Result wrappers
// =============================================================================

class IntResult {
 public:
  explicit IntResult(vef_vdf_result_t *r) : r_(r) {}

  void set(long long v) {
    r_->int_value = v;
    r_->type = VEF_RESULT_VALUE;
  }
  void set_null() { r_->type = VEF_RESULT_NULL; }
  void warning(std::string_view msg) {
    r_->type = VEF_RESULT_WARNING;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }
  void error(std::string_view msg) {
    r_->type = VEF_RESULT_ERROR;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }

 private:
  vef_vdf_result_t *r_;
};

class RealResult {
 public:
  explicit RealResult(vef_vdf_result_t *r) : r_(r) {}

  void set(double v) {
    r_->real_value = v;
    r_->type = VEF_RESULT_VALUE;
  }
  void set_null() { r_->type = VEF_RESULT_NULL; }
  void warning(std::string_view msg) {
    r_->type = VEF_RESULT_WARNING;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }
  void error(std::string_view msg) {
    r_->type = VEF_RESULT_ERROR;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }

 private:
  vef_vdf_result_t *r_;
};

// Write into buffer(), then call set_length() with the number of chars written.
// buffer().size() is the maximum usable capacity.
class StringResult {
 public:
  explicit StringResult(vef_vdf_result_t *r) : r_(r) {}

  Span<char> buffer() { return {r_->str_buf, r_->max_str_len}; }
  void set_length(size_t len) {
    r_->actual_len = len;
    r_->type = VEF_RESULT_VALUE;
  }
  void set(std::string_view sv) {
    size_t len = sv.size() < r_->max_str_len ? sv.size() : r_->max_str_len;
    memcpy(r_->str_buf, sv.data(), len);
    set_length(len);
  }
  void set_null() { r_->type = VEF_RESULT_NULL; }
  void warning(std::string_view msg) {
    r_->type = VEF_RESULT_WARNING;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }
  void error(std::string_view msg) {
    r_->type = VEF_RESULT_ERROR;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }

 private:
  vef_vdf_result_t *r_;
};

// CustomResult: result wrapper for non-parameterized custom type outputs.
// For parameterized types (e.g., TVECTOR(N)), use CustomResultWith<P> instead.
class CustomResult {
 public:
  explicit CustomResult(vef_vdf_result_t *r) : r_(r) {}

  Span<unsigned char> buffer() { return {r_->bin_buf, r_->max_bin_len}; }
  void set_length(size_t len) {
    r_->actual_len = len;
    r_->type = VEF_RESULT_VALUE;
  }
  void set_null() { r_->type = VEF_RESULT_NULL; }
  void warning(std::string_view msg) {
    r_->type = VEF_RESULT_WARNING;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }
  void error(std::string_view msg) {
    r_->type = VEF_RESULT_ERROR;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }

 private:
  vef_vdf_result_t *r_;
};

// CustomResultWith<P>: result wrapper for parameterized custom type outputs.
// Use instead of CustomResult when the VDF needs to access the type parameters
// (e.g., the dimension of a TVECTOR(N) column). Requires the parse function
// to be registered via .params<P, &parse_fn>() in the type builder.
//
// Use params() to read the type parameters of the output column.
template <typename P>
class CustomResultWith {
 public:
  explicit CustomResultWith(vef_vdf_result_t *r) : r_(r) {}

  Span<unsigned char> buffer() { return {r_->bin_buf, r_->max_bin_len}; }
  void set_length(size_t len) {
    r_->actual_len = len;
    r_->type = VEF_RESULT_VALUE;
  }
  void set_null() { r_->type = VEF_RESULT_NULL; }
  void warning(std::string_view msg) {
    r_->type = VEF_RESULT_WARNING;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }
  void error(std::string_view msg) {
    r_->type = VEF_RESULT_ERROR;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }
  const P &params() const {
    return type_params_cache_for<P>().get(r_->type_params);
  }

 private:
  vef_vdf_result_t *r_;
};

}  // namespace vsql

// TODO(villagesql): Remove these villagesql:: aliases once all extensions have
// migrated to the vsql:: namespace.
namespace villagesql {
template <typename T>
using Span = vsql::Span<T>;
using IntArg = vsql::IntArg;
using RealArg = vsql::RealArg;
using StringArg = vsql::StringArg;
using CustomArg = vsql::CustomArg;
template <typename P>
using CustomArgWith = vsql::CustomArgWith<P>;
using IntResult = vsql::IntResult;
using RealResult = vsql::RealResult;
using StringResult = vsql::StringResult;
using CustomResult = vsql::CustomResult;
template <typename P>
using CustomResultWith = vsql::CustomResultWith<P>;
}  // namespace villagesql

#endif  // VILLAGESQL_VSQL_FUNC_TYPES_H
