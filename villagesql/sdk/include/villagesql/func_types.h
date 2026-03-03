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

#ifndef VILLAGESQL_SDK_FUNC_TYPES_H
#define VILLAGESQL_SDK_FUNC_TYPES_H

// Typed wrappers for VDF function parameters and results.
//
// These types let extension authors write VDF functions in idiomatic C++
// without accessing the raw ABI structs (vef_invalue_t, vef_vdf_result_t)
// directly. The func_builder framework automatically converts to/from the
// underlying ABI types based on the function's declared parameter types.
//
// Input args: IntArg, RealArg, StringArg, BinaryArg
//   Each provides is_null() and value() appropriate for its SQL type.
//
// Result types: IntResult, RealResult, StringResult, BinaryResult
//   IntResult / RealResult: call set(value), set_null(), or error(msg).
//   StringResult / BinaryResult: write into buffer(), then call set_length().
//
// Example - integer add:
//   void add(vef_context_t*, IntArg a, IntArg b, IntResult out) {
//     if (a.is_null() || b.is_null()) { out.set_null(); return; }
//     out.set(a.value() + b.value());
//   }
//
// Example - binary transform (ROT13 on a fixed-size BYTEARRAY):
//   void rot13(vef_context_t*, BinaryArg in, BinaryResult out) {
//     if (in.is_null()) { out.set_null(); return; }
//     auto src = in.value();          // villagesql::Span<const unsigned char>
//     auto dst = out.buffer();        // villagesql::Span<unsigned char>
//     for (size_t i = 0; i < src.size(); i++) {
//       dst[i] = transform(src[i]);
//     }
//     out.set_length(src.size());
//   }

#include <cstddef>
#include <cstring>
#include <string_view>

#include <villagesql/abi/types.h>

// In C++20, Span<T> is std::span<T>. In C++17, it is a minimal compatible
// implementation. User code written against villagesql::Span<T> works in
// either standard.
#if __cplusplus >= 202002L
#include <span>
#endif

namespace villagesql {

// =============================================================================
// Span<T>
// =============================================================================

#if __cplusplus >= 202002L

template <typename T>
using Span = std::span<T>;

#else

// Non-owning view over a contiguous sequence of T, compatible with C++17.
// Mirrors the std::span interface so code written against villagesql::Span<T>
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

class BinaryArg {
 public:
  explicit BinaryArg(const vef_invalue_t *v) : v_(v) {}
  bool is_null() const { return v_->is_null; }
  Span<const unsigned char> value() const {
    return {v_->bin_value, v_->bin_len};
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
  void set_null() { r_->type = VEF_RESULT_NULL; }
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

// Write into buffer(), then call set_length() with the number of bytes written.
// buffer().size() is the maximum usable capacity.
class BinaryResult {
 public:
  explicit BinaryResult(vef_vdf_result_t *r) : r_(r) {}

  Span<unsigned char> buffer() { return {r_->bin_buf, r_->max_bin_len}; }
  void set_length(size_t len) {
    r_->actual_len = len;
    r_->type = VEF_RESULT_VALUE;
  }
  void set_null() { r_->type = VEF_RESULT_NULL; }
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

}  // namespace villagesql

#endif  // VILLAGESQL_SDK_FUNC_TYPES_H
