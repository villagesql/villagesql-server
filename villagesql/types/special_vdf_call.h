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

// SpecialVdfCall<ResultTag, ArgTags...> wraps a VDF function descriptor for
// repeated calls where the argument and result types are known at compile time.
// The constructor sets up the call context and argument arrays. init() must be
// called once after construction to initialize the type/is_null fields of each
// input slot. invoke() updates only the per-call data fields and dispatches to
// the VDF.
//
// Non-overlapping: invoke() is non-const and modifies shared input state.
// Multiple concurrent invoke() calls on the same object are not safe.
//
// Example (IntResult, two args):
//   SpecialVdfCall<IntResult, CustomArg, CustomArg> call(vdf_);
//   call.init();
//   auto r = call.invoke(BinarySlice{data1, len1}, BinarySlice{data2, len2});
//   if (!r) LogVSQL(ERROR_LEVEL, "%s: %s", call.name(), call.error_msg());
//
// Example (CustomResult with type parameters):
//   SpecialVdfCall<CustomResult> call(vdf_);
//   call.init(TypeParameterSlice{count, keys, values});
//   auto r = call.invoke(out_buf, max_len);
//   if (!r) snprintf(error_msg, VEF_MAX_ERROR_LEN, "%s", call.error_msg());

#ifndef VILLAGESQL_TYPES_SPECIAL_VDF_CALL_H_
#define VILLAGESQL_TYPES_SPECIAL_VDF_CALL_H_

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <type_traits>

#include "sql_string.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {

// Value types for SpecialVdfCall arguments that require both a pointer and a
// length. Each accepts either a MySQL String (implicit conversion) or an
// explicit raw pointer + length.
struct StringSlice {
  const char *data;
  size_t len;
  StringSlice(const char *d, size_t l) : data(d), len(l) {}
  StringSlice(const String &s) : data(s.ptr()), len(s.length()) {}
};

struct BinarySlice {
  const unsigned char *data;
  size_t len;
  BinarySlice(const unsigned char *d, size_t l) : data(d), len(l) {}
  BinarySlice(const String &s)
      : data(reinterpret_cast<const unsigned char *>(s.ptr())),
        len(s.length()) {}
};

struct TypeParameterSlice {
  unsigned int count;
  const char *const *keys;
  const char *const *values;
  TypeParameterSlice() : count(0), keys(nullptr), values(nullptr) {}
  TypeParameterSlice(unsigned int c, const char *const *k, const char *const *v)
      : count(c), keys(k), values(v) {}
};

// Placeholder for arg tags that need no init-time data.
struct NoInitData {};

// Arg type tags for SpecialVdfCall. Each tag defines:
//   cpp_t  — the C++ type passed to invoke() for this argument
//   init_t — the type passed to init() for one-time setup of this argument
//   init() — called once by SpecialVdfCall::init() to set type/is_null
//   fill() — called per invoke() to update the per-call data fields
struct IntArg {
  using cpp_t = int64_t;
  using init_t = NoInitData;
  template <typename T>
  static void init(T &v, NoInitData) {
    v.type = VEF_TYPE_INT;
    v.is_null = false;
  }
  template <typename T>
  static void fill(T &v, int64_t val) {
    v.int_value = val;
  }
};

struct StringArg {
  using cpp_t = StringSlice;
  using init_t = NoInitData;
  template <typename T>
  static void init(T &v, NoInitData) {
    v.type = VEF_TYPE_STRING;
    v.is_null = false;
  }
  template <typename T>
  static void fill(T &v, StringSlice s) {
    v.str_len = s.len;
    v.str_value = s.data;
  }
};

struct CustomArg {
  using cpp_t = BinarySlice;
  using init_t = TypeParameterSlice;
  template <typename T>
  static void init(T &v, TypeParameterSlice tp) {
    v.type = VEF_TYPE_CUSTOM;
    v.is_null = false;
    if constexpr (std::is_same_v<T, vef_invalue_t>) {
      v.type_params.count = tp.count;
      v.type_params.keys = tp.keys;
      v.type_params.values = tp.values;
    }
  }
  template <typename T>
  static void fill(T &v, BinarySlice b) {
    v.bin_len = b.len;
    v.bin_value = b.data;
  }
};

// Result type tags for SpecialVdfCall. Each tag defines:
//   init_t — the type passed to init() for one-time setup of the result
// The tag also determines which invoke() overload is available:
//   IntResult    → invoke(args...)                    → std::optional<int64_t>
//   StringResult → invoke(args..., char *, size_t)   → std::optional<size_t>
//   CustomResult → invoke(args..., uchar *, size_t)  → std::optional<size_t>
struct IntResult {
  using init_t = NoInitData;
};
struct StringResult {
  using init_t = TypeParameterSlice;
};
struct CustomResult {
  using init_t = TypeParameterSlice;
};

template <typename ResultTag, typename... ArgTags>
class SpecialVdfCall {
 public:
  static constexpr size_t kN = sizeof...(ArgTags);

  explicit SpecialVdfCall(const vef_func_desc_t *fd) : fd_(fd) {
    assert(fd != nullptr);
    assert(fd->prerun == nullptr && fd->postrun == nullptr);
    ctx_.protocol = fd->protocol;
    vdf_args_.user_data = nullptr;
    vdf_args_.value_count = static_cast<unsigned int>(kN);
    if (fd->protocol >= VEF_PROTOCOL_3) {
      for (size_t i = 0; i < kN; i++) input_ptrs_[i] = &inputs_[i];
      vdf_args_.values = input_ptrs_.data();
    } else {
      vdf_args_.values_v1 = inputs_v1_.data();
    }
  }

  // Initialize the result and input slots. Must be called once after
  // construction and before the first invoke().
  // The no-arg overload default-constructs each tag's init_t (e.g. empty
  // TypeParameterSlice for CustomArg/CustomResult). The explicit overload
  // accepts a result init_t followed by one init_t value per arg tag.
  void init() {
    result_init_ = typename ResultTag::init_t{};
    init_arg_slots(typename ArgTags::init_t{}...);
  }

  void init(typename ResultTag::init_t result_init,
            typename ArgTags::init_t... init_args) {
    result_init_ = result_init;
    init_arg_slots(init_args...);
  }

  // Not copyable or movable: vdf_args_ points into
  // inputs_/inputs_v1_/input_ptrs_.
  SpecialVdfCall(const SpecialVdfCall &) = delete;
  SpecialVdfCall &operator=(const SpecialVdfCall &) = delete;
  SpecialVdfCall(SpecialVdfCall &&) = delete;
  SpecialVdfCall &operator=(SpecialVdfCall &&) = delete;

  const char *name() const { return fd_->name; }
  // Error message from the last invoke() call.
  const char *error_msg() const { return error_msg_; }
  // Alternate output buffer set by the VDF in the last StringResult invoke(),
  // or nullptr if the VDF used the caller-provided buffer.
  const char *alt_str_buf() const { return alt_str_buf_; }
  // Alternate output buffer set by the VDF in the last CustomResult invoke(),
  // or nullptr if the VDF used the caller-provided buffer.
  const unsigned char *alt_bin_buf() const { return alt_bin_buf_; }

  // For IntResult: fills inputs, calls the VDF, returns the integer result.
  // Returns nullopt on error; error message available via error_msg().
  template <typename RT = ResultTag,
            std::enable_if_t<std::is_same_v<RT, IntResult>, int> = 0>
  std::optional<int64_t> invoke(typename ArgTags::cpp_t... args) {
    fill_inputs(args...);
    vef_vdf_result_t result = {};
    result.error_msg = error_msg_;
    result.type = VEF_RESULT_VALUE;
    fd_->vdf(&ctx_, &vdf_args_, &result);
    if (result.type != VEF_RESULT_VALUE) {
      if (error_msg_[0] == '\0')
        snprintf(error_msg_, VEF_MAX_ERROR_LEN,
                 "VDF returned unexpected result type");
      return std::nullopt;
    }
    return result.int_value;
  }

  // For StringResult: fills inputs, calls the VDF writing into out_buf.
  // Returns the actual output length on success, nullopt on error.
  // Error message available via error_msg(). If the VDF returned an alternate
  // buffer, alt_str_buf() is non-null and holds the output instead.
  template <typename RT = ResultTag,
            std::enable_if_t<std::is_same_v<RT, StringResult>, int> = 0>
  std::optional<size_t> invoke(typename ArgTags::cpp_t... args, char *out_buf,
                               size_t max_len) {
    fill_inputs(args...);
    alt_str_buf_ = nullptr;
    vef_vdf_result_t result = {};
    result.error_msg = error_msg_;
    result.type = VEF_RESULT_VALUE;
    result.str_buf = out_buf;
    result.max_str_len = max_len;
    result.alt_str_buf = &alt_str_buf_;
    result.type_params = {result_init_.count, result_init_.keys,
                          result_init_.values};
    fd_->vdf(&ctx_, &vdf_args_, &result);
    if (result.type != VEF_RESULT_VALUE) {
      if (error_msg_[0] == '\0')
        snprintf(error_msg_, VEF_MAX_ERROR_LEN,
                 "VDF returned unexpected result type");
      return std::nullopt;
    }
    return result.actual_len;
  }

  // For CustomResult: fills inputs, calls the VDF writing into out_buf.
  // Returns the actual output length on success, nullopt on error.
  // Error message available via error_msg(). If the VDF returned an alternate
  // buffer, alt_bin_buf() is non-null and holds the output instead.
  template <typename RT = ResultTag,
            std::enable_if_t<std::is_same_v<RT, CustomResult>, int> = 0>
  std::optional<size_t> invoke(typename ArgTags::cpp_t... args,
                               unsigned char *out_buf, size_t max_len) {
    fill_inputs(args...);
    alt_bin_buf_ = nullptr;
    vef_vdf_result_t result = {};
    result.error_msg = error_msg_;
    result.type = VEF_RESULT_VALUE;
    result.bin_buf = out_buf;
    result.max_bin_len = max_len;
    result.alt_bin_buf = &alt_bin_buf_;
    result.type_params = {result_init_.count, result_init_.keys,
                          result_init_.values};
    fd_->vdf(&ctx_, &vdf_args_, &result);
    if (result.type != VEF_RESULT_VALUE) {
      if (error_msg_[0] == '\0')
        snprintf(error_msg_, VEF_MAX_ERROR_LEN,
                 "VDF returned unexpected result type");
      return std::nullopt;
    }
    return result.actual_len;
  }

 private:
  void init_arg_slots(typename ArgTags::init_t... init_args) {
    if (ctx_.protocol >= VEF_PROTOCOL_3) {
      unsigned int i = 0;
      ((ArgTags::init(inputs_[i++], init_args)), ...);
    } else {
      unsigned int i = 0;
      ((ArgTags::init(inputs_v1_[i++], init_args)), ...);
    }
  }

  void fill_inputs(typename ArgTags::cpp_t... args) {
    if (ctx_.protocol >= VEF_PROTOCOL_3) {
      unsigned int i = 0;
      ((ArgTags::fill(inputs_[i++], args)), ...);
    } else {
      unsigned int i = 0;
      ((ArgTags::fill(inputs_v1_[i++], args)), ...);
    }
  }

  const vef_func_desc_t *fd_;
  vef_context_t ctx_{};
  std::array<vef_invalue_t, kN> inputs_{};
  std::array<vef_invalue_v1_t, kN> inputs_v1_{};
  std::array<vef_invalue_t *, kN> input_ptrs_{};
  vef_vdf_args_t vdf_args_{};
  typename ResultTag::init_t result_init_{};
  char error_msg_[VEF_MAX_ERROR_LEN]{};
  char *alt_str_buf_{nullptr};
  unsigned char *alt_bin_buf_{nullptr};
};

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_SPECIAL_VDF_CALL_H_
