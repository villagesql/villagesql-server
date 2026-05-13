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

#ifndef VILLAGESQL_PREVIEW_SYS_VAR_H
#define VILLAGESQL_PREVIEW_SYS_VAR_H

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>

#include <villagesql/abi/preview/sys_var.h>
#include <villagesql/abi/types.h>
#include <villagesql/vsql/func_types.h>

namespace vsql::preview_sys_var {

// Type tag for SysVarDescriptor — matches vef_var_type_t values.
enum Type {
  BOOL = VEF_VAR_BOOL,
  INT = VEF_VAR_INT,
  DOUBLE = VEF_VAR_DOUBLE,
  STR = VEF_VAR_STR,
};

// Typed wrapper around vef_sys_var_change_t passed to on_change callbacks.
// Use var_name() to identify which variable changed when one callback handles
// multiple variables.
class SysVarChange {
 public:
  explicit SysVarChange(const vef_sys_var_change_t *c)
      : c_(c), v_(make_invalue(c)) {}

  std::string_view var_name() const { return c_->var_name; }

  bool is_bool() const { return c_->type == VEF_VAR_BOOL; }
  bool is_int() const { return c_->type == VEF_VAR_INT; }
  bool is_real() const { return c_->type == VEF_VAR_DOUBLE; }
  bool is_str() const { return c_->type == VEF_VAR_STR; }

  IntArg as_int() const { return IntArg(&v_); }
  RealArg as_real() const { return RealArg(&v_); }
  StringArg as_str() const { return StringArg(&v_); }

 private:
  static vef_invalue_t make_invalue(const vef_sys_var_change_t *c) {
    vef_invalue_t v{};
    switch (c->type) {
      case VEF_VAR_BOOL:
        v.int_value = static_cast<long long>(c->bool_val);
        break;
      case VEF_VAR_INT:
        v.int_value = c->int_val;
        break;
      case VEF_VAR_DOUBLE:
        v.real_value = c->dbl_val;
        break;
      case VEF_VAR_STR:
        v.str_value = c->str_val;
        v.str_len = c->str_val ? strlen(c->str_val) : 0;
        break;
    }
    return v;
  }

  const vef_sys_var_change_t *c_;
  vef_invalue_t v_;
};

// Aggregate descriptor for a single system variable. Use brace-init:
//
//   {sv::BOOL,   "enabled",      "Enable feature",   &g_enabled,   true}
//   {sv::INT,    "threshold_ms", "Threshold in ms",  &g_threshold, 1000, 0,
//   3600000} {sv::DOUBLE, "ratio",        "Ratio", &g_ratio,     1.0,
//   0.0, 10.0} {sv::STR,    "log_file",     "Log file path",    &g_log_file,
//   "/tmp/myext.log"}
//
// To register an on_change callback, use .on_change<&fn>() on the descriptor:
//
//   {sv::INT, "threshold_ms", "Threshold", &g_threshold, 1000, 0, 3600000}
//       .on_change<&my_on_change>()
struct SysVarDescriptor {
  Type type;
  const char *name;
  const char *comment;
  union {
    struct {
      bool *value_ptr;
      bool def_val;
    } boolean;
    struct {
      long long *value_ptr;
      long long def_val;
      long long min_val;
      long long max_val;
    } integer;
    struct {
      double *value_ptr;
      double def_val;
      double min_val;
      double max_val;
    } dbl;
    struct {
      char **value_ptr;
      const char *def_val;
    } str;
  };
  vef_sys_var_on_change_func_t on_change_fn = nullptr;

  template <void (*Fn)(SysVarChange)>
  constexpr SysVarDescriptor on_change() const {
    SysVarDescriptor copy = *this;
    copy.on_change_fn = [](const vef_sys_var_change_t *c) {
      Fn(SysVarChange(c));
    };
    return copy;
  }
};

// SysVarCapability<N> holds N system variable descriptors and the pointer
// array the server reads during on_populate. Use make_capability() to
// construct one without specifying N explicitly.
//
// Usage:
//
//   static long long g_threshold = 1000;
//   static bool g_enabled = true;
//
//   namespace sv = vsql::preview_sys_var;
//
//   static auto g_sys_vars = sv::make_capability({
//       {sv::BOOL, "enabled",      "Enable feature",  &g_enabled,   true},
//       {sv::INT,  "threshold_ms", "Threshold in ms", &g_threshold, 1000, 0,
//       3600000}});
//
//   VEF_GENERATE_ENTRY_POINTS(
//       make_extension()
//           .func(...)
//           .with(g_sys_vars));
//
// The server populates `abi` during registration and reads `descriptor_list`
// in the on_populate callback to register the variables with MySQL.
template <size_t N>
class SysVarCapability {
 public:
  explicit SysVarCapability(SysVarDescriptor const (&descs)[N]) {
    for (size_t i = 0; i < N; ++i) {
      descs_[i].name = descs[i].name;
      descs_[i].comment = descs[i].comment;
      descs_[i].type = static_cast<vef_var_type_t>(descs[i].type);
      descs_[i].on_change = descs[i].on_change_fn;
      switch (descs[i].type) {
        case BOOL:
          descs_[i].boolean.value_ptr = descs[i].boolean.value_ptr;
          descs_[i].boolean.def_val = descs[i].boolean.def_val;
          break;
        case INT:
          descs_[i].integer.value_ptr = descs[i].integer.value_ptr;
          descs_[i].integer.def_val = descs[i].integer.def_val;
          descs_[i].integer.min_val = descs[i].integer.min_val;
          descs_[i].integer.max_val = descs[i].integer.max_val;
          break;
        case DOUBLE:
          descs_[i].dbl.value_ptr = descs[i].dbl.value_ptr;
          descs_[i].dbl.def_val = descs[i].dbl.def_val;
          descs_[i].dbl.min_val = descs[i].dbl.min_val;
          descs_[i].dbl.max_val = descs[i].dbl.max_val;
          break;
        case STR:
          descs_[i].str.value_ptr = descs[i].str.value_ptr;
          descs_[i].str.def_val = descs[i].str.def_val;
          break;
      }
      ptrs_[i] = &descs_[i];
    }
    descriptor_list.vars = ptrs_.data();
    descriptor_list.var_count = static_cast<uint32_t>(N);
  }

  // Get a system variable value.
  //
  // extension_name: extension name as registered (e.g. "vsql_my_ext")
  // var_name:       variable name without extension prefix (e.g. "threshold")
  // out:            on success, set to the current value as a string
  //
  // Returns false on success, true on error.
  bool get(std::string_view extension_name, std::string_view var_name,
           std::string &out) const {
    if (abi == nullptr || abi->get == nullptr) return true;
    void *val = nullptr;
    size_t val_len = 0;
    if (abi->get(extension_name.data(), var_name.data(), &val, &val_len))
      return true;
    out.assign(static_cast<const char *>(val), val_len);
    free(val);
    return false;
  }

  // Set an INT system variable.
  //
  // extension_name: extension name as registered (e.g. "vsql_my_ext")
  // var_name:       variable name without extension prefix (e.g. "threshold")
  // scope:          nullptr        → update running value only (not persisted)
  //                 "PERSIST"      → update running value AND persist
  //                 "PERSIST_ONLY" → persist only; running value unchanged
  //
  // Returns false on success, true on error.
  bool set(std::string_view extension_name, std::string_view var_name,
           const char *scope, long long value) const {
    if (abi == nullptr || abi->set == nullptr) return true;
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", value);
    return abi->set(extension_name.data(), var_name.data(), scope, buf);
  }

  // Set a STRING system variable.
  bool set(std::string_view extension_name, std::string_view var_name,
           const char *scope, std::string_view value) const {
    if (abi == nullptr || abi->set == nullptr) return true;
    // ABI requires null-terminated string; string_view may not be.
    std::string val_str(value);
    return abi->set(extension_name.data(), var_name.data(), scope,
                    val_str.c_str());
  }

  // VEF writes this during registration. Public for the registration
  // glue; do not access from extension code.
  const vef_preview_sys_var_t *abi = nullptr;

  // Read by the server's on_populate callback. Public so CapabilityTraits
  // can return its address as extension_data.
  vef_sys_var_descriptor_list_t descriptor_list{};

 private:
  std::array<vef_sys_var_desc_t, N> descs_;
  std::array<const vef_sys_var_desc_t *, N> ptrs_;
};

// Type-specific factory functions for SysVarDescriptor. Use these for
// clean brace-free construction inside make_capability():
//
//   sv::make_capability({
//       sv::make_bool  ("enabled",      "Enable feature",   &g_enabled, true),
//       sv::make_int   ("threshold_ms", "Threshold in ms",  &g_threshold, 1000,
//       0, 3600000), sv::make_double("ratio",        "Ratio",
//       &g_ratio,     1.0, 0.0, 10.0), sv::make_str   ("log_file",     "Log
//       file path",    &g_log_file,  "/tmp/myext.log"),
//   });
//
// TODO(villagesql-beta): add equivalent factories to status_var for
// consistency.
inline SysVarDescriptor make_bool(const char *name, const char *comment,
                                  bool *value_ptr, bool def_val) {
  SysVarDescriptor d;
  d.type = BOOL;
  d.name = name;
  d.comment = comment;
  d.boolean.value_ptr = value_ptr;
  d.boolean.def_val = def_val;
  return d;
}

inline SysVarDescriptor make_int(const char *name, const char *comment,
                                 long long *value_ptr, long long def_val,
                                 long long min_val, long long max_val) {
  SysVarDescriptor d;
  d.type = INT;
  d.name = name;
  d.comment = comment;
  d.integer.value_ptr = value_ptr;
  d.integer.def_val = def_val;
  d.integer.min_val = min_val;
  d.integer.max_val = max_val;
  return d;
}

inline SysVarDescriptor make_double(const char *name, const char *comment,
                                    double *value_ptr, double def_val,
                                    double min_val, double max_val) {
  SysVarDescriptor d;
  d.type = DOUBLE;
  d.name = name;
  d.comment = comment;
  d.dbl.value_ptr = value_ptr;
  d.dbl.def_val = def_val;
  d.dbl.min_val = min_val;
  d.dbl.max_val = max_val;
  return d;
}

inline SysVarDescriptor make_str(const char *name, const char *comment,
                                 char **value_ptr, const char *def_val) {
  SysVarDescriptor d;
  d.type = STR;
  d.name = name;
  d.comment = comment;
  d.str.value_ptr = value_ptr;
  d.str.def_val = def_val;
  return d;
}

// Factory: deduces N from the array size.
//
//   namespace sv = vsql::preview_sys_var;
//   auto g_sv = sv::make_capability({
//       sv::make_int("threshold_ms", "Threshold", &g_threshold, 1000, 0,
//       3600000), sv::make_str("log_file",     "Log file",  &g_log_file,
//       "/tmp/myext.log"),
//   });
template <size_t N>
auto make_capability(SysVarDescriptor const (&descs)[N]) {
  return SysVarCapability<N>(descs);
}

}  // namespace vsql::preview_sys_var

#include <villagesql/preview/sys_var_register.h>

#endif  // VILLAGESQL_PREVIEW_SYS_VAR_H
