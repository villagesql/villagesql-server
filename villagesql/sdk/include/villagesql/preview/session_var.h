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

#ifndef VILLAGESQL_PREVIEW_SESSION_VAR_H
#define VILLAGESQL_PREVIEW_SESSION_VAR_H

#include <array>
#include <cstdlib>
#include <string>
#include <string_view>

#include <villagesql/abi/preview/session_var.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

// Preview capability: per-session (THD-local) system variables.
//
// Declare a SessionVarCapability with make_capability(), populate it with
// INT/STR descriptors, and pass it to .with() on the extension builder. Each
// declared variable becomes a per-session system variable: every connection
// has its own value (SET SESSION), with def_val as the global default.
//
// Read the caller's per-session value from a VDF with get_session_int /
// get_session_str — the equivalent of a plugin's THDVAR(thd, var). These
// resolve the current connection thread, so they must be called on it (a VDF
// or callback running on the connection), never from a background thread
// worker.
//
// Usage:
//
//   static auto g_session_vars = vsql::preview_session_var::make_capability({
//       vsql::preview_session_var::make_int(
//           "ef_search", "Search width", 20, 1, 4096),
//       vsql::preview_session_var::make_str(
//           "label", "Per-session label", "default"),
//   });
//
//   VEF_GENERATE_ENTRY_POINTS(make_extension().func(...).with(g_session_vars));
namespace vsql::preview_session_var {

// Type tag for SessionVarDescriptor — matches vef_session_var_type_t values.
enum Type {
  INT = VEF_SESSION_VAR_INT,
  STR = VEF_SESSION_VAR_STR,
};

// A single session-variable declaration. Prefer the make_int / make_str
// factories over brace-initializing this directly.
struct SessionVarDescriptor {
  Type type;
  const char *name;
  const char *comment;
  union {
    struct {
      long long def_val;
      long long min_val;
      long long max_val;
    } integer;
    struct {
      const char *def_val;
    } str;
  };
};

// SessionVarCapability<N> holds N descriptors and the pointer array the server
// reads during on_populate. Use make_capability() to construct one without
// specifying N explicitly.
template <size_t N>
class SessionVarCapability
    : public ::vsql::detail::CapabilityBase<SessionVarCapability<N>> {
 public:
  explicit SessionVarCapability(SessionVarDescriptor const (&descs)[N]) {
    for (size_t i = 0; i < N; ++i) {
      descs_[i].name = descs[i].name;
      descs_[i].comment = descs[i].comment;
      descs_[i].type = static_cast<vef_session_var_type_t>(descs[i].type);
      switch (descs[i].type) {
        case INT:
          descs_[i].integer.def_val = descs[i].integer.def_val;
          descs_[i].integer.min_val = descs[i].integer.min_val;
          descs_[i].integer.max_val = descs[i].integer.max_val;
          break;
        case STR:
          descs_[i].str.def_val = descs[i].str.def_val;
          break;
      }
      ptrs_[i] = &descs_[i];
    }
    descriptor_list.vars = ptrs_.data();
    descriptor_list.var_count = static_cast<uint32_t>(N);
  }

  // Read the caller's per-session value of an INT variable — the equivalent of
  // a plugin's THDVAR(thd, var). Must be called on the connection thread (a VDF
  // or callback), never from a background thread worker.
  //
  // extension_name: extension name as registered (e.g. "vsql_my_ext")
  // var_name:       variable name without extension prefix (e.g. "ef_search")
  //
  // Returns false on success, true on error.
  bool get_session_int(std::string_view extension_name,
                       std::string_view var_name, long long &out) const {
    if (abi_ == nullptr || abi_->get_session_int == nullptr) return true;
    // The ABI takes NUL-terminated names; a string_view may not be.
    const std::string ext(extension_name);
    const std::string var(var_name);
    return abi_->get_session_int(ext.c_str(), var.c_str(), &out);
  }

  // Read the caller's per-session value of a STRING variable. Same thread rules
  // as get_session_int.
  //
  // Returns false on success, true on error.
  bool get_session_str(std::string_view extension_name,
                       std::string_view var_name, std::string &out) const {
    if (abi_ == nullptr || abi_->get_session_str == nullptr) return true;
    // The ABI takes NUL-terminated names; a string_view may not be.
    const std::string ext(extension_name);
    const std::string var(var_name);
    void *val = nullptr;
    size_t val_len = 0;
    if (abi_->get_session_str(ext.c_str(), var.c_str(), &val, &val_len))
      return true;
    out.assign(static_cast<const char *>(val), val_len);
    free(val);
    return false;
  }

  // Read by the server's on_populate callback. Public so CapabilityTraits can
  // return its address as capability_config.
  vef_session_var_descriptor_list_t descriptor_list{};

 private:
  template <typename Capability>
  friend struct ::vsql::detail::CapabilityTraits;

  const vef_preview_session_var_t *abi_ = nullptr;

  std::array<vef_session_var_desc_t, N> descs_;
  std::array<const vef_session_var_desc_t *, N> ptrs_;
};

// Factory: deduces N from the array size.
template <size_t N>
SessionVarCapability<N> make_capability(
    SessionVarDescriptor const (&descs)[N]) {
  return SessionVarCapability<N>(descs);
}

// Type-specific factories for brace-free construction inside make_capability().
inline SessionVarDescriptor make_int(const char *name, const char *comment,
                                     long long def_val, long long min_val,
                                     long long max_val) {
  SessionVarDescriptor d;
  d.type = INT;
  d.name = name;
  d.comment = comment;
  d.integer.def_val = def_val;
  d.integer.min_val = min_val;
  d.integer.max_val = max_val;
  return d;
}

inline SessionVarDescriptor make_str(const char *name, const char *comment,
                                     const char *def_val) {
  SessionVarDescriptor d;
  d.type = STR;
  d.name = name;
  d.comment = comment;
  d.str.def_val = def_val;
  return d;
}

}  // namespace vsql::preview_session_var

#include <villagesql/preview/detail/session_var_register.h>

#endif  // VILLAGESQL_PREVIEW_SESSION_VAR_H
