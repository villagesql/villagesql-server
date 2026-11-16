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
#include <climits>
#include <cstdlib>
#include <mutex>
#include <string>

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
// Read the caller's per-session value from a VDF — the equivalent of a plugin's
// THDVAR(thd, var). For an INT variable read repeatedly, bind it once to an
// IntVar (int_var()) and read through that; the fast-path read is lock-free.
// For occasional or STRING reads use get_session_int / get_session_str. All of
// these resolve the current connection thread, so they must be called on it (a
// VDF or callback running on the connection), never from a background thread
// worker.
//
// Usage:
//
//   static auto g_session_vars = vsql::preview_session_var::make_capability({
//       vsql::preview_session_var::make_int("session_int_var", "Search width")
//           .default_(20)
//           .min(1)
//           .max(4096),
//       vsql::preview_session_var::make_str("label", "Per-session label")
//           .default_("default"),
//   });
//
//   // Bind once (names the variable here); read with no name at the call site.
//   static auto g_ef = g_session_vars.int_var("session_int_var");
//   //   long long v; if (g_ef.read(v)) { ... }
//
//   VEF_GENERATE_ENTRY_POINTS(make_extension().func(...).with(g_session_vars));
namespace vsql::preview_session_var {

// Type tag for SessionVarDescriptor — matches vef_session_var_type_t values.
enum Type {
  INT = VEF_SESSION_VAR_INT,
  STR = VEF_SESSION_VAR_STR,
};

// A single session-variable declaration. Build one with the make_int / make_str
// factories, then chain the setters below to override the defaults; do not
// brace-initialize this directly.
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

  // Fluent setters. Chain in any order after the factory:
  //   make_int("n", "c").default_(20).min(1).max(4096)
  //   make_str("n", "c").default_("hello")
  // Unset INT bounds leave the variable unconstrained (full long long range)
  // with default 0; an unset STR default is the empty string. min()/max() apply
  // to INT variables only.
  SessionVarDescriptor &default_(long long v) {
    integer.def_val = v;
    return *this;
  }
  SessionVarDescriptor &default_(const char *v) {
    str.def_val = v;
    return *this;
  }
  SessionVarDescriptor &min(long long v) {
    integer.min_val = v;
    return *this;
  }
  SessionVarDescriptor &max(long long v) {
    integer.max_val = v;
    return *this;
  }
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
  // a plugin's THDVAR(thd, var). Reads a variable this capability declared;
  // the extension identity is supplied automatically. Must be called on the
  // connection thread (a VDF or callback), never from a background thread
  // worker.
  //
  // var_name: variable name without extension prefix (e.g. "session_int_var")
  //
  // Returns false on success, true on error.
  bool get_session_int(const char *var_name, long long &out) const {
    if (abi_ == nullptr || abi_->get_session_int == nullptr) return true;
    return abi_->get_session_int(&descriptor_list, var_name, &out);
  }

  // Read the caller's per-session value of a STRING variable. Same thread rules
  // as get_session_int.
  //
  // There is deliberately no str_var()/StrVar analogue of the INT fast path.
  // An INT value lives inline in the per-session slot, so IntVar can cache a
  // storage offset and read it lock-free. A STRING slot holds a pointer to a
  // separately allocated buffer, so every read must copy the value out under
  // the server lock — a cached-offset read could chase a pointer freed by a
  // concurrent SET SESSION or UNINSTALL. get_session_str therefore takes the
  // lock on every call; it is the right primitive for occasional reads. For a
  // string read on a hot path, cache the value in the extension's own session
  // state and refresh it when it changes.
  //
  // Returns false on success, true on error.
  bool get_session_str(const char *var_name, std::string &out) const {
    if (abi_ == nullptr || abi_->get_session_str == nullptr) return true;
    void *val = nullptr;
    size_t val_len = 0;
    if (abi_->get_session_str(&descriptor_list, var_name, &val, &val_len))
      return true;
    out.assign(static_cast<const char *>(val), val_len);
    free(val);
    return false;
  }

  // Move-only RAII handle to a resolved INT session variable. Obtain one from
  // resolve_int() once (at load or first use) and reuse it for lock-free
  // per-query reads with read(); the handle frees itself on destruction.
  class IntHandle {
   public:
    IntHandle() = default;
    IntHandle(const vef_preview_session_var_t *abi, vef_session_var_handle_t *h)
        : abi_(abi), h_(h) {}
    IntHandle(const IntHandle &) = delete;
    IntHandle &operator=(const IntHandle &) = delete;
    IntHandle(IntHandle &&o) noexcept : abi_(o.abi_), h_(o.h_) {
      o.h_ = nullptr;
    }
    IntHandle &operator=(IntHandle &&o) noexcept {
      if (this != &o) {
        reset();
        abi_ = o.abi_;
        h_ = o.h_;
        o.h_ = nullptr;
      }
      return *this;
    }
    ~IntHandle() { reset(); }

    // True if this handle resolved to a variable.
    bool valid() const { return h_ != nullptr; }

    // Read the caller's current per-session value — the lock-free hot path.
    // Must be called on the connection thread. Returns false on success, true
    // on error (unresolved handle or no connection thread).
    bool read(long long &out) const {
      if (abi_ == nullptr || abi_->read_int == nullptr || h_ == nullptr)
        return true;
      return abi_->read_int(h_, &out);
    }

   private:
    void reset() {
      if (abi_ != nullptr && abi_->free_handle != nullptr && h_ != nullptr)
        abi_->free_handle(h_);
      h_ = nullptr;
    }

    const vef_preview_session_var_t *abi_ = nullptr;
    vef_session_var_handle_t *h_ = nullptr;
  };

  // Resolve an INT variable this capability declared to a reusable handle for
  // lock-free repeated reads — the fast-path equivalent of get_session_int. The
  // extension identity is supplied automatically. Resolving is the slow step
  // (done once); each subsequent IntHandle::read() is lock-free. Returns an
  // invalid handle (valid() == false) on error.
  //
  // Prefer int_var() below for the common case of a variable read repeatedly:
  // it names the variable once (at the IntVar declaration) and resolves lazily,
  // so it can be declared at static-init before the capability has loaded.
  //
  // var_name: variable name without extension prefix (e.g. "session_int_var")
  IntHandle resolve_int(const char *var_name) const {
    if (abi_ == nullptr || abi_->resolve_int_handle == nullptr)
      return IntHandle();
    vef_session_var_handle_t *h = nullptr;
    if (abi_->resolve_int_handle(&descriptor_list, var_name, &h))
      return IntHandle();
    return IntHandle(abi_, h);
  }

  // A named INT variable bound to this capability, resolved lazily on first
  // read(). Unlike IntHandle (which must be resolved after load), an IntVar can
  // be declared at static-init next to the capability — it holds a reference to
  // the capability and the variable name, and resolves itself the first time
  // read() runs (by which point the extension has loaded and read() is on a
  // connection thread). The resolved handle is cached, so subsequent reads are
  // lock-free.
  //
  // This bound-handle fast path is INT-only; STRING variables are read with the
  // name-keyed get_session_str (see the note there for why a lock-free string
  // handle is not possible).
  //
  //   static auto VARS = make_capability({make_int("ef_search", "...")});
  //   static auto EF   = VARS.int_var("ef_search");
  //   ...
  //   long long v; if (EF.read(v)) { ... }   // no name at the read site
  class IntVar {
   public:
    IntVar(const SessionVarCapability &cap, const char *name)
        : cap_(cap), name_(name) {}

    // Read the caller's current per-session value. Resolves once on the first
    // call (thread-safe), then reads lock-free. Must be called on the
    // connection thread. Returns false on success, true on error.
    bool read(long long &out) const {
      std::call_once(resolved_, [this] { handle_ = cap_.resolve_int(name_); });
      if (!handle_.valid()) return true;
      return handle_.read(out);
    }

   private:
    const SessionVarCapability &cap_;
    const char *name_;
    mutable IntHandle handle_;
    mutable std::once_flag resolved_;
  };

  // Bind an INT variable this capability declared to a lazily-resolved IntVar.
  // The variable name is given here, once; read through the returned IntVar
  // with no name at the read site.
  IntVar int_var(const char *var_name) const { return IntVar(*this, var_name); }

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
//
// make_int starts unconstrained: default 0, bounds the full long long range.
// make_str starts with an empty-string default. Chain the fluent setters
// (.default_() / .min() / .max()) to override.
inline SessionVarDescriptor make_int(const char *name, const char *comment) {
  SessionVarDescriptor d;
  d.type = INT;
  d.name = name;
  d.comment = comment;
  d.integer.def_val = 0;
  d.integer.min_val = LLONG_MIN;
  d.integer.max_val = LLONG_MAX;
  return d;
}

inline SessionVarDescriptor make_str(const char *name, const char *comment) {
  SessionVarDescriptor d;
  d.type = STR;
  d.name = name;
  d.comment = comment;
  d.str.def_val = "";
  return d;
}

}  // namespace vsql::preview_session_var

#include <villagesql/preview/detail/session_var_register.h>

#endif  // VILLAGESQL_PREVIEW_SESSION_VAR_H
