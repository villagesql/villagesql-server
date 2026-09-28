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

#ifndef VILLAGESQL_VSQL_BIND_CHECK_TYPES_H
#define VILLAGESQL_VSQL_BIND_CHECK_TYPES_H

// Typed wrapper for the bind_and_check_types hook.
//
// Registered via .bind_and_check_types<&fn>() with the typed signature
//
//   void(BindArgs, BindResult)
//
// Called once at resolution time, before any row is read, for a function whose
// return-type parameters the built-in rules cannot work out. The hook reads the
// arguments and answers with the parameters for the return type and, where
// it needs to, for individual arguments.

#include <cstddef>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/type_params.h>

namespace vsql {

// One argument as the bind hook sees it: its kind, its resolved type
// parameters, and its literal value when it is a string constant.
class BindArgType {
 public:
  BindArgType(const vef_type_t *t, const vef_type_params_t *params,
              const char *const_value, size_t const_len)
      : t_(t),
        params_(params),
        const_value_(const_value),
        const_len_(const_len) {}

  vef_type_id type() const { return t_->id; }
  bool is_int() const { return t_->id == VEF_TYPE_INT; }
  bool is_real() const { return t_->id == VEF_TYPE_REAL; }
  bool is_str() const { return t_->id == VEF_TYPE_STRING; }
  bool is_custom() const { return t_->id == VEF_TYPE_CUSTOM; }

  // For CUSTOM arguments: the unqualified type name. Empty otherwise.
  std::string_view custom_type() const {
    return t_->custom_type ? std::string_view(t_->custom_type)
                           : std::string_view{};
  }

  // True when this argument arrived with its type parameters resolved.
  bool has_params() const { return params_ != nullptr && params_->count > 0; }

  // This argument's parameters as your own params struct, or nullptr when they
  // are not known. P must be the type registered with
  // .params<P, &P::parse, &P::to_strings>() on the type builder.
  //
  // Also returns nullptr when no custom type registered P, rather than
  // dereferencing the cache's null parse function. A fixed-arity function is
  // caught at registration by check_params_cache_bound, but that check is
  // only derived for typed argument lists, so a varargs hook has no other
  // safety net.
  template <typename P>
  const P *params() const {
    if (!has_params()) return nullptr;
    if (!type_params_cache_for<P>().is_bound()) return nullptr;
    return &type_params_cache_for<P>().get(*params_);
  }

  // The text of a constant string argument -- this is what lets a hook derive
  // a type from a literal, e.g. TYPEID('user'). Empty when the argument is not
  // a string constant the server could evaluate during resolution.
  bool has_const_value() const { return const_value_ != nullptr; }
  std::string_view const_value() const {
    return const_value_ ? std::string_view(const_value_, const_len_)
                        : std::string_view{};
  }

 private:
  const vef_type_t *t_;
  const vef_type_params_t *params_;
  const char *const_value_;
  size_t const_len_;
};

class BindArgs {
 public:
  explicit BindArgs(const vef_bind_types_args_t *a) : a_(a) {}

  size_t size() const { return a_->arg_count; }

  BindArgType at(size_t i) const {
    return BindArgType(a_->arg_types[i],
                       a_->arg_params ? a_->arg_params[i] : nullptr,
                       a_->const_values ? a_->const_values[i] : nullptr,
                       a_->const_lengths ? a_->const_lengths[i] : 0);
  }

 private:
  const vef_bind_types_args_t *a_;
};

class BindResult {
 public:
  explicit BindResult(vef_bind_types_result_t *r) : r_(r) {}

  // The parameters the function's return type should have. Serialized through
  // the to_strings you registered for P, so you never format "k=v" by hand.
  template <typename P>
  void set_return(const P &p) {
    write<P>(r_->out_return_params, p);
  }

  // Optionally decide argument i's parameters, for an argument the server
  // could not resolve on its own -- typically a bare string literal bound for
  // a parameterized type. Arguments you say nothing about keep whatever the
  // server worked out.
  template <typename P>
  void set_arg(size_t i, const P &p) {
    if (r_->out_arg_params == nullptr) return;
    write<P>(r_->out_arg_params[i], p);
  }

  // Varargs only: read argument i as the given custom type. Pass the type
  // object you registered -- out.set_arg_type(0, TVECTOR) -- so the name
  // always matches a real type of this extension.
  //
  // Only an argument the server could not type at all, in practice a string
  // constant, can be named this way; naming a type for an argument that
  // already carries one is checked rather than applied, and a disagreement
  // aborts the statement. Silent for a fixed-arity function, whose signature
  // already says what each argument is.
  void set_arg_type(size_t i, const char *type_name) {
    if (r_->out_arg_types == nullptr || type_name == nullptr) return;
    write_name(r_->out_arg_types[i], type_name);
  }

  // The two halves of typing a constant argument in one gesture: what it
  // should be read as, and the parameters to read it with. Prefer this over
  // calling set_arg_type() and set_arg() separately -- naming a type makes
  // these parameters the only ones the server will use for the argument, so
  // supplying one without the other leaves it unresolvable.
  template <typename P>
  void set_arg(size_t i, const char *type_name, const P &p) {
    set_arg_type(i, type_name);
    set_arg<P>(i, p);
  }

  // Reject the call. The statement is aborted with this message.
  void error(std::string_view msg) {
    r_->type = VEF_RESULT_ERROR;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    std::memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }

 private:
  template <typename P>
  void write(vef_inferred_type_params_t *slot, const P &p) {
    // No registered to_strings for P means there is no way to serialize p, so
    // P is not the parameters type of any custom type this extension
    // registered. Say so here rather than leaving the parameters unset, which
    // would surface far from the cause as an unresolved-parameters error on
    // the query.
    if (!type_params_cache_for<P>().has_to_strings()) {
      error(
          "the parameters type passed to set_return() or set_arg() is not "
          "registered with any custom type in this extension (the type needs "
          ".params<P, &P::parse, &P::to_strings>())");
      return;
    }
    std::map<std::string, std::string> m;
    type_params_cache_for<P>().to_strings(p, m);
    write_params_to(slot, m);
  }

  // Same snprintf-style contract as write_params_to: actual_len is always the
  // length the whole name would take, and overflow says whether buf is safe
  // to read.
  static void write_name(vef_inferred_type_params_t *slot,
                         std::string_view name) {
    slot->actual_len = name.size();
    slot->overflow = name.size() > slot->max_buf_len;
    if (!slot->overflow) std::memcpy(slot->buf, name.data(), name.size());
  }

  vef_bind_types_result_t *r_;
};

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_BIND_CHECK_TYPES_H
