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

#ifndef VILLAGESQL_VSQL_PRE_POST_RUN_H
#define VILLAGESQL_VSQL_PRE_POST_RUN_H

// Per-statement lifecycle hooks (prerun and postrun).
//
// Extension authors register hooks via .prerun<&fn>() / .postrun<&fn>() on
// the func builder.
//
//   prerun:  void(PrerunArgs, PrerunResult)
//   postrun: void(PostrunArgs)
//
// Raw vef_prerun_func_t / vef_postrun_func_t signatures are rejected at
// compile time. Extensions that need to drop to the raw ABI for these
// hooks would have to skip the vsql builder entirely.
//
// State lifetime is explicit: if prerun stores a pointer via set_user_data,
// postrun is responsible for freeing it. PostrunArgs::delete_state<T>() is
// the typed convenience for the common case of `new T{}` + `delete`.
//
// Prerun and postrun can be a cumbersome API. The func builder already handles
// the simple cases (e.g. type checking for functions with fixed args and
// allocating fixed buffer sizes), and we want to cover more. If you find you
// need to reach for prerun or postrun, please come talk to us so we can
// understand your use case.
//
// TODO(villagesql-general): add a typed-state mechanism (working name
// `.state<T>()` on FuncBuilder) so extensions can declare a per-statement
// state type and have the SDK manage its lifetime automatically — no
// matched prerun/postrun pair required just to free state. Implementable
// without ABI changes by having .state<T>() install SDK-generated prerun
// and postrun wrappers that new/delete the T into the existing user_data
// slot; the user's optional prerun/postrun receive the typed reference.
// .state<T>() and raw set_user_data become mutually exclusive.

#include <cstddef>
#include <cstring>
#include <string_view>
#include <utility>

#include <villagesql/abi/types.h>

namespace vsql {

// Type-only view of one prerun-time argument. Prerun runs before any rows
// are read, so it sees only the declared type of each argument, not the
// runtime values.
class PrerunArgType {
 public:
  explicit PrerunArgType(const vef_type_t *t) : t_(t) {}

  vef_type_id type() const { return t_->id; }
  bool is_int() const { return t_->id == VEF_TYPE_INT; }
  bool is_real() const { return t_->id == VEF_TYPE_REAL; }
  bool is_str() const { return t_->id == VEF_TYPE_STRING; }
  bool is_custom() const { return t_->id == VEF_TYPE_CUSTOM; }

  // For CUSTOM types: the unqualified type name (no extension prefix).
  // Empty string_view for non-CUSTOM types.
  std::string_view custom_type() const {
    return t_->custom_type ? std::string_view(t_->custom_type)
                           : std::string_view{};
  }

 private:
  const vef_type_t *t_;
};

class PrerunArgs {
 public:
  explicit PrerunArgs(const vef_prerun_args_t *a) : a_(a) {}

  size_t size() const { return a_->arg_count; }

  PrerunArgType type_at(size_t i) const {
    return PrerunArgType(&a_->arg_types[i]);
  }

  // TODO(villagesql-general): expose const_at(i) returning the serialized
  // literal bytes for constant arguments. Blocked on vdf_handler.cc
  // populating vef_prerun_args_t::const_values / const_lengths; today both
  // are unconditionally nullptr at the prerun call site.

 private:
  const vef_prerun_args_t *a_;
};

class PrerunResult {
 public:
  explicit PrerunResult(vef_prerun_result_t *r) : r_(r) {}

  // Stash a pointer the extension owns. The VDF and postrun read it back
  // via PostrunArgs::user_data() or PostrunArgs::state<T>(). The SDK does
  // not free anything stashed here; pair this with a postrun that calls
  // PostrunArgs::delete_state<T>() (or free()) on the same pointer.
  void set_user_data(void *p) { r_->user_data = p; }

  // Request that the per-row result buffer be at least n bytes. The server
  // allocates the buffer before invoking the VDF; useful for STRING/CUSTOM
  // return types where the default size is too small for this function.
  void request_buffer_size(size_t n) { r_->result_buffer_size = n; }

  // Report a prerun failure. The query is aborted with this error message.
  void error(std::string_view msg) {
    r_->type = VEF_RESULT_ERROR;
    size_t n =
        msg.size() < VEF_MAX_ERROR_LEN - 1 ? msg.size() : VEF_MAX_ERROR_LEN - 1;
    std::memcpy(r_->error_msg, msg.data(), n);
    r_->error_msg[n] = '\0';
  }

 private:
  vef_prerun_result_t *r_;
};

class PostrunArgs {
 public:
  explicit PostrunArgs(const vef_postrun_args_t *a) : a_(a) {}

  // Read the raw user_data slot — whatever prerun stored via set_user_data.
  // Use this for non-C++ allocations (malloc/arena/pool) or anything that
  // doesn't fit a single typed T.
  void *user_data() const { return a_->user_data; }

  // Convenience: static_cast user_data to a T*. Pair with prerun's
  // set_user_data(new T{...}).
  template <typename T>
  T *state() const {
    return static_cast<T *>(a_->user_data);
  }

  // Convenience: `delete state<T>()` in one call. Pair with prerun's
  // set_user_data(new T{...}).
  template <typename T>
  void delete_state() const {
    delete state<T>();
  }

 private:
  const vef_postrun_args_t *a_;
};

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_PRE_POST_RUN_H
