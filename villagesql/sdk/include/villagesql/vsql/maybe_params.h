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

#ifndef VILLAGESQL_VSQL_MAYBE_PARAMS_H
#define VILLAGESQL_VSQL_MAYBE_PARAMS_H

#include <cassert>
#include <optional>
#include <utility>

namespace vsql {

// MaybeParams<P> carries a parameterized type's parsed parameters in either
// "known" or "unknown" state. It is the first argument to from_string for
// parameterized custom types in the ::vsql API.
//
// At row time the SDK constructs MaybeParams<P> in the known state from the
// cached parsed parameters. The unknown state is used by the fix_fields-time
// pre-execute path, which asks the extension to infer params from a constant
// string literal.
//
// Usage in an extension's from_string:
//
//   void mytype_from_string(vsql::MaybeParams<MyParams> &p,
//                           std::string_view from,
//                           vsql::CustomResult out) {
//     // ... parse the string ...
//     if (p.is_known()) {
//       // Validate that what was parsed matches p.value().
//     } else {
//       // Infer params from the string and store them.
//       p.set(MyParams{...});
//     }
//     // Encode using p.value() (now always known).
//   }
template <typename P>
class MaybeParams {
 public:
  using value_type = P;

  // Construct in unknown state.
  MaybeParams() = default;

  // Construct in known state with the given params.
  explicit MaybeParams(P params) : params_(std::move(params)) {}

  // True if params are populated (either set on construction or via set()).
  bool is_known() const { return params_.has_value(); }

  // Returns the params. is_known() must be true.
  const P &value() const {
    assert(params_.has_value());
    return *params_;
  }

  // Stores the given params, transitioning from unknown to known. May also
  // overwrite an existing known value.
  void set(P params) { params_ = std::move(params); }

 private:
  std::optional<P> params_ = std::nullopt;
};

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_MAYBE_PARAMS_H
