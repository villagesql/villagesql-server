// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// Built against stable_sdk/v3. A v3 prerun sees a NULL literal as STRING, so
// int_or_null_sum admits STRING to accept NULL, and the body skips anything
// that is not a non-NULL INT.

#include <villagesql/vsql.h>

#include <string>

void int_or_null_sum_prerun(vsql::PrerunArgs args, vsql::PrerunResult result) {
  for (size_t i = 0; i < args.size(); i++) {
    if (!args.type_at(i).is_int() && !args.type_at(i).is_str()) {
      result.error("int_or_null_sum: argument " + std::to_string(i) +
                   " must be INT or NULL");
      return;
    }
  }
}

void int_or_null_sum(vsql::VarArgs args, vsql::IntResult out) {
  long long total = 0;
  for (auto a : args) {
    if (!a.is_null() && a.is_int()) total += a.as_int();
  }
  out.set(total);
}

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension().func(make_func<&int_or_null_sum>("int_or_null_sum")
                              .returns(INT)
                              .varargs()
                              .prerun<&int_or_null_sum_prerun>()
                              .build()))
