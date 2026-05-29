// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// VSQL_VEC_CHAIN_TEST extension entry point. The custom type
// VSQL_VEC_CHAIN and its VDFs live in type.{h,cc}; the vec_chain custom
// index registration lives in index.{h,cc}.

#include "index.h"
#include "type.h"

#include <villagesql/preview/table_storage.h>
#include <villagesql/vsql.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>

vsql::preview_table_storage::TableStorageCapability g_hidden_table;

// Inspector VDF: vec_chain_scan_count() returns INT. Reports the
// process-wide count of vec_chain index scans started. Tests use this
// to assert that an ORDER BY ... LIMIT actually routed through the
// custom index via the optimizer hook.
static void vec_chain_scan_count_vdf(vsql::IntResult out) {
  out.set(static_cast<int64_t>(vec_chain_scan_count()));
}

// Inspector VDF: vec_chain_inspect_knn(dim INT, query STRING, limit INT)
// returns STRING. Bridges the SDK's typed VDF surface to the
// vec_chain_inspect_knn helper defined in index.cc.
static void vec_chain_inspect_knn_vdf(vsql::IntArg dim,
                                      vsql::StringArg query_text,
                                      vsql::IntArg limit,
                                      vsql::StringResult out) {
  if (dim.is_null() || query_text.is_null() || limit.is_null()) {
    out.set_null();
    return;
  }
  std::string result;
  char err[256]{};
  if (vec_chain_inspect_knn(dim.value(), query_text.value(),
                            static_cast<uint32_t>(limit.value()), &result, err,
                            sizeof(err))) {
    out.error(err[0] == '\0' ? "vec_chain_inspect_knn failed" : err);
    return;
  }
  auto buf = out.buffer();
  const size_t n = std::min(result.size(), buf.size());
  memcpy(buf.data(), result.data(), n);
  out.set_length(n);
}

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .with(g_hidden_table)
        .with(VEC_CHAIN_INDEXES)
        .with(VEC_CHAIN_PROFILES)
        .type(VSQL_VEC_CHAIN_TYPE)
        .func(make_intrinsic_default<&vec_chain_default>(
            "vec_chain_intrinsic_default"))
        .func(make_func<&vec_chain_l2_distance>("vec_chain_l2_distance")
                  .returns(REAL)
                  .param(VSQL_VEC_CHAIN_TYPE)
                  .param(VSQL_VEC_CHAIN_TYPE)
                  .deterministic()
                  .build())
        .func(make_func<&vec_chain_dot_product>("vec_chain_dot_product")
                  .returns(REAL)
                  .param(VSQL_VEC_CHAIN_TYPE)
                  .param(VSQL_VEC_CHAIN_TYPE)
                  .deterministic()
                  .build())
        .func(make_func<&vec_chain_inspect_knn_vdf>("vec_chain_inspect_knn")
                  .returns(STRING)
                  .param(INT)
                  .param(STRING)
                  .param(INT)
                  .build())
        .func(make_func<&vec_chain_scan_count_vdf>("vec_chain_scan_count")
                  .returns(INT)
                  .no_params()
                  .build()))
