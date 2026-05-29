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

#ifndef VSQL_VEC_CHAIN_TEST_INDEX_H_
#define VSQL_VEC_CHAIN_TEST_INDEX_H_

#include <villagesql/preview/index_builder.h>

#include <cstdint>
#include <string>
#include <string_view>

// The IndexTypeCapability bundle that extension.cc passes to
// make_extension().with(). All the per-callback registration, hidden-table
// descriptor, and storage definitions live in index.cc as TU-local internals.
// The template argument tracks how many index types this bundle registers —
// bump if index.cc adds more index types to it.
extern vsql::preview_index_builder::IndexTypeCapability<1> VEC_CHAIN_INDEXES;

// IndexProfileCapability bundle exposed in index.cc. Binds
// vec_chain_l2_distance to the vec_chain index type so the optimizer can
// route ORDER BY vec_chain_l2_distance(...) LIMIT k through the index.
extern vsql::preview_index_builder::IndexProfileCapability<1>
    VEC_CHAIN_PROFILES;

// Returns the process-wide count of vec_chain index scans started.
// Used by the vec_chain_scan_count VDF so tests can prove the
// optimizer actually routed an ORDER BY ... LIMIT through the custom
// index.
uint64_t vec_chain_scan_count();

// Inspector entry point used by the vec_chain_inspect_knn VDF.
// Encodes `query_text` (a "[x,y,z]" string) into a float vector of the
// given dimension, walks the chain in the hidden table, sorts by L2
// distance to the query, and returns the top `limit` PK ids as a
// comma-separated string. PKs are decoded as int32 from the stored
// VARBINARY bytes (matching INT PRIMARY KEY columns used in the
// tests). Returns true on error; *result is unchanged on error and
// `error_msg` is populated.
bool vec_chain_inspect_knn(int64_t dimension, std::string_view query_text,
                           uint32_t limit, std::string *result, char *error_msg,
                           uint32_t error_msg_len);

#endif  // VSQL_VEC_CHAIN_TEST_INDEX_H_
