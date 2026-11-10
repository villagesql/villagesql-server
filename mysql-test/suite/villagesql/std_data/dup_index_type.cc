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

// Bad-registration test: an extension that registers two index types with the
// SAME name ("DUP_INDEX"). Registration must reject the extension at INSTALL
// EXTENSION time. The hooks are shared no-op stubs -- registration fails on the
// duplicate before any of them is ever called.

#include "index_build_common.h"

using namespace vsql::preview_index_builder;

// Two index types with the SAME name. Written as two explicit chains (not a
// template helper) so the builder's dependent template methods resolve without
// a `.template` qualifier.
static constexpr const char kDupIndex1[] = "DUP_INDEX";
static constexpr const char kDupIndex2[] = "DUP_INDEX";

// clang-format off
static constexpr auto DUP_INDEX_1 =
    make_index_type<kDupIndex1, ibc::DupCtx>()
        .lifecycle()
            .create<&ibc::dup_create>()
            .load<&ibc::dup_load>()
            .drop<&ibc::dup_drop>()
        .dml()
            .insert<&ibc::dup_insert>()
            .mark_delete<&ibc::dup_mark_delete>()
            .purge<&ibc::dup_purge>()
        .scan()
            .begin<&ibc::dup_begin>()
            .position<&ibc::dup_position>()
            .fetch<&ibc::dup_fetch>()
            .save<&ibc::dup_save>()
            .restore<&ibc::dup_restore>()
            .end<&ibc::dup_end>()
        .global()
            .capabilities(Index::Support::KNN)
            .storage_props(Index::Storage::HAS_COLUMN_REF | Index::Storage::REF_LOOKUP)
        .build();

static constexpr auto DUP_INDEX_2 =
    make_index_type<kDupIndex2, ibc::DupCtx>()
        .lifecycle()
            .create<&ibc::dup_create>()
            .load<&ibc::dup_load>()
            .drop<&ibc::dup_drop>()
        .dml()
            .insert<&ibc::dup_insert>()
            .mark_delete<&ibc::dup_mark_delete>()
            .purge<&ibc::dup_purge>()
        .scan()
            .begin<&ibc::dup_begin>()
            .position<&ibc::dup_position>()
            .fetch<&ibc::dup_fetch>()
            .save<&ibc::dup_save>()
            .restore<&ibc::dup_restore>()
            .end<&ibc::dup_end>()
        .global()
            .capabilities(Index::Support::KNN)
            .storage_props(Index::Storage::HAS_COLUMN_REF | Index::Storage::REF_LOOKUP)
        .build();
// clang-format on

static auto INDEX_TYPE =
    IndexTypeCapability().index_type(DUP_INDEX_1).index_type(DUP_INDEX_2);

VEF_GENERATE_ENTRY_POINTS(vsql::make_extension().with(INDEX_TYPE))
