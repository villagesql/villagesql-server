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

// Bad-registration test: an extension that registers two DIFFERENTLY named
// index profiles that both claim to be the default for the same (data type,
// index type) pair. Only one default may exist per pair, so registration must
// reject the extension at INSTALL EXTENSION time. The index hooks and the type
// ops are shared no-op stubs, because registration fails before any of them is
// ever called.

#include "index_build_common.h"
#include "type_build_common.h"

using namespace vsql::preview_index_builder;

static constexpr const char kDefType[] = "DUP_DEFAULT_TYPE";
static constexpr const char kDefIndex[] = "DUP_DEFAULT_INDEX";

// Custom type referenced by the profiles (uses shared stub type-ops).
constexpr auto DEF_TYPE = vsql::make_type<kDefType>()
                              .persisted_length(8)
                              .max_decode_buffer_length(8)
                              .from_string<&tbc::stub_encode>()
                              .to_string<&tbc::stub_decode>()
                              .compare<&tbc::stub_compare>()
                              .build();

// One index type referenced by the profiles.
// clang-format off
static constexpr auto DEF_INDEX =
    make_index_type<kDefIndex, ibc::DupCtx>()
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

// Two distinctly named index profiles, both default for the same
// (DUP_DEFAULT_TYPE, DUP_DEFAULT_INDEX) pair.
static constexpr const char kFirstProfile[] = "DUP_DEFAULT_PROFILE_1";
static constexpr const char kSecondProfile[] = "DUP_DEFAULT_PROFILE_2";

static const auto DEF_PROFILE_1 = make_index_profile(kFirstProfile)
                                      .for_type(kDefType)
                                      .using_index(kDefIndex)
                                      .default_for_type(true)
                                      .build();
static const auto DEF_PROFILE_2 = make_index_profile(kSecondProfile)
                                      .for_type(kDefType)
                                      .using_index(kDefIndex)
                                      .default_for_type(true)
                                      .build();

static auto INDEX_TYPE = IndexTypeCapability().index_type(DEF_INDEX);
static auto INDEX_PROFILE = IndexProfileCapability()
                                .index_profile(DEF_PROFILE_1)
                                .index_profile(DEF_PROFILE_2);

VEF_GENERATE_ENTRY_POINTS(
    vsql::make_extension().with(INDEX_TYPE).with(INDEX_PROFILE).type(DEF_TYPE))
