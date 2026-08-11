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

// Bad-registration test: an extension that registers two index profiles with
// the SAME name ("DUP_PROFILE"). Both profiles reference one real type and one
// real index type so profile-reference validation passes and registration
// reaches the duplicate-profile check, which must reject the extension at
// INSTALL EXTENSION time. The index hooks and the type ops are shared no-op
// stubs -- registration fails before any of them is ever called.

#include "index_build_common.h"
#include "type_build_common.h"

using namespace vsql::preview_index_builder;

static constexpr const char kDupType[] = "DUP_PROFILE_TYPE";
static constexpr const char kDupIndex[] = "DUP_PROFILE_INDEX";

// Custom type referenced by the profiles (uses shared stub type-ops).
constexpr auto DUP_TYPE = vsql::make_type<kDupType>()
                              .persisted_length(8)
                              .max_decode_buffer_length(8)
                              .from_string<&tbc::stub_encode>()
                              .to_string<&tbc::stub_decode>()
                              .compare<&tbc::stub_compare>()
                              .build();

// One index type referenced by the profiles.
// clang-format off
static constexpr auto DUP_INDEX =
    make_index_type<kDupIndex, ibc::DupCtx>()
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

// Two index profiles with the SAME name, both referencing the type + index
// type above.
static constexpr const char kDupProfile[] = "DUP_PROFILE";

static const auto DUP_PROFILE_1 = make_index_profile(kDupProfile)
                                      .for_type(kDupType)
                                      .using_index(kDupIndex)
                                      .build();
static const auto DUP_PROFILE_2 = make_index_profile(kDupProfile)
                                      .for_type(kDupType)
                                      .using_index(kDupIndex)
                                      .build();

static auto INDEX_TYPE = IndexTypeCapability().index_type(DUP_INDEX);
static auto INDEX_PROFILE = IndexProfileCapability()
                                .index_profile(DUP_PROFILE_1)
                                .index_profile(DUP_PROFILE_2);

VEF_GENERATE_ENTRY_POINTS(
    vsql::make_extension().with(INDEX_TYPE).with(INDEX_PROFILE).type(DUP_TYPE))
