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

// Shared no-op index hooks for the bad-registration test extensions. Those
// tests are rejected during registration, before any hook is ever called, so
// the bodies are irrelevant to what is under test and are defined once here.
//
// The `make_index_type<>` chains stay in the individual extension sources: the
// builder's chained methods return dependent types, so wrapping a chain in a
// function template would require a `.template` qualifier on every step.

#ifndef VILLAGESQL_TEST_STD_DATA_INDEX_BUILD_COMMON_H
#define VILLAGESQL_TEST_STD_DATA_INDEX_BUILD_COMMON_H

#include <villagesql/preview/index_builder.h>
#include <villagesql/vsql.h>

#include <cstdint>

namespace ibc {

using vsql::preview_index_builder::Index;
using vsql::preview_index_builder::IndexScanDesc;
using vsql::preview_index_builder::IndexScanKey;

using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Segment;
using vsql::preview_storage::Space;

struct DupCtx {};
using Ctx = Index::StorageCtx<DupCtx>;
struct DupCursor {};

// ---- Lifecycle stubs ----
inline bool dup_create(Ctx * /*ctx*/, const Index & /*index*/,
                       Space::Ref /*space_ref*/, Segment::TrxRef /*trx_ref*/,
                       char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

inline bool dup_drop(Ctx * /*ctx*/, const Index & /*index*/,
                     Segment::TrxRef /*trx_ref*/, char * /*err*/,
                     uint32_t /*err_len*/) {
  return false;
}

inline bool dup_load(Ctx * /*ctx*/, const Index & /*index*/,
                     Index::StorageRef /*storage_ref*/, char * /*err*/,
                     uint32_t /*err_len*/) {
  return false;
}

// ---- DML stubs ----
inline bool dup_insert(Ctx * /*ctx*/, const Index & /*index*/,
                       Segment::TrxRef /*trx_ref*/,
                       IndexScanKey::KeyPartData * /*key_columns*/,
                       IndexScanKey::KeyPartData * /*pkey_columns*/,
                       IndexScanKey::KeyPartRef * /*key_ref*/, char * /*err*/,
                       uint32_t /*err_len*/) {
  return false;
}

inline bool dup_mark_delete(Ctx * /*ctx*/, const Index & /*index*/,
                            Segment::TrxRef /*trx_ref*/,
                            IndexScanKey::KeyPartRef * /*key_ref*/,
                            IndexScanKey::KeyPartData * /*key_columns*/,
                            IndexScanKey::KeyPartData * /*pkey_columns*/,
                            bool /*delete_mark*/, char * /*err*/,
                            uint32_t /*err_len*/) {
  return false;
}

inline bool dup_purge(Ctx * /*ctx*/, const Index & /*index*/,
                      Segment::TrxRef /*trx_ref*/,
                      IndexScanKey::KeyPartRef * /*key_ref*/,
                      IndexScanKey::KeyPartData * /*key_columns*/,
                      IndexScanKey::KeyPartData * /*pkey_columns*/,
                      char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

// ---- Scan stubs ----
inline bool dup_begin(Ctx * /*ctx*/, const Index & /*index*/,
                      MtrCtx::Ref /*mctx*/, const IndexScanDesc & /*scan_desc*/,
                      Index::Cursor *cursor, bool *eof, char * /*err*/,
                      uint32_t /*err_len*/) {
  *cursor = new DupCursor{};
  *eof = true;
  return false;
}

inline bool dup_position(Index::Cursor /*cursor*/, Index::CursorOp /*op*/,
                         bool *eof, char * /*err*/, uint32_t /*err_len*/) {
  *eof = true;
  return false;
}

inline bool dup_fetch(Index::Cursor /*cursor*/,
                      IndexScanKey::KeyPartRef * /*key_ref*/,
                      IndexScanKey::KeyPartData * /*key_columns*/,
                      IndexScanKey::KeyPartData * /*pkey_columns*/,
                      char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

inline bool dup_save(Index::Cursor /*cursor*/, char * /*err*/,
                     uint32_t /*err_len*/) {
  return false;
}

inline bool dup_restore(Index::Cursor /*cursor*/, MtrCtx::Ref /*mctx*/,
                        bool *eof, char * /*err*/, uint32_t /*err_len*/) {
  *eof = true;
  return false;
}

inline void dup_end(Index::Cursor *cursor) {
  delete static_cast<DupCursor *>(*cursor);
  *cursor = nullptr;
}

}  // namespace ibc

#endif  // VILLAGESQL_TEST_STD_DATA_INDEX_BUILD_COMMON_H
