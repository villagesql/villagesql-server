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
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// Imaginary future ping ABI v2 — used only by vsql-preview-ping-v2-test.
//
// This header represents what the ping capability ABI would look like after
// adding pong(). It is NOT the real SDK header. Its purpose is to let the
// test extension pretend it was compiled against a newer server than currently
// exists, exercising the "extension requires higher capability version than
// server provides" error path.

#ifndef VSQL_PREVIEW_PING_V2_TEST_PING_ABI_V2_H
#define VSQL_PREVIEW_PING_V2_TEST_PING_ABI_V2_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define VEF_PREVIEW_PING_NAME "vsql::preview::ping"

// This extension was compiled against v2 of the ping ABI.
#define VEF_PREVIEW_PING_V2_ABI_VERSION 2

typedef uint64_t (*vef_ping_fn)(void);
typedef uint64_t (*vef_pong_fn)(void);

typedef struct {
  // Capability ABI version. Always the first field in every capability vtable.
  uint32_t version;

  // version >= 1
  vef_ping_fn ping;

  // version >= 2
  vef_pong_fn pong;
} vef_preview_ping_v2_t;

#ifdef __cplusplus
}
#endif

#endif  // VSQL_PREVIEW_PING_V2_TEST_PING_ABI_V2_H
