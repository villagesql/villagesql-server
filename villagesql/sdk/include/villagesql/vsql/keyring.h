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

#ifndef VILLAGESQL_VSQL_KEYRING_H_
#define VILLAGESQL_VSQL_KEYRING_H_

// Keyring access for extensions.
//
// Use vsql::preview::keyring::Capability via .with<preview_keyring<cap>>()
// in VEF_GENERATE_ENTRY_POINTS. See <villagesql/preview/keyring.h>.
//
// This header is kept for backward compatibility but no longer defines
// globals — keyring access is now a declared preview capability so that
// the server can reject extensions that require it on servers where the
// keyring component is unavailable.

#include <villagesql/preview/keyring.h>

#endif  // VILLAGESQL_VSQL_KEYRING_H_
