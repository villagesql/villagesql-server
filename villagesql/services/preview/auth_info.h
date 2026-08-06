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

#ifndef VILLAGESQL_SERVICES_PREVIEW_AUTH_INFO_H
#define VILLAGESQL_SERVICES_PREVIEW_AUTH_INFO_H

namespace villagesql::services {

// Opaque per-login state staged by a VEF auth handler (e.g. the roles set via
// set_active_roles). Owned and defined by the VEF auth seam in
// villagesql/services/preview/auth.cc; core auth only forwards it to
// maybe_apply_vef_auth_state() after account resolution and never inspects it.
struct VefAuthState;

// The VEF-auth-specific fields on MPVIO_EXT, grouped so core auth carries a
// single member instead of one field per VEF feature. Core reads/forwards these
// but does not interpret them; the VEF auth seam populates them. All nullptr
// for a normal (non-VEF) plugin-based login.
struct VefAuthInfo {
  // For a VEF extension-provided auth method there is no MySQL plugin
  // (`plugin` is null), so the client-side auth plugin the handshake should
  // advertise/expect comes from the method's config instead of from
  // client_plugin_name(plugin). Set by the seam before invoking the handler.
  const char *vef_client_auth_plugin;
  villagesql::services::VefAuthState *vef_auth_state;
};

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_AUTH_INFO_H
