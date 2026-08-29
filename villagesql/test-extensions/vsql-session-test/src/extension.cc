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

#include <villagesql/vsql.h>

using namespace vsql;

void session_schema(Session s, StringResult out) { out.set(s.schema()); }

void session_priv_user(Session s, StringResult out) { out.set(s.priv_user()); }

void session_priv_host(Session s, StringResult out) { out.set(s.priv_host()); }

void session_conn_id(Session s, IntResult out) {
  out.set(static_cast<long long>(s.connection_id()));
}

void session_kill_status(Session s, IntResult out) {
  out.set(static_cast<long long>(s.kill_status()));
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&session_schema>("session_schema")
                  .returns(STRING)
                  .no_params()
                  .build())
        .func(make_func<&session_priv_user>("session_priv_user")
                  .returns(STRING)
                  .no_params()
                  .build())
        .func(make_func<&session_priv_host>("session_priv_host")
                  .returns(STRING)
                  .no_params()
                  .build())
        .func(make_func<&session_conn_id>("session_conn_id")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&session_kill_status>("session_kill_status")
                  .returns(INT)
                  .no_params()
                  .build()))
