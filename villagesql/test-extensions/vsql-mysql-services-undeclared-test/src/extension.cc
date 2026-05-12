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

// vsql_mysql_services_undeclared_test extension: code requests a MySQL
// service that exists in the registry but is NOT in the manifest's
// required_mysql_services list. INSTALL EXTENSION must fail with an error
// naming the undeclared service.

#include <cstddef>

#include <mysql/components/services/keyring_metadata_query.h>  // keyring_component_status
#include <villagesql/vsql.h>
#include <vsql/mysql_services.h>

using namespace vsql;

// keyring_component_status is a real, registered service when the keyring
// component is loaded. The manifest, however, declares an empty
// required_mysql_services list — so the bridge must refuse this acquire.
VSQL_DECLARE_SERVICE(keyring_component_status, status);

void noop(IntResult out) { out.set(0); }

VEF_GENERATE_ENTRY_POINTS(make_extension().with_airlock(status).func(
    make_func<&noop>("noop").returns(INT).no_params().build()))
