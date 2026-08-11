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

// vsql_mysql_services_missing_test extension: requests a MySQL service that
// is guaranteed not to be registered. INSTALL EXTENSION must fail with an
// error message naming the missing service.

#include <cstddef>

#include <mysql/components/service.h>
#include <villagesql/preview/mysql_services.h>
#include <villagesql/vsql.h>

using namespace vsql;

// Define a service the registry will never have an implementation of.
BEGIN_SERVICE_DEFINITION(vsql_intentionally_missing)
DECLARE_BOOL_METHOD(unused, (void));
END_SERVICE_DEFINITION(vsql_intentionally_missing)

static preview_mysql_services::MysqlServices services;
VSQL_REQUIRE_SERVICE(services, vsql_intentionally_missing, missing);

// VDF only exists so the extension has a func to register; never callable
// because INSTALL EXTENSION fails before the extension becomes usable.
void noop(IntResult out) { out.set(0); }

VEF_GENERATE_ENTRY_POINTS(make_extension().with(services).func(
    make_func<&noop>("noop").returns(INT).no_params().build()))
