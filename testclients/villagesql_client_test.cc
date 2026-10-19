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

// Client tests for VillageSQL features that need the binary protocol, which
// mysqltest cannot drive.

#include <stdio.h>
#include <stdlib.h>
#include <iterator>

#include "my_inttypes.h"
#include "mysql_client_fw.cc"

// Stream a custom-type value into a column with mysql_stmt_send_long_data(),
// which assembles one parameter from several packets. The pieces must reach
// the type's encoder as one value.
static void test_custom_type_long_data() {
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  MYSQL_RES *result;
  int int_data;
  char str_data[64];
  int rc;

  myheader("test_custom_type_long_data");

  rc = mysql_query(mysql, "INSTALL EXTENSION vsql_tvector");
  myquery(rc);

  rc = mysql_query(mysql, "DROP TABLE IF EXISTS t_long_data");
  myquery(rc);

  rc = mysql_query(mysql, "CREATE TABLE t_long_data(id INT, vec TVECTOR(3))");
  myquery(rc);

  stmt = mysql_simple_prepare(mysql, "INSERT INTO t_long_data VALUES(?, ?)");
  check_stmt(stmt);

  verify_param_count(stmt, 2);

  memset(my_bind, 0, sizeof(my_bind));

  my_bind[0].buffer = (void *)&int_data;
  my_bind[0].buffer_type = MYSQL_TYPE_LONG;

  my_bind[1].buffer = str_data;
  my_bind[1].buffer_type = MYSQL_TYPE_STRING;

  rc = mysql_stmt_bind_named_param(stmt, my_bind, std::size(my_bind), nullptr);
  check_execute(stmt, rc);

  int_data = 1;

  // The vector arrives split across packets.
  rc = mysql_stmt_send_long_data(stmt, 1, "[1,2", 4);
  check_execute(stmt, rc);
  rc = mysql_stmt_send_long_data(stmt, 1, ",3]", 3);
  check_execute(stmt, rc);

  rc = mysql_stmt_execute(stmt);
  check_execute(stmt, rc);

  mysql_stmt_close(stmt);

  // The row reads back, so the streamed bytes were encoded as the type.
  rc = mysql_query(mysql, "SELECT id, vec FROM t_long_data");
  myquery(rc);

  result = mysql_store_result(mysql);
  mytest(result);

  rc = my_process_result_set(result);
  DIE_UNLESS(rc == 1);
  mysql_free_result(result);

  rc = mysql_query(mysql, "DROP TABLE t_long_data");
  myquery(rc);

  rc = mysql_query(mysql, "UNINSTALL EXTENSION vsql_tvector");
  myquery(rc);
}

static struct my_tests_st my_tests[] = {
    {"test_custom_type_long_data", test_custom_type_long_data},
    {nullptr, nullptr}};

static struct my_tests_st *get_my_tests() { return my_tests; }
