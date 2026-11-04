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
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_SERVICES_ABI_SPECIALIZATIONS_H
#define VILLAGESQL_SERVICES_ABI_SPECIALIZATIONS_H

// Everything abi_signature_compute.h needs to fingerprint the capability ABI
// structs that Boost.PFR cannot walk on its own.  Kept out of the SDK so
// extensions never depend on Boost.
//
// Include this header, not abi_signature_compute.h directly, wherever a
// capability ABI type is hashed: a type hashed before its specialization is
// visible gets the PFR walk instead, which fails to compile.

#include <cstddef>

#include "villagesql/sdk/include/villagesql/abi/preview/auth.h"
#include "villagesql/sdk/include/villagesql/abi/preview/index.h"
#include "villagesql/sdk/include/villagesql/abi/preview/keyring.h"
#include "villagesql/sdk/include/villagesql/abi/preview/mysql_services.h"
#include "villagesql/sdk/include/villagesql/abi/preview/ping.h"
#include "villagesql/sdk/include/villagesql/abi/preview/session_var.h"
#include "villagesql/sdk/include/villagesql/abi/preview/sql_query.h"
#include "villagesql/sdk/include/villagesql/abi/preview/statement_event.h"
#include "villagesql/sdk/include/villagesql/abi/preview/status_var.h"
#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"
#include "villagesql/sdk/include/villagesql/abi/preview/sys_var.h"
#include "villagesql/sdk/include/villagesql/abi/preview/thread_worker.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/services/abi_signature_compute.h"

// Server-side handles that extensions only ever hold by pointer.
VEF_ABI_REGISTER_OPAQUE_HANDLE(vef_auth_ctx_t);
VEF_ABI_REGISTER_OPAQUE_HANDLE(vef_session_var_handle);
VEF_ABI_REGISTER_OPAQUE_HANDLE(vef_sql_result_t);
VEF_ABI_REGISTER_OPAQUE_HANDLE(vef_sql_session_t);
VEF_ABI_REGISTER_OPAQUE_HANDLE(vef_storage_arena);
VEF_ABI_REGISTER_OPAQUE_HANDLE(vef_thread_handle_t);

// Structs with an anonymous union.  Each definition carries a comment
// pointing here; a field or union arm added there must be added here too.
//
// The core VDF types are reached from the index_profile capability, whose
// config binds vef_vdf_func_t functions.
VEF_ABI_MANUAL_FIELDS(vef_invalue_v1_t, type, is_null, str_len, str_value,
                      bin_len, bin_value, real_value, int_value);
VEF_ABI_MANUAL_FIELDS(vef_invalue_t, type, is_null, str_len, str_value, bin_len,
                      bin_value, type_params, binary_len, binary_value,
                      real_value, int_value);
VEF_ABI_MANUAL_FIELDS(vef_vdf_args_t, user_data, value_count, values_v1,
                      values);
VEF_ABI_MANUAL_FIELDS(vef_vdf_result_t, type, actual_len, error_msg, str_buf,
                      max_str_len, alt_str_buf, bin_buf, max_bin_len,
                      alt_bin_buf, type_params, binary_buf, max_binary_len,
                      alt_binary_buf, real_value, int_value, out_type_params);

VEF_ABI_MANUAL_FIELDS(vef_session_var_desc_t, name, comment, type, integer,
                      str);
VEF_ABI_MANUAL_FIELDS(vef_status_var_desc_t, name, type, integer_ptr,
                      double_ptr);
VEF_ABI_MANUAL_FIELDS(vef_sys_var_change_t, var_name, type, bool_val, int_val,
                      dbl_val, str_val, enum_val);
VEF_ABI_MANUAL_FIELDS(vef_sys_var_desc_t, name, comment, type, on_change,
                      boolean, integer, dbl, str, enumeration);

#endif  // VILLAGESQL_SERVICES_ABI_SPECIALIZATIONS_H
