/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

// Layout of the unstable protocol in the dev headers
// (villagesql/sdk/include/villagesql/abi/types.h): the fields added since the
// newest stable protocol, and the current size of every struct that a stable
// list allows to grow.  Included by abi_dev_check-t.cc with exact sizes.  See
// abi_layout_check.h.
//
// When you add a field to the dev ABI, add it here and update its struct's
// size.  When the unstable protocol is promoted to stable version N, this
// file becomes abi_vN_layout.inc, abi_vN_check-t.cc checks it against the
// stable_sdk/vN snapshot, and a new, empty dev list replaces it.

#define ABI_LIST "dev"

ABI_SIZE(vef_context_t, 4);
ABI_SIZE(vef_register_arg_t, 56);
ABI_SIZE(vef_unregister_arg_t, 4);
ABI_SIZE(vef_inferred_type_params_t, 32);
ABI_SIZE(vef_vdf_args_t, 24);
ABI_SIZE(vef_signature_t, 32);
ABI_SIZE(vef_prerun_result_t, 32);
ABI_SIZE(vef_postrun_args_t, 8);

ABI_SIZE(vef_invalue_t, 48);
ABI_FIELD(vef_invalue_t, binary_len, 8);
ABI_FIELD(vef_invalue_t, binary_value, 16);

ABI_SIZE(vef_vdf_result_t, 80);
ABI_FIELD(vef_vdf_result_t, binary_buf, 24);
ABI_FIELD(vef_vdf_result_t, max_binary_len, 32);
ABI_FIELD(vef_vdf_result_t, alt_binary_buf, 40);

ABI_SIZE(vef_prerun_args_t, 40);
ABI_FIELD(vef_prerun_args_t, const_args, 32);

ABI_SIZE(vef_postrun_result_t, 16);
ABI_FIELD(vef_postrun_result_t, type, 0);
ABI_FIELD(vef_postrun_result_t, error_msg, 8);

ABI_SIZE(vef_bind_types_args_t, 40);
ABI_FIELD(vef_bind_types_args_t, arg_count, 0);
ABI_FIELD(vef_bind_types_args_t, arg_types, 8);
ABI_FIELD(vef_bind_types_args_t, const_values, 16);
ABI_FIELD(vef_bind_types_args_t, const_lengths, 24);
ABI_FIELD(vef_bind_types_args_t, arg_params, 32);

ABI_SIZE(vef_bind_types_result_t, 40);
ABI_FIELD(vef_bind_types_result_t, type, 0);
ABI_FIELD(vef_bind_types_result_t, error_msg, 8);
ABI_FIELD(vef_bind_types_result_t, out_return_params, 16);
ABI_FIELD(vef_bind_types_result_t, out_arg_params, 24);
ABI_FIELD(vef_bind_types_result_t, out_arg_types, 32);

ABI_SIZE(vef_func_desc_t, 96);
ABI_FIELD(vef_func_desc_t, max_result_length, 80);
ABI_FIELD(vef_func_desc_t, bind_and_check_types, 88);

ABI_SIZE(vef_type_desc_t, 152);
ABI_FIELD(vef_type_desc_t, variable_length, 136);
ABI_FIELD(vef_type_desc_t, from_binary_vdf_name, 144);

ABI_SIZE(vef_registration_t, 120);
ABI_FIELD(vef_registration_t, on_init, 104);
ABI_FIELD(vef_registration_t, on_deinit, 112);

#undef ABI_LIST
