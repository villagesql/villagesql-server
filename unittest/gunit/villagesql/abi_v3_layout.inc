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

// Layout of the protocol 3 ABI, as frozen in
// villagesql/stable_sdk/v3/include/villagesql/abi/types.h.  Included by
// abi_v3_check-t.cc and abi_dev_check-t.cc.  See abi_layout_check.h.
//
// Never edit an existing entry: a failure here means a change broke
// extensions compiled against the v3 headers.

#define ABI_LIST "v3"

ABI_SIZE_FIXED(vef_version_t, 24);
ABI_FIELD(vef_version_t, major, 0);
ABI_FIELD(vef_version_t, minor, 4);
ABI_FIELD(vef_version_t, patch, 8);
ABI_FIELD(vef_version_t, extra, 16);

ABI_SIZE(vef_context_t, 4);
ABI_FIELD(vef_context_t, protocol, 0);

ABI_SIZE(vef_register_arg_t, 56);
ABI_FIELD(vef_register_arg_t, protocol, 0);
ABI_FIELD(vef_register_arg_t, mysql_version, 8);
ABI_FIELD(vef_register_arg_t, vef_version, 32);

ABI_SIZE(vef_unregister_arg_t, 4);
ABI_FIELD(vef_unregister_arg_t, protocol, 0);

ABI_SIZE_FIXED(vef_invalue_v1_t, 24);
ABI_FIELD(vef_invalue_v1_t, type, 0);
ABI_FIELD(vef_invalue_v1_t, is_null, 4);
ABI_FIELD(vef_invalue_v1_t, str_len, 8);
ABI_FIELD(vef_invalue_v1_t, str_value, 16);
ABI_FIELD(vef_invalue_v1_t, bin_len, 8);
ABI_FIELD(vef_invalue_v1_t, bin_value, 16);
ABI_FIELD(vef_invalue_v1_t, real_value, 8);
ABI_FIELD(vef_invalue_v1_t, int_value, 8);

// Embedded by value in vef_invalue_t and vef_vdf_result_t.
ABI_SIZE_FIXED(vef_type_params_t, 24);
ABI_FIELD(vef_type_params_t, count, 0);
ABI_FIELD(vef_type_params_t, keys, 8);
ABI_FIELD(vef_type_params_t, values, 16);

ABI_SIZE(vef_inferred_type_params_t, 32);
ABI_FIELD(vef_inferred_type_params_t, buf, 0);
ABI_FIELD(vef_inferred_type_params_t, max_buf_len, 8);
ABI_FIELD(vef_inferred_type_params_t, actual_len, 16);
ABI_FIELD(vef_inferred_type_params_t, overflow, 24);

// Passed as an array of pointers, so it may grow.
ABI_SIZE(vef_invalue_t, 48);
ABI_FIELD(vef_invalue_t, type, 0);
ABI_FIELD(vef_invalue_t, is_null, 4);
ABI_FIELD(vef_invalue_t, str_len, 8);
ABI_FIELD(vef_invalue_t, str_value, 16);
ABI_FIELD(vef_invalue_t, bin_len, 8);
ABI_FIELD(vef_invalue_t, bin_value, 16);
ABI_FIELD(vef_invalue_t, type_params, 24);
ABI_FIELD(vef_invalue_t, real_value, 8);
ABI_FIELD(vef_invalue_t, int_value, 8);

ABI_SIZE(vef_vdf_result_t, 80);
ABI_FIELD(vef_vdf_result_t, type, 0);
ABI_FIELD(vef_vdf_result_t, actual_len, 8);
ABI_FIELD(vef_vdf_result_t, error_msg, 16);
ABI_FIELD(vef_vdf_result_t, str_buf, 24);
ABI_FIELD(vef_vdf_result_t, max_str_len, 32);
ABI_FIELD(vef_vdf_result_t, alt_str_buf, 40);
ABI_FIELD(vef_vdf_result_t, bin_buf, 24);
ABI_FIELD(vef_vdf_result_t, max_bin_len, 32);
ABI_FIELD(vef_vdf_result_t, alt_bin_buf, 40);
ABI_FIELD(vef_vdf_result_t, type_params, 48);
ABI_FIELD(vef_vdf_result_t, real_value, 24);
ABI_FIELD(vef_vdf_result_t, int_value, 24);
ABI_FIELD(vef_vdf_result_t, out_type_params, 72);

ABI_SIZE(vef_vdf_args_t, 24);
ABI_FIELD(vef_vdf_args_t, user_data, 0);
ABI_FIELD(vef_vdf_args_t, value_count, 8);
ABI_FIELD(vef_vdf_args_t, values_v1, 16);
ABI_FIELD(vef_vdf_args_t, values, 16);

ABI_SIZE_FIXED(vef_type_t, 16);
ABI_FIELD(vef_type_t, id, 0);
ABI_FIELD(vef_type_t, custom_type, 8);

ABI_SIZE(vef_signature_t, 32);
ABI_FIELD(vef_signature_t, param_count, 0);
ABI_FIELD(vef_signature_t, params, 8);
ABI_FIELD(vef_signature_t, return_type, 16);

ABI_SIZE(vef_prerun_args_t, 32);
ABI_FIELD(vef_prerun_args_t, arg_count, 0);
ABI_FIELD(vef_prerun_args_t, arg_types, 8);
ABI_FIELD_RENAMED(vef_prerun_args_t, const_values, deprecated_const_values,
                  16);
ABI_FIELD_RENAMED(vef_prerun_args_t, const_lengths, deprecated_const_lengths,
                  24);

ABI_SIZE(vef_prerun_result_t, 32);
ABI_FIELD(vef_prerun_result_t, type, 0);
ABI_FIELD(vef_prerun_result_t, error_msg, 8);
ABI_FIELD(vef_prerun_result_t, result_buffer_size, 16);
ABI_FIELD(vef_prerun_result_t, user_data, 24);

ABI_SIZE(vef_postrun_args_t, 8);
ABI_FIELD(vef_postrun_args_t, user_data, 0);

ABI_SIZE(vef_func_desc_t, 80);
ABI_FIELD(vef_func_desc_t, protocol, 0);
ABI_FIELD(vef_func_desc_t, name, 8);
ABI_FIELD(vef_func_desc_t, signature, 16);
ABI_FIELD(vef_func_desc_t, vdf, 24);
ABI_FIELD(vef_func_desc_t, prerun, 32);
ABI_FIELD(vef_func_desc_t, postrun, 40);
ABI_FIELD(vef_func_desc_t, buffer_size, 48);
ABI_FIELD(vef_func_desc_t, deterministic, 56);
ABI_FIELD(vef_func_desc_t, clear, 64);
ABI_FIELD(vef_func_desc_t, accumulate, 72);

ABI_SIZE(vef_type_desc_t, 136);
ABI_FIELD(vef_type_desc_t, protocol, 0);
ABI_FIELD(vef_type_desc_t, name, 8);
ABI_FIELD(vef_type_desc_t, persisted_length, 16);
ABI_FIELD(vef_type_desc_t, max_decode_buffer_length, 24);
ABI_FIELD(vef_type_desc_t, encode_func, 32);
ABI_FIELD(vef_type_desc_t, decode_func, 40);
ABI_FIELD(vef_type_desc_t, compare_func, 48);
ABI_FIELD(vef_type_desc_t, hash_func, 56);
ABI_FIELD(vef_type_desc_t, encode_vdf_name, 64);
ABI_FIELD(vef_type_desc_t, decode_vdf_name, 72);
ABI_FIELD(vef_type_desc_t, compare_vdf_name, 80);
ABI_FIELD(vef_type_desc_t, hash_vdf_name, 88);
ABI_FIELD(vef_type_desc_t, int_to_params_vdf_name, 96);
ABI_FIELD(vef_type_desc_t, resolve_params_vdf_name, 104);
ABI_FIELD(vef_type_desc_t, intrinsic_default_vdf_name, 112);
ABI_FIELD(vef_type_desc_t, intrinsic_default_str, 120);
ABI_FIELD(vef_type_desc_t, max_persisted_length, 128);

// Laid out as an array.
ABI_SIZE_FIXED(vef_required_capability_t, 40);
ABI_FIELD(vef_required_capability_t, name, 0);
ABI_FIELD(vef_required_capability_t, vtable_hash, 8);
ABI_FIELD(vef_required_capability_t, vtable_dest, 16);
ABI_FIELD(vef_required_capability_t, capability_config_hash, 24);
ABI_FIELD(vef_required_capability_t, capability_config, 32);

ABI_SIZE(vef_registration_t, 104);
ABI_FIELD(vef_registration_t, protocol, 0);
ABI_FIELD(vef_registration_t, error_msg, 8);
ABI_FIELD(vef_registration_t, deprecated_extension_version, 16);
ABI_FIELD(vef_registration_t, sdk_version, 24);
ABI_FIELD(vef_registration_t, deprecated_extension_name, 48);
ABI_FIELD(vef_registration_t, func_count, 56);
ABI_FIELD(vef_registration_t, funcs, 64);
ABI_FIELD(vef_registration_t, type_count, 72);
ABI_FIELD(vef_registration_t, types, 80);
ABI_FIELD(vef_registration_t, required_capability_count, 88);
ABI_FIELD(vef_registration_t, required_capabilities, 96);

#undef ABI_LIST
