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

// Test extension that sets clear but not accumulate on a VDF.
// This should be rejected at extension registration time.
//
// Uses VEF_GENERATE_REGISTRATION to get the registration helper, then patches
// the func descriptor to set only clear (bypassing the builder's compile-time
// check).

#include <villagesql/extension.h>

void dummy_clear(vef_context_t *ctx, vef_vdf_args_t *args) {}

void dummy_result(vef_context_t *ctx, vef_vdf_args_t *args,
                  vef_vdf_result_t *out) {
  out->int_value = 0;
  out->type = VEF_RESULT_VALUE;
}

VEF_GENERATE_REGISTRATION(make_extension().func(
    make_func<&dummy_result>("bad_agg").returns(INT).param(INT).build()))

// Override vef_register to patch the descriptor after the builder runs.
// The builder's compile-time check prevents setting clear without accumulate,
// so we must bypass it to test that the server rejects this at load time.
extern "C" vef_registration_t *vef_register(vef_register_arg_t *arg) {
  auto *reg = _vef_do_register(arg);
  if (reg->funcs != nullptr && reg->func_count > 0) {
    reg->funcs[0]->clear = &dummy_clear;
  }
  return reg;
}

extern "C" void vef_unregister(vef_unregister_arg_t *arg,
                               vef_registration_t *reg) {
  (void)arg;
  (void)reg;
}
