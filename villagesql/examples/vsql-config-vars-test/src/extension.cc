// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// VillageSQL extension for testing config variable registration.
// Declares one INT variable and one STRING variable with known defaults,
// plus VDFs for reading and writing any system variable via the context API.

#include <villagesql/extension.h>

static long long g_max_items = 100;
static char *g_label = nullptr;

// Returns the global value of a system variable as a string.
// Arguments: component_name (e.g. "vsql_foo" or "mysql_server" for built-ins),
//            variable_name (e.g. "my_var" or "max_connections").
// Returns STRING (the current value) or NULL on error.
void get_variable_impl(vef_context_t *ctx, vef_invalue_t *component_arg,
                       vef_invalue_t *name_arg, vef_vdf_result_t *out) {
  void *val = out->str_buf;
  size_t val_len = out->max_str_len;

  if (component_arg->is_null || name_arg->is_null ||
      ctx->get_variable(ctx, component_arg->str_value, name_arg->str_value,
                        &val, &val_len)) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  out->actual_len = val_len;
}

// Sets a system variable. Returns 0 on success, 1 on error.
// Arguments: component_name, variable_name (same as get_variable),
//            scope ("GLOBAL", "PERSIST", "PERSIST_ONLY", or NULL for GLOBAL),
//            value as string.
void set_variable_impl(vef_context_t *ctx, vef_invalue_t *component_arg,
                       vef_invalue_t *name_arg, vef_invalue_t *scope_arg,
                       vef_invalue_t *value_arg, vef_vdf_result_t *out) {
  if (component_arg->is_null || name_arg->is_null || value_arg->is_null ||
      ctx->set_variable == nullptr) {
    out->int_value = 1;
    return;
  }

  const char *scope = scope_arg->is_null ? nullptr : scope_arg->str_value;

  out->int_value = 0;
  if (ctx->set_variable(ctx, component_arg->str_value, name_arg->str_value,
                        scope, value_arg->str_value))
    out->int_value = 1;
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension("vsql_config_vars_test", "0.0.1")
        .config_var(make_config_var_int("max_items",
                                        "Maximum number of items to process",
                                        &g_max_items, 100, 0, 1000000))
        .config_var(make_config_var_str("label",
                                        "A label string for this extension",
                                        &g_label, "default_label"))
        .func(make_func<&get_variable_impl>("get_variable")
                  .returns(STRING)
                  .buffer_size(512)
                  .param(STRING)
                  .param(STRING)
                  .build())
        .func(make_func<&set_variable_impl>("set_variable")
                  .returns(INT)
                  .param(STRING)
                  .param(STRING)
                  .param(STRING)
                  .param(STRING)
                  .build()))
