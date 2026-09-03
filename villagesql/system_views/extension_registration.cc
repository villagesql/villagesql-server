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

// INFORMATION_SCHEMA.EXTENSION_REGISTRATION fill-type table.
// Exposes in-memory registration metadata for loaded VillageSQL extensions,
// including a full JSON serialization of the vef_registration_t ABI struct.

#include "villagesql/system_views/extension_registration.h"

#include <cstdio>
#include <string>

#include "my_rapidjson_size_t.h"  // IWYU pragma: keep

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "my_sys.h"
#include "sql/field.h"
#include "sql/sql_show.h"
#include "sql/table.h"
#include "villagesql/schema/descriptor/extension_descriptor.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/veb/veb_file.h"

namespace {

// Field indices matching the order in villagesql_extension_registration_fields.
enum {
  FIELD_EXTENSION_NAME,
  FIELD_NEGOTIATED_PROTOCOL,
  FIELD_REGISTRATION_JSON,
};

static const char *type_id_to_str(vef_type_id id) {
  switch (id) {
    case VEF_TYPE_STRING:
      return "STRING";
    case VEF_TYPE_REAL:
      return "REAL";
    case VEF_TYPE_INT:
      return "INT";
    case VEF_TYPE_CUSTOM:
      return "CUSTOM";
  }
  return "UNKNOWN";
}

static void write_type(rapidjson::Writer<rapidjson::StringBuffer> &w,
                       const vef_type_t &t) {
  if (t.id == VEF_TYPE_CUSTOM && t.custom_type)
    w.String(t.custom_type);
  else
    w.String(type_id_to_str(t.id));
}

static std::string registration_to_json(const vef_registration_t *r) {
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> w(buf);

  w.StartObject();

  w.Key("extension_name");
  w.String(r->deprecated_extension_name ? r->deprecated_extension_name : "");

  w.Key("extension_version");
  w.String(r->deprecated_extension_version ? r->deprecated_extension_version
                                           : "");

  std::string sdk_ver = std::to_string(r->sdk_version.major) + "." +
                        std::to_string(r->sdk_version.minor) + "." +
                        std::to_string(r->sdk_version.patch);
  if (r->sdk_version.extra) sdk_ver += std::string("-") + r->sdk_version.extra;
  w.Key("sdk_version");
  w.String(sdk_ver.c_str());

  w.Key("requested_protocol");
  w.Uint(static_cast<unsigned int>(r->protocol));

  w.Key("funcs");
  w.StartArray();
  for (unsigned int i = 0; i < r->func_count; i++) {
    const vef_func_desc_t *f = r->funcs[i];
    w.StartObject();
    w.Key("name");
    w.String(f->name ? f->name : "");
    if (f->signature) {
      w.Key("return_type");
      write_type(w, f->signature->return_type);
      w.Key("params");
      if (f->signature->param_count == VEF_PARAM_VARARGS) {
        w.String("varargs");
      } else {
        w.StartArray();
        for (unsigned int j = 0; j < f->signature->param_count; j++) {
          write_type(w, f->signature->params[j]);
        }
        w.EndArray();
      }
    }
    if (f->protocol >= VEF_PROTOCOL_3) {
      w.Key("deterministic");
      w.Bool(f->deterministic);
      w.Key("is_aggregate");
      w.Bool(f->clear != nullptr && f->accumulate != nullptr);
    }
    w.EndObject();
  }
  w.EndArray();

  w.Key("types");
  w.StartArray();
  for (unsigned int i = 0; i < r->type_count; i++) {
    const vef_type_desc_t *t = r->types[i];
    w.StartObject();
    w.Key("name");
    w.String(t->name ? t->name : "");
    w.Key("persisted_length");
    w.Int64(t->persisted_length);
    w.Key("max_decode_buffer_length");
    w.Int64(t->max_decode_buffer_length);
    if (t->protocol >= VEF_PROTOCOL_4) {
      w.Key("variable_length");
      w.Bool(t->variable_length);
    }
    if (t->protocol >= VEF_PROTOCOL_3) {
      if (t->encode_vdf_name) {
        w.Key("encode_vdf");
        w.String(t->encode_vdf_name);
      }
      if (t->decode_vdf_name) {
        w.Key("decode_vdf");
        w.String(t->decode_vdf_name);
      }
      if (t->compare_vdf_name) {
        w.Key("compare_vdf");
        w.String(t->compare_vdf_name);
      }
      if (t->hash_vdf_name) {
        w.Key("hash_vdf");
        w.String(t->hash_vdf_name);
      }
      if (t->int_to_params_vdf_name) {
        w.Key("int_to_params_vdf");
        w.String(t->int_to_params_vdf_name);
      }
      if (t->resolve_params_vdf_name) {
        w.Key("resolve_params_vdf");
        w.String(t->resolve_params_vdf_name);
      }
      if (t->intrinsic_default_vdf_name) {
        w.Key("intrinsic_default_vdf");
        w.String(t->intrinsic_default_vdf_name);
      }
      if (t->intrinsic_default_str) {
        w.Key("intrinsic_default_str");
        w.String(t->intrinsic_default_str);
      }
    }
    w.EndObject();
  }
  w.EndArray();

  // TODO(villagesql-preview): Serialize per-capability data (sys_vars,
  // status_vars, etc.) into the JSON by adding a to_json callback to
  // CapabilityRegistration. Each capability provides its own serializer;
  // extension_registration.cc iterates required_capabilities, looks up the
  // CapabilityRegistration by name, and calls to_json(capability_config, w) if
  // present. This keeps this file capability-agnostic and automatically
  // includes new capabilities.

  w.EndObject();
  return buf.GetString();
}

}  // namespace

ST_FIELD_INFO villagesql_extension_registration_fields[] = {
    {"EXTENSION_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"NEGOTIATED_PROTOCOL", 10, MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED,
     nullptr, 0},
    {"REGISTRATION_JSON", 65535, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {nullptr, 0, MYSQL_TYPE_STRING, 0, 0, nullptr, 0}};

int fill_extension_registration(THD *thd, Table_ref *tables, Item *) {
  TABLE *table = tables->table;
  CHARSET_INFO *cs = system_charset_info;

  villagesql::VictionaryClient &vclient =
      villagesql::VictionaryClient::instance();
  auto lock = vclient.get_read_lock();

  for (const villagesql::ExtensionDescriptor *desc :
       vclient.extension_descriptors().get_all_committed()) {
    const villagesql::veb::ExtensionRegistration &reg = desc->registration();
    const vef_registration_t *r = reg.registration;

    restore_record(table, s->default_values);

    const std::string &name = desc->extension_name();
    table->field[FIELD_EXTENSION_NAME]->store(name.c_str(), name.length(), cs);

    table->field[FIELD_NEGOTIATED_PROTOCOL]->store(
        static_cast<longlong>(reg.negotiated_protocol), true);

    const std::string json = registration_to_json(r);
    table->field[FIELD_REGISTRATION_JSON]->store(json.c_str(), json.length(),
                                                 cs);

    if (schema_table_store_record(thd, table)) return 1;
  }

  return 0;
}
