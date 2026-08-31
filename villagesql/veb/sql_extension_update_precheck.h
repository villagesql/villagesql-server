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

#ifndef VILLAGESQL_VEB_SQL_EXTENSION_UPDATE_PRECHECK_H_
#define VILLAGESQL_VEB_SQL_EXTENSION_UPDATE_PRECHECK_H_

#include <cstdint>
#include <string>
#include <vector>

namespace villagesql {

class VictionaryClient;

namespace veb {

// Pure pre-check entry point for the extension version-update path.
//
// The function owns the entire target-side load: it dlopens the target .so,
// calls vef_register, inspects the returned registration, runs all
// compatibility checks against the supplied current-state snapshot, and
// unloads the target before returning. It does NOT run any capability
// populate hooks against the live server -- the dlopen+vef_register cycle
// is the only thing that runs against the target binary in this process.
//
// Purity contract (load-bearing):
//   - No THD, no victionary handle, no globals beyond what is in the input.
//   - No logging, no SQL-error emission, no catalog access.
//   - Side effects are limited to dlopen / vef_register / vef_unregister /
//     dlclose on the target .so. No capability populate or depopulate.
//
// This shape is what lets the function relocate to a subprocess later
// without changing its body. In v2, a helper binary's main() would
// deserialize UpdatePreCheckInput from stdin, call this function, and
// serialize UpdatePreCheckResult back to stdout. The v1 in-process caller
// stays as-is.
//
// Capability on_check_update hooks are NOT invoked from here; they require
// an ABI shape that does not depend on THD or live catalog handles. See
// Docs/EXTENSION_UPDATE_AT_RESTART.md "Pre-Check API Shape" for the planned
// hook contract.
//
// TODO(villagesql-general): relocate the precheck out of the live server
// process. Phase 1 (this file) runs the dlopen + vef_register harvest
// in-process, which exposes the live server to static initializers and
// any side effects in the target .so. Phase 2 is a subprocess: the parent
// re-execs mysqld with a pre-check-mode flag, the child does the harvest
// under seccomp + rlimits + privilege drop, writes a framed result to a
// pipe, and exits. Phase 3 swaps the re-exec'd mysqld for a dedicated
// mysqld-vef-precheck helper binary for faster spawn. See
// Docs/VEF_PRECHECK_SUBPROCESS_DESIGN.md for the full plan.

struct CurrentTypeSnapshot {
  std::string type_name;
  int64_t persisted_length{0};
};

struct DependentColumnSnapshot {
  std::string db_name;
  std::string table_name;
  std::string column_name;
  std::string type_name;
};

struct DependentSpParamSnapshot {
  std::string db_name;
  std::string sp_name;
  std::string param_name;
  std::string type_name;
};

struct UpdatePreCheckInput {
  // Extension identity.
  std::string extension_name;
  std::string current_version;
  std::string target_version;

  // Filesystem path to the target .so. The caller resolves the VEB and
  // hands the path in; the precheck dlopens it.
  std::string target_so_path;

  // Highest VEF protocol the server supports. The precheck passes this to
  // vef_register; the negotiated protocol is min(server, target). Caller
  // reads this from villagesql::veb::vef_server_protocol_version.
  int server_protocol{0};

  // Current types provided by the installed extension version.
  std::vector<CurrentTypeSnapshot> current_types;

  // Columns and SP params that currently depend on this extension's types.
  // The dropped-type check fails if any dependent references a type that
  // is not present in the loaded target registration.
  std::vector<DependentColumnSnapshot> dependent_columns;
  std::vector<DependentSpParamSnapshot> dependent_sp_params;

  // TODO(villagesql-general): the apply path rewrites custom_indexes rows
  // to the target version unconditionally, but this precheck does not
  // verify that every in-use index_type / index_profile survives in the
  // target registration (analogous to the dropped-type check for
  // columns). Without that check, an apply can leave custom_indexes
  // rows pointing at a version that no longer defines their index_type
  // -- catalog corruption. Add a DependentIndexSnapshot list and a
  // dropped-index-type / dropped-index-profile check before beta.
};

struct UpdatePreCheckResult {
  bool ok{false};
  // Operator-facing error message. Empty when ok is true.
  std::string error_message;
};

// Run the pre-checks against the supplied snapshot. Returns a structured
// verdict. See file header comment for the purity contract.
//
// TODO(villagesql-preview): the target .so is dlopened in the live server
// here. vef_register itself is observably side-effect-free for a well-
// behaved extension (no capability populate is invoked), but static
// initializers in the target binary still run in this process. The planned
// v2 mitigation runs this function in a separate process so the target
// .so never touches the live server at all. The function signature is
// already shaped to make that lift mechanical -- the input struct is the
// wire format. See Docs/EXTENSION_UPDATE_AT_RESTART.md "Pre-Check API
// Shape: Subprocess-Ready" and "Extension Author Contract".
UpdatePreCheckResult RunUpdatePreCheck(const UpdatePreCheckInput &input);

// Populate `input` from the victionary: sets extension_name / current_version
// / target_version / target_so_path / server_protocol, then walks the
// type_descriptors, columns, and sp_params committed sets to populate the
// three dependent snapshots.
//
// The caller is responsible for holding the victionary read lock (or a write
// lock) across the call. The caller resolves the target VEB and constructs
// the .so path; this helper does not touch the filesystem.
void BuildUpdatePreCheckSnapshot(const VictionaryClient &victionary,
                                 const std::string &extension_name,
                                 const std::string &current_version,
                                 const std::string &target_version,
                                 std::string target_so_path,
                                 UpdatePreCheckInput *input);

}  // namespace veb
}  // namespace villagesql

#endif  // VILLAGESQL_VEB_SQL_EXTENSION_UPDATE_PRECHECK_H_
