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

#ifndef VILLAGESQL_VEB_VEB_FILE_H_
#define VILLAGESQL_VEB_VEB_FILE_H_

#include <set>
#include <string>

#include "villagesql/sdk/include/villagesql/abi/types.h"

class THD;

namespace villagesql {
namespace veb {

// Get full path to a file/directory within veb_dir
// e.g., get_veb_path("foo.veb") → "/usr/local/mysql/lib/veb/foo.veb"
// Returns empty string on error
std::string get_veb_path(const std::string &filename);

// Calculate SHA256 hash of a file, return as 64-character hex string
// Uses SHA_EVP256() from MySQL's sha2.h
// Returns false on success, true on error
bool calculate_file_sha256(const std::string &filepath, std::string &hash_hex);

// Load manifest.json from a VEB file and extract the "version" field
// Opens {name}.veb as a tar archive using libarchive
// Parses manifest.json using RapidJSON
// Returns false on success, true on error
// On success, version is populated with the extension version
bool load_veb_manifest(const std::string &name, std::string &version);

// Expand VEB archive to directory:
//   {datadir}/.veb_expansion_cache/{name}/{sha256}/
//
// Directory structure created:
//   .veb_expansion_cache/
//     my_extension/
//       abc123def.../        (SHA256 of my_extension.veb)
//         manifest.json
//         lib/
//           my_extension.so
//
// If .veb_expansion_cache/{name}/{sha256}/ already exists, skips extraction
// Returns false on success, true on error
// On success, expanded_path contains full path and sha256_hash contains hash
bool expand_veb_to_directory(const std::string &name,
                             std::string &expanded_path,
                             std::string &sha256_hash);

// Load all installed extensions from villagesql.extensions table
// Called during server startup after VictionaryClient initialization
// For each extension:
//   - Validates the .veb file exists
//   - Validates manifest.json version matches database
//   - Re-expands archive if .so is missing from expansion cache
//   - Does NOT execute SQL
// After loading all extensions, calls cleanup_orphaned_expansion_directories()
// Returns false on success, true on error
bool load_installed_extensions(THD *thd);

// Remove orphaned expansion directories
// Scans .veb_expansion_cache/{name}/ subdirectories for each extension
// Removes SHA256 subdirectories if the extension is not in installed_extensions
// set
// Called during startup loading
void cleanup_orphaned_expansion_directories(
    const std::set<std::string> &installed_extensions);

// The highest VEF protocol version this server build supports.
// Exposed as the @@villagesql_vef_server_protocol system variable.
extern const vef_protocol_t vef_server_protocol_version;

struct ExtensionRegistration {
  vef_registration_t *registration;

  // Negotiated protocol: min(server_protocol, extension_protocol).
  // Set during load_vef_extension; used for all VDF calls and unregistration.
  vef_protocol_t negotiated_protocol{VEF_PROTOCOL_1};

  std::string so_path;
  void *dlhandle;
  vef_unregister_func_t unregister_func;
};

// Register type descriptors from an extension. This creates
// TypeDescriptors in the victionary for each type defined by the
// extension.
// REQUIRES: Caller must hold victionary write lock.
// Returns false on success, true on error.
bool register_types_from_extension(THD &thd, const std::string &extension_name,
                                   const std::string &extension_version,
                                   const ExtensionRegistration &ext_reg);

// Register VDFs (VillageSQL Defined Functions) from an extension.
// REQUIRES: Caller must hold victionary write lock.
// Returns false on success, true on error.
bool register_funcs_from_extension(THD &thd, const std::string &extension_name,
                                   const std::string &extension_version,
                                   const ExtensionRegistration &ext_reg);

// Load a VEF extension from a .so file and get the registration.
//
// The caller is responsible for calling unload_vef_extension() when done.
//
// Returns false on success, true on error.
// On success, all fields in registration are populated.
// On error, a message is written to error_out so that it can be logged, and/or
// returned to the client.
bool load_vef_extension(const std::string &so_path,
                        std::string_view expected_name,
                        ExtensionRegistration &registration,
                        vef_protocol_t max_protocol,
                        std::string &error_message);

// Unload a VEF extension .so file
//
// After this call, the ExtensionRegistration should not be used.
void unload_vef_extension(const ExtensionRegistration &registration);

// Get the path to the .so file for an extension
// Uses the convention:
// {datadir}/.veb_expansion_cache/{name}/{sha256}/lib/{name}.so Returns empty
// string if path cannot be constructed
std::string get_extension_so_path(const std::string &extension_name,
                                  const std::string &sha256);

}  // namespace veb
}  // namespace villagesql

#endif  // VILLAGESQL_VEB_VEB_FILE_H_
