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

#include "villagesql/veb/veb_file.h"

#include <dirent.h>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string_view>
#include <system_error>

#include "my_config.h"
#include "my_dir.h"
#include "my_sharedlib.h"
#include "my_sys.h"
#include "mysqld_error.h"
#include "scope_guard.h"
#include "sha2.h"
#include "sql/auth/auth_common.h"
#include "sql/field.h"
#include "sql/iterators/row_iterator.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_udf.h"
#include "sql/table.h"
#include "sql_string.h"
#include "villagesql/include/error.h"
#include "villagesql/include/version.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/veb/sql_extension.h"

#include <archive.h>
#include <archive_entry.h>

// RapidJSON for manifest parsing
// IMPORTANT: my_rapidjson_size_t.h must come BEFORE rapidjson headers
// clang-format off
#include "my_rapidjson_size_t.h"
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/error/error.h>
// clang-format on

namespace villagesql {
namespace veb {

std::string get_extension_so_path(const std::string &extension_name,
                                  const std::string &sha256) {
  // Construct path: _expanded/{name}/{sha256}/lib/{name}.so
  std::string expanded_base = get_veb_path("_expanded");
  if (expanded_base.empty()) {
    return "";
  }

  char path_buf[FN_REFLEN];

  // _expanded/{name}/
  fn_format(path_buf, extension_name.c_str(), expanded_base.c_str(), "", 0);
  std::string name_dir(path_buf);

  // _expanded/{name}/{sha256}/
  fn_format(path_buf, sha256.c_str(), name_dir.c_str(), "", 0);
  std::string sha_dir(path_buf);

  // _expanded/{name}/{sha256}/lib/
  fn_format(path_buf, "lib", sha_dir.c_str(), "", 0);
  std::string lib_dir(path_buf);

  // TODO(villagesql-windows): should be .dll on windows.
  // _expanded/{name}/{sha256}/lib/{name}.so
  std::string so_filename = extension_name + ".so";
  fn_format(path_buf, so_filename.c_str(), lib_dir.c_str(), "", 0);

  return std::string(path_buf);
}

// Helper to format error messages like "manifest.json" inside "foo.veb"
static void format_archive_file_path(char *buffer, size_t buffer_size,
                                     const char *filename,
                                     const char *archive_name) {
  snprintf(buffer, buffer_size, "\"%s\" inside \"%s\"", filename, archive_name);
}

std::string get_veb_path(const std::string &filename) {
  char path_buffer[FN_REFLEN];
  char dir_buffer[FN_REFLEN];

  // Use the configured VEB directory
  strncpy(dir_buffer, opt_veb_dir, FN_REFLEN - 1);
  dir_buffer[FN_REFLEN - 1] = '\0';

  // Construct the full file path
  if (fn_format(path_buffer, filename.c_str(), dir_buffer, "",
                MY_RELATIVE_PATH | MY_UNPACK_FILENAME | MY_SAFE_PATH)) {
    return std::string(path_buffer);
  }

  LogVSQL(ERROR_LEVEL, "Failed to format VEB path for %s", filename.c_str());
  return "";
}

bool calculate_file_sha256(const std::string &filepath, std::string &hash_hex) {
  // Read entire file into memory
  std::ifstream file(filepath, std::ios::binary);
  if (!file.is_open()) {
    LogVSQL(ERROR_LEVEL, "Failed to open file for SHA256: %s",
            filepath.c_str());
    return true;
  }

  std::string content((std::istreambuf_iterator<char>(file)),
                      std::istreambuf_iterator<char>());
  file.close();

  // Calculate SHA256 hash
  unsigned char hash[SHA256_DIGEST_LENGTH];  // 32 bytes
  SHA_EVP256(reinterpret_cast<const unsigned char *>(content.data()),
             content.size(), hash);

  // Convert to hex string
  char hex_str[65];  // 64 hex chars + null terminator
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    snprintf(&hex_str[i * 2], 3, "%02x", hash[i]);
  }
  hash_hex = std::string(hex_str);

  return false;
}

bool load_veb_manifest(const std::string &name, std::string &version) {
  LogVSQL(INFORMATION_LEVEL, "Loading VEB manifest for extension '%s'",
          name.c_str());

  // Construct VEB filename
  std::string veb_filename = name + ".veb";
  std::string full_path = get_veb_path(veb_filename);

  if (full_path.empty()) {
    villagesql_error("Cannot locate VEB file for '%s'", MYF(0), name.c_str());
    return true;
  }

  // Check if file exists
  MY_STAT file_stat;
  if (!my_stat(full_path.c_str(), &file_stat, MYF(0))) {
    villagesql_error("VEB file not found: %s", MYF(0), veb_filename.c_str());
    return true;
  }

  // Open archive for reading
  struct archive *a = archive_read_new();
  if (!a) {
    villagesql_error("Failed to initialize archive reader", MYF(0));
    return true;
  }

  archive_read_support_filter_all(a);
  archive_read_support_format_tar(a);

  int r = archive_read_open_filename(a, full_path.c_str(), 10240);
  if (r != ARCHIVE_OK) {
    villagesql_error("Cannot open VEB file '%s': %s", MYF(0),
                     veb_filename.c_str(), archive_error_string(a));
    archive_read_free(a);
    return true;
  }

  // Search for manifest.json in the archive
  bool manifest_found = false;
  std::string manifest_content;
  struct archive_entry *entry;

  while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
    const char *pathname = archive_entry_pathname(entry);

    if (strcmp(pathname, "manifest.json") == 0) {
      manifest_found = true;
      int64_t size = archive_entry_size(entry);
      manifest_content.resize(size);

      ssize_t bytes_read = archive_read_data(a, &manifest_content[0], size);

      if (bytes_read < 0) {
        char error_path[512];
        format_archive_file_path(error_path, sizeof(error_path),
                                 "manifest.json", veb_filename.c_str());
        villagesql_error("Failed to read %s", MYF(0), error_path);
        archive_read_free(a);
        return true;
      } else if (bytes_read != size) {
        char error_path[512];
        format_archive_file_path(error_path, sizeof(error_path),
                                 "manifest.json", veb_filename.c_str());
        villagesql_error("Incomplete read of %s", MYF(0), error_path);
        archive_read_free(a);
        return true;
      }
      break;
    }
    archive_read_data_skip(a);
  }

  archive_read_free(a);

  if (!manifest_found) {
    villagesql_error("manifest.json not found in VEB file '%s'", MYF(0),
                     veb_filename.c_str());
    return true;
  }

  // Parse JSON manifest
  rapidjson::Document manifest;
  manifest.Parse(manifest_content.c_str());

  if (manifest.HasParseError()) {
    villagesql_error("Failed to parse manifest.json in '%s': %s at offset %zu",
                     MYF(0), veb_filename.c_str(),
                     rapidjson::GetParseError_En(manifest.GetParseError()),
                     manifest.GetErrorOffset());
    return true;
  }

  // Extract version field
  if (!manifest.IsObject() || !manifest.HasMember("version")) {
    villagesql_error("manifest.json in '%s' missing 'version' field", MYF(0),
                     veb_filename.c_str());
    return true;
  }

  const rapidjson::Value &version_value = manifest["version"];
  if (!version_value.IsString()) {
    villagesql_error("'version' field in manifest.json must be a string",
                     MYF(0));
    return true;
  }

  version = version_value.GetString();

  // Validate name field
  if (!manifest.HasMember("name")) {
    villagesql_error("manifest.json in '%s' missing 'name' field", MYF(0),
                     veb_filename.c_str());
    return true;
  }

  const rapidjson::Value &name_value = manifest["name"];
  if (!name_value.IsString()) {
    villagesql_error("'name' field in manifest.json must be a string", MYF(0));
    return true;
  }

  std::string manifest_name = name_value.GetString();

  // TODO(villagesql-beta): Consider relaxing this requirement to allow VEB
  // filename to differ from manifest name.
  // Validate manifest name matches expected extension name (VEB basename)
  if (manifest_name != name) {
    villagesql_error("Manifest name '%s' does not match VEB basename '%s'",
                     MYF(0), manifest_name.c_str(), name.c_str());
    return true;
  }

  LogVSQL(INFORMATION_LEVEL, "Extension '%s' has version '%s'", name.c_str(),
          version.c_str());

  return false;
}

bool expand_veb_to_directory(const std::string &name,
                             std::string &expanded_path,
                             std::string &sha256_hash) {
  // Note: Name validation is done by caller (sql_extension.cc) before calling
  // this
  LogVSQL(INFORMATION_LEVEL, "Expanding VEB for extension '%s'", name.c_str());

  // Get VEB file path and calculate SHA256
  std::string veb_filename = name + ".veb";
  std::string full_veb_path = get_veb_path(veb_filename);

  if (full_veb_path.empty()) {
    return true;  // Error already logged
  }

  // Check if VEB file exists
  MY_STAT veb_stat;
  if (!my_stat(full_veb_path.c_str(), &veb_stat, MYF(0))) {
    villagesql_error("VEB file not found: %s", MYF(0), veb_filename.c_str());
    return true;
  }

  // Calculate SHA256 of VEB file
  if (calculate_file_sha256(full_veb_path, sha256_hash)) {
    villagesql_error("Failed to calculate SHA256 for '%s'", MYF(0),
                     veb_filename.c_str());
    return true;
  }

  // Construct expansion path: _expanded/{name}/{sha256}/
  std::string base_path = get_veb_path("_expanded");

  char name_dir_buf[FN_REFLEN];
  fn_format(name_dir_buf, name.c_str(), base_path.c_str(), "", 0);
  std::string name_dir(name_dir_buf);

  char expanded_path_buf[FN_REFLEN];
  fn_format(expanded_path_buf, sha256_hash.c_str(), name_dir.c_str(), "", 0);
  expanded_path = expanded_path_buf;

  LogVSQL(INFORMATION_LEVEL, "Expansion path: %s", expanded_path.c_str());

  // Check if already expanded with this SHA256
  MY_STAT dir_stat;
  if (my_stat(expanded_path.c_str(), &dir_stat, MYF(0)) &&
      MY_S_ISDIR(dir_stat.st_mode)) {
    LogVSQL(INFORMATION_LEVEL,
            "Extension '%s' already expanded at %s, skipping extraction",
            name.c_str(), expanded_path.c_str());
    return false;  // Already expanded, success
  }

  // Create directory structure: _expanded/, _expanded/{name}/,
  // _expanded/{name}/{sha256}/ Create _expanded/ if needed
  if (!my_stat(base_path.c_str(), &dir_stat, MYF(0))) {
    if (my_mkdir(base_path.c_str(), 0755, MYF(0)) != 0) {
      villagesql_error("Failed to create _expanded directory", MYF(0));
      return true;
    }
    LogVSQL(INFORMATION_LEVEL, "Created _expanded directory");
  }

  // Create _expanded/{name}/ if needed
  if (!my_stat(name_dir.c_str(), &dir_stat, MYF(0))) {
    if (my_mkdir(name_dir.c_str(), 0755, MYF(0)) != 0) {
      villagesql_error("Failed to create extension directory for '%s'", MYF(0),
                       name.c_str());
      return true;
    }
    LogVSQL(INFORMATION_LEVEL, "Created directory: %s", name_dir.c_str());
  }

  // Create _expanded/{name}/{sha256}/
  if (my_mkdir(expanded_path.c_str(), 0755, MYF(0)) != 0) {
    villagesql_error("Failed to create SHA256 expansion directory", MYF(0));
    return true;
  }

  // Extract archive to expansion directory using libarchive
  struct archive *a = archive_read_new();
  struct archive *ext = archive_write_disk_new();

  if (!a || !ext) {
    villagesql_error("Failed to initialize archive handlers", MYF(0));
    if (a) archive_read_free(a);
    if (ext) archive_write_free(ext);
    return true;
  }

  archive_read_support_filter_all(a);
  archive_read_support_format_tar(a);
  // Note: We intentionally do NOT use ARCHIVE_EXTRACT_SECURE_SYMLINKS here.
  // That flag prevents extraction when the destination path traverses any
  // symlink in the filesystem, which breaks legitimate setups like tmpfs
  // (used by mysql-test-run.pl --mem). We manually validate symlink targets
  // within the archive in the loop below, which addresses the actual
  // security concern of malicious symlinks in VEB content.
  archive_write_disk_set_options(
      ext, ARCHIVE_EXTRACT_TIME | ARCHIVE_EXTRACT_PERM |
               ARCHIVE_EXTRACT_FFLAGS | ARCHIVE_EXTRACT_SECURE_NODOTDOT);

  int r = archive_read_open_filename(a, full_veb_path.c_str(), 10240);
  if (r != ARCHIVE_OK) {
    villagesql_error("Failed to open VEB archive '%s': %s", MYF(0),
                     veb_filename.c_str(), archive_error_string(a));
    archive_read_free(a);
    archive_write_free(ext);
    return true;
  }

  // Extract all files
  bool extraction_error = false;
  struct archive_entry *entry;

  while ((r = archive_read_next_header(a, &entry)) == ARCHIVE_OK) {
    const char *current_file = archive_entry_pathname(entry);

    // Validate file path (prevent directory traversal attacks)
    // Attack scenario: Archive contains "../../../etc/cron.d/evil" which would
    // write outside the extraction directory. This check prevents such paths.
    // Note: ARCHIVE_EXTRACT_SECURE_NODOTDOT also helps, but we double-check
    // here.
    if (strstr(current_file, "../") != nullptr || current_file[0] == '/') {
      villagesql_error("Suspicious file path in VEB: '%s'", MYF(0),
                       current_file);
      extraction_error = true;
      break;
    }

    // Validate symlink targets to prevent directory escape attacks
    // Attack scenario: Archive contains a symlink at "lib/plugin.so" pointing
    // to
    // "../../system/important_file". When extracted and later deleted during
    // uninstall, we might delete files outside the extraction directory.
    // Even though my_delete() doesn't follow symlinks when deleting, we block
    // dangerous symlinks as defense-in-depth.
    if (archive_entry_filetype(entry) == AE_IFLNK) {
      const char *link_target = archive_entry_symlink(entry);
      if (link_target) {
        // Reject absolute symlinks
        if (link_target[0] == '/') {
          villagesql_error(
              "VEB contains symlink '%s' with absolute target: '%s'", MYF(0),
              current_file, link_target);
          extraction_error = true;
          break;
        }

        // Compute the resolved path of the symlink target
        // Strategy: combine symlink location with target, then normalize
        // Example: symlink at "subdir/link" → "../../etc/passwd"
        //          combined: "subdir/../../etc/passwd"
        //          normalized: "../etc/passwd" (escapes!)

        // Get directory containing the symlink
        char link_dir[FN_REFLEN];
        size_t result_len = 0;
        size_t dir_len = dirname_part(link_dir, current_file, &result_len);
        link_dir[dir_len] = '\0';

        // Combine symlink directory with target
        char combined_path[FN_REFLEN];
        fn_format(combined_path, link_target, link_dir, "", 0);

        // Normalize to collapse ".." sequences
        char normalized_path[FN_REFLEN];
        cleanup_dirname(normalized_path, combined_path);

        // Check if normalized path escapes (starts with ".." or "/")
        if (normalized_path[0] == '/' ||
            (normalized_path[0] == '.' && normalized_path[1] == '.')) {
          villagesql_error(
              "VEB contains symlink '%s' pointing outside extraction directory "
              "(target: '%s', resolves to: '%s')",
              MYF(0), current_file, link_target, normalized_path);
          extraction_error = true;
          break;
        }
      }
    }

    // Construct target path: expanded_path + current_file
    // Use fn_format with MY_RELATIVE_PATH to prepend directory to relative
    // paths.
    // current_file may contain subdirectories (e.g., "lib/simple_udf.so")
    char target_path_buf[FN_REFLEN];
    if (!fn_format(target_path_buf, current_file, expanded_path.c_str(), "",
                   MY_RELATIVE_PATH | MY_SAFE_PATH)) {
      // fn_format returns NULL if path is too long (>512 bytes total or >256
      // bytes filename)
      villagesql_error("Path or filename too long for extraction: %s/%s",
                       MYF(0), expanded_path.c_str(), current_file);
      extraction_error = true;
      break;
    }
    archive_entry_set_pathname(entry, target_path_buf);

    // Write header
    r = archive_write_header(ext, entry);
    if (r != ARCHIVE_OK) {
      villagesql_error("Failed to write header for '%s': %s", MYF(0),
                       current_file, archive_error_string(ext));
      extraction_error = true;
      break;
    }

    // Copy data if it's a regular file
    if (archive_entry_size(entry) > 0) {
      const void *buff;
      size_t size;
      int64_t offset;

      while ((r = archive_read_data_block(a, &buff, &size, &offset)) ==
             ARCHIVE_OK) {
        r = archive_write_data_block(ext, buff, size, offset);
        if (r != ARCHIVE_OK) {
          villagesql_error("Failed to write data for '%s': %s", MYF(0),
                           current_file, archive_error_string(ext));
          extraction_error = true;
          break;
        }
      }

      if (extraction_error) break;
    }

    // Finish the entry
    r = archive_write_finish_entry(ext);
    if (r != ARCHIVE_OK) {
      villagesql_error("Failed to finish entry for '%s': %s", MYF(0),
                       current_file, archive_error_string(ext));
      extraction_error = true;
      break;
    }
  }

  archive_read_free(a);
  archive_write_free(ext);

  if (extraction_error) {
    villagesql_error("VEB expansion failed for '%s'", MYF(0), name.c_str());

    // Clean up partial expansion directory on failure
    // This removes the SHA256 hash subdirectory and parent name directory if
    // empty
    if (!expanded_path.empty()) {
      LogVSQL(INFORMATION_LEVEL, "Cleaning up failed expansion at: %s",
              expanded_path.c_str());
      std::error_code ec;
      std::filesystem::remove_all(expanded_path, ec);
      if (ec) {
        LogVSQL(WARNING_LEVEL,
                "Failed to clean up expansion directory: %s (error: %s)",
                expanded_path.c_str(), ec.message().c_str());
      }

      // Also try to remove parent directory (name dir) if it's now empty
      // Get the parent by going up one level from expanded_path
      char parent_dir[FN_REFLEN];
      size_t parent_len = 0;
      dirname_part(parent_dir, expanded_path.c_str(), &parent_len);
      parent_dir[parent_len] = '\0';
      if (parent_len > 0) {
        rmdir(parent_dir);  // Ignore errors - might not be empty
      }
    }

    return true;
  }

  LogVSQL(INFORMATION_LEVEL, "Successfully expanded '%s' to %s", name.c_str(),
          expanded_path.c_str());
  return false;  // Success
}

bool load_installed_extensions(THD *thd) {
  LogVSQL(INFORMATION_LEVEL,
          "Loading installed extensions from villagesql.extensions table");

  if (!thd) {
    LogVSQL(ERROR_LEVEL, "No THD context for loading extensions");
    return true;
  }

  // Use VictionaryClient to access cached extension data
  VictionaryClient &victionary = VictionaryClient::instance();
  int row_count = -1;
  int success_count = 0;
  std::set<std::string> installed_extensions;

  {
    auto lock_guard = victionary.get_write_lock();

    // Get all committed extensions from cache
    std::vector<const ExtensionEntry *> all_extensions =
        victionary.extensions().get_all_committed();

    row_count = all_extensions.size();

    // Validate and register each extension
    for (const ExtensionEntry *entry : all_extensions) {
      if (!entry) continue;

      const std::string &extension_name = entry->extension_name();
      const std::string &expected_version = entry->extension_version;
      const std::string &sha256 = entry->veb_sha256;

      installed_extensions.insert(extension_name);

      // Validate extension: load manifest and check version matches
      std::string actual_version;
      if (load_veb_manifest(extension_name, actual_version)) {
        LogVSQL(ERROR_LEVEL, "Failed to load VEB manifest for extension '%s'",
                extension_name.c_str());
        return true;
      }

      // Verify version matches
      if (actual_version != expected_version) {
        LogVSQL(ERROR_LEVEL,
                "Extension '%s' version mismatch: database has '%s', manifest "
                "has '%s'",
                extension_name.c_str(), expected_version.c_str(),
                actual_version.c_str());
        return true;
      }

      LogVSQL(INFORMATION_LEVEL, "Validated extension '%s' version '%s'",
              extension_name.c_str(), actual_version.c_str());

      std::string so_path = get_extension_so_path(extension_name, sha256);
      if (so_path.empty()) {
        LogVSQL(ERROR_LEVEL, "Failed to construct .so path for extension '%s'",
                extension_name.c_str());
        return true;
      }

      ExtensionRegistration registration;
      std::string load_error;
      if (load_vef_extension(so_path, extension_name, registration,
                             VEF_PROTOCOL_2, load_error)) {
        LogVSQL(ERROR_LEVEL, "Failed to load VEF extension '%s': %s",
                extension_name.c_str(), load_error.c_str());
        return true;
      }

      if (register_types_from_extension(*thd, extension_name, expected_version,
                                        registration)) {
        LogVSQL(ERROR_LEVEL, "Failed to register types for extension '%s'",
                extension_name.c_str());
        return true;
      }

      if (register_funcs_from_extension(*thd, extension_name, expected_version,
                                        registration)) {
        LogVSQL(ERROR_LEVEL, "Failed to register VDFs for extension '%s'",
                extension_name.c_str());
        return true;
      }

      if (victionary.extension_descriptors().MarkForInsertion(
              *thd, ExtensionDescriptor(ExtensionDescriptorKey(
                                            extension_name, expected_version),
                                        std::move(registration)))) {
        LogVSQL(ERROR_LEVEL, "Failed to register descriptor for extension '%s'",
                extension_name.c_str());
        return true;
      }
      success_count++;

      LogVSQL(INFORMATION_LEVEL,
              "Successfully registered VEF extension '%s' from '%s'",
              extension_name.c_str(), so_path.c_str());
    }
  }

  LogVSQL(INFORMATION_LEVEL, "Validated %d of %d installed extensions",
          success_count, row_count);

  // Clean up orphaned expansion directories
  cleanup_orphaned_expansion_directories(installed_extensions);

  return false;
}

void cleanup_orphaned_expansion_directories(
    const std::set<std::string> &installed_extensions) {
  LogVSQL(INFORMATION_LEVEL, "Cleaning up orphaned expansion directories");

  std::string expanded_base_path = get_veb_path("_expanded");

  // Check if _expanded directory exists
  MY_STAT expanded_stat;
  if (!my_stat(expanded_base_path.c_str(), &expanded_stat, MYF(0)) ||
      !MY_S_ISDIR(expanded_stat.st_mode)) {
    LogVSQL(INFORMATION_LEVEL, "No _expanded directory found");
    return;
  }

  // Open _expanded directory
  DIR *expanded_dir = opendir(expanded_base_path.c_str());
  if (!expanded_dir) {
    LogVSQL(WARNING_LEVEL, "Failed to open _expanded directory");
    return;
  }

  // Scan for extension name directories
  struct dirent *entry;
  int removed_count = 0;

  while ((entry = readdir(expanded_dir)) != nullptr) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }

    std::string extension_name = entry->d_name;

    char name_dir_path_buf[FN_REFLEN];
    fn_format(name_dir_path_buf, extension_name.c_str(),
              expanded_base_path.c_str(), "", 0);
    std::string name_dir_path(name_dir_path_buf);

    // Check if this is a directory
    MY_STAT name_stat;
    if (!my_stat(name_dir_path.c_str(), &name_stat, MYF(0)) ||
        !MY_S_ISDIR(name_stat.st_mode)) {
      continue;
    }

    // If extension is not installed, remove entire {name}/ directory
    if (installed_extensions.find(extension_name) ==
        installed_extensions.end()) {
      LogVSQL(INFORMATION_LEVEL, "Removing orphaned expansion directory: %s",
              name_dir_path.c_str());

      std::error_code ec;
      std::filesystem::remove_all(name_dir_path, ec);
      if (!ec) {
        removed_count++;
      } else {
        LogVSQL(WARNING_LEVEL,
                "Failed to remove orphaned directory: %s (error: %s)",
                name_dir_path.c_str(), ec.message().c_str());
      }
    }
  }

  closedir(expanded_dir);

  if (removed_count > 0) {
    LogVSQL(INFORMATION_LEVEL, "Cleaned up %d orphaned expansion directories",
            removed_count);
  } else {
    LogVSQL(INFORMATION_LEVEL, "No orphaned expansion directories found");
  }
}

// Look up a VDF by name in the extension's funcs[] array.
// Returns nullptr if not found.
static const vef_func_desc_t *find_vdf_by_name(const vef_registration_t &reg,
                                               const char *name) {
  for (unsigned int i = 0; i < reg.func_count; i++) {
    const vef_func_desc_t *fd = reg.funcs[i];
    if (fd != nullptr && fd->name != nullptr && strcmp(fd->name, name) == 0) {
      return fd;
    }
  }
  return nullptr;
}

// Validate a type method VDF name that uses the "::" convention.
// If the name contains "::", checks:
//   - prefix (before "::") matches type_name (case-insensitive)
//   - suffix (after "::") is one of: from_string, to_string, compare, hash
// Returns false on success, true on validation error.
// Names without "::" are always accepted (backward compatibility).
static bool validate_type_method_vdf_name(const char *vdf_name,
                                          const char *type_name,
                                          const char *field_name,
                                          const std::string &extension_name) {
  std::string_view name(vdf_name);
  size_t sep = name.find("::");
  if (sep == std::string_view::npos) {
    // No "::" — plain name, allowed for backward compatibility.
    return false;
  }

  std::string_view prefix = name.substr(0, sep);
  std::string_view suffix = name.substr(sep + 2);

  // Case-insensitive prefix == type_name check.
  std::string_view tname(type_name);
  if (prefix.size() != tname.size()) {
    LogVSQL(ERROR_LEVEL,
            "Type '%s' in extension '%s': %s '%s' uses '::' but prefix '%.*s' "
            "does not match type name (case-insensitive)",
            type_name, extension_name.c_str(), field_name, vdf_name,
            static_cast<int>(prefix.size()), prefix.data());
    return true;
  }
  for (size_t i = 0; i < prefix.size(); i++) {
    if (std::tolower(static_cast<unsigned char>(prefix[i])) !=
        std::tolower(static_cast<unsigned char>(tname[i]))) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s': %s '%s' uses '::' but prefix "
              "'%.*s' does not match type name (case-insensitive)",
              type_name, extension_name.c_str(), field_name, vdf_name,
              static_cast<int>(prefix.size()), prefix.data());
      return true;
    }
  }

  // Suffix must be one of the valid type method names.
  if (suffix != "from_string" && suffix != "to_string" && suffix != "compare" &&
      suffix != "hash") {
    LogVSQL(
        ERROR_LEVEL,
        "Type '%s' in extension '%s': %s '%s' uses '::' but suffix "
        "'%.*s' is not a valid type method (must be from_string, to_string, "
        "compare, or hash)",
        type_name, extension_name.c_str(), field_name, vdf_name,
        static_cast<int>(suffix.size()), suffix.data());
    return true;
  }

  return false;
}

// Validate that a VDF's signature matches the expected pattern for a type
// operation. Returns false on success, true on error.
static bool validate_type_vdf_signature(
    const vef_func_desc_t *fd, const char *op_name, const char *type_name,
    unsigned int expected_param_count, const vef_type_id expected_param_ids[],
    const char *expected_custom_params[], vef_type_id expected_return_id,
    const char *expected_custom_return, const std::string &extension_name) {
  if (fd->signature == nullptr) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' in extension '%s' used as %s for type '%s' has no "
            "signature",
            fd->name, extension_name.c_str(), op_name, type_name);
    return true;
  }

  if (fd->prerun != nullptr || fd->postrun != nullptr) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' used as %s for type '%s' must not have prerun/postrun "
            "hooks",
            fd->name, op_name, type_name);
    return true;
  }

  const vef_signature_t *sig = fd->signature;

  if (sig->param_count != expected_param_count) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' used as %s for type '%s' must have %u parameter(s), "
            "has %u",
            fd->name, op_name, type_name, expected_param_count,
            sig->param_count);
    return true;
  }

  for (unsigned int i = 0; i < expected_param_count; i++) {
    if (sig->params[i].id != expected_param_ids[i]) {
      LogVSQL(ERROR_LEVEL,
              "VDF '%s' used as %s for type '%s': parameter %u has wrong type",
              fd->name, op_name, type_name, i);
      return true;
    }
    if (expected_custom_params[i] != nullptr &&
        (sig->params[i].custom_type == nullptr ||
         strcmp(sig->params[i].custom_type, expected_custom_params[i]) != 0)) {
      LogVSQL(ERROR_LEVEL,
              "VDF '%s' used as %s for type '%s': parameter %u must be "
              "CUSTOM('%s')",
              fd->name, op_name, type_name, i, expected_custom_params[i]);
      return true;
    }
  }

  if (sig->return_type.id != expected_return_id) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' used as %s for type '%s' has wrong return type", fd->name,
            op_name, type_name);
    return true;
  }

  if (expected_custom_return != nullptr &&
      (sig->return_type.custom_type == nullptr ||
       strcmp(sig->return_type.custom_type, expected_custom_return) != 0)) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' used as %s for type '%s' must return CUSTOM('%s')",
            fd->name, op_name, type_name, expected_custom_return);
    return true;
  }

  return false;
}

bool register_types_from_extension(THD &thd, const std::string &extension_name,
                                   const std::string &extension_version,
                                   const ExtensionRegistration &ext_reg) {
  auto &victionary = VictionaryClient::instance();
  victionary.assert_write_lock_held();

  if (ext_reg.registration == nullptr ||
      ext_reg.registration->type_count == 0) {
    LogVSQL(INFORMATION_LEVEL, "No types to register for extension '%s'",
            extension_name.c_str());
    return false;
  }

  const vef_registration_t &reg = *ext_reg.registration;

  LogVSQL(INFORMATION_LEVEL,
          "Registering %d types from extension '%s' version '%s'",
          reg.type_count, extension_name.c_str(), extension_version.c_str());

  for (unsigned int i = 0; i < reg.type_count; i++) {
    const vef_type_desc_t *td = reg.types[i];
    if (td == nullptr || td->name == nullptr) {
      LogVSQL(ERROR_LEVEL,
              "Extension '%s' has NULL type descriptor at index %u",
              extension_name.c_str(), i);
      return true;
    }

    std::string type_name(td->name);

    if (td->max_decode_buffer_length <= 0) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s' must set max_decode_buffer_length",
              type_name.c_str(), extension_name.c_str());
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Registering type '%s' from extension '%s'",
            type_name.c_str(), extension_name.c_str());

    // Resolve VDF name fields (protocol >= VEF_PROTOCOL_2).
    const vef_func_desc_t *encode_vdf = nullptr;
    const vef_func_desc_t *decode_vdf = nullptr;
    const vef_func_desc_t *compare_vdf = nullptr;
    const vef_func_desc_t *hash_vdf = nullptr;
    const vef_func_desc_t *int_to_params_vdf = nullptr;
    const vef_func_desc_t *resolve_params_vdf = nullptr;
    const vef_func_desc_t *intrinsic_default_vdf = nullptr;
    const vef_type_storage_intf_t *storage_intf = nullptr;

    if (td->protocol >= VEF_PROTOCOL_2 &&
        ext_reg.negotiated_protocol >= VEF_PROTOCOL_2) {
      const vef_type_id custom_id[] = {VEF_TYPE_CUSTOM};
      const char *custom_name[] = {td->name};
      const vef_type_id two_custom_ids[] = {VEF_TYPE_CUSTOM, VEF_TYPE_CUSTOM};
      const char *two_custom_names[] = {td->name, td->name};
      const vef_type_id string_id[] = {VEF_TYPE_STRING};
      const char *no_custom[] = {nullptr};

      if (td->encode_func != nullptr && td->encode_vdf_name != nullptr) {
        LogVSQL(ERROR_LEVEL,
                "Type '%s' in extension '%s' sets both encode_func and "
                "encode_vdf_name",
                type_name.c_str(), extension_name.c_str());
        return true;
      }
      if (td->encode_vdf_name != nullptr) {
        if (validate_type_method_vdf_name(td->encode_vdf_name, td->name,
                                          "encode_vdf_name", extension_name)) {
          return true;
        }
        encode_vdf = find_vdf_by_name(reg, td->encode_vdf_name);
        if (encode_vdf == nullptr) {
          LogVSQL(ERROR_LEVEL,
                  "Type '%s' in extension '%s': encode_vdf_name '%s' not found",
                  type_name.c_str(), extension_name.c_str(),
                  td->encode_vdf_name);
          return true;
        }
        if (validate_type_vdf_signature(encode_vdf, "encode", td->name, 1,
                                        string_id, no_custom, VEF_TYPE_CUSTOM,
                                        td->name, extension_name)) {
          return true;
        }
      }

      if (td->decode_func != nullptr && td->decode_vdf_name != nullptr) {
        LogVSQL(ERROR_LEVEL,
                "Type '%s' in extension '%s' sets both decode_func and "
                "decode_vdf_name",
                type_name.c_str(), extension_name.c_str());
        return true;
      }
      if (td->decode_vdf_name != nullptr) {
        if (validate_type_method_vdf_name(td->decode_vdf_name, td->name,
                                          "decode_vdf_name", extension_name)) {
          return true;
        }
        decode_vdf = find_vdf_by_name(reg, td->decode_vdf_name);
        if (decode_vdf == nullptr) {
          LogVSQL(ERROR_LEVEL,
                  "Type '%s' in extension '%s': decode_vdf_name '%s' not found",
                  type_name.c_str(), extension_name.c_str(),
                  td->decode_vdf_name);
          return true;
        }
        if (validate_type_vdf_signature(decode_vdf, "decode", td->name, 1,
                                        custom_id, custom_name, VEF_TYPE_STRING,
                                        nullptr, extension_name)) {
          return true;
        }
      }

      if (td->compare_func != nullptr && td->compare_vdf_name != nullptr) {
        LogVSQL(ERROR_LEVEL,
                "Type '%s' in extension '%s' sets both compare_func and "
                "compare_vdf_name",
                type_name.c_str(), extension_name.c_str());
        return true;
      }
      if (td->compare_vdf_name != nullptr) {
        if (validate_type_method_vdf_name(td->compare_vdf_name, td->name,
                                          "compare_vdf_name", extension_name)) {
          return true;
        }
        compare_vdf = find_vdf_by_name(reg, td->compare_vdf_name);
        if (compare_vdf == nullptr) {
          LogVSQL(ERROR_LEVEL,
                  "Type '%s' in extension '%s': compare_vdf_name '%s' not "
                  "found",
                  type_name.c_str(), extension_name.c_str(),
                  td->compare_vdf_name);
          return true;
        }
        if (validate_type_vdf_signature(
                compare_vdf, "compare", td->name, 2, two_custom_ids,
                two_custom_names, VEF_TYPE_INT, nullptr, extension_name)) {
          return true;
        }
      }

      if (td->hash_func != nullptr && td->hash_vdf_name != nullptr) {
        LogVSQL(ERROR_LEVEL,
                "Type '%s' in extension '%s' sets both hash_func and "
                "hash_vdf_name",
                type_name.c_str(), extension_name.c_str());
        return true;
      }
      if (td->hash_vdf_name != nullptr) {
        if (validate_type_method_vdf_name(td->hash_vdf_name, td->name,
                                          "hash_vdf_name", extension_name)) {
          return true;
        }
        hash_vdf = find_vdf_by_name(reg, td->hash_vdf_name);
        if (hash_vdf == nullptr) {
          LogVSQL(ERROR_LEVEL,
                  "Type '%s' in extension '%s': hash_vdf_name '%s' not found",
                  type_name.c_str(), extension_name.c_str(), td->hash_vdf_name);
          return true;
        }
        if (validate_type_vdf_signature(hash_vdf, "hash", td->name, 1,
                                        custom_id, custom_name, VEF_TYPE_INT,
                                        nullptr, extension_name)) {
          return true;
        }
      }

      if (td->int_to_params_vdf_name != nullptr) {
        int_to_params_vdf = find_vdf_by_name(reg, td->int_to_params_vdf_name);
        if (int_to_params_vdf == nullptr) {
          LogVSQL(ERROR_LEVEL,
                  "Type '%s' in extension '%s': int_to_params_vdf_name "
                  "'%s' not found",
                  type_name.c_str(), extension_name.c_str(),
                  td->int_to_params_vdf_name);
          return true;
        }
        const vef_type_id int_id[] = {VEF_TYPE_INT};
        if (validate_type_vdf_signature(
                int_to_params_vdf, "int_to_params", td->name, 1, int_id,
                no_custom, VEF_TYPE_STRING, nullptr, extension_name)) {
          return true;
        }
      }

      if (td->resolve_params_vdf_name != nullptr) {
        resolve_params_vdf = find_vdf_by_name(reg, td->resolve_params_vdf_name);
        if (resolve_params_vdf == nullptr) {
          LogVSQL(ERROR_LEVEL,
                  "Type '%s' in extension '%s': resolve_params_vdf_name "
                  "'%s' not found",
                  type_name.c_str(), extension_name.c_str(),
                  td->resolve_params_vdf_name);
          return true;
        }
        if (validate_type_vdf_signature(
                resolve_params_vdf, "resolve_params", td->name, 1, string_id,
                no_custom, VEF_TYPE_STRING, nullptr, extension_name)) {
          return true;
        }
      }

      if (td->intrinsic_default_vdf_name != nullptr) {
        intrinsic_default_vdf =
            find_vdf_by_name(reg, td->intrinsic_default_vdf_name);
        if (intrinsic_default_vdf == nullptr) {
          LogVSQL(ERROR_LEVEL,
                  "Type '%s' in extension '%s': intrinsic_default_vdf_name "
                  "'%s' not found",
                  type_name.c_str(), extension_name.c_str(),
                  td->intrinsic_default_vdf_name);
          return true;
        }
        if (validate_type_vdf_signature(
                intrinsic_default_vdf, "intrinsic_default", td->name, 0,
                nullptr, nullptr, VEF_TYPE_CUSTOM, td->name, extension_name)) {
          return true;
        }
      }

      if (td->storage_intf != nullptr) {
        if (!td->storage_intf->create || !td->storage_intf->drop ||
            !td->storage_intf->load || !td->storage_intf->insert ||
            !td->storage_intf->select || !td->storage_intf->mark_delete ||
            !td->storage_intf->purge) {
          LogVSQL(ERROR_LEVEL,
                  "Type '%s' in extension '%s': Not all column storage"
                  " interface are registered.",
                  type_name.c_str(), extension_name.c_str());
          return true;
        }
        storage_intf = td->storage_intf;
      }
    }

    // Validate that int_to_params requires resolve_params.
    if (int_to_params_vdf != nullptr && resolve_params_vdf == nullptr) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s' has int_to_params but no "
              "resolve_params",
              type_name.c_str(), extension_name.c_str());
      return true;
    }

    EncodeFunction encode_fn = encode_vdf ? EncodeFunction(encode_vdf)
                                          : EncodeFunction(td->encode_func);
    DecodeFunction decode_fn = decode_vdf ? DecodeFunction(decode_vdf)
                                          : DecodeFunction(td->decode_func);
    CompareFunction compare_fn = compare_vdf
                                     ? CompareFunction(compare_vdf)
                                     : CompareFunction(td->compare_func);
    std::optional<HashFunction> hash_fn;
    if (hash_vdf != nullptr)
      hash_fn.emplace(hash_vdf);
    else if (td->hash_func != nullptr)
      hash_fn.emplace(td->hash_func);

    std::optional<IntToParamsFunction> int_to_params_fn;
    if (int_to_params_vdf != nullptr)
      int_to_params_fn.emplace(int_to_params_vdf);

    std::optional<ResolveParamsFunction> resolve_params_fn;
    if (resolve_params_vdf != nullptr)
      resolve_params_fn.emplace(resolve_params_vdf);

    std::optional<StorageInterface> storage_intf_fns;
    if (storage_intf != nullptr) storage_intf_fns.emplace(*storage_intf);

    TypeDescriptor descriptor(
        TypeDescriptorKey(type_name, extension_name, extension_version),
        MYSQL_TYPE_VARCHAR, td->persisted_length, td->max_decode_buffer_length,
        std::move(encode_fn), std::move(decode_fn), std::move(compare_fn),
        std::move(hash_fn), std::move(int_to_params_fn),
        std::move(resolve_params_fn), std::move(storage_intf_fns));

    if (intrinsic_default_vdf != nullptr)
      descriptor.set_intrinsic_default_fn(
          IntrinsicDefaultFunction(intrinsic_default_vdf));

    const TypeDescriptor *existing =
        victionary.type_descriptors().get_committed(descriptor.key());
    if (existing) {
      LogVSQL(ERROR_LEVEL, "Type '%s' from extension '%s' already exists",
              type_name.c_str(), extension_name.c_str());
      return true;
    }

    if (victionary.type_descriptors().MarkForInsertion(thd,
                                                       std::move(descriptor))) {
      LogVSQL(ERROR_LEVEL, "Failed to mark type descriptor '%s' for insertion",
              type_name.c_str());
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Successfully registered type '%s'",
            type_name.c_str());
  }

  return false;
}

// Convert VEF type to MySQL Item_result
static Item_result vef_type_to_item_result(const vef_type_t &type) {
  switch (type.id) {
    case VEF_TYPE_STRING:
      return STRING_RESULT;
    case VEF_TYPE_REAL:
      return REAL_RESULT;
    case VEF_TYPE_INT:
      return INT_RESULT;
    case VEF_TYPE_CUSTOM:
      // Custom types are passed as binary strings
      return STRING_RESULT;
    default:
      return STRING_RESULT;
  }
}

bool register_funcs_from_extension(THD &thd, const std::string &extension_name,
                                   const std::string &extension_version,
                                   const ExtensionRegistration &ext_reg) {
  auto &victionary = VictionaryClient::instance();
  victionary.assert_write_lock_held();

  if (ext_reg.registration == nullptr ||
      ext_reg.registration->func_count == 0) {
    LogVSQL(INFORMATION_LEVEL, "No VDFs to register for extension '%s'",
            extension_name.c_str());
    return false;
  }

  const vef_registration_t &reg = *ext_reg.registration;

  LogVSQL(INFORMATION_LEVEL, "Registering %d VDFs from extension '%s'",
          reg.func_count, extension_name.c_str());

  for (unsigned int i = 0; i < reg.func_count; i++) {
    const vef_func_desc_t *func_desc = reg.funcs[i];
    if (func_desc == nullptr || func_desc->name == nullptr) {
      LogVSQL(ERROR_LEVEL,
              "Extension '%s' has NULL func descriptor at index %u",
              extension_name.c_str(), i);
      return true;
    }

    std::string func_name(func_desc->name);

    LogVSQL(INFORMATION_LEVEL, "Registering VDF '%s' from extension '%s'",
            func_desc->name, extension_name.c_str());

    FuncKey key(func_name, extension_name);

    // Check if VDF already exists
    const FuncDescriptor *existing = victionary.funcs().get_committed(key);
    if (existing) {
      LogVSQL(ERROR_LEVEL, "VDF '%s' from extension '%s' already exists",
              func_name.c_str(), extension_name.c_str());
      return true;
    }

    // Convert return type from VEF signature
    Item_result return_type =
        vef_type_to_item_result(func_desc->signature->return_type);

    FuncDescriptor descriptor(key, extension_version, func_desc,
                              ext_reg.negotiated_protocol, return_type);

    if (victionary.funcs().MarkForInsertion(thd, std::move(descriptor))) {
      LogVSQL(ERROR_LEVEL, "Failed to mark VDF '%s' for insertion",
              func_name.c_str());
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Successfully registered VDF '%s'",
            func_desc->name);
  }

  return false;
}

static std::string format_dlerror() {
  const char *errmsg;
  int error_number = dlopen_errno;
  DLERROR_GENERATE(errmsg, error_number);
  char buf[256];
  snprintf(buf, sizeof(buf), "error %d (%s)", error_number,
           errmsg && errmsg[0] ? errmsg : "unknown");
  return buf;
}

template <typename T>
static T lookup_symbol(void *handle, const char *symbol_name,
                       std::string &error_message) {
  void *sym = dlsym(handle, symbol_name);
  if (sym == nullptr) {
    error_message =
        std::string(symbol_name) + " not found: " + format_dlerror();
    return nullptr;
  }
  return reinterpret_cast<T>(sym);
}

// Validate a vef_registration_t returned by vef_register().
// TODO(villagesql-beta): Add more validation of the returned registration
// object (e.g. func/type descriptors, protocol version, null pointers).
// Returns false on success, true on error.
static bool validate_vef_registration(const vef_registration_t *reg,
                                      const std::string_view expected_name,
                                      std::string &error_msg) {
  if (reg->extension_name == nullptr || expected_name != reg->extension_name) {
    error_msg = std::string("extension name mismatch: expected '") +
                std::string(expected_name) + "' but .so registered '" +
                (reg->extension_name ? reg->extension_name : "(null)") + "'";
    return true;
  }
  return false;
}

bool load_vef_extension(const std::string &so_path,
                        const std::string_view expected_name,
                        ExtensionRegistration &registration,
                        vef_protocol_t max_protocol,
                        std::string &error_message) {
  LogVSQL(INFORMATION_LEVEL, "Loading VEF extension from: %s", so_path.c_str());

  registration.so_path.clear();
  registration.dlhandle = nullptr;
  registration.registration = nullptr;
  registration.unregister_func = nullptr;

  // RTLD_LOCAL ensures each extension's symbols are isolated. Without it,
  // macOS defaults to RTLD_GLOBAL, allowing the dynamic linker to coalesce
  // weak symbols (e.g. C++ template instantiations) across extensions, causing
  // one extension to call another's function implementations.
  void *handle = dlopen(so_path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (handle == nullptr) {
    error_message = "failed to load so: " + format_dlerror();
    return true;
  }

  auto vef_register = lookup_symbol<vef_register_func_t>(
      handle, VEF_REGISTER_FUNC_NAME, error_message);
  if (vef_register == nullptr) {
    dlclose(handle);
    return true;
  }

  auto vef_unregister = lookup_symbol<vef_unregister_func_t>(
      handle, VEF_UNREGISTER_FUNC_NAME, error_message);
  if (vef_unregister == nullptr) {
    dlclose(handle);
    return true;
  }

  vef_register_arg_t register_arg = {
      max_protocol,
      {MYSQL_VERSION_MAJOR, MYSQL_VERSION_MINOR, MYSQL_VERSION_PATCH, nullptr},
      {VSQL_MAJOR_VERSION, VSQL_MINOR_VERSION, VSQL_PATCH_VERSION, nullptr}};

  vef_registration_t *reg = vef_register(&register_arg);
  if (reg == nullptr) {
    error_message = "vef_register returned NULL";
    dlclose(handle);
    return true;
  }

  const vef_protocol_t negotiated_protocol =
      std::min(max_protocol, reg->protocol);

  if (reg->error_msg != nullptr) {
    error_message =
        std::string("vef_register returned an error: ") + reg->error_msg;
    vef_unregister_arg_t unregister_arg = {negotiated_protocol};
    vef_unregister(&unregister_arg, reg);
    dlclose(handle);
    return true;
  }

  if (validate_vef_registration(reg, expected_name, error_message)) {
    vef_unregister_arg_t unregister_arg = {negotiated_protocol};
    vef_unregister(&unregister_arg, reg);
    dlclose(handle);
    return true;
  }

  LogVSQL(INFORMATION_LEVEL,
          "Successfully loaded VEF extension '%s' (protocol %d, %d funcs, %d "
          "types)",
          so_path.c_str(), negotiated_protocol, reg->func_count,
          reg->type_count);

  registration.registration = reg;
  registration.negotiated_protocol = negotiated_protocol;
  registration.so_path = so_path;
  registration.dlhandle = handle;
  registration.unregister_func = vef_unregister;
  return false;
}

void unload_vef_extension(const ExtensionRegistration &registration) {
  if (registration.dlhandle == nullptr) {
    return;
  }

  if (registration.registration != nullptr) {
    vef_unregister_arg_t unregister_arg = {registration.negotiated_protocol};
    LogVSQL(INFORMATION_LEVEL, "Calling vef_unregister for extension '%s'",
            registration.so_path.c_str());
    registration.unregister_func(&unregister_arg, registration.registration);
  }

  dlclose(registration.dlhandle);
}

}  // namespace veb
}  // namespace villagesql
