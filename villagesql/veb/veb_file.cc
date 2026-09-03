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
#include <vector>

#include "my_config.h"
#include "my_dir.h"
#include "my_sharedlib.h"
#include "my_sys.h"
#include "mysqld_error.h"
#include "scope_guard.h"
#include "sha2.h"
#include "sql/auth/auth_common.h"
#include "sql/iterators/row_iterator.h"
#include "sql/mysqld.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_udf.h"
#include "sql/table.h"
#include "sql_string.h"
#include "villagesql/include/error.h"
#include "villagesql/include/version.h"
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/services/capability_registry.h"
#include "villagesql/services/sys_var_access.h"
#include "villagesql/veb/register.h"
#include "villagesql/veb/sql_extension.h"
#include "villagesql/veb/sql_extension_update_precheck.h"
#include "villagesql/veb/validate.h"

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

const vef_protocol_t vef_server_protocol_version = VEF_PROTOCOL_4;

static std::string get_expansion_cache_base_path() {
  char path_buf[FN_REFLEN];
  fn_format(path_buf, ".veb_expansion_cache", mysql_real_data_home, "", 0);
  return std::string(path_buf);
}

std::string get_extension_so_path(const std::string &extension_name,
                                  const std::string &sha256) {
  // Construct path:
  // {datadir}/.veb_expansion_cache/{name}/{sha256}/lib/{name}.so
  std::string expanded_base = get_expansion_cache_base_path();
  if (expanded_base.empty()) {
    return "";
  }

  char path_buf[FN_REFLEN];

  // .veb_expansion_cache/{name}/
  fn_format(path_buf, extension_name.c_str(), expanded_base.c_str(), "", 0);
  std::string name_dir(path_buf);

  // .veb_expansion_cache/{name}/{sha256}/
  fn_format(path_buf, sha256.c_str(), name_dir.c_str(), "", 0);
  std::string sha_dir(path_buf);

  // .veb_expansion_cache/{name}/{sha256}/lib/
  fn_format(path_buf, "lib", sha_dir.c_str(), "", 0);
  std::string lib_dir(path_buf);

  // TODO(villagesql-windows): should be .dll on windows.
  // .veb_expansion_cache/{name}/{sha256}/lib/{name}.so
  std::string so_filename = extension_name + ".so";
  fn_format(path_buf, so_filename.c_str(), lib_dir.c_str(), "", 0);

  return std::string(path_buf);
}

bool ResolveTargetSoPath(const std::string &extension_name,
                         const std::string &target_version,
                         std::string *resolved_sha, std::string *so_path,
                         std::string *error_message) {
  std::string expanded_path;
  if (expand_veb_to_directory(extension_name, target_version, expanded_path,
                              *resolved_sha)) {
    char msg[512];
    snprintf(msg, sizeof(msg),
             "Cannot resolve target VEB for '%s' version '%s'",
             extension_name.c_str(), target_version.c_str());
    *error_message = msg;
    return true;
  }

  *so_path = get_extension_so_path(extension_name, *resolved_sha);
  if (so_path->empty()) {
    char msg[512];
    snprintf(msg, sizeof(msg), "Failed to construct .so path for '%s'",
             extension_name.c_str());
    *error_message = msg;
    return true;
  }
  return false;
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

static std::string make_veb_filename(const std::string &name,
                                     const std::string &veb_version) {
  if (veb_version.empty()) return name + ".veb";
  return name + "-" + veb_version + ".veb";
}

bool veb_file_exists(const std::string &name, const std::string &veb_version) {
  std::string full_path = get_veb_path(make_veb_filename(name, veb_version));
  if (full_path.empty()) return false;

  MY_STAT file_stat;
  return my_stat(full_path.c_str(), &file_stat, MYF(0)) != nullptr;
}

bool find_veb_version(const std::string &name, std::string &version) {
  std::string veb_dir(opt_veb_dir);
  std::string prefix = name + "-";
  std::string suffix = ".veb";
  std::string unversioned = name + ".veb";

  DIR *dir = opendir(veb_dir.c_str());
  if (!dir) {
    villagesql_error("Cannot open VEB directory '%s'", MYF(0), veb_dir.c_str());
    return true;
  }

  std::vector<std::string> found_versions;
  bool found_unversioned = false;
  while (dirent *entry = readdir(dir)) {
    std::string filename(entry->d_name);
    if (filename == unversioned) {
      found_unversioned = true;
      continue;
    }

    if (filename.size() > prefix.size() + suffix.size() &&
        filename.rfind(prefix, 0) == 0 &&
        filename.compare(filename.size() - suffix.size(), suffix.size(),
                         suffix) == 0) {
      found_versions.push_back(filename.substr(
          prefix.size(), filename.size() - prefix.size() - suffix.size()));
    }
  }
  closedir(dir);

  if (found_unversioned) {
    version.clear();
    return false;
  }

  if (found_versions.size() > 1) {
    villagesql_error(
        "Multiple versions of extension '%s' found in '%s'; specify a version "
        "with INSTALL EXTENSION %s VERSION 'x.y.z'",
        MYF(0), name.c_str(), veb_dir.c_str(), name.c_str());
    return true;
  }

  if (found_versions.size() == 1) {
    version = found_versions[0];
    return false;
  }

  villagesql_error("VEB file not found: %s.veb", MYF(0), name.c_str());
  return true;
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
  LogVSQL(INFORMATION_LEVEL,
          "Loading VEB manifest for extension '%s' version '%s'", name.c_str(),
          version.c_str());

  // Construct VEB filename: {name}.veb if version is empty, else
  // {name}-{version}.veb. When version is empty, it is populated from the
  // manifest below; when non-empty, it is asserted against the manifest.
  std::string veb_filename = make_veb_filename(name, version);
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

  std::string manifest_version = version_value.GetString();
  if (version.empty()) {
    // Unversioned file: take version from manifest.
    version = manifest_version;
  } else if (manifest_version != version) {
    villagesql_error(
        "Version mismatch in '%s': filename says '%s' but manifest says '%s'",
        MYF(0), veb_filename.c_str(), version.c_str(),
        manifest_version.c_str());
    return true;
  }

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

  // TODO(villagesql-general): Consider relaxing this requirement to allow VEB
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
                             const std::string &veb_version,
                             std::string &expanded_path,
                             std::string &sha256_hash) {
  // Note: Name validation is done by caller (sql_extension.cc) before calling
  // this
  LogVSQL(INFORMATION_LEVEL, "Expanding VEB for extension '%s'", name.c_str());

  // Get VEB file path and calculate SHA256
  std::string veb_filename = make_veb_filename(name, veb_version);
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

  // Construct expansion path: {datadir}/.veb_expansion_cache/{name}/{sha256}/
  std::string base_path = get_expansion_cache_base_path();

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

  // Create directory structure: .veb_expansion_cache/,
  // .veb_expansion_cache/{name}/, .veb_expansion_cache/{name}/{sha256}/ Create
  // .veb_expansion_cache/ if needed
  // TODO(villagesql-windows): On Windows, dotfiles aren't hidden by default.
  // After creating .veb_expansion_cache/, set the hidden attribute
  // (e.g., SetFileAttributes with FILE_ATTRIBUTE_HIDDEN).
  if (!my_stat(base_path.c_str(), &dir_stat, MYF(0))) {
    if (my_mkdir(base_path.c_str(), 0755, MYF(0)) != 0) {
      villagesql_error("Failed to create .veb_expansion_cache directory: %s",
                       MYF(0), base_path.c_str());
      return true;
    }
    LogVSQL(INFORMATION_LEVEL, "Created .veb_expansion_cache directory");
  }

  // Create .veb_expansion_cache/{name}/ if needed
  if (!my_stat(name_dir.c_str(), &dir_stat, MYF(0))) {
    if (my_mkdir(name_dir.c_str(), 0755, MYF(0)) != 0) {
      villagesql_error("Failed to create extension directory for '%s'", MYF(0),
                       name.c_str());
      return true;
    }
    LogVSQL(INFORMATION_LEVEL, "Created directory: %s", name_dir.c_str());
  }

  // Create .veb_expansion_cache/{name}/{sha256}/
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

// Validate, load, and register one installed extension. Resolves the VEB
// on disk, re-expands if necessary, dlopens the .so, parses the
// registration, registers types/VDFs/preview capabilities, and marks the
// in-memory descriptor for insertion.
//
// Returns false on success with `*registration` populated. Returns true on
// failure after logging an error; `*registration` may be partially
// populated on failure and should not be used by callers.
//
// Caller must hold the victionary write lock.
static bool load_one_extension(THD *thd, const std::string &extension_name,
                               const std::string &expected_version,
                               const std::string &sha256,
                               ExtensionRegistration *registration) {
  VictionaryClient &victionary = VictionaryClient::instance();

  // Validate extension: load manifest and check version matches.
  // Prefer {name}-{version}.veb if present; fall back to {name}.veb.
  std::string veb_version;
  if (veb_file_exists(extension_name, expected_version)) {
    veb_version = expected_version;
  }
  std::string actual_version = veb_version;
  if (load_veb_manifest(extension_name, actual_version)) {
    LogVSQL(ERROR_LEVEL, "Failed to load VEB manifest for extension '%s'",
            extension_name.c_str());
    return true;
  }

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

  // Re-expand VEB if the .so is missing from the expansion cache.
  MY_STAT so_stat;
  if (!my_stat(so_path.c_str(), &so_stat, MYF(0))) {
    LogVSQL(INFORMATION_LEVEL,
            "Extension '%s' .so not found at '%s', re-expanding from VEB",
            extension_name.c_str(), so_path.c_str());
    std::string expanded_path;
    std::string reexpand_sha256;
    if (expand_veb_to_directory(extension_name, veb_version, expanded_path,
                                reexpand_sha256)) {
      LogVSQL(ERROR_LEVEL, "Failed to re-expand VEB for extension '%s'",
              extension_name.c_str());
      return true;
    }
    if (reexpand_sha256 != sha256) {
      LogVSQL(ERROR_LEVEL,
              "Extension '%s' VEB file has changed: database has SHA256 "
              "'%s', current VEB has '%s'",
              extension_name.c_str(), sha256.c_str(), reexpand_sha256.c_str());
      return true;
    }
  }

  std::string load_error;
  if (load_vef_extension({.extension_name = extension_name,
                          .reason = villagesql::services::LoadReason::kStartup,
                          .thd = thd},
                         so_path, vef_server_protocol_version, *registration,
                         load_error)) {
    LogVSQL(ERROR_LEVEL, "Failed to load VEF extension '%s': %s",
            extension_name.c_str(), load_error.c_str());
    return true;
  }

  std::string reg_error;
  std::optional<ValidatedRegistration> validated = parse_extension_registration(
      *registration, extension_name, expected_version, reg_error);
  if (!validated) {
    LogVSQL(ERROR_LEVEL, "Failed to parse extension '%s': %s",
            extension_name.c_str(), reg_error.c_str());
    return true;
  }

  std::optional<ValidatedPreviewCapabilities> preview =
      parse_preview_capabilities(*registration, extension_name,
                                 expected_version, reg_error);
  if (!preview) {
    LogVSQL(ERROR_LEVEL, "Failed to parse extension '%s': %s",
            extension_name.c_str(), reg_error.c_str());
    return true;
  }

  if (register_preview_capabilities(*thd, std::move(*preview), *validated,
                                    reg_error) ||
      register_validated_extension(*thd, std::move(*validated), reg_error)) {
    LogVSQL(ERROR_LEVEL, "Failed to register extension '%s': %s",
            extension_name.c_str(), reg_error.c_str());
    return true;
  }

  // Build the descriptor's registration by copy: the caller keeps its own
  // copy via `*registration` so a future rollback path can unload it.
  ExtensionRegistration desc_reg = *registration;
  if (victionary.extension_descriptors().MarkForInsertion(
          *thd, ExtensionDescriptor(
                    ExtensionDescriptorKey(extension_name, expected_version),
                    std::move(desc_reg)))) {
    LogVSQL(ERROR_LEVEL, "Failed to register descriptor for extension '%s'",
            extension_name.c_str());
    return true;
  }

  LogVSQL(INFORMATION_LEVEL,
          "Successfully registered VEF extension '%s' from '%s'",
          extension_name.c_str(), so_path.c_str());
  return false;
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

  // Collected during the main pass.
  //
  // pending_failures_to_persist: rows whose pending action we could not
  // apply this restart (precheck failed, extension has custom indexes we
  // don't yet support swapping, or target .so failed to load). The row
  // stays at its current version; pending_action is stamped MarkFailed
  // so it's queryable via INFORMATION_SCHEMA.EXTENSIONS.
  //
  // pending_applies_to_persist: rows whose pending action we successfully
  // applied. The extension row is rewritten to the target version + sha
  // with pending_action cleared, and every dependent custom_columns and
  // custom_sp_params row referencing the extension is rewritten to the
  // new version.
  //
  // Both kept by value so we don't depend on victionary pointers after
  // the write lock is released.
  struct PendingFailureToPersist {
    ExtensionKey key;
    PendingAction updated;
    std::string original_extension_version;
    std::string original_veb_sha256;
  };
  struct PendingApplyToPersist {
    ExtensionKey key;
    std::string target_version;
    std::string target_veb_sha256;
    std::string current_version;  // for filtering dependent rows below
  };
  std::vector<PendingFailureToPersist> pending_failures_to_persist;
  std::vector<PendingApplyToPersist> pending_applies_to_persist;

  {
    auto lock_guard = victionary.get_write_lock();

    // Get all committed extensions from cache
    std::vector<const ExtensionEntry *> all_extensions =
        victionary.extensions().get_all_committed();

    row_count = all_extensions.size();

    // Log a warning once if --villagesql-skip-extension-updates is set
    // AND there are pending actions to bypass. The operator queries
    // I_S.EXTENSIONS to see them and clears each via a same-version
    // ALTER EXTENSION ... AT RESTART on the next startup (without the
    // flag). Nothing is mutated by the flag itself.
    if (opt_villagesql_skip_extension_updates) {
      int pending_count = 0;
      for (const ExtensionEntry *e : all_extensions) {
        if (e && e->has_pending_action()) ++pending_count;
      }
      if (pending_count > 0) {
        LogVSQL(
            WARNING_LEVEL,
            "--villagesql-skip-extension-updates is set: bypassing %d "
            "pending extension update(s). Extensions load at their "
            "currently-installed version. Query "
            "INFORMATION_SCHEMA.EXTENSIONS to see the pending actions; clear "
            "each via ALTER EXTENSION <name> "
            "VERSION '<current>' AT RESTART, then restart without the flag.",
            pending_count);
      }
    }

    // Validate and register each extension
    for (const ExtensionEntry *entry : all_extensions) {
      if (!entry) continue;

      const std::string &extension_name = entry->extension_name();
      const std::string &expected_version = entry->extension_version;
      const std::string &sha256 = entry->veb_sha256;

      installed_extensions.insert(extension_name);

      // A pending action means we have an upgrade to perform. Decide
      // whether we can apply it this restart. If yes, load the target
      // .so and stage row rewrites; if no, stamp MarkFailed with the
      // reason and load the current .so instead. version_to_load /
      // sha_to_load below control which .so we hand to load_one_extension.
      // staged_apply records whether an apply record was pushed for this
      // extension, so the load-failure branch can roll it back without
      // peeking at pending_applies_to_persist.back().
      std::string version_to_load = expected_version;
      std::string sha_to_load = sha256;
      bool staged_apply = false;

      // --villagesql-skip-extension-updates bypasses pending-action
      // processing (warning logged once above). The row is left
      // untouched on disk.
      if (entry->has_pending_action() &&
          !opt_villagesql_skip_extension_updates) {
        const std::string target_version =
            entry->pending_action->target_version();
        const std::string target_sha =
            entry->pending_action->target_veb_sha256();

        std::string failure_reason;

        // At the restart-apply site we surface pending-update failures via
        // pending_last_error, not the SQL diagnostics area. The precheck
        // callees below (ResolveTargetSoPath, RunUpdatePreCheck, ...) all
        // return structured error_message out-params, but internally they
        // may transitively call villagesql_error (e.g.
        // expand_veb_to_directory) which stashes a condition onto THD's
        // diagnostics area via current_thd. Push a scratch diagnostics
        // area for the duration of the precheck block so any such
        // condition lands in the scratch and is discarded when we pop; the
        // parent DA is never touched. This is MySQL's designed-for-purpose
        // mechanism for exactly this pattern.
        //
        // TODO(villagesql-general): consolidate internal helpers on structured
        // error-string out-params and translate to villagesql_error only at
        // the SQL boundary (ALTER call site). That would remove the need
        // for this scratch DA entirely and align the internal call surface
        // with the subprocess-precheck story. Remove the NOTE in
        // sql_extension_update_precheck.cc mentioning this TODO.
        //
        // TODO(villagesql): load_installed_extensions has grown large;
        // break the per-extension work into a purpose-driven helper.
        //
        // Manual push/pop: scope is short and linear; no guard needed.
        Diagnostics_area scratch_da(false);
        thd->push_diagnostics_area(&scratch_da);

        // Precheck: build the same snapshot the live ALTER path uses.
        std::string resolved_target_sha;
        std::string target_so_path;
        if (villagesql::veb::ResolveTargetSoPath(
                extension_name, target_version, &resolved_target_sha,
                &target_so_path, &failure_reason)) {
          // failure_reason already populated by the helper.
        } else if (resolved_target_sha != target_sha) {
          char msg[512];
          snprintf(msg, sizeof(msg),
                   "Target VEB sha256 changed since ALTER: expected %s, "
                   "got %s",
                   target_sha.c_str(), resolved_target_sha.c_str());
          failure_reason = msg;
        } else {
          // The enclosing scope already holds the victionary write lock,
          // which is stronger than the read lock the helper requires.
          villagesql::veb::UpdatePreCheckInput input;
          villagesql::veb::BuildUpdatePreCheckSnapshot(
              victionary, extension_name, expected_version, target_version,
              std::move(target_so_path), &input);
          const villagesql::veb::UpdatePreCheckResult result =
              villagesql::veb::RunUpdatePreCheck(input);
          if (!result.ok) failure_reason = result.error_message;
        }

        thd->pop_diagnostics_area();

        if (failure_reason.empty()) {
          // Apply the pending update. Load the target .so instead of the
          // current one; stage row rewrites for the extensions row and the
          // dependent custom_columns / custom_sp_params rows.
          LogVSQL(INFORMATION_LEVEL,
                  "Extension '%s': applying pending update from '%s' to '%s' "
                  "(requested at %s)",
                  extension_name.c_str(), expected_version.c_str(),
                  target_version.c_str(),
                  entry->pending_action->requested_at().c_str());
          pending_applies_to_persist.push_back(
              {entry->key(), target_version, target_sha, expected_version});
          staged_apply = true;
          version_to_load = target_version;
          sha_to_load = target_sha;
        } else {
          LogVSQL(WARNING_LEVEL,
                  "Extension '%s' pending update to version '%s' not applied: "
                  "%s. Loading current version '%s'.",
                  extension_name.c_str(), target_version.c_str(),
                  failure_reason.c_str(), expected_version.c_str());
          PendingAction updated = *entry->pending_action;
          updated.MarkFailed(std::move(failure_reason));
          pending_failures_to_persist.push_back(
              {entry->key(), std::move(updated), expected_version, sha256});
        }
      }

      ExtensionRegistration registration;
      if (load_one_extension(thd, extension_name, version_to_load, sha_to_load,
                             &registration)) {
        // If we were trying to apply a pending update and the target .so
        // failed to load, roll back to the current version: pop the apply
        // record, add a failure record with the load-failure reason, and
        // retry with the current .so. The server always comes up.
        // staged_apply implies the apply record was pushed in this
        // iteration and no other push_back has happened since, so it is
        // safe to pop_back() -- but we also assert via extension_name
        // match to catch code changes that break the invariant.
        if (staged_apply) {
          assert(!pending_applies_to_persist.empty() &&
                 pending_applies_to_persist.back().key.extension_name() ==
                     extension_name);
          pending_applies_to_persist.pop_back();
          LogVSQL(WARNING_LEVEL,
                  "Extension '%s' pending update to version '%s' not applied: "
                  "target .so failed to load. Falling back to current version "
                  "'%s'.",
                  extension_name.c_str(), version_to_load.c_str(),
                  expected_version.c_str());
          PendingAction updated = *entry->pending_action;
          updated.MarkFailed("Target .so failed to load at restart");
          pending_failures_to_persist.push_back(
              {entry->key(), std::move(updated), expected_version, sha256});
          if (load_one_extension(thd, extension_name, expected_version, sha256,
                                 &registration)) {
            return true;
          }
        } else {
          return true;
        }
      }
      success_count++;
    }
  }

  LogVSQL(INFORMATION_LEVEL, "Validated %d of %d installed extensions",
          success_count, row_count);

  // Persist any pending-update decisions we made during the loop: either
  // stamp a failure onto the row (row stays at current version) or apply
  // the update (rewrite extensions + custom_columns + custom_sp_params to
  // the new version). Done outside the victionary write lock so
  // open_and_lock_tables can acquire its own MDLs cleanly.
  //
  // Reachability: everything in this persist block only executes when at
  // least one extension had a pending action at startup. A vanilla startup
  // with no pending actions skips the block entirely (both decision
  // vectors are empty) and the atomicity logic below is unreachable.
  //
  // Transaction lifecycle: the enclosing frame in
  // do_init_extension_infrastructure (villagesql/sql/initialize.cc) wraps
  // this whole function in an autocommit-off transaction and issues
  // trans_commit_stmt + trans_commit if we return false, or trans_rollback
  // if we return true. So write_all_uncommitted_entries below just needs
  // to flush the victionary's uncommitted operations into the row buffer;
  // the caller commits the storage engine transaction.
  //
  // Failure policy: any MarkForUpdate or write_all_uncommitted_entries
  // failure below returns true (server startup fails). Realistic causes
  // are (a) OOM, which the underlying my_error already marks
  // ME_FATALERROR, or (b) a bug in this code or the victionary layer
  // (e.g. divergence between the in-memory cache and the on-disk system
  // tables). Both are conditions where refusing to come up is safer than
  // committing partial pending-action state.
  //
  // Operator recovery: --villagesql-skip-extension-updates bypasses the
  // pending-action processing entirely for the restart, so a corrupt
  // pending_action row that would otherwise trigger a startup failure
  // here can be worked around. See the warning-log-and-gate above.
  if (!pending_failures_to_persist.empty() ||
      !pending_applies_to_persist.empty()) {
    // Open all four tables the persist step may need. custom_columns,
    // custom_sp_params, and custom_indexes are only touched when we're
    // applying an update, but it's simpler to open them unconditionally
    // than to branch on the decision vector shapes.
    Table_ref ext_table(SchemaManager::VILLAGESQL_SCHEMA_NAME,
                        SchemaManager::EXTENSIONS_TABLE_NAME, TL_WRITE,
                        MDL_SHARED_WRITE);
    Table_ref columns_table(SchemaManager::VILLAGESQL_SCHEMA_NAME,
                            SchemaManager::COLUMNS_TABLE_NAME, TL_WRITE,
                            MDL_SHARED_WRITE);
    Table_ref sp_params_table(SchemaManager::VILLAGESQL_SCHEMA_NAME,
                              SchemaManager::SP_PARAMS_TABLE_NAME, TL_WRITE,
                              MDL_SHARED_WRITE);
    Table_ref indexes_table(SchemaManager::VILLAGESQL_SCHEMA_NAME,
                            SchemaManager::INDEXES_TABLE_NAME, TL_WRITE,
                            MDL_SHARED_WRITE);
    ext_table.next_global = ext_table.next_local = &columns_table;
    columns_table.next_global = columns_table.next_local = &sp_params_table;
    sp_params_table.next_global = sp_params_table.next_local = &indexes_table;
    indexes_table.next_global = indexes_table.next_local = nullptr;
    if (open_and_lock_tables(thd, &ext_table, MYSQL_LOCK_IGNORE_TIMEOUT)) {
      LogVSQL(ERROR_LEVEL,
              "Failed to open %s.%s / %s / %s / %s to persist pending-update "
              "decisions; state will not be persisted this restart",
              SchemaManager::VILLAGESQL_SCHEMA_NAME,
              SchemaManager::EXTENSIONS_TABLE_NAME,
              SchemaManager::COLUMNS_TABLE_NAME,
              SchemaManager::SP_PARAMS_TABLE_NAME,
              SchemaManager::INDEXES_TABLE_NAME);
      return true;
    } else {
      // Close the open tables on any exit from the persist block. The
      // enclosing bootstrap context doesn't run statement-end cleanup, so
      // leaked open tables trip an assertion in THD::cleanup at shutdown.
      auto close_guard =
          create_scope_guard([thd] { close_thread_tables(thd); });

      bool any_marked = false;
      {
        // Hold the victionary write lock only for the in-memory mark step;
        // write_all_uncommitted_entries below takes a read lock internally
        // and the rwlock is not reentrant.
        auto write_lock = victionary.get_write_lock();

        // Failures: rewrite the extensions row with a MarkFailed pending
        // action. Version + sha stay at the current values.
        for (auto &pending : pending_failures_to_persist) {
          ExtensionEntry updated(pending.key,
                                 std::move(pending.original_extension_version),
                                 std::move(pending.original_veb_sha256));
          updated.pending_action = std::move(pending.updated);
          if (victionary.extensions().MarkForUpdate(*thd, std::move(updated),
                                                    pending.key)) {
            LogVSQL(ERROR_LEVEL,
                    "Failed to mark pending-update failure for extension '%s'",
                    pending.key.extension_name().c_str());
            return true;
          }
          any_marked = true;
        }

        // Applies: rewrite the extensions row to the target version + sha
        // with pending_action cleared, then rewrite every dependent
        // custom_columns and custom_sp_params row that references the
        // current version.
        for (auto &pending : pending_applies_to_persist) {
          ExtensionEntry updated(pending.key, pending.target_version,
                                 pending.target_veb_sha256);
          // pending_action left std::nullopt: the swap is done.
          if (victionary.extensions().MarkForUpdate(*thd, std::move(updated),
                                                    pending.key)) {
            LogVSQL(ERROR_LEVEL, "Failed to stage extension-row apply for '%s'",
                    pending.key.extension_name().c_str());
            return true;
          }
          any_marked = true;

          const std::string &ext_name = pending.key.extension_name();
          const std::string &from_version = pending.current_version;
          const std::string &to_version = pending.target_version;

          // TODO(villagesql-general): the set of extension-owned systables
          // (columns, sp_params, custom_indexes, ...) is enumerated by
          // name at every caller that operates on an extension's rows:
          // UNINSTALL EXTENSION (delete sweep + use-count check),
          // update-precheck (snapshot build), and this apply site
          // (rewrite extension_version). Adding a new extension-owned
          // table today requires editing every caller. Centralize the
          // enumeration on VictionaryClient -- e.g. an
          // UninstallExtensionRows(match) closed operation, a
          // RewriteExtensionVersion(match, to_version) closed operation,
          // and a visit_extension_dependent_maps(visitor) for the
          // non-uniform callers (precheck snapshot, use-count check).
          // Design once, migrate all sites, one follow-up PR.

          // Snapshot the committed dependent rows before mutating, then
          // rewrite each one to the new extension_version.
          std::vector<const ColumnEntry *> dep_cols =
              victionary.columns().get_all_committed();
          for (const auto *col : dep_cols) {
            if (col == nullptr || col->extension_name != ext_name ||
                col->extension_version != from_version)
              continue;
            ColumnEntry new_col(col->key(), col->extension_name, to_version,
                                col->type_name, col->type_parameters);
            if (victionary.columns().MarkForUpdate(*thd, std::move(new_col),
                                                   col->key())) {
              LogVSQL(ERROR_LEVEL,
                      "Failed to stage custom_columns rewrite for '%s' "
                      "column '%s.%s.%s'",
                      ext_name.c_str(), col->db_name().c_str(),
                      col->table_name().c_str(), col->column_name().c_str());
              return true;
            }
          }

          std::vector<const SpParamEntry *> dep_sps =
              victionary.sp_params().get_all_committed();
          for (const auto *sp : dep_sps) {
            if (sp == nullptr || sp->extension_name != ext_name ||
                sp->extension_version != from_version)
              continue;
            SpParamEntry new_sp(sp->key(), sp->extension_name, to_version,
                                sp->type_name, sp->type_parameters);
            if (victionary.sp_params().MarkForUpdate(*thd, std::move(new_sp),
                                                     sp->key())) {
              LogVSQL(ERROR_LEVEL,
                      "Failed to stage custom_sp_params rewrite for '%s' "
                      "param '%s.%s.%s'",
                      ext_name.c_str(), sp->db_name().c_str(),
                      sp->sp_name().c_str(), sp->param_name().c_str());
              return true;
            }
          }

          std::vector<const IndexEntry *> dep_indexes =
              victionary.custom_indexes().get_all_committed();
          for (const auto *idx : dep_indexes) {
            if (idx == nullptr || idx->extension_name != ext_name ||
                idx->extension_version != from_version)
              continue;
            IndexEntry new_idx(idx->key(), idx->index_id, idx->extension_name,
                               to_version, idx->index_type_name,
                               idx->index_type_parameters);
            if (victionary.custom_indexes().MarkForUpdate(
                    *thd, std::move(new_idx), idx->key())) {
              LogVSQL(ERROR_LEVEL,
                      "Failed to stage custom_indexes rewrite for '%s' "
                      "index '%s.%s.%s'",
                      ext_name.c_str(), idx->db_name().c_str(),
                      idx->table_name().c_str(), idx->index_name().c_str());
              return true;
            }
          }
        }
      }
      // DBUG hook: with --debug=+d,villagesql_fail_pending_persist the
      // write is treated as if the SE-layer call failed. Used by mtr
      // tests to exercise the fail-startup and recovery-via-skip-flag
      // paths without needing a real SE-layer failure.
      if (any_marked &&
          (victionary.write_all_uncommitted_entries(thd) ||
           DBUG_EVALUATE_IF("villagesql_fail_pending_persist", true, false))) {
        LogVSQL(ERROR_LEVEL,
                "Failed to persist pending-update decision(s) to %s.%s / %s / "
                "%s / %s",
                SchemaManager::VILLAGESQL_SCHEMA_NAME,
                SchemaManager::EXTENSIONS_TABLE_NAME,
                SchemaManager::COLUMNS_TABLE_NAME,
                SchemaManager::SP_PARAMS_TABLE_NAME,
                SchemaManager::INDEXES_TABLE_NAME);
        return true;
      }
    }
  }

  // Remove expansion-cache directories for extensions no longer installed.
  // The cache is a fast path only -- nothing consults it as authoritative
  // state -- so skipping cleanup on failure paths (which all return true
  // and abort startup) is safe. The sweep is idempotent and authoritative:
  // it walks every top-level directory in the cache and removes any not
  // present in the installed_extensions set. Nothing is missed by skipped
  // runs; whenever the operator does get a successful startup, the cache
  // is fully reconciled in one pass.
  cleanup_orphaned_expansion_directories(installed_extensions);

  return false;
}

void cleanup_orphaned_expansion_directories(
    const std::set<std::string> &installed_extensions) {
  LogVSQL(INFORMATION_LEVEL, "Cleaning up orphaned expansion directories");

  std::string expanded_base_path = get_expansion_cache_base_path();

  // Check if .veb_expansion_cache directory exists
  MY_STAT expanded_stat;
  if (!my_stat(expanded_base_path.c_str(), &expanded_stat, MYF(0)) ||
      !MY_S_ISDIR(expanded_stat.st_mode)) {
    LogVSQL(INFORMATION_LEVEL, "No .veb_expansion_cache directory found");
    return;
  }

  // Open .veb_expansion_cache directory
  DIR *expanded_dir = opendir(expanded_base_path.c_str());
  if (!expanded_dir) {
    LogVSQL(WARNING_LEVEL, "Failed to open .veb_expansion_cache directory");
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

bool open_vef_extension(const std::string &so_path, vef_protocol_t max_protocol,
                        ExtensionRegistration &registration,
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

  // Reject extensions compiled against an old unstable protocol version.
  // Even-numbered protocol versions are unstable; only the current one
  // (max_protocol) is accepted. Odd versions are stable and always accepted.
  if (reg->protocol % 2 == 0 && reg->protocol != max_protocol) {
    error_message = "extension uses obsolete unstable protocol version " +
                    std::to_string(reg->protocol) +
                    " (current: " + std::to_string(max_protocol) + ")";
    vef_unregister_arg_t unregister_arg = {negotiated_protocol};
    vef_unregister(&unregister_arg, reg);
    dlclose(handle);
    return true;
  }

  // TODO(villagesql-production): Add more validation of the returned
  // registration object (e.g. func/type descriptors, protocol version, null
  // pointers).

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

void close_vef_extension(const ExtensionRegistration &registration) {
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

bool load_vef_extension(const villagesql::services::PopulateContext &ctx,
                        const std::string &so_path, vef_protocol_t max_protocol,
                        ExtensionRegistration &registration,
                        std::string &error_message) {
  if (open_vef_extension(so_path, max_protocol, registration, error_message)) {
    return true;
  }

  // Populate any capabilities the extension requires.
  if (villagesql::services::populate_capabilities(
          ctx, registration.registration, error_message)) {
    // Roll back any capabilities that were successfully populated before the
    // failure. Mirror the load reason to its unload counterpart.
    villagesql::services::DepopulateContext depop_ctx;
    depop_ctx.reason = ctx.reason == villagesql::services::LoadReason::kStartup
                           ? villagesql::services::UnloadReason::kShutdown
                           : villagesql::services::UnloadReason::kUninstall;
    depop_ctx.thd = ctx.thd;
    villagesql::services::depopulate_capabilities(depop_ctx,
                                                  registration.registration);
    close_vef_extension(registration);
    registration.so_path.clear();
    registration.dlhandle = nullptr;
    registration.registration = nullptr;
    registration.unregister_func = nullptr;
    return true;
  }
  return false;
}

void unload_vef_extension(const villagesql::services::DepopulateContext &ctx,
                          const ExtensionRegistration &registration) {
  if (registration.dlhandle == nullptr) {
    return;
  }

  if (registration.registration != nullptr) {
    villagesql::services::depopulate_capabilities(ctx,
                                                  registration.registration);
  }

  close_vef_extension(registration);
}

}  // namespace veb
}  // namespace villagesql
