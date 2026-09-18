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

#include "villagesql/include/code_base_version.h"

#include <cctype>

#include "villagesql/include/string_utils.h"

namespace villagesql {

namespace {

// Map a code base string to its canonical, statically-stored equivalent.
//
// Only the known code bases are accepted. Returning the static constant (rather
// than the caller's string) lets CodeBaseVersion::code_base_ hold a string_view
// that never dangles, regardless of where the input came from (a parsed
// substring, a temporary, etc.).
//
// @param code_base Candidate code base string
// @return The matching static code base view, or an empty view if unknown.
std::string_view canonical_code_base(std::string_view code_base) {
  if (code_base == CodeBaseVersion::kMysql84CodeBase)
    return CodeBaseVersion::kMysql84CodeBase;
  if (code_base == CodeBaseVersion::kMysql97CodeBase)
    return CodeBaseVersion::kMysql97CodeBase;
  if (code_base == CodeBaseVersion::kPercona84CodeBase)
    return CodeBaseVersion::kPercona84CodeBase;
  if (code_base == CodeBaseVersion::kPercona97CodeBase)
    return CodeBaseVersion::kPercona97CodeBase;
  return {};
}

}  // namespace

bool CodeBaseVersion::parse(std::string_view s, std::string *error) {
  // Reset state
  *this = CodeBaseVersion();

  if (s.empty()) {
    if (error) *error = "Empty version string";
    return false;
  }

  // The code base prefix is mandatory and is separated from the semver by
  // kSeparator. Semver identifiers never contain '_', so the first '_' is the
  // unambiguous separator.
  size_t sep = s.find(kSeparator);
  if (sep == std::string_view::npos) {
    if (error) *error = "Missing codebase prefix";
    return false;
  }
  std::string_view code_base = s.substr(0, sep);
  std::string_view body = s.substr(sep + 1);

  // Validate the codebase, with error messages
  if (canonical_code_base(code_base).empty()) {
    if (error) *error = "Invalid codebase " + sanitize_for_output(code_base);
    return false;
  }

  Semver version;
  if (!version.parse(body, error)) return false;

  return from_semver(code_base, version);
}

bool CodeBaseVersion::parse_schema_version(std::string_view s,
                                           std::string *error) {
  // Reset state
  *this = CodeBaseVersion();

  // Legacy stored versions predate code bases: they have no prefix and begin
  // with the numeric MAJOR component. Those builds were always based on
  // mysql-8.4, so parse the historical layout and assign the legacy code base.
  // Anything else must carry a code base prefix, exactly like parse().
  if (!s.empty() && std::isdigit(static_cast<unsigned char>(s[0]))) {
    Semver version;
    if (!version.parse(s, error)) return false;
    return from_semver(kLegacyCodeBase, version);
  }

  return parse(s, error);
}

bool CodeBaseVersion::from_semver(std::string_view code_base,
                                  const Semver &version) {
  // The code base is mandatory and must be one of the known values. Resolving
  // it to the static constant keeps code_base_ pointing at storage that
  // outlives any caller-supplied string.
  std::string_view canonical = canonical_code_base(code_base);
  if (canonical.empty()) return false;
  if (!version.is_valid()) return false;

  code_base_ = canonical;
  semver_ = version;
  return true;
}

std::string CodeBaseVersion::to_string() const {
  if (!is_valid()) return "";

  const std::string version_str = semver_.to_string();

  std::string r;
  r.reserve(code_base_.size() + 1 + version_str.size());
  r.append(code_base_.data(), code_base_.size());
  r.push_back(kSeparator);
  r.append(version_str);
  return r;
}

}  // namespace villagesql
