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

#ifndef VILLAGESQL_INCLUDE_CODE_BASE_VERSION_H_
#define VILLAGESQL_INCLUDE_CODE_BASE_VERSION_H_

#include <string>
#include <string_view>
#include <vector>

#include "villagesql/include/semver.h"

namespace villagesql {

/**
 * A VillageSQL version: a semantic version (see Semver) paired with the code
 * base it was built from.
 * Format: CODEBASE_MAJOR.MINOR.PATCH[-PRERELEASE][+BUILDMETADATA]
 *
 * The code base names the upstream base this build derives from (e.g.
 * "mysql-8.4", "percona-9.7") and is separated from the semver by '_'.
 *
 * Examples:
 *   mysql-8.4_1.0.0
 *   mysql-8.4_1.0.0-alpha
 *   percona-9.7_1.0.0-alpha.1
 *   percona-9.7_1.0.0+20130313144700
 *   percona-9.7_1.0.0-beta+exp.sha.5114f85
 *
 * This class is not comparable. To compare two versions, establish that they
 * share a code base with same_code_base() and then compare their semver()s.
 */
class CodeBaseVersion {
 public:
  // Known code base values.
  static constexpr std::string_view kMysql84CodeBase = "mysql-8.4";
  static constexpr std::string_view kMysql97CodeBase = "mysql-9.7";

  static constexpr std::string_view kPercona84CodeBase = "percona-8.4";
  static constexpr std::string_view kPercona97CodeBase = "percona-9.7";

  // Code base assigned to legacy versions persisted before code bases existed.
  // Those builds were always based on mysql-8.4.
  static constexpr std::string_view kLegacyCodeBase = kMysql84CodeBase;

  // Separates the code base from the semver. Semver identifiers never contain
  // '_', so the first '_' is the unambiguous separator.
  static constexpr char kSeparator = '_';

  /**
   * Default constructor creates an invalid version (no code base, 0.0.0)
   */
  CodeBaseVersion() = default;

  /**
   * Parse a full "CODEBASE_MAJOR.MINOR.PATCH[-PRERELEASE][+BUILDMETADATA]"
   * string. The code base prefix is mandatory and must be one of the known
   * values.
   *
   * @param version_str String to parse (e.g. "mysql-8.4_1.2.3-alpha")
   * @param[out] error Optional error message if parsing fails
   * @return true if parsing succeeded, false otherwise;
   * NOTE this is the opposite of mysql normal.
   */
  bool parse(std::string_view version_str, std::string *error = nullptr);

  /**
   * Parse a version string read from persistent storage, tolerating the
   * historical (pre-code-base) format.  These are used to record the
   * version assigned to the database schema.
   *
   * Versions persisted before code bases existed have no code base prefix and
   * begin with the numeric MAJOR component. Those builds were always based on
   * mysql-8.4, so such a string is parsed as a bare semver and assigned the
   * "mysql-8.4" code base. Any other string is parsed exactly like parse(),
   * i.e. a code base prefix is required.
   *
   * @param version_str Stored version string (legacy or current format)
   * @param[out] error Optional error message if parsing fails
   * @return true if parsing succeeded, false otherwise.
   */
  bool parse_schema_version(std::string_view version_str,
                            std::string *error = nullptr);

  /**
   * Populate this version from a code base and an already-built Semver. If
   * either fails validation, the result is false and this object is unchanged.
   *
   * @param code_base Code base identifier (e.g. "mysql-8.4")
   * @param version The semantic version
   * @return true if the code base is known and version is valid
   */
  bool from_semver(std::string_view code_base, const Semver &version);

  /**
   * Check if this is a valid version, i.e. it names a known code base and
   * carries a valid semver.
   */
  bool is_valid() const { return !code_base_.empty() && semver_.is_valid(); }

  /**
   * Get the code base identifier (empty if none)
   */
  std::string_view code_base() const { return code_base_; }

  /**
   * Get the semantic version
   */
  const Semver &semver() const { return semver_; }

  /**
   * Check whether two versions name the same code base. Versions from
   * different code bases are not comparable; see the operators below.
   */
  bool same_code_base(const CodeBaseVersion &other) const {
    return code_base_ == other.code_base_;
  }

  /**
   * Generate string representation of the version
   * @return String in CODEBASE_semver format, or "" if this version is invalid
   */
  std::string to_string() const;

  // Deliberately not comparable. A code base change is not a numeric upgrade,
  // so there is no meaningful ordering between versions of different code
  // bases, and an operator that silently answered "false" for every relation
  // would hide that. Compare same_code_base() first, then compare the
  // semver()s, which carry the semver precedence rules.

 private:
  // Always points at one of the static code base constants above, so it never
  // dangles regardless of where the code base string came from (a parsed
  // substring, a temporary, etc.). Empty when this version is invalid.
  std::string_view code_base_;
  Semver semver_;
};

}  // namespace villagesql

#endif  // VILLAGESQL_INCLUDE_CODE_BASE_VERSION_H_
