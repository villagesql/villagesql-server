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

#ifndef VILLAGESQL_INCLUDE_SEMVER_H_
#define VILLAGESQL_INCLUDE_SEMVER_H_

#include <string>
#include <string_view>
#include <vector>

namespace villagesql {

/**
 * Extends semantic version (defined by the semver.org specification) to
 * include a prefix segment that identifies a code base.
 * Format: CODEBASE_MAJOR.MINOR.PATCH[-PRERELEASE][+BUILDMETADATA]
 *
 * The code base names the upstream base this build derives from (e.g.
 * "mysql-8.4", "percona-9.7") and is separated from the core version by '_'.
 *
 * Examples:
 *   mysql-8.4_1.0.0
 *   mysql-8.4_1.0.0-alpha
 *   percona-9.7_1.0.0-alpha.1
 *   percona-9.7_1.0.0+20130313144700
 *   percona-9.7_1.0.0-beta+exp.sha.5114f85
 */
class Semver {
 public:
  // Inclusive upper bounds for each core version position. The bounds differ
  // by position so that, for example, the patch number has the widest range.
  static constexpr unsigned long kMajorMax = 10;
  static constexpr unsigned long kMinorMax = 100;
  static constexpr unsigned long kPatchMax = 1000;

  // Known code base values.
  static constexpr std::string_view kMysql84CodeBase = "mysql-8.4";
  static constexpr std::string_view kMysql97CodeBase = "mysql-9.7";

  static constexpr std::string_view kPercona84CodeBase = "percona-8.4";
  static constexpr std::string_view kPercona97CodeBase = "percona-9.7";

  // Code base assigned to legacy versions persisted before code bases existed.
  // Those builds were always based on mysql-8.4.
  static constexpr std::string_view kLegacyCodeBase = kMysql84CodeBase;

  /**
   * Default constructor creates an invalid semver (0.0.0)
   */
  Semver();

  /**
   * Parse a semantic version string.
   *
   * @param version_str String in semver format (e.g., "1.2.3-alpha+build")
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
   * mysql-8.4, so such a string is parsed with the historical layout and
   * assigned the "mysql-8.4" code base. Any other string is parsed exactly
   * like parse(), i.e. a code base prefix is required.
   *
   * @param version_str Stored version string (legacy or current format)
   * @param[out] error Optional error message if parsing fails
   * @return true if parsing succeeded, false otherwise.
   */
  bool parse_schema_version(std::string_view version_str,
                            std::string *error = nullptr);

  /**
   * Populate a Semver from components.  If any component fails validation,
   * the result is false and this Semver is unchanged.
   *
   * @param major Major version number
   * @param minor Minor version number
   * @param patch Patch version number
   * @param code_base Code base identifier (e.g. "mysql-8.4")
   * @param prerelease Optional pre-release identifiers (e.g., {"alpha", "1"})
   * @param build_metadata Optional build metadata identifiers
   * @return true if component validation passes
   */
  bool from_components(unsigned long major, unsigned long minor,
                       unsigned long patch, const std::string_view code_base,
                       const std::vector<std::string> &prerelease = {},
                       const std::vector<std::string> &build_metadata = {});

  /**
   * Check if this is a valid semver
   * @return true if valid, false otherwise
   */
  bool is_valid() const { return valid_; }

  /**
   * Get the MAJOR version number
   */
  unsigned long major() const { return major_; }

  /**
   * Get the MINOR version number
   */
  unsigned long minor() const { return minor_; }

  /**
   * Get the PATCH version number
   */
  unsigned long patch() const { return patch_; }

  /**
   * Get the code base identifier (empty if none)
   */
  const std::string_view code_base() const { return code_base_; }

  /**
   * Get the pre-release identifiers (empty if none)
   */
  const std::vector<std::string> &prerelease() const { return prerelease_; }

  /**
   * Get the build metadata identifiers (empty if none)
   */
  const std::vector<std::string> &build_metadata() const {
    return build_metadata_;
  }

  /**
   * Check if this version has pre-release identifiers
   */
  bool has_prerelease() const { return !prerelease_.empty(); }

  /**
   * Check if this version has build metadata
   */
  bool has_build_metadata() const { return !build_metadata_.empty(); }

  /**
   * Generate string representation of the version
   * @return String in semver format
   */
  std::string to_string() const;

  /**
   * Comparison operators.
   * Note: Build metadata is ignored for precedence comparison per semver spec.
   * Pre-release versions have lower precedence than normal versions.
   */
  bool operator==(const Semver &other) const;
  bool operator!=(const Semver &other) const;
  bool operator<(const Semver &other) const;
  bool operator<=(const Semver &other) const;
  bool operator>(const Semver &other) const;
  bool operator>=(const Semver &other) const;

 private:
  unsigned long major_;
  unsigned long minor_;
  unsigned long patch_;
  std::string_view code_base_;
  std::vector<std::string> prerelease_;
  std::vector<std::string> build_metadata_;
  bool valid_;
};

}  // namespace villagesql

#endif  // VILLAGESQL_INCLUDE_SEMVER_H_
