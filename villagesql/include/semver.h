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
 * Represents a semantic version according to semver.org specification.
 * Format: MAJOR.MINOR.PATCH[-PRERELEASE][+BUILDMETADATA]
 *
 * Examples:
 *   1.0.0
 *   1.0.0-alpha
 *   1.0.0-alpha.1
 *   1.0.0+20130313144700
 *   1.0.0-beta+exp.sha.5114f85
 */
class Semver {
 public:
  // Inclusive upper bounds for each core version position. The bounds differ
  // by position so that, for example, the patch number has the widest range.
  static constexpr unsigned long kMajorMax = 10;
  static constexpr unsigned long kMinorMax = 100;
  static constexpr unsigned long kPatchMax = 1000;

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
   * Populate a Semver from components.  If any component fails validation,
   * the result is false and this Semver is unchanged.
   *
   * @param major Major version number
   * @param minor Minor version number
   * @param patch Patch version number
   * @param prerelease Optional pre-release identifiers (e.g., {"alpha", "1"})
   * @param build_metadata Optional build metadata identifiers
   * @return true if component validation passes
   */
  bool from_components(unsigned long major, unsigned long minor,
                       unsigned long patch,
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
  std::vector<std::string> prerelease_;
  std::vector<std::string> build_metadata_;
  bool valid_;
};

}  // namespace villagesql

#endif  // VILLAGESQL_INCLUDE_SEMVER_H_
