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

  // Maximum number of source characters reproduced by safe_for_output().
  static constexpr size_t kMaxOutputChars = 10;

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
   * Static factory method to parse and create a Semver.
   *
   * @param version_str String in semver format
   * @param[out] error Optional error message if parsing fails
   * @return Semver object, check is_valid() to see if parsing succeeded
   */
  static Semver from_string(std::string_view version_str,
                            std::string *error = nullptr);

  /**
   * Static factory method to create a Semver from components.
   *
   * @param major Major version number
   * @param minor Minor version number
   * @param patch Patch version number
   * @param prerelease Optional pre-release identifiers (e.g., {"alpha", "1"})
   * @param build_metadata Optional build metadata identifiers
   * @return Semver object, always valid if component validation passes
   */
  static Semver from_components(
      unsigned long major, unsigned long minor, unsigned long patch,
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
   * Sanitize an arbitrary string for safe inclusion in output such as error
   * messages. At most kMaxOutputChars source characters are reproduced; any
   * unprintable character is rendered as a readable escape sequence (e.g.
   * "\n", "\x1b"); if the input is longer than kMaxOutputChars an ellipsis is
   * appended.
   *
   * @param str The string to sanitize
   * @return A printable, length-limited representation of str
   */
  static std::string safe_for_output(std::string_view str);

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
  /**
   * Compare pre-release identifiers according to semver rules.
   * @return -1 if this < other, 0 if equal, 1 if this > other
   */
  int compare_prerelease(const Semver &other) const;

  /**
   * Parse and validate a single core version component.
   *
   * Validates that the component is numeric with no leading zeros, converts it,
   * and range-checks the result, rejecting values that overflow or exceed the
   * (inclusive) maximum allowed for this position.
   *
   * @param version_str Component string
   * @param version_max Inclusive maximum value allowed for this position
   * @param name Position name used in error messages (e.g. "MAJOR")
   * @param[out] out Parsed value
   * @param[out] error Optional error message if validation fails
   * @return true on success, false otherwise
   */
  static bool parse_core_component(std::string_view version_str,
                                   unsigned long version_max, const char *name,
                                   unsigned long *out, std::string *error);

  /**
   * Check that a parsed core version value is within its allowed bound.
   *
   * @param version_val The parsed value to check
   * @param version_max Inclusive maximum value allowed for this position
   * @param name Position name used in the error message (e.g. "MAJOR")
   * @param[out] error Optional error message if the value exceeds the bound
   * @return true if version_val <= version_max, false otherwise
   */
  static bool check_version_bound(unsigned long version_val,
                                  unsigned long version_max, const char *name,
                                  std::string *error);

  /**
   * Parse and validate the core "MAJOR.MINOR.PATCH" segment.
   *
   * @param core The core segment, with no pre-release or build metadata
   * @param[out] major Parsed MAJOR number
   * @param[out] minor Parsed MINOR number
   * @param[out] patch Parsed PATCH number
   * @param[out] error Optional error message if validation fails
   * @return true on success, false otherwise
   */
  static bool parse_core(std::string_view core, unsigned long *major,
                         unsigned long *minor, unsigned long *patch,
                         std::string *error);

  /**
   * Parse and validate the dot-separated pre-release segment.
   *
   * @param segment Pre-release identifiers, without the leading '-'
   * @param[out] out Parsed pre-release identifiers
   * @param[out] error Optional error message if validation fails
   * @return true on success, false otherwise
   */
  static bool parse_prerelease(std::string_view segment,
                               std::vector<std::string> *out,
                               std::string *error);

  /**
   * Parse and validate the dot-separated build metadata segment.
   *
   * @param segment Build metadata identifiers, without the leading '+'
   * @param[out] out Parsed build metadata identifiers
   * @param[out] error Optional error message if validation fails
   * @return true on success, false otherwise
   */
  static bool parse_build_metadata(std::string_view segment,
                                   std::vector<std::string> *out,
                                   std::string *error);

  /**
   * Check if a string is a valid identifier (alphanumeric + hyphen)
   */
  static bool is_valid_identifier(std::string_view id);

  /**
   * Check if a string contains only digits
   */
  static bool is_numeric(std::string_view str);

  /**
   * Check if a numeric identifier has a disallowed leading zero, i.e. it is
   * more than one digit long and starts with '0'. Per the semver spec, numeric
   * identifiers must not include leading zeros.
   */
  static bool has_leading_zero(std::string_view str);

  unsigned long major_;
  unsigned long minor_;
  unsigned long patch_;
  std::vector<std::string> prerelease_;
  std::vector<std::string> build_metadata_;
  bool valid_;
};

}  // namespace villagesql

#endif  // VILLAGESQL_INCLUDE_SEMVER_H_
