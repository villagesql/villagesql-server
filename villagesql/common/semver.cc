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

#include "villagesql/include/semver.h"

#include <algorithm>
#include <cctype>
#include <charconv>

namespace villagesql {

namespace {

// Check if a string contains only digits.
bool is_numeric(std::string_view str) {
  if (str.empty()) return false;
  return std::all_of(str.begin(), str.end(),
                     [](unsigned char c) { return std::isdigit(c); });
}

// Check if a numeric identifier has a disallowed leading zero, i.e. it is
// more than one digit long and starts with '0'. Per the semver spec, numeric
// identifiers must not include leading zeros.
bool has_leading_zero(std::string_view str) {
  return str.length() > 1 && str[0] == '0';
}

// Check if a string is a valid identifier (alphanumeric + hyphen).
bool is_valid_identifier(std::string_view id) {
  if (id.empty()) return false;
  return std::all_of(id.begin(), id.end(), [](unsigned char c) {
    return std::isalnum(c) || c == '-';
  });
}

// Check that a parsed core version value is within its allowed bound.
//
// @param version_val The parsed value to check
// @param version_max Inclusive maximum value allowed for this position
// @param name Position name used in the error message (e.g. "MAJOR")
// @param[out] error Optional error message if the value exceeds the bound
// @return true if version_val <= version_max, false otherwise
bool check_version_bound(unsigned long version_val, unsigned long version_max,
                         const char *name, std::string *error) {
  // Easy case if things are valid
  if (version_val <= version_max) return true;

  // It must be too big
  if (error)
    *error = std::string(name) + " version value(" +
             std::to_string(version_val) + ") must not exceed " +
             std::to_string(version_max);
  return false;
}

// Maximum number of source characters reproduced by safe_for_output().
constexpr size_t kMaxOutputChars = 10;

// Sanitize an arbitrary string for safe inclusion in output such as error
// messages. At most kMaxOutputChars source characters are reproduced; any
// unprintable character is rendered as a readable escape sequence (e.g.
// "\n", "\x1b"); if the input is longer than kMaxOutputChars an ellipsis is
// appended.
//
// @param str The string to sanitize
// @return A printable, length-limited representation of str
std::string safe_for_output(std::string_view str) {
  std::string result;
  size_t limit = std::min(str.size(), kMaxOutputChars);
  for (size_t i = 0; i < limit; ++i) {
    unsigned char c = static_cast<unsigned char>(str[i]);
    switch (c) {
      case '\t':
        result += "\\t";
        break;
      case '\n':
        result += "\\n";
        break;
      case '\r':
        result += "\\r";
        break;
      case '\\':  // Keep out safe with backslash.
        result += "\\\\";
        break;
      default:
        if (std::isprint(c)) {
          result += static_cast<char>(c);
        } else {
          char buf[5];
          snprintf(buf, sizeof(buf), "\\x%02X", c);
          result += buf;
        }
    }
  }
  // Truncated strings get an unicode ellipsis appended at the end
  if (str.size() > kMaxOutputChars) result += "\u2026";
  return result;
}

// Parse and validate a single core version component.
//
// Validates that the component is numeric with no leading zeros, converts it,
// and range-checks the result, rejecting values that overflow or exceed the
// (inclusive) maximum allowed for this position.
//
// @param version_str Component string
// @param version_max Inclusive maximum value allowed for this position
// @param name Position name used in error messages (e.g. "MAJOR")
// @param[out] out Parsed value
// @param[out] error Optional error message if validation fails
// @return true on success, false otherwise
bool parse_core_component(std::string_view version_str,
                          unsigned long version_max, const char *name,
                          unsigned long *out, std::string *error) {
  if (!is_numeric(version_str)) {
    if (error)
      *error = std::string(name) + " must be numeric, not " +
               safe_for_output(version_str);
    return false;
  }

  // Check for leading zeros
  if (has_leading_zero(version_str)) {
    if (error)
      *error = std::string(name) + " version numbers (" +
               safe_for_output(version_str) + ") must not have leading zeros";
    return false;
  }

  unsigned long value = 0;
  auto [ptr, ec] = std::from_chars(
      version_str.data(), version_str.data() + version_str.size(), value);
  if (ec != std::errc()) {
    if (error)
      *error = std::string(name) + " version (" + safe_for_output(version_str) +
               ") is out of range";
    return false;
  }

  if (!check_version_bound(value, version_max, name, error)) return false;

  *out = value;
  return true;
}

// Parse and validate the core "MAJOR.MINOR.PATCH" segment.
//
// @param core The core segment, with no pre-release or build metadata
// @param[out] major Parsed MAJOR number
// @param[out] minor Parsed MINOR number
// @param[out] patch Parsed PATCH number
// @param[out] error Optional error message if validation fails
// @return true on success, false otherwise
bool parse_core(std::string_view core, unsigned long *major,
                unsigned long *minor, unsigned long *patch,
                std::string *error) {
  if (std::count(core.begin(), core.end(), '.') != 2) {
    if (error)
      *error = "Invalid core version format, expected MAJOR.MINOR.PATCH";
    return false;
  }

  size_t minor_pos = core.find('.');
  size_t patch_pos = core.find('.', minor_pos + 1);
  std::string_view major_str = core.substr(0, minor_pos);
  std::string_view minor_str =
      core.substr(minor_pos + 1, patch_pos - minor_pos - 1);
  std::string_view patch_str = core.substr(patch_pos + 1);

  // Parse each position into its own bounds.
  return parse_core_component(major_str, Semver::kMajorMax, "MAJOR", major,
                              error) &&
         parse_core_component(minor_str, Semver::kMinorMax, "MINOR", minor,
                              error) &&
         parse_core_component(patch_str, Semver::kPatchMax, "PATCH", patch,
                              error);
}

// Parse and validate the dot-separated pre-release segment.
//
// @param segment Pre-release identifiers, without the leading '-'
// @param[out] out Parsed pre-release identifiers
// @param[out] error Optional error message if validation fails
// @return true on success, false otherwise
bool parse_prerelease(std::string_view segment, std::vector<std::string> *out,
                      std::string *error) {
  // Split by dots
  while (true) {
    size_t dot = segment.find('.');
    std::string_view identifier = segment.substr(0, dot);

    if (!is_valid_identifier(identifier)) {
      if (error) *error = "Invalid pre-release identifier";
      return false;
    }
    // Check for leading zeros in numeric identifiers
    if (is_numeric(identifier) && has_leading_zero(identifier)) {
      if (error)
        *error = "Numeric pre-release identifiers must not have leading zeros";
      return false;
    }

    out->push_back(std::string(identifier));
    if (dot == std::string_view::npos) break;
    segment.remove_prefix(dot + 1);
  }
  return true;
}

// Parse and validate the dot-separated build metadata segment.
//
// @param segment Build metadata identifiers, without the leading '+'
// @param[out] out Parsed build metadata identifiers
// @param[out] error Optional error message if validation fails
// @return true on success, false otherwise
bool parse_build_metadata(std::string_view segment,
                          std::vector<std::string> *out, std::string *error) {
  // Split by dots
  while (true) {
    size_t dot = segment.find('.');
    std::string_view identifier = segment.substr(0, dot);

    if (!is_valid_identifier(identifier)) {
      if (error) *error = "Invalid build metadata identifier";
      return false;
    }

    out->push_back(std::string(identifier));
    if (dot == std::string_view::npos) break;
    segment.remove_prefix(dot + 1);
  }
  return true;
}

// Compare two pre-release identifier lists according to semver rules.
// @return -1 if lhs < rhs, 0 if equal, 1 if lhs > rhs
int compare_prerelease(const std::vector<std::string> &lhs,
                       const std::vector<std::string> &rhs) {
  // No pre-release > has pre-release
  if (lhs.empty() && !rhs.empty()) return 1;
  if (!lhs.empty() && rhs.empty()) return -1;
  if (lhs.empty() && rhs.empty()) return 0;

  // Compare identifier by identifier
  size_t min_size = std::min(lhs.size(), rhs.size());
  for (size_t i = 0; i < min_size; ++i) {
    const std::string &l = lhs[i];
    const std::string &r = rhs[i];

    bool l_numeric = is_numeric(l);
    bool r_numeric = is_numeric(r);

    if (l_numeric && r_numeric) {
      // Both numeric - compare numerically
      unsigned long l_val = std::stoul(l);
      unsigned long r_val = std::stoul(r);
      if (l_val < r_val) return -1;
      if (l_val > r_val) return 1;
    } else if (l_numeric && !r_numeric) {
      // Numeric < alphanumeric
      return -1;
    } else if (!l_numeric && r_numeric) {
      // Alphanumeric > numeric
      return 1;
    } else {
      // Both alphanumeric - compare lexically
      if (l < r) return -1;
      if (l > r) return 1;
    }
  }

  // All compared identifiers are equal, check length
  if (lhs.size() < rhs.size()) return -1;
  if (lhs.size() > rhs.size()) return 1;
  return 0;
}

// Map a code base string to its canonical, statically-stored equivalent.
//
// Only the known code bases are accepted. Returning the static constant (rather
// than the caller's string) lets Semver::code_base_ hold a string_view that
// never dangles, regardless of where the input came from (a parsed substring,
// a temporary, etc.).
//
// @param code_base Candidate code base string
// @return The matching static code base view, or an empty view if unknown.
std::string_view canonical_code_base(std::string_view code_base) {
  if (code_base == Semver::kMysql84CodeBase) return Semver::kMysql84CodeBase;
  if (code_base == Semver::kMysql97CodeBase) return Semver::kMysql97CodeBase;
  if (code_base == Semver::kPercona84CodeBase)
    return Semver::kPercona84CodeBase;
  if (code_base == Semver::kPercona97CodeBase)
    return Semver::kPercona97CodeBase;
  return {};
}

// Parse and validate a core version body, i.e. everything after the code base
// prefix: "MAJOR.MINOR.PATCH[-PRERELEASE][+BUILDMETADATA]".
//
// @param s The version body, with no code base prefix
// @param[out] major Parsed MAJOR number
// @param[out] minor Parsed MINOR number
// @param[out] patch Parsed PATCH number
// @param[out] prerelease Parsed pre-release identifiers (unchanged if none)
// @param[out] build_metadata Parsed build metadata identifiers (unchanged if
//             none)
// @param[out] error Optional error message if validation fails
// @return true on success, false otherwise
bool parse_version_body(std::string_view s, unsigned long *major,
                        unsigned long *minor, unsigned long *patch,
                        std::vector<std::string> *prerelease,
                        std::vector<std::string> *build_metadata,
                        std::string *error) {
  if (s.empty()) {
    if (error) *error = "Empty version string";
    return false;
  }

  // Split into core[-prerelease][+build_metadata] segments. The core segment
  // ends at the first '-' or '+'; any pre-release runs up to a trailing '+'.
  std::string_view core = s.substr(0, std::min(s.find('+'), s.find('-')));
  s.remove_prefix(core.size());

  bool has_prerelease = !s.empty() && s[0] == '-';
  std::string_view prerelease_seg;
  if (has_prerelease) {
    std::string_view seg = s.substr(0, s.find('+'));
    s.remove_prefix(seg.size());
    prerelease_seg = seg.substr(1);  // skip leading '-'
  }

  bool has_build_metadata = !s.empty() && s[0] == '+';
  std::string_view build_seg;
  if (has_build_metadata) {
    build_seg = s.substr(1);  // skip leading '+'
  }

  if (!parse_core(core, major, minor, patch, error)) {
    return false;
  }
  if (has_prerelease && !parse_prerelease(prerelease_seg, prerelease, error)) {
    return false;
  }
  if (has_build_metadata &&
      !parse_build_metadata(build_seg, build_metadata, error)) {
    return false;
  }
  return true;
}

}  // namespace

Semver::Semver() : major_(0), minor_(0), patch_(0), valid_(false) {}

bool Semver::parse(std::string_view s, std::string *error) {
  // Reset state
  *this = Semver();

  if (s.empty()) {
    if (error) *error = "Empty version string";
    return false;
  }

  // The code base prefix is mandatory and is separated from the core version by
  // '_'. Semver identifiers never contain '_', so the first '_' is the
  // unambiguous separator.
  size_t sep = s.find('_');
  if (sep == std::string_view::npos) {
    if (error) *error = "Missing codebase prefix";
    return false;
  }
  std::string_view code_base = s.substr(0, sep);
  std::string_view body = s.substr(sep + 1);

  // Validate the codebase, with error messages
  std::string_view canonical = canonical_code_base(code_base);
  if (canonical.empty()) {
    if (error) *error = "Invalid codebase " + safe_for_output(code_base);
    return false;
  }

  unsigned long major = 0, minor = 0, patch = 0;
  std::vector<std::string> prerelease;
  std::vector<std::string> build_metadata;
  if (!parse_version_body(body, &major, &minor, &patch, &prerelease,
                          &build_metadata, error)) {
    return false;
  }

  // Accept a small amount of duplicate validation for consistency in behavior.
  if (!from_components(major, minor, patch, code_base, prerelease,
                       build_metadata)) {
    if (error && error->empty())
      *error = "Invalid version string: " + safe_for_output(s);
    return false;
  }
  return true;
}

bool Semver::parse_schema_version(std::string_view s, std::string *error) {
  // Reset state
  *this = Semver();

  // Legacy stored versions predate code bases: they have no prefix and begin
  // with the numeric MAJOR component. Those builds were always based on
  // mysql-8.4, so parse the historical layout and assign the legacy code base.
  // Anything else must carry a code base prefix, exactly like parse().
  if (!s.empty() && std::isdigit(static_cast<unsigned char>(s[0]))) {
    unsigned long major = 0, minor = 0, patch = 0;
    std::vector<std::string> prerelease;
    std::vector<std::string> build_metadata;
    if (!parse_version_body(s, &major, &minor, &patch, &prerelease,
                            &build_metadata, error)) {
      return false;
    }
    return from_components(major, minor, patch, kLegacyCodeBase, prerelease,
                           build_metadata);
  }

  return parse(s, error);
}

bool Semver::from_components(unsigned long major, unsigned long minor,
                             unsigned long patch,
                             const std::string_view code_base,
                             const std::vector<std::string> &prerelease,
                             const std::vector<std::string> &build_metadata) {
  // The code base is mandatory and must be one of the known values. Resolving
  // it to the static constant keeps code_base_ pointing at storage that
  // outlives any caller-supplied string.
  std::string_view canonical = canonical_code_base(code_base);
  if (canonical.empty()) return false;

  if (!check_version_bound(major, kMajorMax, "MAJOR", nullptr)) return false;
  if (!check_version_bound(minor, kMinorMax, "MINOR", nullptr)) return false;
  if (!check_version_bound(patch, kPatchMax, "PATCH", nullptr)) return false;

  // Validate identifiers if provided
  for (const auto &id : prerelease) {
    if (!is_valid_identifier(id)) return false;

    // Check for leading zeros in numeric identifiers
    if (is_numeric(id) && has_leading_zero(id)) return false;
  }

  for (const auto &id : build_metadata) {
    if (!is_valid_identifier(id)) return false;
  }

  // All of the components are valid
  code_base_ = canonical;
  major_ = major;
  minor_ = minor;
  patch_ = patch;
  prerelease_ = prerelease;
  build_metadata_ = build_metadata;
  valid_ = true;
  return true;
}

std::string Semver::to_string() const {
  if (!valid_) return "";

  const int sz = 33;  // 3x ten digits + 2 dots + 1 null
  char buf[sz];
  int n = snprintf(buf, sz, "%lu.%lu.%lu", major_, minor_, patch_);

  // Count the size of the optional segments
  int extra = 0;
  for (size_t i = 0; i < prerelease_.size(); ++i) {
    extra += 1 + prerelease_[i].size();
  }
  for (size_t i = 0; i < build_metadata_.size(); ++i) {
    extra += 1 + build_metadata_[i].size();
  }

  std::string r;
  r.reserve(code_base_.size() + 1 + n + 1 + extra);
  r.append(code_base_.data(), code_base_.size());
  r.push_back('_');
  r.append(buf, n);

  for (size_t i = 0; i < prerelease_.size(); ++i) {
    r.push_back(i == 0 ? '-' : '.');
    r.append(prerelease_[i]);
  }

  for (size_t i = 0; i < build_metadata_.size(); ++i) {
    r.push_back(i == 0 ? '+' : '.');
    r.append(build_metadata_[i]);
  }

  return r;
}

bool Semver::operator==(const Semver &other) const {
  if (!valid_ || !other.valid_) return false;
  // Equality requires a matching code base. Build metadata is ignored per the
  // semver spec.
  return code_base_ == other.code_base_ && major_ == other.major_ &&
         minor_ == other.minor_ && patch_ == other.patch_ &&
         prerelease_ == other.prerelease_;
}

bool Semver::operator!=(const Semver &other) const { return !(*this == other); }

bool Semver::operator<(const Semver &other) const {
  if (!valid_ || !other.valid_) return false;

  // Versions from different code bases are unordered: every relational operator
  // (built on < and ==) is false, so neither is "less than" the other.
  if (code_base_ != other.code_base_) return false;

  // Compare major.minor.patch
  if (major_ != other.major_) return major_ < other.major_;
  if (minor_ != other.minor_) return minor_ < other.minor_;
  if (patch_ != other.patch_) return patch_ < other.patch_;

  // Core versions are equal, compare pre-release
  return compare_prerelease(prerelease_, other.prerelease_) < 0;
}

bool Semver::operator<=(const Semver &other) const {
  return *this < other || *this == other;
}

// Defined in terms of < and == (not negation) so that versions from different
// code bases stay mutually unordered: all four relational operators are false.
bool Semver::operator>(const Semver &other) const { return other < *this; }

bool Semver::operator>=(const Semver &other) const {
  return *this > other || *this == other;
}

}  // namespace villagesql
