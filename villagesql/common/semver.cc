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
#include <cerrno>
#include <cstdlib>

namespace villagesql {

Semver::Semver() : major_(0), minor_(0), patch_(0), valid_(false) {}

bool Semver::is_numeric(std::string_view str) {
  if (str.empty()) return false;
  return std::all_of(str.begin(), str.end(),
                     [](unsigned char c) { return std::isdigit(c); });
}

bool Semver::has_leading_zero(std::string_view str) {
  return str.length() > 1 && str[0] == '0';
}

bool Semver::is_valid_identifier(std::string_view id) {
  if (id.empty()) return false;
  return std::all_of(id.begin(), id.end(), [](unsigned char c) {
    return std::isalnum(c) || c == '-';
  });
}

bool Semver::parse_core_component(const std::string &version_str, unsigned long version_max,
                                  const char *name, unsigned long *out,
                                  std::string *error) {
  if (!is_numeric(version_str)) {
    if (error)
      *error = std::string(name) + " must be numeric, not " +
               safe_for_output(version_str);
    return false;
  }

  // Check for leading zeros
  if (has_leading_zero(version_str)) {
    if (error)
      *error = std::string(name) + " version numbers ("
               + safe_for_output(version_str)
               + ") must not have leading zeros";
    return false;
  }

  errno = 0;
  unsigned long value = strtoul(version_str.c_str(), nullptr, 10);
  if (errno == ERANGE ) {
    if (error)
      *error = std::string(name) + " version ("
               + safe_for_output(version_str)
               + ") is out of range";
    return false;
  }

  if (!check_version_bound(value, version_max, name, error)) return false;

  *out = value;
  return true;
}

bool Semver::check_version_bound(unsigned long version_val, unsigned long version_max,
                                 const char *name, std::string *error) {

  // Easy case if things are valid
  if (version_val <= version_max) return true;

  // It must be too big
  if (error)
    *error = std::string(name) + " version value("
              + std::to_string(version_val)
              + ") must not exceed " +
              std::to_string(version_max);
  return false;
}

bool Semver::parse_core(std::string_view core, unsigned long *major,
                        unsigned long *minor, unsigned long *patch,
                        std::string *error) {
  if (std::count(core.begin(), core.end(), '.') != 2) {
    if (error)
      *error = "Invalid core version format, expected MAJOR.MINOR.PATCH";
    return false;
  }

  size_t minor_pos = core.find('.');
  size_t patch_pos = core.find('.', minor_pos + 1);
  std::string major_str(core.substr(0, minor_pos));
  std::string minor_str(core.substr(minor_pos + 1, patch_pos - minor_pos - 1));
  std::string patch_str(core.substr(patch_pos + 1));

  // Parse each position into its own bounds.
  return parse_core_component(major_str, kMajorMax, "MAJOR", major, error) &&
         parse_core_component(minor_str, kMinorMax, "MINOR", minor, error) &&
         parse_core_component(patch_str, kPatchMax, "PATCH", patch, error);
}

bool Semver::parse_prerelease(std::string_view segment,
                              std::vector<std::string> *out,
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

bool Semver::parse_build_metadata(std::string_view segment,
                                  std::vector<std::string> *out,
                                  std::string *error) {
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

bool Semver::parse(std::string_view s, std::string *error) {
  // Reset state
  *this = Semver();

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

  // Parse each component into temporaries so that a failure partway through
  // leaves the existing object state untouched.
  unsigned long major_tmp = 0, minor_tmp = 0, patch_tmp = 0;
  if (!parse_core(core, &major_tmp, &minor_tmp, &patch_tmp, error)) {
    return false;
  }

  std::vector<std::string> prerelease_tmp;
  if (has_prerelease &&
      !parse_prerelease(prerelease_seg, &prerelease_tmp, error)) {
    return false;
  }

  std::vector<std::string> build_metadata_tmp;
  if (has_build_metadata &&
      !parse_build_metadata(build_seg, &build_metadata_tmp, error)) {
    return false;
  }

  major_ = major_tmp;
  minor_ = minor_tmp;
  patch_ = patch_tmp;
  prerelease_.swap(prerelease_tmp);
  build_metadata_.swap(build_metadata_tmp);
  valid_ = true;
  return true;
}

Semver Semver::from_string(std::string_view version_str, std::string *error) {
  Semver ver;
  ver.parse(version_str, error);
  return ver;
}

Semver Semver::from_components(unsigned long major, unsigned long minor,
                               unsigned long patch,
                               const std::vector<std::string> &prerelease,
                               const std::vector<std::string> &build_metadata) {
  Semver ver;

  // Assume failure unless everything succeeds
  ver.valid_ = false;

  if (!check_version_bound(major, kMajorMax, "MAJOR", nullptr)) return ver;
  if (!check_version_bound(major, kMinorMax, "MINOR", nullptr)) return ver;
  if (!check_version_bound(major, kPatchMax, "PATCH", nullptr)) return ver;

  // Validate identifiers if provided
  for (const auto &id : prerelease) {
    if (!is_valid_identifier(id))  return ver;

    // Check for leading zeros in numeric identifiers
    if (is_numeric(id) && has_leading_zero(id)) return ver;
  }

  for (const auto &id : build_metadata) {
    if (!is_valid_identifier(id)) return ver;
  }

  // All of the components are valid
  ver.major_ = major;
  ver.minor_ = minor;
  ver.patch_ = patch;
  ver.prerelease_ = prerelease;
  ver.build_metadata_ = build_metadata;
  ver.valid_ = true;

  return ver;
}

std::string Semver::to_string() const {
  if (!valid_) return "";

  const int sz = 33;  // 3x ten digits + 2 dots + 1 null
  char buf[sz];
  int n = snprintf(buf, sz, "%lu.%lu.%lu", major_, minor_, patch_);
  std::string r(buf, n);

  int extra = 0;
  for (size_t i = 0; i < prerelease_.size(); ++i) {
    extra += 1 + prerelease_[i].size();
  }
  for (size_t i = 0; i < build_metadata_.size(); ++i) {
    extra += 1 + build_metadata_[i].size();
  }

  r.reserve(r.size() + 1 + extra);

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

std::string Semver::safe_for_output(std::string_view str) {
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

int Semver::compare_prerelease(const Semver &other) const {
  // No pre-release > has pre-release
  if (prerelease_.empty() && !other.prerelease_.empty()) return 1;
  if (!prerelease_.empty() && other.prerelease_.empty()) return -1;
  if (prerelease_.empty() && other.prerelease_.empty()) return 0;

  // Compare identifier by identifier
  size_t min_size = std::min(prerelease_.size(), other.prerelease_.size());
  for (size_t i = 0; i < min_size; ++i) {
    const std::string &l = prerelease_[i];
    const std::string &r = other.prerelease_[i];

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
  if (prerelease_.size() < other.prerelease_.size()) return -1;
  if (prerelease_.size() > other.prerelease_.size()) return 1;
  return 0;
}

bool Semver::operator==(const Semver &other) const {
  if (!valid_ || !other.valid_) return false;
  return major_ == other.major_ && minor_ == other.minor_ &&
         patch_ == other.patch_ && prerelease_ == other.prerelease_;
  // Build metadata is ignored per semver spec
}

bool Semver::operator!=(const Semver &other) const { return !(*this == other); }

bool Semver::operator<(const Semver &other) const {
  if (!valid_ || !other.valid_) return false;

  // Compare major.minor.patch
  if (major_ != other.major_) return major_ < other.major_;
  if (minor_ != other.minor_) return minor_ < other.minor_;
  if (patch_ != other.patch_) return patch_ < other.patch_;

  // Core versions are equal, compare pre-release
  return compare_prerelease(other) < 0;
}

bool Semver::operator<=(const Semver &other) const {
  return *this < other || *this == other;
}

bool Semver::operator>(const Semver &other) const { return !(*this <= other); }

bool Semver::operator>=(const Semver &other) const { return !(*this < other); }

}  // namespace villagesql
