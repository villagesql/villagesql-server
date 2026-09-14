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

#include "villagesql/include/string_utils.h"

#include <algorithm>
#include <cctype>
#include <cstdio>

namespace villagesql {

std::string sanitize_for_output(std::string_view str, size_t max_chars) {
  std::string result;
  size_t limit = std::min(str.size(), max_chars);
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
  if (str.size() > max_chars) result += "\u2026";
  return result;
}

}  // namespace villagesql
