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

#ifndef VILLAGESQL_INCLUDE_STRING_UTILS_H_
#define VILLAGESQL_INCLUDE_STRING_UTILS_H_

#include <cstddef>
#include <string>
#include <string_view>

// General-purpose string helpers for VillageSQL. This header deliberately
// depends only on the C++ standard library, so it can be included from any
// layer, including code that must not pull in the MySQL server headers.

namespace villagesql {

// Default maximum number of source characters reproduced by
// sanitize_for_output(). Long enough for the identifiers and names that usually
// appear in a message, short enough that a runaway string cannot flood the
// error log.
inline constexpr size_t kDefaultMaxOutputChars = 64;

// Sanitize an arbitrary string for safe inclusion in output such as error
// messages and log lines.
//
// Untrusted text must never travel verbatim into the error log or a
// client-visible error: a stray control character can garble a terminal, and
// an unbounded string can flood the log. At most max_chars source characters
// are reproduced; any unprintable character is rendered as a readable escape
// sequence (e.g. "\n", "\x1b"); if the input is longer than max_chars an
// ellipsis is appended.
//
// @param str The string to sanitize
// @param max_chars Maximum number of source characters to reproduce
// @return A printable, length-limited representation of str
std::string sanitize_for_output(std::string_view str,
                                size_t max_chars = kDefaultMaxOutputChars);

}  // namespace villagesql

#endif  // VILLAGESQL_INCLUDE_STRING_UTILS_H_
