/*
 * Copyright (c) 2014, 2025, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is designed to work with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have either included with
 * the program or referenced in the documentation.
 *
 * This program is distributed in the hope that it will be useful,  but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See
 * the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include "utils/utils_general.h"

#include <regex>
#include <string_view>
#include <utility>

#include "mysql/harness/logging/logging.h"

namespace shcore {

Scoped_callback::~Scoped_callback() noexcept {
  try {
    call();
  } catch (const std::exception &e) {
    mysql_harness::logging::log_error("Unexpected exception: %s", e.what());
  }
}

bool is_valid_identifier(std::string_view name) {
  if (name.empty()) return false;

  std::locale locale;

  if (!std::isalpha(name.front(), locale) && (name.front() != '_'))
    return false;

  return std::all_of(
      std::next(name.begin()), name.end(), [&locale](auto cur_char) {
        return std::isalnum(cur_char, locale) || (cur_char == '_');
      });
}

}  // namespace shcore
