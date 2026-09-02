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

#ifndef VILLAGESQL_INCLUDE_ERROR_H_
#define VILLAGESQL_INCLUDE_ERROR_H_

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "include/my_compiler.h"
#include "include/my_sys.h"
#include "include/mysqld_error.h"
#include "mysql/components/services/log_builtins.h"

// Macro to report VillageSQL errors using the generic error number.
// Usage: villagesql_error("Custom type %s not found", MYF(0), type_name);
#define villagesql_error(formatmsg, flags, ...) \
  my_printf_error(ER_VILLAGESQL_GENERIC_ERROR, formatmsg, flags, ##__VA_ARGS__)

// A generic error that indicates the logs may have more information.
inline void villagesql_check_error_log() {
  villagesql_error("Check error log for more info", MYF(0));
}

#define LogVSQL(severity, ...)                \
  LogEvent()                                  \
      .prio(severity)                         \
      .errcode(ER_VILLAGESQL_GENERIC_MESSAGE) \
      .subsys(LOG_SUBSYSTEM_TAG)              \
      .source_line(__LINE__)                  \
      .source_file(MY_BASENAME)               \
      .function(__FUNCTION__)                 \
      .message_quoted("VillageSQL", ##__VA_ARGS__)

// An "assertion" that has error handling for non-debug releases. If the
// expected value passed in is not true, it asserts, and returns true if it
// would have asserted.
//
// These are macros so that __LINE__ and __FILE__ refer to the caller's
// location, not this header file.
//
// Example:
//   if (should_assert_if_false(expected)) {
//     villagesql_error(...);
//     return true;
//   }
#define should_assert_if_false(expected) \
  should_assert_if_false_impl((expected), #expected, __FILE__, __LINE__)

inline bool should_assert_if_false_impl([[maybe_unused]] bool result,
                                        [[maybe_unused]] const char *expr_str,
                                        [[maybe_unused]] const char *file,
                                        [[maybe_unused]] int line) {
#ifndef NDEBUG
  if (!result) {
#if defined(__APPLE__)
    __assert_rtn(__func__, file, line, expr_str);
#elif defined(__GLIBC__)
    __assert_fail(expr_str, file, line, __func__);
#else
    fprintf(stderr, "Assertion failed: %s at %s:%d\n", expr_str, file, line);
    abort();
#endif
  }
#endif
  return unlikely(!result);
}

// Same as above, but an alias for pointers.
#define should_assert_if_null(ptr) should_assert_if_false(nullptr != (ptr))

// Like the above, but asserts if the unexpected condition is true. Returns true
// if it would have asserted.
#define should_assert_if_true(unexpected) should_assert_if_false(!(unexpected))

#endif  // VILLAGESQL_INCLUDE_ERROR_H_
