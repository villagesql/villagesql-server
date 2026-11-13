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

// Custom test main for VillageSQL unit tests.
//
// This is similar to gunit_test_main.cc but does NOT define
// system_charset_info. This ensures that tests linking with
// server_unittest_library use the single definition from that library, avoiding
// symbol duplication issues on macOS where the dynamic linker uses two-level
// namespace by default.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#ifndef _WIN32
#include <sys/resource.h>
#include <sys/time.h>
#endif
#include <sys/types.h>

#include "my_getopt.h"
#include "my_inttypes.h"
#include "my_stacktrace.h"
#include "my_sys.h"

#ifdef _WIN32
#define SIGNAL_FMT "exception 0x%x"
#else
#define SIGNAL_FMT "signal %d"
#endif

static void signal_handler(int sig) {
  my_safe_printf_stderr("unit test got " SIGNAL_FMT "\n", sig);

#ifdef HAVE_STACKTRACE
  my_print_stacktrace(nullptr, 0);
#endif

  exit(EXIT_FAILURE);
}

#ifdef _WIN32

LONG WINAPI exception_filter(EXCEPTION_POINTERS *exp) {
  __try {
    my_set_exception_pointers(exp);
    signal_handler(exp->ExceptionRecord->ExceptionCode);
  } __except (EXCEPTION_EXECUTE_HANDLER) {
    fputs("Got exception in exception handler!\n", stderr);
  }

  return EXCEPTION_CONTINUE_SEARCH;
}

static void init_signal_handling() {
  _CrtSetReportMode(_CRT_WARN, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDERR);
  _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDERR);
  _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDERR);

  UINT mode = SetErrorMode(0) | SEM_FAILCRITICALERRORS | SEM_NOOPENFILEERRORBOX;
  SetErrorMode(mode);

  SetUnhandledExceptionFilter(exception_filter);
}

#else

static void init_signal_handling() {
#ifdef HAVE_STACKTRACE
  my_init_stacktrace();
#endif

  struct sigaction sa;
  sa.sa_flags = SA_RESETHAND | SA_NODEFER;
  sigemptyset(&sa.sa_mask);
  sigprocmask(SIG_SETMASK, &sa.sa_mask, nullptr);

  sa.sa_handler = signal_handler;
  sigaction(SIGABRT, &sa, nullptr);
  sigaction(SIGFPE, &sa, nullptr);
#if defined(HANDLE_FATAL_SIGNALS)
  sigaction(SIGBUS, &sa, nullptr);
  sigaction(SIGILL, &sa, nullptr);
  sigaction(SIGSEGV, &sa, nullptr);
#endif
}

#endif

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::InitGoogleMock(&argc, argv);
  MY_INIT(argv[0]);

#ifndef _WIN32
  rlimit core_limit;
  core_limit.rlim_cur = 0;
  core_limit.rlim_max = 0;
  setrlimit(RLIMIT_CORE, &core_limit);
#endif

  init_signal_handling();

  return RUN_ALL_TESTS();
}
