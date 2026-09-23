/* Copyright (c) 2010, 2026, Oracle and/or its affiliates.
   Copyright (c) 2026 VillageSQL Contributors

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef _welcome_copyright_notice_h_
#define _welcome_copyright_notice_h_

/**
  @file include/welcome_copyright_notice.h
*/

#include <string.h>

#define COPYRIGHT_NOTICE_CURRENT_YEAR "2026"

// The VillageSQL copyright line shown alongside Oracle's in the startup banner
// and --help output of every VillageSQL binary. It is tied to
// COPYRIGHT_NOTICE_CURRENT_YEAR so the year follows whatever upstream bumps
// that constant to rather than going stale on its own.
//
// At least one caller passes the surrounding notice straight to fprintf as a
// format string (mysql_secure_installation), so this must never contain a '%'.
//
// This file is listed in the villint-ignore of the commit that added these
// lines: clang-format would reflow Oracle's hand-aligned text below, and the
// alignment is kept byte-identical to upstream on purpose. Re-apply the
// villint-ignore if you edit this file.
#define VILLAGESQL_WELCOME_COPYRIGHT_LINE                                      \
  "Copyright (c) " COPYRIGHT_NOTICE_CURRENT_YEAR " VillageSQL Contributors\n"

// The same line indented for the copyright comment block that the build-time
// generators write into generated sources. Three spaces is what villint.sh
// produces for hand-written files, so generated headers match the tree.
#define VILLAGESQL_SOURCE_COPYRIGHT_LINE "   " VILLAGESQL_WELCOME_COPYRIGHT_LINE

/*
  This define specifies copyright notice which is displayed by every MySQL
  program on start, or on help screen.
*/
#define ORACLE_WELCOME_COPYRIGHT_NOTICE(first_year)                            \
  (strcmp(first_year, COPYRIGHT_NOTICE_CURRENT_YEAR)                           \
       ? VILLAGESQL_WELCOME_COPYRIGHT_LINE                                     \
         "Copyright (c) " first_year ", " COPYRIGHT_NOTICE_CURRENT_YEAR        \
         ", "                                                                  \
         "Oracle and/or its affiliates.\n\nOracle is a "                       \
         "registered trademark of Oracle Corporation and/or its\naffiliates. " \
         "Other names may be trademarks of their respective\nowners.\n"        \
       : VILLAGESQL_WELCOME_COPYRIGHT_LINE                                     \
         "Copyright (c) " first_year                                           \
         ", Oracle and/or its affiliates."                                     \
         "\n\nOracle is a registered trademark of "                            \
         "Oracle Corporation and/or its\naffiliates. Other names may be "      \
         "trademarks of their respective\nowners.\n")

#define ORACLE_GPL_LICENSE_TEXT                                               \
  "   This program is free software; you can redistribute it and/or modify\n" \
  "   it under the terms of the GNU General Public License, version 2.0,\n"   \
  "   as published by the Free Software Foundation.\n"                        \
  "\n"                                                                        \
  "   This program is designed to work with certain software (including\n"    \
  "   but not limited to OpenSSL) that is licensed under separate terms,\n"   \
  "   as designated in a particular file or component or in included "        \
  "license\n"                                                                 \
  "   documentation.  The authors of MySQL hereby grant you an additional\n"  \
  "   permission to link the program and your derivative works with the\n"    \
  "   separately licensed software that they have either included with\n"     \
  "   the program or referenced in the documentation.\n"                      \
  "\n"                                                                        \
  "   This program is distributed in the hope that it will be useful,\n"      \
  "   but WITHOUT ANY WARRANTY; without even the implied warranty of\n"       \
  "   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"        \
  "   GNU General Public License, version 2.0, for more details.\n"           \
  "\n"                                                                        \
  "   You should have received a copy of the GNU General Public License\n"    \
  "   along with this program; if not, write to the Free Software\n"          \
  "   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  " \
  "USA */\n"

#define ORACLE_COPYRIGHT_NOTICE(first_year)                                \
  (strcmp(first_year, COPYRIGHT_NOTICE_CURRENT_YEAR)                       \
       ? "/* Copyright (c) " first_year ", " COPYRIGHT_NOTICE_CURRENT_YEAR \
         ", Oracle and/or its affiliates.  */\n"                           \
         "\n"                                                              \
       : "/* Copyright (c) " first_year                                    \
         ", Oracle and/or its affiliates.  */\n")

#define ORACLE_GPL_COPYRIGHT_NOTICE(first_year)                            \
  (strcmp(first_year, COPYRIGHT_NOTICE_CURRENT_YEAR)                       \
       ? "/* Copyright (c) " first_year ", " COPYRIGHT_NOTICE_CURRENT_YEAR \
         ", Oracle and/or its affiliates.\n"                               \
         VILLAGESQL_SOURCE_COPYRIGHT_LINE                                  \
         "\n" ORACLE_GPL_LICENSE_TEXT                                      \
       : "/* Copyright (c) " first_year                                    \
         ", Oracle and/or its affiliates.\n"                               \
         VILLAGESQL_SOURCE_COPYRIGHT_LINE                                  \
         "\n" ORACLE_GPL_LICENSE_TEXT)

#define ORACLE_GPL_FOSS_LICENSE_TEXT                                          \
  "   This program is free software; you can redistribute it and/or modify\n" \
  "   it under the terms of the GNU General Public License, version 2.0,\n"   \
  "   as published by the Free Software Foundation.\n"                        \
  "\n"                                                                        \
  "   This program is designed to work with certain software (including\n"    \
  "   but not limited to OpenSSL) that is licensed under separate terms,\n"   \
  "   as designated in a particular file or component or in included "        \
  "license\n"                                                                 \
  "   documentation.  The authors of MySQL hereby grant you an additional\n"  \
  "   permission to link the program and your derivative works with the\n"    \
  "   separately licensed software that they have either included with\n"     \
  "   the program or referenced in the documentation.\n"                      \
  "\n"                                                                        \
  "   Without limiting anything contained in the foregoing, this file,\n"     \
  "   which is part of C Driver for MySQL (Connector/C), is also subject to " \
  "the\n"                                                                     \
  "   Universal FOSS Exception, version 1.0, a copy of which can be found "   \
  "at\n"                                                                      \
  "   http://oss.oracle.com/licenses/universal-foss-exception.\n"             \
  "\n"                                                                        \
  "   This program is distributed in the hope that it will be useful,\n"      \
  "   but WITHOUT ANY WARRANTY; without even the implied warranty of\n"       \
  "   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"        \
  "   GNU General Public License, version 2.0, for more details.\n"           \
  "\n"                                                                        \
  "   You should have received a copy of the GNU General Public License\n"    \
  "   along with this program; if not, write to the Free Software\n"          \
  "   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  " \
  "USA */\n"

#define ORACLE_GPL_FOSS_COPYRIGHT_NOTICE(first_year)                       \
  (strcmp(first_year, COPYRIGHT_NOTICE_CURRENT_YEAR)                       \
       ? "/* Copyright (c) " first_year ", " COPYRIGHT_NOTICE_CURRENT_YEAR \
         ", Oracle and/or its affiliates.\n"                               \
         VILLAGESQL_SOURCE_COPYRIGHT_LINE                                  \
         "\n" ORACLE_GPL_FOSS_LICENSE_TEXT                                 \
       : "/* Copyright (c) " first_year                                    \
         ", Oracle and/or its affiliates.\n"                               \
         VILLAGESQL_SOURCE_COPYRIGHT_LINE                                  \
         "\n" ORACLE_GPL_FOSS_LICENSE_TEXT)

#endif /* _welcome_copyright_notice_h_ */
