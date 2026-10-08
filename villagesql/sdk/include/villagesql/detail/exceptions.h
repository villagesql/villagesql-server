// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_DETAIL_EXCEPTIONS_H
#define VILLAGESQL_DETAIL_EXCEPTIONS_H

// NOTE: The following two macros are "decorated" when used to
// emphasize variable scoping.  The TRY macro is followed
// " {".  The CATCH macro is preceded by "} " and followed
// ";".  All of the decorations are redundant.  Again,
// the purpose is to highlight variable scoping between.

#define VDF_EXCEPTIONS_TRY try {
// TODO(villagesql-general): need a way to log these errors
#define VDF_EXCEPTIONS_CATCH(result__)                      \
  }                                                         \
  catch (const std::exception &ex) {                        \
    if (VEF_PROTOCOL_4 <= ctx->protocol) {                  \
      (result__)->type = VEF_RESULT_ERROR;                  \
      if (nullptr != (result__)->error_msg) {               \
        snprintf((result__)->error_msg, VEF_MAX_ERROR_LEN,  \
                 "VDF threw an exception (%s)", ex.what()); \
      }                                                     \
    }                                                       \
  }                                                         \
  catch (...) {                                             \
    if (VEF_PROTOCOL_4 <= ctx->protocol) {                  \
      (result__)->type = VEF_RESULT_ERROR;                  \
      if (nullptr != (result__)->error_msg) {               \
        snprintf((result__)->error_msg, VEF_MAX_ERROR_LEN,  \
                 "VDF threw an exception");                 \
      }                                                     \
    }                                                       \
  }

#endif  // VILLAGESQL_DETAIL_EXCEPTIONS_H
