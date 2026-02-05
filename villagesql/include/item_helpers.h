/* Copyright (c) 2026 VillageSQL Contributors

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

#ifndef VILLAGESQL_ITEM_HELPERS_INCLUDED
#define VILLAGESQL_ITEM_HELPERS_INCLUDED

// VillageSQL: Helper macros for Item class extensions

// Macro to forward TypeContext from a wrapped/referenced item.
// This ensures proper TypeContext forwarding through Item hierarchy layers.
// Usage: VILLAGESQL_FORWARD_TYPE_CONTEXT(expression_returning_item_pointer)
#define VILLAGESQL_FORWARD_TYPE_CONTEXT(source_expr)                 \
  const villagesql::TypeContext *get_type_context() const override { \
    auto *src = (source_expr);                                       \
    return src ? src->get_type_context() : nullptr;                  \
  }                                                                  \
  bool has_type_context() const override {                           \
    auto *src = (source_expr);                                       \
    return src && src->has_type_context();                           \
  }

#endif  // VILLAGESQL_ITEM_HELPERS_INCLUDED
