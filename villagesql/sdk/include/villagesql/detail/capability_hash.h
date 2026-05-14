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

#ifndef VILLAGESQL_DETAIL_CAPABILITY_HASH_H
#define VILLAGESQL_DETAIL_CAPABILITY_HASH_H

#include <cstddef>

namespace villagesql::detail {

// constexpr FNV-1a hash over a string.
constexpr size_t fnv1a_hash(const char *s, size_t len) {
  size_t h = 14695981039346656037ULL;
  for (size_t i = 0; i < len; ++i)
    h = (h ^ static_cast<size_t>(s[i])) * 1099511628211ULL;
  return h;
}

// Returns a compile-time hash of T's fully qualified type name and size via
// __PRETTY_FUNCTION__ and sizeof(T). Used to verify that the capability ABI
// struct type an extension was compiled against matches the type the server
// registered. Catches both type name changes and struct size changes.
template <typename T>
constexpr size_t abi_type_hash() {
  const char *s = __PRETTY_FUNCTION__;
  size_t len = 0;
  while (s[len]) ++len;
  return fnv1a_hash(s, len) ^ (sizeof(T) * 1099511628211ULL);
}

}  // namespace villagesql::detail

#endif  // VILLAGESQL_DETAIL_CAPABILITY_HASH_H
