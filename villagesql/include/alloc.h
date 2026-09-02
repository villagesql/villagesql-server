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

// Nothrow allocation helpers.
//
// The server does not handle exceptions on the statement execution path, so an
// allocation that throws std::bad_alloc reaches std::terminate and aborts the
// process. Prefer a MEM_ROOT (`new (mem_root) T`, which returns nullptr rather
// than throwing) whenever the lifetime allows it. Where it does not - anything
// that must outlive the statement, or that the victionary caches across
// connections - use these helpers so an allocation failure surfaces as a null
// pointer that the caller can check, matching `new (std::nothrow)`.

#ifndef VILLAGESQL_INCLUDE_ALLOC_H_
#define VILLAGESQL_INCLUDE_ALLOC_H_

#include <memory>
#include <new>
#include <utility>

namespace villagesql {

// Nothrow analogue of std::make_shared: returns nullptr if the allocation
// fails. std::make_shared has no nothrow overload, and std::allocate_shared
// cannot report failure without throwing (an allocator that returns nullptr
// instead leaves it constructing the object at address zero), so the catch
// has to live here.
//
// Only std::bad_alloc is absorbed. Anything else T's constructor throws hits
// the noexcept and terminates, which is what it already does today.
template <typename T, typename... Args>
std::shared_ptr<T> make_shared_nothrow(Args &&...args) noexcept {
  try {
    return std::make_shared<T>(std::forward<Args>(args)...);
  } catch (const std::bad_alloc &) {
    return nullptr;
  }
}

// Nothrow analogue of std::make_unique. The object itself is allocated with
// new (std::nothrow); the try/catch covers only allocations made by T's own
// constructor (its strings, vectors, and so on).
template <typename T, typename... Args>
std::unique_ptr<T> make_unique_nothrow(Args &&...args) noexcept {
  try {
    return std::unique_ptr<T>(new (std::nothrow)
                                  T(std::forward<Args>(args)...));
  } catch (const std::bad_alloc &) {
    return nullptr;
  }
}

// Take ownership of an already-allocated pointer without throwing, for types
// whose constructor make_shared_nothrow cannot reach (a private constructor,
// for instance):
//
//   wrap_shared_nothrow(new (std::nothrow) Foo(args));
//
// Returns nullptr if ptr is null or if the control block allocation fails. The
// shared_ptr constructor deletes ptr itself when it throws, so the failure
// path does not leak.
template <typename T>
std::shared_ptr<T> wrap_shared_nothrow(T *ptr) noexcept {
  if (ptr == nullptr) return nullptr;
  try {
    return std::shared_ptr<T>(ptr);
  } catch (const std::bad_alloc &) {
    return nullptr;
  }
}

}  // namespace villagesql

#endif  // VILLAGESQL_INCLUDE_ALLOC_H_
