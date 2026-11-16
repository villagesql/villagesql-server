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

#ifndef VILLAGESQL_DETAIL_SPAN_H
#define VILLAGESQL_DETAIL_SPAN_H

#include <cstddef>

// In C++20, vsql::Span<T> is std::span<T>. In C++17, it is a minimal
// compatible implementation. User code written against vsql::Span<T> works
// in either standard.
#if __cplusplus >= 202002L
#include <span>
#endif

namespace vsql {

#if __cplusplus >= 202002L

template <typename T>
using Span = std::span<T>;

#else

// Non-owning view over a contiguous sequence of T, compatible with C++17.
// Mirrors the std::span interface so code written against vsql::Span<T>
// compiles unchanged under C++20 (where Span<T> aliases std::span<T>).
template <typename T>
class Span {
 public:
  Span() noexcept : data_(nullptr), size_(0) {}
  Span(T *data, size_t size) noexcept : data_(data), size_(size) {}

  T *data() const noexcept { return data_; }
  size_t size() const noexcept { return size_; }
  bool empty() const noexcept { return size_ == 0; }

  T &operator[](size_t i) const noexcept { return data_[i]; }

  T *begin() const noexcept { return data_; }
  T *end() const noexcept { return data_ + size_; }

 private:
  T *data_;
  size_t size_;
};

#endif  // __cplusplus >= 202002L

}  // namespace vsql

#endif  // VILLAGESQL_DETAIL_SPAN_H
