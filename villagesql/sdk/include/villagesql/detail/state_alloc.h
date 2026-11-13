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

#ifndef VILLAGESQL_DETAIL_STATE_ALLOC_H
#define VILLAGESQL_DETAIL_STATE_ALLOC_H

// Allocation of per-statement VDF state, on cache lines of its own.
//
// Per-statement state is private to one statement execution and therefore to
// one thread, but privacy of the object does not imply privacy of the cache
// line it sits on. Several concurrent statements allocating a small state at
// about the same time get addresses close together, and a state mutated per row
// then bounces its line between cores. Measured on an 8-byte counter at 8
// threads: about 110 ns per row, a third of the statement, and nothing at 1
// thread.

// How much this buys depends on how small the state is, because the allocator
// rounds every request up to a size class. On an 8-byte state that is a 16-byte
// slot, so eight concurrent states fit one 128-byte line; addresses logged from
// a real server under 8 connections showed six live states at offsets 0, 32,
// 64, 80, 96 and 112 of a single line, touched by all eight threads.

// alloc_state and free_state must be read as a pair: the alignment passed to
// operator delete has to match the one passed to operator new, and a mismatch
// is undefined behaviour no test would catch. They are kept adjacent for that
// reason. Not part of the public API.

#include <cstddef>
#include <new>
#include <utility>

#include <villagesql/vsql/cache_line.h>

namespace vsql {
namespace detail {

// At least a cache line, and never less than the type itself requires.
template <typename State>
constexpr size_t state_alignment() {
  return alignof(State) > kCacheLineSize ? alignof(State) : kCacheLineSize;
}

// Rounded up to a whole number of lines. Aligning the start address alone would
// leave the rest of the line available to the allocator for someone else's
// object, which is the thing being prevented.
template <typename State>
constexpr size_t state_alloc_size() {
  constexpr size_t a = state_alignment<State>();
  return ((sizeof(State) + a - 1) / a) * a;
}

template <typename State, typename... Args>
State *alloc_state(Args &&...args) {
  constexpr std::align_val_t align{state_alignment<State>()};
  void *raw = ::operator new(state_alloc_size<State>(), align);
  try {
    return ::new (raw) State{std::forward<Args>(args)...};
  } catch (...) {
    // State's constructor threw, so nothing owns `raw` yet. The aligned
    // operator delete must match the aligned operator new above.
    ::operator delete(raw, align);
    throw;
  }
}

// Takes void* so it can be called straight from the ABI's user_data slot.
// Deliberately the unsized operator delete: the sized overload would require
// passing back exactly state_alloc_size<State>(), one more thing to keep in
// step with alloc_state.
template <typename State>
void free_state(void *p) {
  if (p == nullptr) return;
  State *state = static_cast<State *>(p);
  state->~State();
  ::operator delete(state, std::align_val_t{state_alignment<State>()});
}

}  // namespace detail
}  // namespace vsql

#endif  // VILLAGESQL_DETAIL_STATE_ALLOC_H
