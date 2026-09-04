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

#ifndef VILLAGESQL_VSQL_CACHE_LINE_H
#define VILLAGESQL_VSQL_CACHE_LINE_H

// Cache-line size for separating data that different threads write.
//
// Two independent variables that share a cache line are written as if they were
// one: a write to either invalidates the line in every other core's cache, and
// each of those cores pays a cross-core transfer on its next access. The
// hardware behaves as though the threads share data even though the program
// shares nothing, hence "false sharing".
//
// That leaves the obvious question: if each statement has its own state, how do
// two of them ever share a line?
//
// The allocator rounds every request up to a fixed slot size, so an 8-byte
// state occupies 16 bytes. It cuts those slots from one large chunk of memory,
// laid end to end, so any two slots it hands out are close together.
//
// Then the statements give them straight back. A state is freed when its
// statement ends, so only as many are alive at once as there are statements
// running at once. The allocator never needs more slots than that. A freed slot
// returns to the front of the free list for the next statement to take, so the
// same few slots circulate for the life of the server.
//
// Eight of those slots is 128 bytes, which is one cache line.
//
// Measured, not reasoned. Allocate 200,000 of these and keep them all: each new
// one sits 16 bytes after the one before. Free them as normal instead, and a
// running server allocated 161,289 of them but only ever used 22 different
// addresses; at 8 connections, six live states sat at offsets 0, 32, 64, 80, 96
// and 112 of one line, written by all eight connection threads.

#include <cstddef>

namespace vsql {

// Fixed per architecture, because no other source works.
//
// std::hardware_destructive_interference_size follows -mcpu and -mtune, so GCC
// warns about every use of it (-Winterference-size), which breaks any extension
// built with -Werror. The value has to be a compile-time constant.
//
// 256 on s390x, and on AArch64 except Apple, because some chips there (A64FX)
// have 256-byte lines. GCC and clang both use 256 for generic AArch64 for the
// same reason, and clang uses 128 for Apple.
//
// 128 everywhere else. POWER lines are 128 bytes. x86-64 lines are 64, but
// Intel's adjacent-line prefetcher fetches them in pairs.
//
// Too small for some CPU costs false sharing on that CPU; too large costs only
// bytes.
constexpr size_t kCacheLineSize =
#if defined(__s390x__) || (defined(__aarch64__) && !defined(__APPLE__))
    256;
#else
    128;
#endif

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_CACHE_LINE_H
