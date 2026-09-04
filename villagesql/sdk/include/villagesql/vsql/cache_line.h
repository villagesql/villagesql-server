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
// This is not a micro-optimization. Benchmarking the VEF call boundary found a
// VDF whose 8-byte per-statement state cost about 110 ns per row at 8 threads
// -- a third of the statement -- while costing nothing single-threaded, purely
// because several threads' states landed in one line.
//
// Use this to separate anything written per row by different threads:
//
//   struct alignas(vsql::kCacheLineSize) MyCounters { ... };
//
// Note that aligning a *pointer* is not sufficient on its own. Over-aligned
// operator new only promises where the block starts, so another allocation can
// still occupy the rest of the line. The size has to cover the line too, which
// alignas on a type does automatically: sizeof is always rounded up to a
// multiple of alignof.

#include <cstddef>
#include <new>

namespace vsql {

// Floored at 128 on purpose.
//
// std::hardware_destructive_interference_size is the only source of this number
// that is both a compile-time constant, and therefore usable in alignas, and
// correct per platform. The runtime alternatives cannot be used here (sysctl
// hw.cachelinesize is not constant; sysconf(_SC_LEVEL1_DCACHE_LINESIZE) does
// not exist on macOS), and a build-time probe is worse still: MySQL's own
// CPU_LEVEL1_DCACHE_LINESIZE skips Apple entirely in cmake/cpu_info.cmake and
// falls back to 64, which is half the true line size on Apple silicon.
//
// The floor covers the case where the standard constant is optimistic. x86-64
// libc++ and libstdc++ both report 64, but Intel's adjacent-line prefetcher
// moves lines in pairs, making the effective granule 128. Over-padding costs
// bytes; under-padding costs the bug this header exists to prevent.
//
// Unlike std::from_chars, this is a header constant with no macOS availability
// gating, so it is safe to use at any deployment target. The #ifdef is for
// standard libraries that do not implement it at all.
constexpr size_t kCacheLineSize =
#ifdef __cpp_lib_hardware_interference_size
    std::hardware_destructive_interference_size > 128
        ? std::hardware_destructive_interference_size
        : 128;
#else
    128;
#endif

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_CACHE_LINE_H
