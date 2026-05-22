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

// Test-only extension demonstrating the difference between the by-value
// (`void*`) and by-reference (`void*&`) user_data wrappers introduced by
// the typed prerun/postrun API (PR #551).
//
// Both VDFs implement the same "lag" function: previous(s) returns the
// s passed on the prior row, or NULL on the first row. Storage is a
// malloc'd buffer holding [size_t length][bytes].
//
//   previous_byval(s):  declares `void* user_data` — pointer is copied
//                       in, assignment inside the body is local.
//                       Expected: NULL on every row, buffers leaked.
//
//   previous_byref(s):  declares `void*& user_data` — routes through
//                       WrapperVoidStarState so assignments survive
//                       across rows and postrun frees the final buffer.
//                       Expected: NULL, then the prior s.

#include <villagesql/vsql.h>

#include <cstdlib>
#include <cstring>

using namespace vsql;

static void common_prerun(PrerunArgs, PrerunResult out) {
  out.set_user_data(nullptr);
}

static void common_postrun(PostrunArgs args) { std::free(args.user_data()); }

// Helper: pop the stored previous string out (or set null), then malloc
// a new buffer holding `s` and return it. Returns the pointer the caller
// should stash into the user_data slot.
static void *emit_prev_and_stash(void *prev, StringArg s, StringResult out) {
  if (prev == nullptr) {
    out.set_null();
  } else {
    size_t len;
    std::memcpy(&len, prev, sizeof(len));
    const char *bytes = static_cast<const char *>(prev) + sizeof(len);
    out.set(std::string_view(bytes, len));
    std::free(prev);
  }
  if (s.is_null()) return nullptr;
  auto sv = s.value();
  size_t k = sizeof(size_t);
  void *p = std::malloc(k + sv.size());
  size_t len = sv.size();
  std::memcpy(p, &len, k);
  std::memcpy(static_cast<char *>(p) + k, sv.data(), sv.size());
  return p;
}

// By-value: user_data is passed as a copy. Assignment is local. Every
// row sees nullptr (the prerun value); the malloc'd buffer is leaked.
void previous_byval(void *user_data, StringArg s, StringResult out) {
  user_data = emit_prev_and_stash(user_data, s, out);
  (void)user_data;  // assignment is local; writeback does not persist
}

// By-reference: user_data is passed as `void*&` and binds to the slot
// directly. The body's assignment updates args->user_data, so subsequent
// rows read the cached pointer and postrun frees the final allocation.
void previous_byref(void *&user_data, StringArg s, StringResult out) {
  user_data = emit_prev_and_stash(user_data, s, out);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&previous_byval>("previous_byval")
                  .returns(STRING)
                  .param(STRING)
                  .buffer_size(1024)
                  .prerun<&common_prerun>()
                  .postrun<&common_postrun>()
                  .build())
        .func(make_func<&previous_byref>("previous_byref")
                  .returns(STRING)
                  .param(STRING)
                  .buffer_size(1024)
                  .prerun<&common_prerun>()
                  .postrun<&common_postrun>()
                  .build()))
