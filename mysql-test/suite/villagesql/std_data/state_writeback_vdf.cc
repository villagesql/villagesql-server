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

// Test-only extension demonstrating the typed prerun/postrun + `void*&`
// user_data slot (WrapperVoidStarRefState) from PR #551.
//
// previous(s) returns the s passed to the previous row, or NULL on the
// first row. Storage is a malloc'd buffer laid out as:
//   [size_t capacity][size_t length][capacity bytes of payload]
// capacity is the payload bytes currently available; length is how many
// of those are in use. When a longer string arrives we free the buffer
// and malloc a new one; shorter strings reuse the existing allocation.

#include <villagesql/vsql.h>

#include <cstdlib>
#include <cstring>

using namespace vsql;

struct Hdr {
  size_t capacity;
  size_t length;
};

static void previous_prerun(PrerunArgs, PrerunResult out) {
  out.set_user_data(nullptr);
}

void previous_vdf(void *&user_data, StringArg s, StringResult out) {
  // Emit the previous value, if any.
  if (user_data != nullptr) {
    auto *hdr = static_cast<Hdr *>(user_data);
    const char *bytes = reinterpret_cast<const char *>(hdr) + sizeof(Hdr);
    out.set(std::string_view(bytes, hdr->length));
  } else {
    out.set_null();
  }

  // Stash the current row's value for the next call. A NULL input clears
  // the slot (and frees any buffer we were carrying).
  if (s.is_null()) {
    std::free(user_data);
    user_data = nullptr;
    return;
  }

  auto sv = s.value();
  size_t needed = sv.size();
  auto *hdr = static_cast<Hdr *>(user_data);
  if (hdr == nullptr || hdr->capacity < needed) {
    std::free(user_data);
    hdr = static_cast<Hdr *>(std::malloc(sizeof(Hdr) + needed));
    hdr->capacity = needed;
  }
  hdr->length = needed;
  std::memcpy(reinterpret_cast<char *>(hdr) + sizeof(Hdr), sv.data(), needed);
  user_data = hdr;
}

static void previous_postrun(PostrunArgs args) { std::free(args.user_data()); }

VEF_GENERATE_ENTRY_POINTS(
    make_extension().func(make_func<&previous_vdf>("previous")
                              .returns(STRING)
                              .param(STRING)
                              .buffer_size(1024)
                              .prerun<&previous_prerun>()
                              .postrun<&previous_postrun>()
                              .build()))
