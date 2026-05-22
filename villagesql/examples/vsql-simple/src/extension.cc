// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// VillageSQL extension demonstrating the vsql API.
//
// The BYTEARRAY type is a fixed 8-byte value.

#include <villagesql/vsql.h>

using namespace vsql;

static const size_t kBytearrayLen = 8;

// from_string: string -> binary (copy up to 8 bytes, space-pad)
void bytearray_from_string(std::string_view from, CustomResult out) {
  auto buf = out.buffer();
  if (buf.size() < kBytearrayLen) return;  // wrapper default warning
  memset(buf.data(), ' ', kBytearrayLen);
  size_t copy_len = from.size() < kBytearrayLen ? from.size() : kBytearrayLen;
  if (copy_len > 0) memcpy(buf.data(), from.data(), copy_len);
  out.set_length(kBytearrayLen);
}

// to_string: binary -> string (copy 8 bytes)
void bytearray_to_string(CustomArg in, StringResult out) {
  auto data = in.value();
  auto buf = out.buffer();
  if (data.size() < kBytearrayLen || buf.size() < kBytearrayLen) return;
  memcpy(buf.data(), data.data(), kBytearrayLen);
  out.set_length(kBytearrayLen);
}

// Compare: lexicographic byte comparison
int bytearray_compare(CustomArg a, CustomArg b) {
  return memcmp(a.value().data(), b.value().data(), kBytearrayLen);
}

// ROT13: apply ROT13 cipher to ASCII letters
void rot13(CustomArg in, CustomResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto src = in.value();
  auto dst = out.buffer();
  for (size_t i = 0; i < kBytearrayLen && i < src.size(); i++) {
    unsigned char c = src[i];
    if (c >= 'A' && c <= 'Z') {
      dst[i] = 'A' + ((c - 'A' + 13) % 26);
    } else if (c >= 'a' && c <= 'z') {
      dst[i] = 'a' + ((c - 'a' + 13) % 26);
    } else {
      dst[i] = c;
    }
  }
  out.set_length(kBytearrayLen);
}

// EVEN_CHARS: extract bytes at positions 0, 2, 4, 6 (returns 4 bytes)
void even_chars(CustomArg in, CustomResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto src = in.value();
  auto dst = out.buffer();
  memset(dst.data(), ' ', kBytearrayLen);
  if (src.size() >= kBytearrayLen) {
    dst[0] = src[0];
    dst[1] = src[2];
    dst[2] = src[4];
    dst[3] = src[6];
  }
  out.set_length(kBytearrayLen);
}

// ODD_CHARS: extract bytes at positions 1, 3, 5, 7 (returns 4 bytes)
void odd_chars(CustomArg in, CustomResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto src = in.value();
  auto dst = out.buffer();
  memset(dst.data(), ' ', kBytearrayLen);
  if (src.size() >= kBytearrayLen) {
    dst[0] = src[1];
    dst[1] = src[3];
    dst[2] = src[5];
    dst[3] = src[7];
  }
  out.set_length(kBytearrayLen);
}

// BA_CALL_INDEX: return the 1-based index of this call within the current
// statement. Demonstrates the typed prerun/postrun + State& pattern: prerun
// allocates a counter, each row reads-and-increments via the VDF's State&
// first parameter, and postrun frees.

struct CallCounter {
  long long n = 0;
};

void ba_call_index_prerun(PrerunArgs, PrerunResult out) {
  out.set_user_data(new CallCounter{});
}

void ba_call_index(CallCounter &state, IntResult out) {
  state.n++;
  out.set(state.n);
}

void ba_call_index_postrun(PostrunArgs args) {
  args.delete_state<CallCounter>();
}

// BA_CONCAT: concatenate two bytearrays (returns STRING with 16 bytes)
void ba_concat(CustomArg a, CustomArg b, StringResult out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  auto dst = out.buffer();
  memset(dst.data(), ' ', kBytearrayLen * 2);
  memcpy(dst.data(), a.value().data(), kBytearrayLen);
  memcpy(dst.data() + kBytearrayLen, b.value().data(), kBytearrayLen);
  out.set_length(kBytearrayLen * 2);
}

// BA_LEN: return the fixed length of a BYTEARRAY (zero-arity constant).
// Demonstrates .param() (zero-arity) on the func builder.
void ba_len(IntResult out) { out.set(static_cast<long long>(kBytearrayLen)); }

// BA_CONCAT_ALL: concatenate any number of BYTEARRAY values into a STRING.
// Demonstrates .varargs() on the func builder paired with a prerun that
// validates argument types and sizes the result buffer.
//
// Prerun: validate that all arguments are BYTEARRAY (or NULL literals,
// which appear as VEF_TYPE_STRING in the prerun arg-type array) and ask
// the server to allocate arg_count * kBytearrayLen bytes of result buffer.
void ba_concat_all_prerun(vsql::PrerunArgs args, vsql::PrerunResult out) {
  if (args.size() == 0) {
    out.error("ba_concat_all requires at least one argument");
    return;
  }
  for (size_t i = 0; i < args.size(); i++) {
    auto t = args.type_at(i);
    if (!t.is_custom() && !t.is_str()) {
      out.error("ba_concat_all: argument " + std::to_string(i) +
                " must be BYTEARRAY");
      return;
    }
  }
  out.request_buffer_size(args.size() * kBytearrayLen);
}

void ba_concat_all(VarArgs args, StringResult out) {
  auto dst = out.buffer();
  size_t off = 0;
  for (auto a : args) {
    // Prerun accepts VEF_TYPE_STRING so NULL literals (which arrive typed as
    // STRING) pass type-check, but it cannot distinguish a NULL literal from
    // a non-NULL string literal like 'abc'. Treat any non-custom argument
    // here as NULL so we never call as_custom() on a STRING value.
    if (a.is_null() || !a.is_custom()) {
      out.set_null();
      return;
    }
    auto bytes = a.as_custom();
    memcpy(dst.data() + off, bytes.data(), bytes.size());
    off += bytes.size();
  }
  out.set_length(off);
}

static constexpr const char kBytearrayTypeName[] = "bytearray";

constexpr auto BYTEARRAY = vsql::make_type<kBytearrayTypeName>()
                               .persisted_length(kBytearrayLen)
                               .max_decode_buffer_length(kBytearrayLen)
                               .from_string<&bytearray_from_string>()
                               .to_string<&bytearray_to_string>()
                               .compare<&bytearray_compare>()
                               .build();

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .type(BYTEARRAY)
        .func(make_func<&rot13>("rot13")
                  .returns(BYTEARRAY)
                  .param(BYTEARRAY)
                  .build())
        .func(make_func<&even_chars>("even_chars")
                  .returns(BYTEARRAY)
                  .param(BYTEARRAY)
                  .build())
        .func(make_func<&odd_chars>("odd_chars")
                  .returns(BYTEARRAY)
                  .param(BYTEARRAY)
                  .build())
        .func(make_func<&ba_concat>("ba_concat")
                  .returns(STRING)
                  .param(BYTEARRAY)
                  .param(BYTEARRAY)
                  .build())
        .func(make_func<&ba_call_index>("ba_call_index")
                  .returns(INT)
                  .param()
                  .prerun<&ba_call_index_prerun>()
                  .postrun<&ba_call_index_postrun>()
                  .build())
        .func(make_func<&ba_len>("ba_len").returns(INT).param().build())
        .func(make_func<&ba_concat_all>("ba_concat_all")
                  .returns(STRING)
                  .varargs()
                  .prerun<&ba_concat_all_prerun>()
                  .build()))
