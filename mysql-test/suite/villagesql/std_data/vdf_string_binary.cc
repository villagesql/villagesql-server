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

// STRING and BINARY parameters and results. The *_hex functions report the
// bytes the VDF received, so a test can see whether the server converted an
// argument; the *_echo functions return their argument, so a test can see the
// charset the server gives the result. The *_const functions do the same for
// the constant their bind_and_check_types hook is given: the hook rejects the
// call with the constant's bytes in hex. TEXT8 stores the bytes its
// from_string was given, so text8_hex shows what a constant was encoded from.

#include <villagesql/vsql.h>

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>

using namespace vsql;

static void write_hex(const unsigned char *data, size_t len, StringResult out) {
  static const char digits[] = "0123456789ABCDEF";
  auto buf = out.buffer();
  if (buf.size() < 2 * len) {
    // Report the full size; the server grows the buffer and calls again.
    out.set_length(2 * len);
    return;
  }
  for (size_t i = 0; i < len; i++) {
    buf.data()[2 * i] = digits[data[i] >> 4];
    buf.data()[2 * i + 1] = digits[data[i] & 0xF];
  }
  out.set_length(2 * len);
}

void str_hex_impl(StringArg in, StringResult out) {
  if (in.is_null()) return out.set_null();
  std::string_view v = in.value();
  write_hex(reinterpret_cast<const unsigned char *>(v.data()), v.size(), out);
}

void bin_hex_impl(BinaryArg in, StringResult out) {
  if (in.is_null()) return out.set_null();
  write_hex(in.value().data(), in.value().size(), out);
}

void str_echo_impl(StringArg in, StringResult out) {
  if (in.is_null()) return out.set_null();
  out.set(in.value());
}

void bin_echo_impl(BinaryArg in, BinaryResult out) {
  if (in.is_null()) return out.set_null();
  out.set(in.value());
}

void int_echo_impl(IntArg in, IntResult out) {
  if (in.is_null()) return out.set_null();
  out.set(in.value());
}

// A hook with nothing to decide, so the call goes ahead.
void pass_bind(BindArgs, BindResult) {}

void const_hex_bind(BindArgs args, BindResult out) {
  if (!args.at(0).has_const_value()) return;
  static const char digits[] = "0123456789ABCDEF";
  std::string msg = "constant ";
  for (unsigned char c : args.at(0).const_value()) {
    msg += digits[c >> 4];
    msg += digits[c & 0xF];
  }
  out.error(msg);
}

// TEXT8: up to 8 bytes of text, zero padded.
static const size_t kText8Len = 8;

void text8_from_string(std::string_view from, CustomResult out) {
  auto buf = out.buffer();
  if (from.size() > kText8Len || buf.size() < kText8Len) return;
  memset(buf.data(), 0, kText8Len);
  memcpy(buf.data(), from.data(), from.size());
  out.set_length(kText8Len);
}

void text8_to_string(CustomArg in, StringResult out) {
  auto data = in.value();
  size_t n = 0;
  while (n < data.size() && data[n] != 0) n++;
  out.set(std::string_view(reinterpret_cast<const char *>(data.data()), n));
}

int text8_compare(CustomArg a, CustomArg b) {
  return memcmp(a.value().data(), b.value().data(), kText8Len);
}

void text8_hex_impl(CustomArg in, StringResult out) {
  if (in.is_null()) return out.set_null();
  write_hex(in.value().data(), in.value().size(), out);
}

static constexpr const char kText8TypeName[] = "TEXT8";

constexpr auto TEXT8 = make_type<kText8TypeName>()
                           .persisted_length(kText8Len)
                           .max_decode_buffer_length(kText8Len)
                           .from_string<&text8_from_string>()
                           .to_string<&text8_to_string>()
                           .compare<&text8_compare>()
                           .build();

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .type(TEXT8)
                              .func(make_func<&str_hex_impl>("str_hex")
                                        .returns(STRING)
                                        .param(STRING)
                                        .build())
                              .func(make_func<&bin_hex_impl>("bin_hex")
                                        .returns(STRING)
                                        .param(BINARY)
                                        .build())
                              .func(make_func<&str_echo_impl>("str_echo")
                                        .returns(STRING)
                                        .param(STRING)
                                        .build())
                              .func(make_func<&bin_echo_impl>("bin_echo")
                                        .returns(BINARY)
                                        .param(BINARY)
                                        .max_result_length(64)
                                        .build())
                              .func(make_func<&int_echo_impl>("int_echo")
                                        .returns(INT)
                                        .param(INT)
                                        .bind_and_check_types<&pass_bind>()
                                        .build())
                              .func(make_func<&str_hex_impl>("str_const")
                                        .returns(STRING)
                                        .param(STRING)
                                        .bind_and_check_types<&const_hex_bind>()
                                        .build())
                              .func(make_func<&bin_hex_impl>("bin_const")
                                        .returns(STRING)
                                        .param(BINARY)
                                        .bind_and_check_types<&const_hex_bind>()
                                        .build())
                              .func(make_func<&int_echo_impl>("int_const")
                                        .returns(INT)
                                        .param(INT)
                                        .bind_and_check_types<&const_hex_bind>()
                                        .build())
                              .func(make_func<&text8_hex_impl>("text8_hex")
                                        .returns(STRING)
                                        .param(TEXT8)
                                        .build())
                              .func(make_func<&text8_hex_impl>("text8_hooked")
                                        .returns(STRING)
                                        .param(TEXT8)
                                        .bind_and_check_types<&pass_bind>()
                                        .build()))
