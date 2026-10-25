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
// call with the constant's bytes in hex.

#include <villagesql/vsql.h>

#include <cstddef>
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

VEF_GENERATE_ENTRY_POINTS(make_extension()
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
                                        .build()))
