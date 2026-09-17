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

#include <villagesql/vsql.h>

#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>

using namespace vsql;

constexpr int64_t kMetersSize = sizeof(double);

void store_double(unsigned char *buffer, double value) {
  uint64_t bits;
  memcpy(&bits, &value, sizeof(bits));
  for (size_t i = 0; i < sizeof(bits); ++i) {
    buffer[i] = static_cast<unsigned char>(bits >> (i * 8));
  }
}

double load_double(const unsigned char *buffer) {
  uint64_t bits = 0;
  for (size_t i = 0; i < sizeof(bits); ++i) {
    bits |= static_cast<uint64_t>(buffer[i]) << (i * 8);
  }
  double value;
  memcpy(&value, &bits, sizeof(value));
  return value;
}

void meters_from_string(std::string_view input, CustomResult out) {
  std::string value_string(input);
  char *end = nullptr;
  errno = 0;
  double value = std::strtod(value_string.c_str(), &end);
  if (errno == ERANGE || end == value_string.c_str() || !std::isfinite(value)) {
    out.warning("invalid METERS value");
    return;
  }

  while (std::isspace(static_cast<unsigned char>(*end))) ++end;
  if (*end == 'm') {
    ++end;
    while (std::isspace(static_cast<unsigned char>(*end))) ++end;
  }
  if (*end != '\0') {
    out.warning("invalid METERS value");
    return;
  }

  auto buffer = out.buffer();
  if (buffer.size() < kMetersSize) {
    out.error("response buffer too small");
    return;
  }
  store_double(buffer.data(), value);
  out.set_length(kMetersSize);
}

void meters_to_string(CustomArg in, StringResult out) {
  if (in.value().size() != kMetersSize) {
    out.error("argument malformed");
    return;
  }
  auto buffer = out.buffer();
  int written = snprintf(buffer.data(), buffer.size(), "%g m",
                         load_double(in.value().data()));
  if (written < 0) {
    out.error("failed to format METERS value");
    return;
  }
  out.set_length(static_cast<size_t>(written));
}

int meters_compare(CustomArg lhs, CustomArg rhs) {
  if (lhs.value().size() != kMetersSize || rhs.value().size() != kMetersSize) {
    return 0;
  }
  double lhs_value = load_double(lhs.value().data());
  double rhs_value = load_double(rhs.value().data());
  if (lhs_value < rhs_value) return -1;
  if (lhs_value > rhs_value) return 1;
  return 0;
}

void meters_real_value(CustomArg in, RealResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  if (in.value().size() != kMetersSize) {
    out.error("argument malformed");
    return;
  }
  out.set(load_double(in.value().data()));
}

static constexpr const char kMetersTypeName[] = "METERS";

constexpr auto METERS = make_type<kMetersTypeName>()
                            .persisted_length(kMetersSize)
                            .max_decode_buffer_length(32)
                            .from_string<&meters_from_string>()
                            .to_string<&meters_to_string>()
                            .compare<&meters_compare>()
                            .real_value<&meters_real_value>()
                            .intrinsic_default_str("0")
                            .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(METERS))
