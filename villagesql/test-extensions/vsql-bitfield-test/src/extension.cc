// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// A variable-length bit field type whose length (in bits) is decided
// per value, with an optional per-column upper bound.
//
// BITFIELD declares a max_persisted_length upper bound and its element is a
// single BIT rather than a byte or a fixed-width number. Each value keeps its
// own bit length, so bit fields of different lengths coexist in the same
// column. The column accepts an optional 'max_number_of_bits' parameter that
// caps how many bits any value in the column may hold:
//   CREATE TABLE t (b vsql_bitfield_test.BITFIELD);                  -- max 8
//   CREATE TABLE t (b vsql_bitfield_test.BITFIELD('max_number_of_bits=64'));
// The bare form has no parameters, so resolve_params runs with an empty map and
// falls back to kBitfieldMaxBits. Because the type is parameterized
// (params + resolve_params), from_string takes a MaybeParams<BitfieldParams>
// and to_string/compare take CustomArgWith<BitfieldParams>.
//
// from_string packs each '0'/'1' character into one BIT (8 bits per byte), so a
// 64-character string of bits stores in 8 bytes of packed bits, not 64. The
// number of bits is not recoverable from the byte length alone (e.g. "101" and
// "10100000" would both occupy one byte), so the stored layout is:
//
//   [ bit count : 2 bytes little-endian ][ packed bits : ceil(bits/8) bytes ]
//
// Bits are packed MSB-first within each byte (bit i -> byte i/8, position
// 7-(i%8)). to_string reads the bit count from the header and emits exactly
// that many '0'/'1' characters, so any bit string round-trips exactly.
//
// The backing field is a VARBINARY(max_persisted_length): the in-memory buffer
// is sized to the upper bound, but only the actual encoded bytes (set via
// out.set_length()) are written, and the VARBINARY length prefix records the
// per-value byte length.

#include <villagesql/vsql.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

// Absolute cap on the number of bits a BITFIELD value may hold. The bit count
// is stored in a 2-byte little-endian header, so it must fit in a uint16. The
// per-column cap (the 'max_number_of_bits' parameter, below) must not exceed
// this.
constexpr int64_t kBitfieldMaxBits = 4096;
constexpr int64_t kBitfieldHeaderLen = 2;
// Upper bound on the stored value: 2-byte header plus ceil(maxBits/8) packed
// bytes. Sizes the backing field and the encode buffer.
constexpr int64_t kBitfieldMaxLen =
    kBitfieldHeaderLen + (kBitfieldMaxBits + 7) / 8;
// Upper bound on the text form: one '0'/'1' character per bit.
constexpr int64_t kBitfieldMaxText = kBitfieldMaxBits;

// Read/write the 2-byte little-endian bit-count header.
static void store_bit_count(unsigned char *buf, size_t nbits) {
  buf[0] = static_cast<unsigned char>(nbits & 0xFF);
  buf[1] = static_cast<unsigned char>((nbits >> 8) & 0xFF);
}

static size_t load_bit_count(const unsigned char *buf) {
  return static_cast<size_t>(buf[0]) | (static_cast<size_t>(buf[1]) << 8);
}

// Read bit i (MSB-first within each byte) from the packed region of `data`
static int get_bit(const unsigned char *data, size_t i) {
  return (data[i / 8] >> (7 - (i % 8))) & 1;
}

// Type parameter: the maximum number of bits any value in the column may hold.
// Defaults to kBitfieldMaxBits when the column omits the parameter.
struct BitfieldParams {
  int64_t max_bits = kBitfieldMaxBits;

  static BitfieldParams parse(
      const std::map<std::string, std::string> &params) {
    BitfieldParams p;
    auto it = params.find("max_number_of_bits");
    if (it != params.end())
      p.max_bits = strtoll(it->second.c_str(), nullptr, 10);
    return p;
  }

  static void to_strings(const BitfieldParams &p,
                         std::map<std::string, std::string> &out) {
    out["max_number_of_bits"] = std::to_string(p.max_bits);
  }
};

// resolve_params validates 'max_number_of_bits' (default kBitfieldMaxBits) and
// computes the storage sizes for this parameterization. The bare form routes
// here with an empty map and falls back to the default, so a missing parameter
// is accepted rather than rejected.
bool bitfield_resolve_params(std::map<std::string, std::string> &params,
                             vsql::ResolvedTypeParams *result,
                             char *error_msg) {
  auto it = params.find("max_number_of_bits");
  int64_t max_bits = it != params.end()
                         ? strtoll(it->second.c_str(), nullptr, 10)
                         : kBitfieldMaxBits;
  if (max_bits <= 0 || max_bits > kBitfieldMaxBits) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "BITFIELD 'max_number_of_bits' must be a positive integer <= %lld",
             static_cast<long long>(kBitfieldMaxBits));
    return true;
  }
  params["max_number_of_bits"] = std::to_string(max_bits);
  // Variable-length: persisted_length is the upper bound on the backing field
  // for this parameterization (header + ceil(max_bits/8)); the per-value length
  // is still reported via out.set_length() in from_string.
  result->max_decode_buffer_length = max_bits;
  return false;
}

// STRING -> binary: pack each '0'/'1' character into one bit. The number of
// bits is decided per value; the actual byte length is reported via
// out.set_length(). The column's 'max_number_of_bits' caps how many bits a
// value may hold. params is unknown only on the constant-string inference path;
// there we fall back to the default cap.
void bitfield_from_string(vsql::MaybeParams<BitfieldParams> &p,
                          std::string_view from, vsql::CustomResult out) {
  if (!p.is_known()) p.set(BitfieldParams{});
  const int64_t max_bits = p.value().max_bits;

  auto buf = out.buffer();
  const size_t nbits = from.size();
  if (static_cast<int64_t>(nbits) > max_bits) {
    out.warning("BITFIELD: value exceeds max_number_of_bits");
    return;
  }
  const size_t nbytes = (nbits + 7) / 8;
  if (kBitfieldHeaderLen + nbytes > buf.size()) {
    out.warning("BITFIELD: value exceeds max length");
    return;
  }

  // Zero the packed region, then set the 1 bits.
  memset(buf.data() + kBitfieldHeaderLen, 0, nbytes);
  for (size_t i = 0; i < nbits; i++) {
    char c = from[i];
    if (c != '0' && c != '1') {
      out.warning("BITFIELD: expected only '0' or '1'");
      return;
    }
    if (c == '1') {
      buf.data()[kBitfieldHeaderLen + i / 8] |=
          static_cast<unsigned char>(1u << (7 - (i % 8)));
    }
  }
  store_bit_count(buf.data(), nbits);
  out.set_length(kBitfieldHeaderLen + nbytes);
}

// binary -> STRING: emit exactly bit-count '0'/'1' characters, MSB-first. The
// bit count is read from the value's header, so the params are not needed here;
// the CustomArgWith<BitfieldParams> signature is required only because the type
// is parameterized.
void bitfield_to_string(vsql::CustomArgWith<BitfieldParams> in,
                        vsql::StringResult out) {
  auto data = in.value();
  auto buf = out.buffer();
  if (data.size() < static_cast<size_t>(kBitfieldHeaderLen)) {
    out.warning("bitfield_to_string: doesn't contain enough data for header");
    out.set_length(0);
    return;
  }
  const size_t nbits = load_bit_count(data.data());
  if (nbits > buf.size()) return;
  for (size_t i = 0; i < nbits; i++) {
    buf[i] = get_bit(data.data() + kBitfieldHeaderLen, i) ? '1' : '0';
  }
  out.set_length(nbits);
}

// Bit-by-bit comparison; a shorter bit field sorts first on a common prefix.
// Comparison is purely on the stored bits, so the params are not consulted.
int bitfield_compare(vsql::CustomArgWith<BitfieldParams> a,
                     vsql::CustomArgWith<BitfieldParams> b) {
  auto da = a.value();
  auto db = b.value();
  size_t nabits = da.size() >= static_cast<size_t>(kBitfieldHeaderLen)
                      ? load_bit_count(da.data())
                      : 0;
  size_t nbbits = db.size() >= static_cast<size_t>(kBitfieldHeaderLen)
                      ? load_bit_count(db.data())
                      : 0;

  assert(nabits == 0 || (da.size() - kBitfieldHeaderLen) * 8 >= nabits);
  assert(nbbits == 0 || (db.size() - kBitfieldHeaderLen) * 8 >= nbbits);

  // compare full bytes first for efficiency, then remaining bits
  size_t nabytes = nabits / 8;
  size_t nbbytes = nbbits / 8;
  size_t nbytes = std::min(nabytes, nbbytes);

  if (nbytes > 0) {
    int rc = memcmp(da.data() + kBitfieldHeaderLen,
                    db.data() + kBitfieldHeaderLen, nbytes);
    if (rc != 0) return rc < 0 ? -1 : 1;
  }

  // compare remaining bits
  assert(nabits >= nbytes * 8);
  assert(nbbits >= nbytes * 8);
  size_t na_remaining_bits = nabits - (nbytes * 8);
  size_t nb_remaining_bits = nbbits - (nbytes * 8);
  size_t n_remaining_bits = std::min(na_remaining_bits, nb_remaining_bits);
  for (size_t i = 0; i < n_remaining_bits; i++) {
    int ba = get_bit(da.data() + kBitfieldHeaderLen + nbytes, i);
    int bb = get_bit(db.data() + kBitfieldHeaderLen + nbytes, i);
    if (ba != bb) return ba < bb ? -1 : 1;
  }
  if (na_remaining_bits != nb_remaining_bits)
    return na_remaining_bits < nb_remaining_bits ? -1 : 1;
  return 0;
}

// Number of bits in this BITFIELD value (read from the header).
void bitfield_length(vsql::CustomArg in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto data = in.value();
  long long nbits = data.size() >= static_cast<size_t>(kBitfieldHeaderLen)
                        ? static_cast<long long>(load_bit_count(data.data()))
                        : 0;
  out.set(nbits);
}

// Number of set bits (population count) in this BITFIELD value.
void bitfield_popcount(vsql::CustomArg in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto data = in.value();
  if (data.size() < static_cast<size_t>(kBitfieldHeaderLen)) {
    out.set(0);
    return;
  }
  const size_t nbits = load_bit_count(data.data());
  long long count = 0;
  for (size_t i = 0; i < nbits; i++) {
    count += get_bit(data.data() + kBitfieldHeaderLen, i);
  }
  out.set(count);
}

static constexpr const char kBitfieldTypeName[] = "BITFIELD";

constexpr auto BITFIELD = vsql::make_type<kBitfieldTypeName>()
                              .variable_length_type()
                              .max_persisted_length(kBitfieldMaxLen)
                              .max_decode_buffer_length(kBitfieldMaxText)
                              .params<BitfieldParams, &BitfieldParams::parse,
                                      &BitfieldParams::to_strings>()
                              .resolve_params<&bitfield_resolve_params>()
                              .from_string<&bitfield_from_string>()
                              .to_string<&bitfield_to_string>()
                              .compare<&bitfield_compare>()
                              .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .type(BITFIELD)
        .func(make_func<&bitfield_length>("bitfield_length")
                  .returns(INT)
                  .param(BITFIELD)
                  .deterministic()
                  .build())
        .func(make_func<&bitfield_popcount>("bitfield_popcount")
                  .returns(INT)
                  .param(BITFIELD)
                  .deterministic()
                  .build()))
