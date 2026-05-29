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

#ifndef VSQL_VEC_CHAIN_TEST_TYPE_H_
#define VSQL_VEC_CHAIN_TEST_TYPE_H_

#include <villagesql/vsql.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

inline constexpr int64_t kVecChainMaxDimension = 4096;
inline constexpr int64_t kVecChainMaxPersistedLength =
    kVecChainMaxDimension * 8;

struct VecChainParams {
  int64_t dimension;
  size_t bytes_per_elem;  // 4 for float, 8 for double

  static VecChainParams parse(const std::map<std::string, std::string> &params);
  static void to_strings(const VecChainParams &p,
                         std::map<std::string, std::string> &out);
};

// Little-endian float/double codec used by the type's binary
// representation. Exposed so the index code can decode stored vectors
// for distance computation.
void store_float(unsigned char *buf, float val);
float load_float(const unsigned char *buf);
void store_double(unsigned char *buf, double val);
double load_double(const unsigned char *buf);

// L2 distance between two raw vectors of identical shape. Used by both
// the VDF and (later) the index's scan.
double vec_chain_l2_distance_raw(const unsigned char *a, const unsigned char *b,
                                 const VecChainParams &p);

// Type registration callbacks.
bool vec_chain_int_to_params(int64_t value,
                             std::map<std::string, std::string> &params,
                             char *error_msg);
bool vec_chain_resolve_params(const std::map<std::string, std::string> &params,
                              vsql::ResolvedTypeParams *result,
                              char *error_msg);
void vec_chain_from_string(vsql::MaybeParams<VecChainParams> &p,
                           std::string_view from, vsql::CustomResult out);
void vec_chain_to_string(vsql::CustomArgWith<VecChainParams> in,
                         vsql::StringResult out);
int vec_chain_compare(vsql::CustomArgWith<VecChainParams> a,
                      vsql::CustomArgWith<VecChainParams> b);
std::string vec_chain_default(const VecChainParams &p, char *error_msg);

// Distance / algebra VDFs.
void vec_chain_l2_distance(vsql::CustomArgWith<VecChainParams> a,
                           vsql::CustomArgWith<VecChainParams> b,
                           vsql::RealResult out);
void vec_chain_dot_product(vsql::CustomArgWith<VecChainParams> a,
                           vsql::CustomArgWith<VecChainParams> b,
                           vsql::RealResult out);

inline constexpr const char kVecChainTypeName[] = "VSQL_VEC_CHAIN";

inline constexpr auto VSQL_VEC_CHAIN_TYPE =
    vsql::make_type<kVecChainTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kVecChainMaxPersistedLength)
        .params<VecChainParams, &VecChainParams::parse,
                &VecChainParams::to_strings>()
        .int_to_params<&vec_chain_int_to_params>()
        .resolve_params<&vec_chain_resolve_params>()
        .from_string<&vec_chain_from_string>()
        .to_string<&vec_chain_to_string>()
        .compare<&vec_chain_compare>()
        .intrinsic_default_vdf("vec_chain_intrinsic_default")
        .build();

#endif  // VSQL_VEC_CHAIN_TEST_TYPE_H_
