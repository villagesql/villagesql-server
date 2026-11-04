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
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// Properties of the structural ABI fingerprint (abi_signature_compute.h).
//
// These pin relationships between hashes, not absolute values: the absolute
// values are pinned per capability in the ABI headers and checked by
// VEF_PIN_VERIFY in capability_registry.cc and by abi_pin_literals-t.
// Most cases change exactly one thing about a type and expect the hash to
// flip; the Equal* cases change only spelling and expect it not to.

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "villagesql/services/abi_specializations.h"

namespace villagesql_unittest {

// Same field types and order as vef_preview_keyring_t.  The fingerprint
// answers "does the shape match", not "is this the right struct"; the
// capability name tells capabilities apart.
struct local_keyring_lookalike {
  uint32_t version;
  vef_read_keyring_fn read;
  vef_write_keyring_fn write;
};

typedef vef_keyring_result_t (*alias_read_fn)(const char *, const char *,
                                              unsigned char *, std::size_t,
                                              std::size_t *);

typedef vef_keyring_result_t (*read_fn_with_int_buflen)(const char *,
                                                        const char *,
                                                        unsigned char *, int,
                                                        std::size_t *);

typedef vef_keyring_result_t (*read_fn_with_const_buf)(const char *,
                                                       const char *,
                                                       const unsigned char *,
                                                       std::size_t,
                                                       std::size_t *);

struct ManualUnion {
  int a;
  union {
    int b;
    float c;
  };
};

struct ManualUnionArmsSwapped {
  int a;
  union {
    float c;
    int b;
  };
};

struct ManualUnionArmRetyped {
  int a;
  union {
    int b;
    double c;
  };
};

// The fields of ManualUnion laid out one after another.
struct SequentialFields {
  int a;
  int b;
  float c;
};

struct OpaqueA;
struct OpaqueB;

}  // namespace villagesql_unittest

VEF_ABI_MANUAL_FIELDS(villagesql_unittest::ManualUnion, a, b, c);
VEF_ABI_MANUAL_FIELDS(villagesql_unittest::ManualUnionArmsSwapped, a, c, b);
VEF_ABI_MANUAL_FIELDS(villagesql_unittest::ManualUnionArmRetyped, a, b, c);
VEF_ABI_REGISTER_OPAQUE_HANDLE(villagesql_unittest::OpaqueA);
VEF_ABI_REGISTER_OPAQUE_HANDLE(villagesql_unittest::OpaqueB);

namespace villagesql_unittest {

using villagesql::detail::abi_signature_hash;

template <typename T1>
inline void expect_distinct_impl() {}
template <typename T1, typename T2, typename... Ts>
inline void expect_distinct_impl() {
  EXPECT_NE(abi_signature_hash<T1>(), abi_signature_hash<T2>());
  if constexpr (sizeof...(Ts) > 0) {
    expect_distinct_impl<T1, Ts...>();
    expect_distinct_impl<T2, Ts...>();
  }
}
// Expects every pair of the given types to hash differently.
#define EXPECT_DISTINCT_HASHES(...) \
  ::villagesql_unittest::expect_distinct_impl<__VA_ARGS__>()

class AbiSignatureTest : public ::testing::Test {};

TEST_F(AbiSignatureTest, FingerprintFormat) {
  constexpr auto fp = abi_signature_hash<vef_preview_keyring_t>();
  static_assert(fp.chars[0] == 'h' && fp.chars[4] == '-',
                "fingerprint should begin with the 'hash-' scheme tag");
  EXPECT_EQ(fp.view().substr(0, 5), std::string_view("hash-"));
  EXPECT_EQ(fp.view().size(), 21u);
}

TEST_F(AbiSignatureTest, StructDiffersFromItsFields) {
  EXPECT_NE(abi_signature_hash<vef_preview_keyring_t>(),
            abi_signature_hash<vef_read_keyring_fn>());
  EXPECT_NE(abi_signature_hash<vef_read_keyring_fn>(),
            abi_signature_hash<vef_write_keyring_fn>());
}

TEST_F(AbiSignatureTest, ParameterTypeChange) {
  // Same sizeof and same struct name, different parameter type.
  EXPECT_NE(abi_signature_hash<vef_read_keyring_fn>(),
            abi_signature_hash<read_fn_with_int_buflen>());
}

TEST_F(AbiSignatureTest, ConstPointeeInSignature) {
  EXPECT_NE(abi_signature_hash<vef_read_keyring_fn>(),
            abi_signature_hash<read_fn_with_const_buf>());
}

TEST_F(AbiSignatureTest, IntegerKindsWithSameSizeof) {
  // Hashing by type identity: see the open question in
  // abi_signature_compute.h.
  EXPECT_DISTINCT_HASHES(long, long long, unsigned long, unsigned long long);
}

TEST_F(AbiSignatureTest, FloatKinds) {
  EXPECT_DISTINCT_HASHES(float, double, long double);
}

TEST_F(AbiSignatureTest, CharKinds) {
  EXPECT_DISTINCT_HASHES(char, signed char, unsigned char, char16_t, char32_t,
                         wchar_t);
}

TEST_F(AbiSignatureTest, Signedness) {
  EXPECT_DISTINCT_HASHES(int, unsigned int);
  EXPECT_DISTINCT_HASHES(short, unsigned short);
  EXPECT_DISTINCT_HASHES(long, unsigned long);
}

TEST_F(AbiSignatureTest, PointeeQualifiers) {
  EXPECT_DISTINCT_HASHES(int *, const int *, volatile int *);
}

TEST_F(AbiSignatureTest, PointerVersusReferences) {
  EXPECT_DISTINCT_HASHES(int *, int &, int &&);
}

TEST_F(AbiSignatureTest, IndirectionDepth) {
  EXPECT_DISTINCT_HASHES(int *, int **, int ***);
}

TEST_F(AbiSignatureTest, PointeeType) {
  EXPECT_NE(abi_signature_hash<int *>(), abi_signature_hash<unsigned int *>());
}

TEST_F(AbiSignatureTest, ArrayExtentElementAndRank) {
  EXPECT_DISTINCT_HASHES(int[4], int[5], unsigned int[4], int[4][1]);
}

enum class EnumInt : int { A };
enum class EnumLong : long { A };

TEST_F(AbiSignatureTest, EnumUnderlyingType) {
  EXPECT_NE(abi_signature_hash<EnumInt>(), abi_signature_hash<EnumLong>());
}

TEST_F(AbiSignatureTest, FunctionSignatures) {
  using Base = int (*)(double, const char *);
  using ParamRetyped = int (*)(int, const char *);
  using ReturnSwappedWithParam = double (*)(int, const char *);
  using ParamsSwapped = int (*)(const char *, double);
  using FewerParams = int (*)(double);
  using ReturnRetyped = long (*)(double, const char *);
  EXPECT_DISTINCT_HASHES(Base, ParamRetyped, ReturnSwappedWithParam,
                         ParamsSwapped, FewerParams, ReturnRetyped);
}

struct Leaf {
  int a;
  long b;
  double c;
};
struct LeafFieldsSwapped {
  long b;
  int a;
  double c;
};
struct LeafFieldRetyped {
  unsigned int a;
  long b;
  double c;
};
struct LeafFieldAppended {
  int a;
  long b;
  double c;
  int d;
};

TEST_F(AbiSignatureTest, StructFields) {
  EXPECT_DISTINCT_HASHES(Leaf, LeafFieldsSwapped, LeafFieldRetyped,
                         LeafFieldAppended);
}

// A change deep inside a callback parameter has to reach the outer struct.
struct Inner {
  int (*cb)(double, const char *);
  long tag;
};
struct Outer {
  int (*read)(const Inner *, std::size_t, unsigned char *);
  int (*(*make)(Inner))(double, const char *);
  void (*write)(Inner, int);
};
struct InnerV2 {
  int (*cb)(double, const wchar_t *);
  long tag;
};
struct OuterV2 {
  int (*read)(const InnerV2 *, std::size_t, unsigned char *);
  int (*(*make)(InnerV2))(double, const wchar_t *);
  void (*write)(InnerV2, int);
};

TEST_F(AbiSignatureTest, DeepChangePropagates) {
  EXPECT_NE(abi_signature_hash<Inner>(), abi_signature_hash<InnerV2>());
  EXPECT_NE(abi_signature_hash<Outer>(), abi_signature_hash<OuterV2>());
}

TEST_F(AbiSignatureTest, ManualFieldsSeeUnionArms) {
  EXPECT_DISTINCT_HASHES(ManualUnion, ManualUnionArmsSwapped,
                         ManualUnionArmRetyped, SequentialFields);
}

TEST_F(AbiSignatureTest, OpaqueHandlesHashByName) {
  EXPECT_NE(abi_signature_hash<OpaqueA *>(), abi_signature_hash<OpaqueB *>());
  EXPECT_NE(abi_signature_hash<OpaqueA *>(), abi_signature_hash<void *>());
}

TEST_F(AbiSignatureTest, EqualSameShapeDifferentName) {
  EXPECT_EQ(abi_signature_hash<vef_preview_keyring_t>(),
            abi_signature_hash<local_keyring_lookalike>());
  struct InnerTwin {
    int (*cb)(double, const char *);
    long tag;
  };
  EXPECT_EQ(abi_signature_hash<Inner>(), abi_signature_hash<InnerTwin>());
}

TEST_F(AbiSignatureTest, EqualThroughTypedefs) {
  using IntAlias = int;
  using LeafAlias = Leaf;
  EXPECT_EQ(abi_signature_hash<vef_read_keyring_fn>(),
            abi_signature_hash<alias_read_fn>());
  EXPECT_EQ(abi_signature_hash<Leaf>(), abi_signature_hash<LeafAlias>());
  EXPECT_EQ(abi_signature_hash<int[4]>(), abi_signature_hash<IntAlias[4]>());
  EXPECT_EQ(abi_signature_hash<int *>(), abi_signature_hash<IntAlias *>());
}

TEST_F(AbiSignatureTest, TopLevelConstIgnored) {
  struct ConstField {
    const int a;
    long b;
    double c;
  };
  EXPECT_EQ(abi_signature_hash<Leaf>(), abi_signature_hash<ConstField>());
}

}  // namespace villagesql_unittest
