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

#ifndef VILLAGESQL_SERVICES_ABI_SIGNATURE_COMPUTE_H
#define VILLAGESQL_SERVICES_ABI_SIGNATURE_COMPUTE_H

// Compile-time structural fingerprints of capability ABI structs.
//
// Server-side and unit-test-side only.  It needs Boost.PFR, which the SDK
// does not ship; extensions take the pinned literal from the ABI header via
// VEF_PIN (villagesql/detail/abi_pin.h) and never compute a hash.
//
// The fingerprint identifies the full shape of a C type: the kind and size of
// every leaf, recursively through pointees, function return and parameter
// types, enum underlying types, array extents and struct fields.  A change
// anywhere in the tree flips it.  Differences that cannot matter to a caller
// across the ABI (typedef names, struct names, the compiler vendor) do not.
//
// abi_type_hash_raw<T>() classifies T into an AbiKind with <type_traits>,
// mixes the kind tag into the hash, and recurses: pointers and references into
// their pointee (with its cv-qualifiers), functions into the return type and
// each parameter, enums into their underlying type, arrays into the element
// type (with the extent), and structs into sizeof plus their fields.
// Aggregate structs are walked with Boost.PFR, which enumerates fields in
// declaration order without any registration.  PFR cannot decompose a struct
// that has an anonymous union, so those list their fields by hand with
// VEF_ABI_MANUAL_FIELDS; forward-declared handles that extensions only hold by
// pointer are registered by name with VEF_ABI_REGISTER_OPAQUE_HANDLE.  Both
// live in abi_specializations.h.
//
// Nothing here uses __PRETTY_FUNCTION__ or other compiler-rendered type
// names: their spelling is implementation-defined, so a server and an
// extension built by different compilers would disagree about identical
// types.
//
// TODO(villagesql-windows): calling-convention attributes (__stdcall,
// __vectorcall) change the ABI on Windows but no type trait exposes them.
// Encode them explicitly when adding Windows support.

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>
#include <utility>

#include <boost/pfr/core.hpp>
#include <boost/pfr/tuple_size.hpp>

namespace villagesql::detail {

// Textual fingerprint "hash-XXXXXXXXXXXXXXXX" plus a NUL: a scheme tag and 16
// lowercase hex digits of the structural hash.
//
// The fingerprint is the capability version.  Versions have no order: the
// server serves a set of shapes and an extension asks for exactly one, so
// there is no number to bump, none to forget, and no collision when two
// changes to the same capability land in parallel.
//
// The scheme tag travels with every pin, so the hashing rules can change
// later under a new tag ("hash2-...") while the server keeps registering
// entries under the old one for extensions already built.
struct AbiFingerprint {
  static constexpr std::size_t kPrefixLen = 5;  // "hash-"
  static constexpr std::size_t kHexLen = 16;
  static constexpr std::size_t kLen = kPrefixLen + kHexLen;
  static constexpr std::size_t kBufSize = kLen + 1;

  std::array<char, kBufSize> chars{};

  constexpr std::string_view view() const {
    return std::string_view(chars.data(), kLen);
  }
  constexpr const char *c_str() const { return chars.data(); }

  constexpr bool operator==(const AbiFingerprint &other) const {
    for (std::size_t i = 0; i < kLen; ++i) {
      if (chars[i] != other.chars[i]) return false;
    }
    return true;
  }
  constexpr bool operator!=(const AbiFingerprint &other) const {
    return !(*this == other);
  }

  template <std::size_t N>
  static constexpr AbiFingerprint from_literal(const char (&literal)[N]) {
    static_assert(N == kBufSize,
                  "pin literal must be \"hash-\" + 16 hex digits");
    AbiFingerprint fp{};
    for (std::size_t i = 0; i < kLen; ++i) fp.chars[i] = literal[i];
    return fp;
  }
};

// Values are part of the hash.  Reordering or removing entries changes every
// fingerprint and requires a new scheme tag in AbiFingerprint.
enum class AbiKind : std::uint8_t {
  Void = 1,
  Bool,
  Char,
  SignedChar,
  UnsignedChar,
  Char16,
  Char32,
  WChar,
  Short,
  UnsignedShort,
  Int,
  UnsignedInt,
  Long,
  UnsignedLong,
  LongLong,
  UnsignedLongLong,
  Float,
  Double,
  LongDouble,
  Pointer,
  LValueReference,
  RValueReference,
  Function,
  Enum,
  Array,
  Struct,
  Union,
  MemberPointer,
  Unknown,
  OpaqueHandle,
};

// FNV-1a 64-bit offset basis, so an empty fold starts from a well-defined
// non-zero value.  Changing it changes every fingerprint and requires a new
// scheme tag.
constexpr std::size_t kAbiSeed = 0xcbf29ce484222325ULL;

// Order-sensitive combine in the shape of boost::hash_combine;
// 0x9e3779b97f4a7c15 is floor(2^64 / phi).  Changing it changes every
// fingerprint and requires a new scheme tag.
constexpr std::size_t abi_combine(std::size_t a, std::size_t b) {
  return a ^ (b + 0x9e3779b97f4a7c15ULL + (a << 6) + (a >> 2));
}

template <typename T>
constexpr std::size_t abi_type_hash_raw();

// Marks a type the ABI deliberately never gives a layout: a forward-declared
// server-side handle that extensions only hold by pointer.  It must be
// registered explicitly (VEF_ABI_REGISTER_OPAQUE_HANDLE) rather than detected
// as "incomplete in this TU", because completeness depends on which headers a
// TU includes: a server TU that sees the definition and an SDK TU that does
// not would otherwise hash the same handle differently.
template <typename T>
struct is_opaque_handle : std::false_type {};

constexpr std::size_t abi_str_fold(const char *s) {
  std::size_t h = kAbiSeed;
  while (*s) {
    h = abi_combine(h, static_cast<std::size_t>(*s));
    ++s;
  }
  return h;
}

template <typename T, std::size_t... Is>
constexpr std::size_t abi_pfr_field_hash(std::index_sequence<Is...>) {
  std::size_t h = kAbiSeed;
  ((h = abi_combine(h,
                    abi_type_hash_raw<boost::pfr::tuple_element_t<Is, T>>())),
   ...);
  return h;
}

// Walks an aggregate's fields in declaration order with Boost.PFR.
// Specializations (abi_specializations.h) take over for types PFR cannot
// walk.
template <typename T>
struct AbiStructFields {
  static constexpr std::size_t field_hash() {
    static_assert(!std::is_union_v<T>,
                  "AbiStructFields<T>: T is a union.  Boost.PFR cannot "
                  "enumerate union alternatives; list the fields of the "
                  "enclosing struct with VEF_ABI_MANUAL_FIELDS.");
    static_assert(std::is_aggregate_v<T>,
                  "AbiStructFields<T>: T is not an aggregate, so Boost.PFR "
                  "cannot enumerate its fields.  ABI structs must be plain C "
                  "aggregates.");
    return abi_pfr_field_hash<T>(
        std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
  }
};

// One entry of a VEF_ABI_MANUAL_FIELDS list.  The offset is hashed so union
// arms (which share an offset) hash differently from consecutive fields of
// the same types.
struct AbiField {
  std::size_t offset;
  std::size_t size;
  std::size_t hash;
};

template <std::size_t N>
constexpr std::size_t abi_manual_fields_hash(const AbiField (&fields)[N]) {
  std::size_t h = kAbiSeed;
  for (const AbiField &f : fields) {
    h = abi_combine(h, f.offset);
    h = abi_combine(h, f.hash);
  }
  return h;
}

// True when the listed fields extend to the end of T (up to tail padding).
// That catches a field appended to T without being added to its list.  It
// cannot catch a missed union arm that is no larger than the others, which is
// why each struct with a manual list carries a comment pointing at it.
template <typename T, std::size_t N>
constexpr bool abi_manual_fields_reach_end(const AbiField (&fields)[N]) {
  std::size_t end = 0;
  for (const AbiField &f : fields) {
    if (f.offset + f.size > end) end = f.offset + f.size;
  }
  const std::size_t align = alignof(T);
  return (end + align - 1) / align * align == sizeof(T);
}

template <typename F>
struct AbiFunctionHash;

template <typename R, typename... Args>
struct AbiFunctionHash<R(Args...)> {
  static constexpr std::size_t hash() {
    std::size_t h = static_cast<std::size_t>(AbiKind::Function);
    h = abi_combine(h, abi_type_hash_raw<R>());
    h = abi_combine(h, sizeof...(Args));
    ((h = abi_combine(h, abi_type_hash_raw<Args>())), ...);
    return h;
  }
};

// Open question: should integers hash by C++ type identity or by
// (signedness, width)?
//
// They hash by type identity today, so `long` and `long long` differ even
// where both are 8 bytes.
//
// For identity:
//   - Swapping one integer type for another flips the digest on every
//     target, including swaps that are harmless on LP64 but not on LLP64
//     (Windows, where `long` is 4 bytes).  Review sees such a change before
//     there is a Windows build to catch it.
//   - The kind tag maps one-to-one onto the C type, which keeps the scheme
//     easy to reason about.
// Against identity:
//   - Fixed-width typedefs resolve to different types per target: uint64_t
//     is `unsigned long long` on macOS and `unsigned long` on Linux.  Every
//     capability that touches one needs a separate pin per target although
//     the C calling convention passes both the same way, so the pins cannot
//     all be computed on one machine.
//   - A per-target difference in pins then usually means nothing, which hides
//     the rare one that reflects a real layout difference.
//
// Hashing by (signedness, width) would give macOS, Linux x86_64 and Linux
// arm64 one shared digest and keep per-target slots for real divergence such
// as LLP64.  Plain `char` would have to stay its own kind either way: it is
// unsigned on Linux arm64 and signed on the other targets.
template <typename T>
constexpr std::size_t abi_type_hash_raw() {
  // Top-level cv does not change layout, and stripping it lets the
  // is_same_v arms below match `const int` fields.
  using U = std::remove_cv_t<T>;
  std::size_t h = kAbiSeed;
  if constexpr (std::is_void_v<U>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Void));
  } else if constexpr (std::is_same_v<U, bool>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Bool));
  } else if constexpr (std::is_same_v<U, char>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Char));
  } else if constexpr (std::is_same_v<U, signed char>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::SignedChar));
  } else if constexpr (std::is_same_v<U, unsigned char>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::UnsignedChar));
  } else if constexpr (std::is_same_v<U, char16_t>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Char16));
  } else if constexpr (std::is_same_v<U, char32_t>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Char32));
  } else if constexpr (std::is_same_v<U, wchar_t>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::WChar));
  } else if constexpr (std::is_same_v<U, short>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Short));
  } else if constexpr (std::is_same_v<U, unsigned short>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::UnsignedShort));
  } else if constexpr (std::is_same_v<U, int>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Int));
  } else if constexpr (std::is_same_v<U, unsigned int>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::UnsignedInt));
  } else if constexpr (std::is_same_v<U, long>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Long));
  } else if constexpr (std::is_same_v<U, unsigned long>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::UnsignedLong));
  } else if constexpr (std::is_same_v<U, long long>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::LongLong));
  } else if constexpr (std::is_same_v<U, unsigned long long>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::UnsignedLongLong));
  } else if constexpr (std::is_same_v<U, float>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Float));
  } else if constexpr (std::is_same_v<U, double>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::Double));
  } else if constexpr (std::is_same_v<U, long double>) {
    return abi_combine(h, static_cast<std::size_t>(AbiKind::LongDouble));
  } else if constexpr (std::is_pointer_v<U>) {
    using P = std::remove_pointer_t<U>;
    h = abi_combine(h, static_cast<std::size_t>(AbiKind::Pointer));
    h = abi_combine(h, std::is_const_v<P> ? 1u : 0u);
    h = abi_combine(h, std::is_volatile_v<P> ? 1u : 0u);
    h = abi_combine(h, abi_type_hash_raw<std::remove_cv_t<P>>());
    return h;
  } else if constexpr (std::is_lvalue_reference_v<U>) {
    using P = std::remove_reference_t<U>;
    h = abi_combine(h, static_cast<std::size_t>(AbiKind::LValueReference));
    h = abi_combine(h, std::is_const_v<P> ? 1u : 0u);
    h = abi_combine(h, std::is_volatile_v<P> ? 1u : 0u);
    h = abi_combine(h, abi_type_hash_raw<std::remove_cv_t<P>>());
    return h;
  } else if constexpr (std::is_rvalue_reference_v<U>) {
    using P = std::remove_reference_t<U>;
    h = abi_combine(h, static_cast<std::size_t>(AbiKind::RValueReference));
    h = abi_combine(h, std::is_const_v<P> ? 1u : 0u);
    h = abi_combine(h, std::is_volatile_v<P> ? 1u : 0u);
    h = abi_combine(h, abi_type_hash_raw<std::remove_cv_t<P>>());
    return h;
  } else if constexpr (std::is_function_v<U>) {
    return abi_combine(h, AbiFunctionHash<U>::hash());
  } else if constexpr (std::is_array_v<U>) {
    h = abi_combine(h, static_cast<std::size_t>(AbiKind::Array));
    h = abi_combine(h, std::extent_v<U>);
    h = abi_combine(h, abi_type_hash_raw<std::remove_extent_t<U>>());
    return h;
  } else if constexpr (std::is_enum_v<U>) {
    h = abi_combine(h, static_cast<std::size_t>(AbiKind::Enum));
    h = abi_combine(h, abi_type_hash_raw<std::underlying_type_t<U>>());
    return h;
  } else if constexpr (std::is_union_v<U>) {
    // Fires AbiStructFields' static_assert unless a specialization exists.
    h = abi_combine(h, static_cast<std::size_t>(AbiKind::Union));
    h = abi_combine(h, sizeof(U));
    h = abi_combine(h, AbiStructFields<U>::field_hash());
    return h;
  } else if constexpr (is_opaque_handle<U>::value) {
    // Identity only: no sizeof and no fields, because one side of the ABI
    // never sees a definition.
    h = abi_combine(h, static_cast<std::size_t>(AbiKind::OpaqueHandle));
    h = abi_combine(h, AbiStructFields<U>::field_hash());
    return h;
  } else if constexpr (std::is_class_v<U>) {
    h = abi_combine(h, static_cast<std::size_t>(AbiKind::Struct));
    h = abi_combine(h, sizeof(U));
    h = abi_combine(h, AbiStructFields<U>::field_hash());
    return h;
  } else if constexpr (std::is_member_pointer_v<U>) {
    h = abi_combine(h, static_cast<std::size_t>(AbiKind::MemberPointer));
    h = abi_combine(h, sizeof(U));
    return h;
  } else {
    static_assert(sizeof(U) == 0,
                  "abi_type_hash_raw<T>: T is not a recognized ABI type kind. "
                  " Add an AbiKind and an arm here to hash it.");
    return h;
  }
}

constexpr AbiFingerprint abi_fingerprint_from_hash(std::size_t h) {
  AbiFingerprint fp{};
  constexpr char kPrefix[AbiFingerprint::kPrefixLen] = {'h', 'a', 's', 'h',
                                                        '-'};
  for (std::size_t i = 0; i < AbiFingerprint::kPrefixLen; ++i) {
    fp.chars[i] = kPrefix[i];
  }
  constexpr char kHex[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                             '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  for (std::size_t i = 0; i < AbiFingerprint::kHexLen; ++i) {
    const std::size_t shift = 4 * (AbiFingerprint::kHexLen - 1 - i);
    fp.chars[AbiFingerprint::kPrefixLen + i] = kHex[(h >> shift) & 0xFULL];
  }
  return fp;
}

template <typename T>
constexpr AbiFingerprint abi_signature_hash() {
  return abi_fingerprint_from_hash(abi_type_hash_raw<T>());
}

// True when the pin literal equals T's fingerprint, or when it is the empty
// placeholder.  A literal of the wrong length never matches.  The branches
// are if constexpr so from_literal, which static_asserts on length, is only
// instantiated for well-formed literals.
template <typename T, std::size_t N>
constexpr bool pin_matches(const char (&lit)[N]) {
  if constexpr (N <= 1) {
    return true;
  } else if constexpr (N != AbiFingerprint::kBufSize) {
    return false;
  } else {
    return abi_signature_hash<T>() == AbiFingerprint::from_literal(lit);
  }
}

}  // namespace villagesql::detail

// Registers T as an opaque handle: hashed by its spelled name only.  Both
// sides of the ABI must register the same name next to the forward
// declaration, so swapping one handle type for another in a signature still
// flips the hash.
#define VEF_ABI_REGISTER_OPAQUE_HANDLE(Type)            \
  namespace villagesql::detail {                        \
  template <>                                           \
  struct is_opaque_handle<Type> : std::true_type {};    \
  template <>                                           \
  struct AbiStructFields<Type> {                        \
    static constexpr std::size_t field_hash() {         \
      return ::villagesql::detail::abi_str_fold(#Type); \
    }                                                   \
  };                                                    \
  }                                                     \
  static_assert(true, "force trailing semicolon at call site")

#define VEF_ABI_FIELD(T, f)                                       \
  ::villagesql::detail::AbiField {                                \
    offsetof(T, f), sizeof(T::f),                                 \
        ::villagesql::detail::abi_type_hash_raw<decltype(T::f)>() \
  }

// VEF_ABI_FOR_EACH(M, T, a, b, c) expands to M(T, a), M(T, b), M(T, c).
// TODO(villagesql-windows): MSVC's traditional preprocessor expands
// __VA_ARGS__ as a single argument; build with /Zc:preprocessor.
#define VEF_ABI_FE_1(M, T, a) M(T, a)
#define VEF_ABI_FE_2(M, T, a, ...) M(T, a), VEF_ABI_FE_1(M, T, __VA_ARGS__)
#define VEF_ABI_FE_3(M, T, a, ...) M(T, a), VEF_ABI_FE_2(M, T, __VA_ARGS__)
#define VEF_ABI_FE_4(M, T, a, ...) M(T, a), VEF_ABI_FE_3(M, T, __VA_ARGS__)
#define VEF_ABI_FE_5(M, T, a, ...) M(T, a), VEF_ABI_FE_4(M, T, __VA_ARGS__)
#define VEF_ABI_FE_6(M, T, a, ...) M(T, a), VEF_ABI_FE_5(M, T, __VA_ARGS__)
#define VEF_ABI_FE_7(M, T, a, ...) M(T, a), VEF_ABI_FE_6(M, T, __VA_ARGS__)
#define VEF_ABI_FE_8(M, T, a, ...) M(T, a), VEF_ABI_FE_7(M, T, __VA_ARGS__)
#define VEF_ABI_FE_9(M, T, a, ...) M(T, a), VEF_ABI_FE_8(M, T, __VA_ARGS__)
#define VEF_ABI_FE_10(M, T, a, ...) M(T, a), VEF_ABI_FE_9(M, T, __VA_ARGS__)
#define VEF_ABI_FE_11(M, T, a, ...) M(T, a), VEF_ABI_FE_10(M, T, __VA_ARGS__)
#define VEF_ABI_FE_12(M, T, a, ...) M(T, a), VEF_ABI_FE_11(M, T, __VA_ARGS__)
#define VEF_ABI_FE_13(M, T, a, ...) M(T, a), VEF_ABI_FE_12(M, T, __VA_ARGS__)
#define VEF_ABI_FE_14(M, T, a, ...) M(T, a), VEF_ABI_FE_13(M, T, __VA_ARGS__)
#define VEF_ABI_FE_15(M, T, a, ...) M(T, a), VEF_ABI_FE_14(M, T, __VA_ARGS__)
#define VEF_ABI_FE_16(M, T, a, ...) M(T, a), VEF_ABI_FE_15(M, T, __VA_ARGS__)
#define VEF_ABI_FE_17(M, T, a, ...) M(T, a), VEF_ABI_FE_16(M, T, __VA_ARGS__)
#define VEF_ABI_FE_18(M, T, a, ...) M(T, a), VEF_ABI_FE_17(M, T, __VA_ARGS__)
#define VEF_ABI_FE_19(M, T, a, ...) M(T, a), VEF_ABI_FE_18(M, T, __VA_ARGS__)
#define VEF_ABI_FE_20(M, T, a, ...) M(T, a), VEF_ABI_FE_19(M, T, __VA_ARGS__)
#define VEF_ABI_FE_21(M, T, a, ...) M(T, a), VEF_ABI_FE_20(M, T, __VA_ARGS__)
#define VEF_ABI_FE_22(M, T, a, ...) M(T, a), VEF_ABI_FE_21(M, T, __VA_ARGS__)
#define VEF_ABI_FE_23(M, T, a, ...) M(T, a), VEF_ABI_FE_22(M, T, __VA_ARGS__)
#define VEF_ABI_FE_24(M, T, a, ...) M(T, a), VEF_ABI_FE_23(M, T, __VA_ARGS__)
#define VEF_ABI_FE_PICK(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12,     \
                        _13, _14, _15, _16, _17, _18, _19, _20, _21, _22, _23, \
                        _24, NAME, ...)                                        \
  NAME
#define VEF_ABI_FOR_EACH(M, T, ...)                                           \
  VEF_ABI_FE_PICK(__VA_ARGS__, VEF_ABI_FE_24, VEF_ABI_FE_23, VEF_ABI_FE_22,   \
                  VEF_ABI_FE_21, VEF_ABI_FE_20, VEF_ABI_FE_19, VEF_ABI_FE_18, \
                  VEF_ABI_FE_17, VEF_ABI_FE_16, VEF_ABI_FE_15, VEF_ABI_FE_14, \
                  VEF_ABI_FE_13, VEF_ABI_FE_12, VEF_ABI_FE_11, VEF_ABI_FE_10, \
                  VEF_ABI_FE_9, VEF_ABI_FE_8, VEF_ABI_FE_7, VEF_ABI_FE_6,     \
                  VEF_ABI_FE_5, VEF_ABI_FE_4, VEF_ABI_FE_3, VEF_ABI_FE_2,     \
                  VEF_ABI_FE_1)                                               \
  (M, T, __VA_ARGS__)

// Lists the fields of an ABI struct that Boost.PFR cannot walk because it has
// an anonymous union.  Name every field in declaration order, and every union
// arm (members of an anonymous struct inside the union are named
// individually).  PFR can no longer prove the list complete, so the struct's
// definition must point here, and abi_manual_fields_reach_end fails the build
// when the listed fields stop short of sizeof(T).
#define VEF_ABI_MANUAL_FIELDS(T, ...)                            \
  namespace villagesql::detail {                                 \
  template <>                                                    \
  struct AbiStructFields<T> {                                    \
    static constexpr std::size_t field_hash() {                  \
      constexpr AbiField kFields[] = {                           \
          VEF_ABI_FOR_EACH(VEF_ABI_FIELD, T, __VA_ARGS__)};      \
      static_assert(abi_manual_fields_reach_end<T>(kFields),     \
                    "VEF_ABI_MANUAL_FIELDS(" #T                  \
                    "): the listed fields end before sizeof(" #T \
                    "); add the new field to the list");         \
      return abi_manual_fields_hash(kFields);                    \
    }                                                            \
  };                                                             \
  }                                                              \
  static_assert(true, "force trailing semicolon at call site")

// VEF_PIN_VERIFY(T, mac_lit, x86_lit, arm_lit) yields the pin literal for the
// current target, like VEF_PIN, and fails the build if that literal is not
// T's fingerprint.  It is an immediately invoked lambda so the static_assert
// can sit inside an expression such as a designated initializer.  Run
// abi_pin_literals-t for the value to paste in.
//
// TODO(villagesql-preview): put the new pin in the build error itself, so
// fixing a stale pin needs no second step.  A static_assert message cannot be
// computed before C++26.  What we think works: on a mismatch, call a function
// template that is declared but never defined, from a constexpr context, with
// the computed pin as a class-type template argument:
//
//   struct AbiPin { char s[AbiFingerprint::kBufSize]; };
//   template <typename T, AbiPin NewPin>
//   void abi_pin_mismatch_on_mac();
//
// The compiler then quotes the call in its error.  A clang prototype printed:
//
//   non-constexpr function 'abi_pin_mismatch_on_mac<vef_preview_ping_t,
//   Pin{"hash-b80d82e6befdc3da"}>' cannot be used in a constant expression
//
// Not yet checked with GCC.  An earlier draft passed the pin as a pack of
// chars, which works but prints them one by one ('h', 'a', 's', ...).
#if defined(__APPLE__)
#define VEF_PIN_VERIFY(T, mac_lit, x86_lit, arm_lit)                        \
  ([] {                                                                     \
    static_assert(::villagesql::detail::pin_matches<T>(mac_lit),            \
                  "ABI pin mismatch for " #T                                \
                  " on mac -- run abi_pin_literals-t for the current pin"); \
    (void)(x86_lit);                                                        \
    (void)(arm_lit);                                                        \
    return mac_lit;                                                         \
  }())
#elif defined(__linux__) && defined(__x86_64__)
#define VEF_PIN_VERIFY(T, mac_lit, x86_lit, arm_lit)                         \
  ([] {                                                                      \
    static_assert(::villagesql::detail::pin_matches<T>(x86_lit),             \
                  "ABI pin mismatch for " #T                                 \
                  " on linux_x86 -- run abi_pin_literals-t for the current " \
                  "pin");                                                    \
    (void)(mac_lit);                                                         \
    (void)(arm_lit);                                                         \
    return x86_lit;                                                          \
  }())
#elif defined(__linux__) && defined(__aarch64__)
#define VEF_PIN_VERIFY(T, mac_lit, x86_lit, arm_lit)                         \
  ([] {                                                                      \
    static_assert(::villagesql::detail::pin_matches<T>(arm_lit),             \
                  "ABI pin mismatch for " #T                                 \
                  " on linux_arm -- run abi_pin_literals-t for the current " \
                  "pin");                                                    \
    (void)(mac_lit);                                                         \
    (void)(x86_lit);                                                         \
    return arm_lit;                                                          \
  }())
#else
#define VEF_PIN_VERIFY(T, mac_lit, x86_lit, arm_lit) \
  ((void)(mac_lit), (void)(x86_lit), (void)(arm_lit), (const char *)"")
#endif

// VEF_PIN_VERIFY over the _MAC / _LINUX_X86 / _LINUX_ARM literals sharing a
// prefix, matching VEF_PIN_OF on the extension side.
#define VEF_PIN_VERIFY_OF(T, prefix) \
  VEF_PIN_VERIFY(T, prefix##_MAC, prefix##_LINUX_X86, prefix##_LINUX_ARM)

#endif  // VILLAGESQL_SERVICES_ABI_SIGNATURE_COMPUTE_H
