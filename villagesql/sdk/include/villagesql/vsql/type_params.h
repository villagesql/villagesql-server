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
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_VSQL_TYPE_PARAMS_H
#define VILLAGESQL_VSQL_TYPE_PARAMS_H

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <villagesql/abi/types.h>

namespace vsql {

// MaybeParams<P> carries a parameterized type's parsed parameters in either
// "known" or "unknown" state. It is the first argument to from_string for
// parameterized custom types in the ::vsql API.
//
// If the type parameters are already resolved/known, then the SDK constructs
// MaybeParams<P> in the known state from the cached parsed parameters. The
// unknown state is present instead when the type parameters need to be inferred
// by the extension during a from_string call.
//
// Usage in an extension's from_string:
//
//   void mytype_from_string(vsql::MaybeParams<MyParams> &p,
//                           std::string_view from,
//                           vsql::CustomResult out) {
//     // ... parse the string ...
//     if (p.is_known()) {
//       // Validate that what was parsed matches p.value().
//     } else {
//       // Infer params from the string and store them.
//       p.set(MyParams{...});
//     }
//     // Encode using p.value() (now always known).
//   }
template <typename P>
class MaybeParams {
 public:
  using value_type = P;

  // Construct in unknown state.
  MaybeParams() = default;

  // Construct in known state with the given params.
  explicit MaybeParams(P params) : params_(std::move(params)) {}

  // True if params are populated (either set on construction or via set()).
  bool is_known() const { return params_.has_value(); }

  // Returns the params. is_known() must be true.
  const P &value() const {
    assert(params_.has_value());
    return *params_;
  }

  // Stores the given params, transitioning from unknown to known. May also
  // overwrite an existing known value.
  void set(P params) { params_ = std::move(params); }

 private:
  std::optional<P> params_ = std::nullopt;
};

// Internal cache class used by the SDK framework. Extension authors do not
// call these methods directly — use MaybeParams<P>, CustomArgWith<P>, and
// CustomResultWith<P> instead.
//
// Memoizes parsed type parameters to avoid re-parsing strings on every VDF
// call. There is one cache instance per Type Parameter C++ type. The cache is
// wired up automatically when using the vsql type builder (.from_string(),
// .to_string(), etc.) with a parameterized type. Requires the parse function
// to be registered via .params<P, &parse_fn, &to_strings_fn>() in the type
// builder. See villagesql/vsql/type_builder.h for full usage.
//
// Because the cache is keyed on the C++ Params type P, if the same P is used
// by more than one custom type, each of those types MUST register the same
// parse function.
// TODO(villagesql-beta): remove this restriction.
//
// Thread-safe: a cache hit takes no lock and writes no shared memory, so
// concurrent VDF calls on many cores do not slow each other down. Even a
// shared lock writes the lock word on every lock and unlock, so every caller
// would write the same cache line. Only a miss takes a mutex. Entries are
// only evicted when the extension is uninstalled
// because type parameters are immutable per type instantiation. Also the ABI
// does not currently have a mechanism to know when a type is no longer used.
template <typename T>
class TypeParamsCache {
 public:
  using ParseFn = T (*)(const std::map<std::string, std::string> &);
  using ToStringsFn = void (*)(const T &, std::map<std::string, std::string> &);

  TypeParamsCache() = default;
  TypeParamsCache(const TypeParamsCache &) = delete;
  TypeParamsCache &operator=(const TypeParamsCache &) = delete;
  ~TypeParamsCache() {
    for (auto &head : buckets_) {
      const Node *n = head.load(std::memory_order_relaxed);
      while (n != nullptr) {
        const Node *next = n->next;
        delete n;
        n = next;
      }
    }
  }

  // Binds the parse function used by the no-arg get() overload. Called once
  // during extension initialization (vef_register), before any VDF calls.
  // Not safe to call after registration.
  void bind(ParseFn fn) { parse_fn_ = fn; }

  // Binds the inverse-of-parse function used by inference paths (e.g.
  // constant-string from_string at fix_fields time). Optional; only required
  // for parameterized types whose params are inferable at runtime.
  // Called once during extension initialization, before any VDF calls.
  void bind_to_strings(ToStringsFn fn) { to_strings_fn_ = fn; }

  // Returns true if the parse function has been bound via bind().
  bool is_bound() const { return parse_fn_ != nullptr; }

  // Returns true if a to_strings function has been bound for this type.
  bool has_to_strings() const { return to_strings_fn_ != nullptr; }

  // Invokes the bound to_strings function. has_to_strings() must be true.
  void to_strings(const T &p, std::map<std::string, std::string> &out) const {
    assert(to_strings_fn_ != nullptr);
    to_strings_fn_(p, out);
  }

  // Returns a reference to the cached T using the bound parse function.
  // bind() must have been called before this overload is used.
  const T &get(const vef_type_params_t &raw) {
    assert(parse_fn_ != nullptr);
    return get(raw, parse_fn_);
  }

  // Returns a reference to the cached T for the given params, computing it
  // via parse_fn on a cache miss. parse_fn is called as T(const
  // std::map<std::string, std::string>&).
  template <typename ParseFn>
  const T &get(const std::map<std::string, std::string> &params,
               ParseFn &&parse_fn) {
    std::string key = key_from_map(params);
    if (const T *hit = lookup(key)) return *hit;
    return insert(std::move(key), parse_fn(params));
  }

  // Overload accepting vef_type_params_t directly. Avoids constructing a
  // std::map on the hot per-row path; the map is built only on a cache miss.
  template <typename ParseFn>
  const T &get(const vef_type_params_t &raw, ParseFn &&parse_fn) {
    if (const T *hit = lookup_raw(raw)) return *hit;
    std::string key = key_from_raw(raw);
    if (const T *hit = lookup(key)) return *hit;
    return insert(std::move(key), parse_fn(map_from_raw(raw)));
  }

 private:
  ParseFn parse_fn_ = nullptr;
  ToStringsFn to_strings_fn_ = nullptr;

  // Insert-only hash table that is read without a lock. A node is fully built
  // before it is published at the head of its bucket chain, and it is never
  // changed or freed until the cache itself is destroyed (extension unload).
  // So a reader only does atomic loads and follows immutable pointers, and
  // the const T& references returned to callers are never invalidated.
  // Inserts are rare (once per distinct parameter string) and are serialized
  // by insert_mu_. The bucket count is fixed: a table that never rehashes
  // needs no reader/writer coordination. Chains only grow past one node when
  // an extension is used with more distinct parameter strings than buckets.
  // A lookup compares the key with each node in its chain, so raise
  // kBucketCount if a type is used with many hundreds of parameter strings.
  struct Node {
    Node(std::string k, T v, const Node *n)
        : key(std::move(k)), value(std::move(v)), next(n) {}
    const std::string key;
    const T value;
    const Node *const next;
  };
  static constexpr size_t kBucketCount = 64;
  std::array<std::atomic<const Node *>, kBucketCount> buckets_{};
  std::mutex insert_mu_;

  // FNV-1a over the key's bytes. The same hash can be fed piece by piece from
  // a vef_type_params_t (see lookup_raw), so the hot path finds its entry
  // without building the key string.
  static constexpr uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
  static constexpr uint64_t kFnvPrime = 1099511628211ULL;

  static uint64_t hash_add(uint64_t h, const char *s, size_t n) {
    for (size_t i = 0; i < n; ++i) {
      h ^= static_cast<unsigned char>(s[i]);
      h *= kFnvPrime;
    }
    return h;
  }

  static size_t bucket_of(const std::string &key) {
    return hash_add(kFnvOffsetBasis, key.data(), key.size()) % kBucketCount;
  }

  // Whether key is exactly "k1=v1,k2=v2,..." of raw, in raw's order.
  static bool key_equals_raw(const std::string &key,
                             const vef_type_params_t &raw) {
    const char *p = key.data();
    size_t left = key.size();
    auto take = [&](const char *s, size_t n) {
      if (left < n || std::memcmp(p, s, n) != 0) return false;
      p += n;
      left -= n;
      return true;
    };
    for (unsigned i = 0; i < raw.count; ++i) {
      if (i > 0 && !take(",", 1)) return false;
      if (!take(raw.keys[i], std::strlen(raw.keys[i]))) return false;
      if (!take("=", 1)) return false;
      if (!take(raw.values[i], std::strlen(raw.values[i]))) return false;
    }
    return left == 0;
  }

  // Lookup straight from raw, with no allocation: the per-call path of every
  // function that takes a parameterized type. Returns nullptr on a miss and
  // also when raw's keys are not in sorted order (the caller then builds the
  // canonical key, as before).
  const T *lookup_raw(const vef_type_params_t &raw) const {
    uint64_t h = kFnvOffsetBasis;
    for (unsigned i = 0; i < raw.count; ++i) {
      if (i > 0) {
        if (std::strcmp(raw.keys[i - 1], raw.keys[i]) >= 0) return nullptr;
        h = hash_add(h, ",", 1);
      }
      h = hash_add(h, raw.keys[i], std::strlen(raw.keys[i]));
      h = hash_add(h, "=", 1);
      h = hash_add(h, raw.values[i], std::strlen(raw.values[i]));
    }
    for (const Node *n =
             buckets_[h % kBucketCount].load(std::memory_order_acquire);
         n != nullptr; n = n->next) {
      if (key_equals_raw(n->key, raw)) return &n->value;
    }
    return nullptr;
  }

  const T *lookup(const std::string &key) const {
    for (const Node *n =
             buckets_[bucket_of(key)].load(std::memory_order_acquire);
         n != nullptr; n = n->next) {
      if (n->key == key) return &n->value;
    }
    return nullptr;
  }

  const T &insert(std::string key, T value) {
    std::lock_guard<std::mutex> lock(insert_mu_);
    // Re-check: another thread may have inserted while we waited for the lock.
    if (const T *hit = lookup(key)) return *hit;
    std::atomic<const Node *> &head = buckets_[bucket_of(key)];
    const Node *node = new Node(std::move(key), std::move(value),
                                head.load(std::memory_order_relaxed));
    head.store(node, std::memory_order_release);
    return node->value;
  }

  static std::string key_from_map(
      const std::map<std::string, std::string> &params) {
    // One ',' between each pair of entries.
    size_t size = params.empty() ? 0 : params.size() - 1;
    // Each entry contributes "key=value".
    for (const auto &[k, v] : params) size += k.size() + 1 + v.size();
    std::string key;
    key.reserve(size);
    for (const auto &[k, v] : params) {
      if (!key.empty()) key += ',';
      key += k;
      key += '=';
      key += v;
    }
    return key;
  }

  static std::string key_from_raw(const vef_type_params_t &raw) {
    // Fast path: build the key in one pass while verifying params are sorted.
    // The server normally delivers params in sorted order (matching the
    // std::map ordering used by key_from_map), so this is the common case.
    std::string key;
    // Reserve the full key so the appends below never reallocate.
    key.reserve(raw_key_size(raw));
    for (unsigned i = 0; i < raw.count; ++i) {
      if (i > 0) {
        if (std::strcmp(raw.keys[i - 1], raw.keys[i]) >= 0) {
          return key_from_raw_unsorted(raw);
        }
        key += ',';
      }
      key += raw.keys[i];
      key += '=';
      key += raw.values[i];
    }
    return key;
  }

  // Length of the "k1=v1,k2=v2,..." key built from raw, for reserve().
  static size_t raw_key_size(const vef_type_params_t &raw) {
    // One ',' between each pair of entries.
    size_t size = raw.count == 0 ? 0 : raw.count - 1;
    for (unsigned i = 0; i < raw.count; ++i) {
      // Each entry contributes "key=value".
      size += std::strlen(raw.keys[i]) + 1 + std::strlen(raw.values[i]);
    }
    return size;
  }

  static std::string key_from_raw_unsorted(const vef_type_params_t &raw) {
    std::vector<unsigned> idx(raw.count);
    std::iota(idx.begin(), idx.end(), 0);
    std::sort(idx.begin(), idx.end(), [&](unsigned a, unsigned b) {
      return std::strcmp(raw.keys[a], raw.keys[b]) < 0;
    });
    std::string key;
    // Same length as the sorted key; only the entry order differs.
    key.reserve(raw_key_size(raw));
    for (unsigned j : idx) {
      if (!key.empty()) key += ',';
      key += raw.keys[j];
      key += '=';
      key += raw.values[j];
    }
    return key;
  }

  static std::map<std::string, std::string> map_from_raw(
      const vef_type_params_t &raw) {
    std::map<std::string, std::string> m;
    for (unsigned i = 0; i < raw.count; ++i) {
      m.emplace(raw.keys[i], raw.values[i]);
    }
    return m;
  }
};

// Returns the single shared TypeParamsCache<T> for this .so extension
// instance.
//
// Marking this hidden ensures each extension shared object (or DLL) gets its
// own instance (same as the visibility("hidden") on static methods in
// wrappers). Using a single free function template means all callers within a
// DSO — CustomArgWith<T>, CustomResultWith<T>, and the cache-aware VDF
// wrappers — share one cache rather than each holding their own.
template <typename T>
__attribute__((visibility("hidden"))) inline TypeParamsCache<T> &
type_params_cache_for() {
  static TypeParamsCache<T> instance;
  return instance;
}

// Returns true if the TypeParamsCache<T> for this extension has had its parse
// function bound. Used at registration time to detect missing .params<>()
// calls before any VDF is invoked.
template <typename T>
__attribute__((visibility("hidden"))) inline bool is_params_cache_bound() {
  return type_params_cache_for<T>().is_bound();
}

// Serializes a params map into a caller-supplied buffer, following the
// vef_inferred_type_params_t contract: the canonical "k=v,k=v" form is written
// if it fits; actual_len is always set to the length that *would* have been
// written; and overflow is set when it did not fit, in which case the buffer
// contents are undefined and the caller should retry with a larger one.
inline void write_params_to(vef_inferred_type_params_t *out,
                            const std::map<std::string, std::string> &m) {
  char *const buf_begin = out->buf;
  const size_t cap = out->max_buf_len;
  char *p = buf_begin;
  size_t needed = 0;
  bool ok = true;
  bool first = true;
  for (const auto &[k, v] : m) {
    const size_t pair_size = (first ? 0u : 1u) + k.size() + 1u + v.size();
    if (ok && static_cast<size_t>(p - buf_begin) + pair_size <= cap) {
      if (!first) *p++ = ',';
      std::memcpy(p, k.data(), k.size());
      p += k.size();
      *p++ = '=';
      std::memcpy(p, v.data(), v.size());
      p += v.size();
    } else {
      ok = false;
    }
    needed += pair_size;
    first = false;
  }
  out->actual_len = needed;
  out->overflow = !ok;
}

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_TYPE_PARAMS_H
