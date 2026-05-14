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

#ifndef VILLAGESQL_VSQL_TYPE_PARAMS_CACHE_H
#define VILLAGESQL_VSQL_TYPE_PARAMS_CACHE_H

#include <algorithm>
#include <cassert>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <villagesql/abi/types.h>

namespace vsql {

// Memoizes parsed type parameters to avoid re-parsing strings on every VDF
// call. There is one cache instance per Type Parameter C++ type. The cache is
// wired up automatically when using make_type_encode (and the other
// make_type_* entry points) with a const P& first parameter.  Requires the
// parse function to be registered via .params<P, &parse_fn>() in the type
// builder. See PARAMETERIZED TYPES in extension.h for full usage.
//
// Thread-safe: concurrent readers do not block each other. Entries are only
// evicted when the extension is uninstalled because type parameters are
// immutable per type instantiation. Also the ABI does not currently have
// a mechanism to know when a type is no longer used.
template <typename T>
class TypeParamsCache {
 public:
  using ParseFn = T (*)(const std::map<std::string, std::string> &);
  using ToStringsFn = void (*)(const T &, std::map<std::string, std::string> &);

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
    std::string key = key_from_raw(raw);
    if (const T *hit = lookup(key)) return *hit;
    return insert(std::move(key), parse_fn(map_from_raw(raw)));
  }

 private:
  ParseFn parse_fn_ = nullptr;
  ToStringsFn to_strings_fn_ = nullptr;
  mutable std::shared_mutex mu_;
  // Values are heap-allocated so their addresses remain stable across rehashes.
  // A rehash moves the unordered_map buckets, not the pointed-to T objects,
  // so const T& references returned to callers are never invalidated.
  std::unordered_map<std::string, std::unique_ptr<T>> cache_;

  const T *lookup(const std::string &key) const {
    std::shared_lock lock(mu_);
    auto it = cache_.find(key);
    if (it != cache_.end()) return it->second.get();
    return nullptr;
  }

  const T &insert(std::string key, T value) {
    std::unique_lock lock(mu_);
    // Re-check: another thread may have inserted while we waited for the lock.
    auto [it, inserted] =
        cache_.emplace(std::move(key), std::make_unique<T>(std::move(value)));
    return *it->second;
  }

  static std::string key_from_map(
      const std::map<std::string, std::string> &params) {
    std::string key;
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

  static std::string key_from_raw_unsorted(const vef_type_params_t &raw) {
    std::vector<unsigned> idx(raw.count);
    std::iota(idx.begin(), idx.end(), 0);
    std::sort(idx.begin(), idx.end(), [&](unsigned a, unsigned b) {
      return std::strcmp(raw.keys[a], raw.keys[b]) < 0;
    });
    std::string key;
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

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_TYPE_PARAMS_CACHE_H
