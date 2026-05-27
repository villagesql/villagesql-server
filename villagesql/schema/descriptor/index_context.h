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

// IndexContext: resolved custom index type + parameters for a specific index
// instantiation. Analogous to TypeContext for custom data types.
//
// IndexContext objects are cached in VictionaryClient::m_index_contexts and
// stored as const pointers on KEY::custom_index_context. They are owned by
// the VictionaryClient via shared_ptr, with lifetime scoped to the TABLE_SHARE
// via MEM_ROOT cleanup registration.
//
// A new IndexContextKey is formed from the IndexTypeDescriptorKey plus the
// TypeParameters parsed from the index_type_parameters JSON in IndexEntry.
// Different index instantiations with the same type and parameters share one
// IndexContext.

#ifndef VILLAGESQL_SCHEMA_DESCRIPTOR_INDEX_CONTEXT_H_
#define VILLAGESQL_SCHEMA_DESCRIPTOR_INDEX_CONTEXT_H_

#include <memory>

#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/descriptor/type_context.h"  // TypeParameters

namespace villagesql {

template <typename EntryType>
struct TableTraits;

// Key for IndexContext entries in the VictionaryClient map.
// Combines the IndexTypeDescriptorKey with TypeParameters.
struct IndexContextKey {
 public:
  IndexContextKey() = default;

  IndexContextKey(IndexTypeDescriptorKey desc_key, TypeParameters params)
      : descriptor_key_(std::move(desc_key)), parameters_(std::move(params)) {
    if (parameters_.empty()) {
      normalized_key_ = descriptor_key_.str();
    } else {
      normalized_key_ = descriptor_key_.str() + "." + parameters_.str();
    }
  }

  const std::string &str() const { return normalized_key_; }
  const IndexTypeDescriptorKey &descriptor_key() const {
    return descriptor_key_;
  }
  const TypeParameters &parameters() const { return parameters_; }

  bool operator<(const IndexContextKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const IndexContextKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  IndexTypeDescriptorKey descriptor_key_;
  TypeParameters parameters_;
  std::string normalized_key_;
};

// IndexContext: immutable, cached representation of a custom index
// instantiation. Holds the IndexTypeDescriptor (function pointers registered
// by the extension) and the TypeParameters parsed from index_type_parameters.
//
// Lifetime: shared_ptr inside VictionaryClient; TABLE_SHARE mem_root cleanup
// holds a reference for the duration of the share.
class IndexContext {
 public:
  using key_type = IndexContextKey;

  IndexContext() = delete;
  IndexContext(const IndexContext &) = delete;
  IndexContext &operator=(const IndexContext &) = delete;
  IndexContext(IndexContext &&) = default;
  IndexContext &operator=(IndexContext &&) = delete;
  ~IndexContext() = default;

  const IndexContextKey &key() const { return key_; }
  const TypeParameters &parameters() const { return key_.parameters(); }
  const IndexTypeDescriptor *descriptor() const { return descriptor_; }

  const std::string &extension_name() const {
    return descriptor_->extension_name();
  }
  const std::string &extension_version() const {
    return descriptor_->extension_version();
  }
  const std::string &index_type_name() const {
    return descriptor_->index_type_name();
  }

 private:
  friend struct TableTraits<IndexContext>;

  IndexContext(const IndexContextKey &key,
               const IndexTypeDescriptor *descriptor)
      : descriptor_(descriptor), key_(key) {}

  const IndexTypeDescriptor *descriptor_{nullptr};
  IndexContextKey key_;
};

// TableTraits for IndexContext: MEMORY_ONLY, created on demand by
// acquire_or_create().
template <>
struct TableTraits<IndexContext> {
  static std::shared_ptr<IndexContext> create(
      const IndexContextKey &key, const IndexTypeDescriptor *descriptor) {
    if (!descriptor) return {};
    return std::shared_ptr<IndexContext>(new IndexContext(key, descriptor));
  }
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_DESCRIPTOR_INDEX_CONTEXT_H_
