/*
  Copyright (c) 2024, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#include "mrs/rest/response_cache.h"

#include <cassert>

#include "helper/json/rapid_json_to_struct.h"
#include "helper/json/text_to.h"
#include "mysql/harness/logging/logging.h"

IMPORT_LOG_FUNCTIONS()

namespace mrs {

namespace {
std::string make_table_key(const http::base::Uri &uri,
                           const std::string &user_id) {
  return uri.join_path() + (user_id.empty() ? "" : "\nuser_id=" + user_id);
}

std::string make_routine_key(const http::base::Uri &uri,
                             std::string_view req_body) {
  return std::string(uri.join_path()).append("\n").append(req_body);
}

std::string make_file_key(const mrs::database::entry::UniversalId &id) {
  return std::string(id.to_raw(), id.k_size);
}

class ResponseCacheOptions {
 public:
  std::optional<uint64_t> max_cache_size{};
};

class ParseResponseCacheOptions
    : public helper::json::RapidReaderHandlerToStruct<ResponseCacheOptions> {
 public:
  explicit ParseResponseCacheOptions(const std::string &group_key)
      : group_key_(group_key) {}

  template <typename ValueType>
  uint64_t to_uint(const ValueType &value) {
    return std::stoull(value.c_str());
  }

  template <typename ValueType>
  void handle_object_value(const std::string &key, const ValueType &vt) {
    if (key == group_key_ + ".maxCacheSize") {
      result_.max_cache_size = to_uint(vt);
    }
  }

  template <typename ValueType>
  void handle_value(const ValueType &vt) {
    const auto &key = get_current_key();
    if (is_object_path()) {
      handle_object_value(key, vt);
    }
  }

  bool RawNumber(const Ch *v, rapidjson::SizeType v_len, bool) override {
    handle_value(std::string{v, v_len});
    return true;
  }

  std::string group_key_;
};

auto parse_json_options(const std::string &config_key,
                        const std::string &options) {
  return helper::json::text_to_handler<ParseResponseCacheOptions>(options,
                                                                  config_key);
}

}  // namespace

std::shared_ptr<EndpointResponseCache> ResponseCache::create_endpoint_cache(
    int64_t ttl_ms) {
  auto cache = std::make_shared<EndpointResponseCache>(this, ttl_ms);

  return cache;
}

void ResponseCache::configure(const std::string &options) {
  log_debug("%s", __FUNCTION__);
  auto cache_options = parse_json_options(config_key_, options);

  max_size_ =
      cache_options.max_cache_size.value_or(k_default_object_cache_size);

  if (cache_size_ > max_size_) {
    std::lock_guard<std::mutex> lock(entries_mutex_);
    shrink_object_cache();
  }
}

void ResponseCache::shrink_object_cache(size_t extra_size) {
  log_debug("%s (%zu + %zu -> %zu)", __FUNCTION__, cache_size_.load(),
            extra_size, max_size_.load());

  while (oldest_entry_ && cache_size_ + extra_size > max_size_) {
    oldest_entry_->owner->remove_entry(oldest_entry_);
    remove_nolock(oldest_entry_);
  }
}

void ResponseCache::push(std::shared_ptr<CacheEntry> entry) {
  log_debug("%s", __FUNCTION__);
  size_t size = entry->data.size();

  std::lock_guard<std::mutex> lock(entries_mutex_);
  if (cache_size_ + size > max_size_) {
    shrink_object_cache(size);
  }

  cache_size_ += size;

  entry->next_ptr = newest_entry_;
  if (newest_entry_) newest_entry_->prev_ptr = entry;
  newest_entry_ = entry;
  if (!oldest_entry_) oldest_entry_ = newest_entry_;
}

void ResponseCache::remove(std::shared_ptr<CacheEntry> entry) {
  log_debug("%s", __FUNCTION__);
  std::lock_guard<std::mutex> lock(entries_mutex_);

  remove_nolock(entry);
}

void ResponseCache::remove_nolock(std::shared_ptr<CacheEntry> entry) {
  cache_size_ -= entry->data.size();

  if (entry->prev_ptr)
    entry->prev_ptr->next_ptr = entry->next_ptr;
  else
    newest_entry_ = entry->next_ptr;

  if (entry->next_ptr)
    entry->next_ptr->prev_ptr = entry->prev_ptr;
  else
    oldest_entry_ = entry->prev_ptr;
}

void ResponseCache::remove_all(EndpointResponseCache *cache) {
  log_debug("%s", __FUNCTION__);
  std::lock_guard<std::mutex> lock(entries_mutex_);

  if (!newest_entry_) return;

  std::shared_ptr<CacheEntry> new_start = nullptr;
  std::shared_ptr<CacheEntry> new_end = nullptr;

  std::shared_ptr<CacheEntry> next;
  for (auto ptr = newest_entry_; ptr; ptr = next) {
    next = ptr->next_ptr;

    if (ptr->owner == cache) {
      cache_size_ -= ptr->data.size();
      ptr->next_ptr = nullptr;
      ptr->prev_ptr = nullptr;
    } else {
      if (!new_start) {
        new_start = ptr;
        new_end = ptr;
        ptr->next_ptr = nullptr;
        ptr->prev_ptr = nullptr;
      } else {
        new_end->next_ptr = ptr;
        ptr->next_ptr = nullptr;
        ptr->prev_ptr = new_end;
        new_end = ptr;
      }
    }
  }

  newest_entry_ = new_start;
  oldest_entry_ = new_end;
}

EndpointResponseCache::EndpointResponseCache(ResponseCache *owner,
                                             int64_t ttl_ms)
    : owner_(owner), ttl_(ttl_ms) {}

EndpointResponseCache::~EndpointResponseCache() {
  log_debug("%s", __FUNCTION__);
  std::unique_lock lock(cache_mutex_);

  owner_->remove_all(this);
}

std::shared_ptr<CacheEntry> EndpointResponseCache::create_table_entry(
    const Uri &uri, const std::string &user_id, const std::string &data,
    int64_t items) {
  return create_entry(make_table_key(uri, user_id), data, items);
}

std::shared_ptr<CacheEntry> EndpointResponseCache::create_routine_entry(
    const Uri &uri, std::string_view req_body, const std::string &data,
    int64_t items, std::optional<helper::MediaType> media_type) {
  return create_entry(make_routine_key(uri, req_body), data, items, media_type);
}

std::shared_ptr<CacheEntry> EndpointResponseCache::create_routine_entry(
    const Uri &uri, std::string_view req_body, const std::string &data,
    int64_t items, const std::string &media_type_str) {
  return create_entry(make_routine_key(uri, req_body), data, items, {},
                      media_type_str);
}

std::shared_ptr<CacheEntry> EndpointResponseCache::create_file_entry(
    const UniversalId &id, const std::string &data,
    helper::MediaType media_type) {
  return create_entry(make_file_key(id), data, 0, media_type);
}

std::shared_ptr<CacheEntry> EndpointResponseCache::create_entry(
    const std::string &key, const std::string &data, int64_t items,
    std::optional<helper::MediaType> media_type,
    std::optional<std::string> media_type_str) {
  log_debug("%s key=%s", __FUNCTION__, key.c_str());

  if (owner_->max_cache_size() < data.size()) {
    log_debug("%s key=%s data=%zu max_cache=%zu", __FUNCTION__, key.c_str(),
              data.size(), owner_->max_cache_size());
    return nullptr;
  }

  auto entry = std::make_shared<CacheEntry>();

  entry->data = data;
  entry->items = items;
  entry->media_type = media_type;
  entry->media_type_str = media_type_str;

  entry->owner = this;
  entry->key = key;
  entry->time_created = CacheEntry::TimeType::clock::now();

  owner_->push(entry);
  {
    std::unique_lock lock(cache_mutex_);

    cache_.emplace(entry->key, entry);
  }

  return entry;
}

void EndpointResponseCache::remove_entry(std::shared_ptr<CacheEntry> entry) {
  log_debug("%s key=%s", __FUNCTION__, entry->key.c_str());
  {
    std::unique_lock lock(cache_mutex_);

    cache_.erase(entry->key);
  }
}

std::shared_ptr<CacheEntry> EndpointResponseCache::lookup_table(
    const Uri &uri, const std::string &user_id) {
  return lookup(make_table_key(uri, user_id));
}

std::shared_ptr<CacheEntry> EndpointResponseCache::lookup_routine(
    const Uri &uri, std::string_view req_body) {
  return lookup(make_routine_key(uri, req_body));
}

std::shared_ptr<CacheEntry> EndpointResponseCache::lookup_file(
    const UniversalId &id) {
  return lookup(make_file_key(id));
}

std::shared_ptr<CacheEntry> EndpointResponseCache::lookup(
    const std::string &key) {
  std::shared_lock lock(cache_mutex_);

  auto it = cache_.find(key);
  if (it != cache_.end()) {
    if (ttl_ > 0) {
      auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(
          CacheEntry::TimeType::clock::now() - it->second->time_created);

      if (diff.count() >= ttl_) {
        owner_->remove(it->second);
        cache_.erase(it);

        log_debug("%s key=%s -> expired", __FUNCTION__, key.c_str());
        return {};
      }
    }
    log_debug("%s key=%s -> hit", __FUNCTION__, key.c_str());
    return it->second;
  }

  log_debug("%s key=%s miss", __FUNCTION__, key.c_str());
  return {};
}

}  // namespace mrs
