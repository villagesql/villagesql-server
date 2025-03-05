/*
 * Copyright (c) 2024, 2025, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is designed to work with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have either included with
 * the program or referenced in the documentation.
 *
 * This program is distributed in the hope that it will be useful,  but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See
 * the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

#ifndef ROUTER_SRC_JIT_EXECUTOR_INCLUDE_MYSQLROUTER_JIT_EXECUTOR_CONTEXT_POOL_H_
#define ROUTER_SRC_JIT_EXECUTOR_INCLUDE_MYSQLROUTER_JIT_EXECUTOR_CONTEXT_POOL_H_

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "mysqlrouter/jit_executor_context.h"
#include "mysqlrouter/jit_executor_context_handle.h"
#include "mysqlrouter/jit_executor_value.h"
#include "mysqlrouter/polyglot_file_system.h"

namespace jit_executor {

/**
 * Generic implementation of a pool
 */
template <class T>
class Pool {
 public:
  explicit Pool(size_t size, const std::function<T()> &factory,
                const std::function<void(T)> &destructor = {})
      : m_pool_size{size},
        m_item_factory{factory},
        m_item_destructor{destructor} {}

  T get() {
    {
      std::unique_lock lock(m_mutex);

      if (m_teardown) return {};

      T item = {};

      // Contention mode is turned ON when a memory error happened, for that
      // reason creating new contexts is a no-go so we should wait for a context
      // from the pool to get released
      if (m_contention_mode && m_items.empty()) {
        m_item_availability.wait(
            lock, [this]() { return m_active_items == 0 || !m_items.empty(); });

        if (m_active_items == 0) {
          throw std::runtime_error(
              "All the contexts on the pool have been released.");
        }
      }

      if (!m_items.empty()) {
        // Pop a resource from the pool
        item = m_items.front();
        m_items.pop_front();
        return item;
      }
    }

    m_active_items++;
    return m_item_factory();
  }

  void release(T ctx) {
    {
      std::scoped_lock lock(m_mutex);

      if (!m_teardown && m_items.size() < m_pool_size) {
        m_items.push_back(ctx);
        m_item_availability.notify_one();
        return;
      }
    }

    m_active_items--;
    m_teared_down.notify_one();
    m_item_availability.notify_one();
    if (m_item_destructor) {
      m_item_destructor(ctx);
    }
  }

  void teardown() {
    {
      std::scoped_lock lock(m_mutex);
      m_teardown = true;
    }

    while (!m_items.empty()) {
      auto item = m_items.front();
      m_items.pop_front();

      m_active_items--;
      if (m_item_destructor) {
        m_item_destructor(item);
      }
    }

    // Waits until all the contexts created by the pool get released
    std::unique_lock<std::mutex> lock(m_mutex);
    m_teared_down.wait(lock, [this]() { return m_active_items == 0; });
  }

  size_t active_items() const {
    std::scoped_lock lock(m_mutex);
    return m_active_items;
  }

  /**
   * Discards the affected context and turns ON contention mode for the pool
   */
  void on_memory_error(T ctx) {
    std::scoped_lock lock(m_mutex);
    m_contention_mode = true;

    m_active_items--;
    m_teared_down.notify_one();
    m_item_availability.notify_one();
    if (m_item_destructor) {
      m_item_destructor(ctx);
    }
  }

 private:
  std::mutex m_mutex;
  std::condition_variable m_teared_down;
  std::condition_variable m_item_availability;
  bool m_teardown = false;
  size_t m_pool_size;
  std::deque<T> m_items;
  std::function<T()> m_item_factory;
  std::function<void(T)> m_item_destructor;
  size_t m_active_items = 0;
  bool m_contention_mode = false;
};

class PooledContextHandle;
class CommonContext;
class ContextPool final {
 public:
  ContextPool(size_t size, CommonContext *common_context);

  ~ContextPool();

  std::shared_ptr<PooledContextHandle> get_context();
  void release(IContext *ctx);
  void teardown() { m_pool->teardown(); }

 private:
  CommonContext *m_common_context;
  std::unique_ptr<Pool<IContext *>> m_pool;
};

/**
 * A wrapper that will return a context to the pool as soon as it is released
 */
class PooledContextHandle : public IContextHandle {
 public:
  PooledContextHandle(ContextPool *pool, IContext *ctx)
      : m_pool{pool}, m_context{ctx} {}
  ~PooledContextHandle() override { m_pool->release(m_context); }

  IContext *get() override { return m_context; }

 private:
  ContextPool *m_pool;
  IContext *m_context;
};

}  // namespace jit_executor

#endif  // ROUTER_SRC_JIT_EXECUTOR_INCLUDE_MYSQLROUTER_JIT_EXECUTOR_CONTEXT_POOL_H_