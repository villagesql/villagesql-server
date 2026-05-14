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

#ifndef VILLAGESQL_PREVIEW_SQL_QUERY_H
#define VILLAGESQL_PREVIEW_SQL_QUERY_H

#include <cstdlib>
#include <string_view>

#include <villagesql/abi/preview/sql_query.h>
#include <villagesql/abi/preview/thread_worker.h>
#include <villagesql/vsql/capability_traits.h>

namespace vsql::preview_sql_query {

// Forward declaration.
class Session;

// SqlQueryCapability is the extension-side wrapper for the
// "vsql::preview::sql_query" preview capability. Declare one static instance
// and register it with .with(g_sql_query_cap).
//
// After loading, use g_sql_query_cap.open(handle) to run SQL queries from a
// background thread.
//
// Usage:
//   static vsql::preview_sql_query::SqlQueryCapability g_sql_query_cap;
//
//   static vef_next_wakeup_t worker(vef_wakeup_reason_t reason,
//                                   vef_thread_handle_t *handle, void *) {
//     auto session = g_sql_query_cap.open(handle);
//     if (!session) return {};
//
//     // Option A: for_each — runs fn once per row.
//     session.sql("SELECT 1").for_each([](const auto &r) { ... });
//
//     // Option B: execute + next — iterate manually.
//     auto result = session.sql("SELECT id, name FROM t").execute();
//     while (result.next()) {
//       auto id   = result.column_int(0);
//       auto name = result.column_str(1);
//     }
//     return {};
//   }
//
//   VEF_GENERATE_ENTRY_POINTS(
//       make_extension()
//           .with(g_worker_cap)
//           .with(g_sql_query_cap))
class SqlQueryCapability {
 public:
  // Open a SQL session bound to the given background thread handle.
  // Returns an invalid Session (operator bool == false) on failure.
  Session open(vef_thread_handle_t *handle) const;

  // VEF writes this during registration. Public for the registration
  // glue; do not access from extension code.
  const vef_preview_sql_query_t *abi = nullptr;
};

}  // namespace vsql::preview_sql_query

namespace vsql::detail {

template <>
struct CapabilityTraits<::vsql::preview_sql_query::SqlQueryCapability> {
  static constexpr const char *kName = VEF_PREVIEW_SQL_QUERY_NAME;
  static constexpr uint32_t kAbiVersion = VEF_PREVIEW_SQL_QUERY_ABI_VERSION;
  using AbiType = vef_preview_sql_query_t;

  static constexpr void *vtable_destination(
      ::vsql::preview_sql_query::SqlQueryCapability *p) noexcept {
    return static_cast<void *>(&p->abi);
  }
};

}  // namespace vsql::detail

namespace vsql::preview_sql_query {

// Forward declaration.
class SqlQuery;

// RAII wrapper around vef_sql_session_t.
// Obtain via cap.open(handle); check with operator bool before use.
class Session {
 public:
  static Session open(const SqlQueryCapability &cap,
                      vef_thread_handle_t *handle) {
    vef_sql_session_t *s = nullptr;
    if (cap.abi != nullptr && cap.abi->open_session != nullptr)
      s = cap.abi->open_session(handle);
    return Session{cap, s};
  }

  ~Session() {
    if (handle_ != nullptr && cap_.abi != nullptr &&
        cap_.abi->close_session != nullptr)
      cap_.abi->close_session(handle_);
  }

  Session(const Session &) = delete;
  Session &operator=(const Session &) = delete;

  Session(Session &&other) noexcept : cap_(other.cap_), handle_(other.handle_) {
    other.handle_ = nullptr;
  }

  explicit operator bool() const { return handle_ != nullptr; }

  SqlQuery sql(std::string_view query) const;

  vef_sql_session_t *handle() const { return handle_; }
  const SqlQueryCapability &cap() const { return cap_; }

 private:
  Session(const SqlQueryCapability &cap, vef_sql_session_t *h)
      : cap_(cap), handle_(h) {}
  const SqlQueryCapability &cap_;
  vef_sql_session_t *handle_{nullptr};
};

inline Session SqlQueryCapability::open(vef_thread_handle_t *handle) const {
  return Session::open(*this, handle);
}

// Owns a buffered query result. Rows are fetched one at a time via next().
// Column values from column_str() are valid only until the next next() call or
// until Result goes out of scope — copy them if a longer lifetime is needed.
class Result {
 public:
  Result(const SqlQueryCapability &cap, vef_sql_result_t *handle)
      : cap_(cap), handle_(handle) {}

  ~Result() {
    if (handle_ != nullptr && cap_.abi != nullptr &&
        cap_.abi->close_result != nullptr)
      cap_.abi->close_result(handle_);
  }

  Result(const Result &) = delete;
  Result &operator=(const Result &) = delete;

  Result(Result &&other) noexcept
      : cap_(other.cap_),
        handle_(other.handle_),
        row_(other.row_),
        lengths_(other.lengths_) {
    other.handle_ = nullptr;
  }

  Result &operator=(Result &&other) noexcept {
    if (this != &other) {
      if (handle_ != nullptr && cap_.abi != nullptr &&
          cap_.abi->close_result != nullptr)
        cap_.abi->close_result(handle_);
      handle_ = other.handle_;
      row_ = other.row_;
      lengths_ = other.lengths_;
      other.handle_ = nullptr;
    }
    return *this;
  }

  // Fetch the next row. Returns true if a row was fetched.
  bool next() {
    if (handle_ == nullptr || cap_.abi == nullptr ||
        cap_.abi->fetch_row == nullptr)
      return false;
    return cap_.abi->fetch_row(handle_, &row_, &lengths_);
  }

  // Returns true if the result handle is valid (execute succeeded).
  explicit operator bool() const { return handle_ != nullptr; }

  // Number of columns in the result set.
  unsigned int num_columns() const {
    if (handle_ == nullptr || cap_.abi == nullptr ||
        cap_.abi->num_columns == nullptr)
      return 0;
    return cap_.abi->num_columns(handle_);
  }

  // Column value as a string_view. Valid until the next next() call.
  // Returns a null string_view (data() == nullptr) for SQL NULL.
  std::string_view column_str(unsigned int i) const {
    if (row_ == nullptr || row_[i] == nullptr) return {};
    return {row_[i], lengths_[i]};
  }

  // Column value as a long long. Returns 0 for NULL.
  long long column_int(unsigned int i) const {
    if (row_ == nullptr || row_[i] == nullptr) return 0;
    return std::strtoll(row_[i], nullptr, 10);
  }

  // Column value as a double. Returns 0.0 for NULL.
  double column_real(unsigned int i) const {
    if (row_ == nullptr || row_[i] == nullptr) return 0.0;
    return std::strtod(row_[i], nullptr);
  }

 private:
  const SqlQueryCapability &cap_;
  vef_sql_result_t *handle_{nullptr};
  const char **row_{nullptr};
  const unsigned long *lengths_{nullptr};
};

// A SQL query bound to a session. Obtained via session.sql("...").
// Not intended to be stored — construct and use inline.
class SqlQuery {
 public:
  SqlQuery(const Session &session, std::string_view sql)
      : session_(session), sql_(sql) {}

  // Execute the query. Returns an invalid Result (operator bool == false)
  // on error.
  Result execute() const {
    if (session_.cap().abi == nullptr || session_.cap().abi->execute == nullptr)
      return Result{session_.cap(), nullptr};
    vef_sql_result_t *h = session_.cap().abi->execute(session_.handle(),
                                                      sql_.data(), sql_.size());
    return Result{session_.cap(), h};
  }

  // Execute the query and invoke fn once per row.
  template <typename F>
  void for_each(F &&fn) const {
    Result result = execute();
    while (result.next()) fn(result);
  }

 private:
  const Session &session_;
  std::string_view sql_;
};

inline SqlQuery Session::sql(std::string_view query) const {
  return SqlQuery{*this, query};
}

}  // namespace vsql::preview_sql_query

#endif  // VILLAGESQL_PREVIEW_SQL_QUERY_H
