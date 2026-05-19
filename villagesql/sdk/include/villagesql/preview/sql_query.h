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
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

namespace vsql::preview_sql_query {

// Forward declaration.
class Result;
class Session;
class SqlQuery;

// SqlQueryCapability is the extension-side wrapper for the
// "vsql::preview::sql_query" preview capability. Declare one static instance
// and register it with .with(g_sql_query_cap).
//
// After loading, call g_sql_query_cap.open(handle) from a thread-worker
// callback using that callback's vef_thread_handle_t*. Opening a SQL session
// requires the thread-worker handle because it supplies the worker THD/session
// context; open() is not valid from VDFs or arbitrary extension-created
// threads. The capability wrapper is populated during extension registration
// and then read-only; each valid open() call returns an independent Session.
//
// Usage:
//   static vsql::preview_sql_query::SqlQueryCapability g_sql_query_cap;
//
//   static vef_next_wakeup_t worker(vef_wakeup_reason_t reason,
//                                   vef_thread_handle_t *handle, void *) {
//     auto session = g_sql_query_cap.open(handle);
//     if (!session) return {};
//
//     // Option A: for_each — runs fn once per row. Returned Result holds
//     // no buffered rows; use it for diagnostics only.
//     auto status = session.sql("SELECT 1").for_each(
//         [](const auto &row) { /* ... */ });
//     if (status.has_error()) { /* status.error().message */ }
//     for (unsigned i = 0; i < status.warning_count(); ++i) {
//       auto w = status.warning(i);  // w.errno_, w.severity, w.message
//     }
//
//     // Option B: execute + next — iterate manually.
//     auto result = session.sql("SELECT id, name FROM t").execute();
//     if (result.has_error()) { /* result.error().message */ return {}; }
//     while (result.next()) {
//       auto id   = result.column_int(0);
//       auto name = result.column_str(1);
//     }
//     for (unsigned i = 0; i < result.warning_count(); ++i) {
//       auto w = result.warning(i);
//     }
//     return {};
//   }
//
//   VEF_GENERATE_ENTRY_POINTS(
//       make_extension()
//           .with(g_worker_cap)
//           .with(g_sql_query_cap))
class SqlQueryCapability
    : public ::vsql::detail::CapabilityBase<SqlQueryCapability> {
 public:
  // Open a SQL session bound to the given background thread handle.
  // Returns an invalid Session (operator bool == false) on failure.
  Session open(vef_thread_handle_t *handle) const;

 private:
  template <typename Capability>
  friend struct ::vsql::detail::CapabilityTraits;
  friend class Result;
  friend class Session;
  friend class SqlQuery;

  const vef_preview_sql_query_t *abi_ = nullptr;
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
    return static_cast<void *>(&p->abi_);
  }
};

}  // namespace vsql::detail

namespace vsql::preview_sql_query {

// RAII wrapper around vef_sql_session_t.
// Obtain via cap.open(handle); check with operator bool before use.
class Session {
 public:
  static Session open(const SqlQueryCapability &cap,
                      vef_thread_handle_t *handle) {
    vef_sql_session_t *s = nullptr;
    if (cap.abi_ != nullptr && cap.abi_->open_session != nullptr)
      s = cap.abi_->open_session(handle);
    return Session{cap, s};
  }

  ~Session() {
    if (handle_ != nullptr && cap_.abi_ != nullptr &&
        cap_.abi_->close_session != nullptr)
      cap_.abi_->close_session(handle_);
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

// One diagnostic (statement error or warning/note) returned from a query.
// The server copies diagnostic strings into the result handle; the string_view
// members stay valid until the parent Result is destroyed. Copy them if a
// longer lifetime is needed.
struct Diag {
  uint32_t errno_{0};
  vef_sql_diag_severity_t severity{VEF_SQL_DIAG_ERROR};
  std::string_view sqlstate;
  std::string_view message;
};

// Owns a query result handle. Returned from both execute() (buffered rows
// + diagnostics) and for_each() (diagnostics only — rows are delivered via
// the callback). For the buffered case, rows are fetched one at a time via
// next(), and column values from column_str() are valid only until the next
// next() call or until Result goes out of scope — copy them if a longer
// lifetime is needed.
class Result {
 public:
  Result(const SqlQueryCapability &cap, vef_sql_result_t *handle)
      : cap_(cap), handle_(handle) {}

  ~Result() {
    if (handle_ != nullptr && cap_.abi_ != nullptr &&
        cap_.abi_->close_result != nullptr)
      cap_.abi_->close_result(handle_);
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
      if (handle_ != nullptr && cap_.abi_ != nullptr &&
          cap_.abi_->close_result != nullptr)
        cap_.abi_->close_result(handle_);
      handle_ = other.handle_;
      row_ = other.row_;
      lengths_ = other.lengths_;
      other.handle_ = nullptr;
    }
    return *this;
  }

  // Fetch the next row. Returns true if a row was fetched.
  bool next() {
    if (handle_ == nullptr || cap_.abi_ == nullptr ||
        cap_.abi_->fetch_row == nullptr)
      return false;
    return cap_.abi_->fetch_row(handle_, &row_, &lengths_);
  }

  // Returns true if the result handle is valid (the query reached the server
  // and was scheduled — not whether it succeeded). Use has_error() to check
  // whether the statement itself failed.
  explicit operator bool() const { return handle_ != nullptr; }

  // Number of columns in the result set.
  unsigned int num_columns() const {
    if (handle_ == nullptr || cap_.abi_ == nullptr ||
        cap_.abi_->num_columns == nullptr)
      return 0;
    return cap_.abi_->num_columns(handle_);
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

  // True if the statement produced an error.
  bool has_error() const {
    if (handle_ == nullptr || cap_.abi_ == nullptr ||
        cap_.abi_->has_error == nullptr)
      return false;
    return cap_.abi_->has_error(handle_);
  }

  // Statement error, if any. Returns a default-constructed Diag (errno_ == 0)
  // when the statement succeeded.
  Diag error() const {
    Diag d;
    if (handle_ == nullptr || cap_.abi_ == nullptr ||
        cap_.abi_->get_error == nullptr)
      return d;
    vef_sql_diag_t raw;
    if (!cap_.abi_->get_error(handle_, &raw)) return d;
    d.errno_ = raw.errno_;
    d.severity = raw.severity;
    d.sqlstate =
        raw.sqlstate ? std::string_view{raw.sqlstate} : std::string_view{};
    d.message = (raw.message != nullptr)
                    ? std::string_view{raw.message, raw.message_len}
                    : std::string_view{};
    return d;
  }

  // Number of warnings/notes attached to the result.
  unsigned int warning_count() const {
    if (handle_ == nullptr || cap_.abi_ == nullptr ||
        cap_.abi_->warning_count == nullptr)
      return 0;
    return cap_.abi_->warning_count(handle_);
  }

  // i-th warning (0-based). Returns a default Diag if i is out of range.
  Diag warning(unsigned int i) const {
    Diag d;
    if (handle_ == nullptr || cap_.abi_ == nullptr ||
        cap_.abi_->get_warning == nullptr)
      return d;
    vef_sql_diag_t raw;
    if (!cap_.abi_->get_warning(handle_, i, &raw)) return d;
    d.errno_ = raw.errno_;
    d.severity = raw.severity;
    d.sqlstate =
        raw.sqlstate ? std::string_view{raw.sqlstate} : std::string_view{};
    d.message = (raw.message != nullptr)
                    ? std::string_view{raw.message, raw.message_len}
                    : std::string_view{};
    return d;
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
  // only on setup failure (no session, OOM). A valid Result may still
  // represent a failed query — check has_error() / warning_count().
  Result execute() const {
    if (session_.cap().abi_ == nullptr ||
        session_.cap().abi_->execute == nullptr)
      return Result{session_.cap(), nullptr};
    vef_sql_result_t *h = session_.cap().abi_->execute(
        session_.handle(), sql_.data(), sql_.size());
    return Result{session_.cap(), h};
  }

  // Streaming row passed to the for_each callback. Valid only for the
  // duration of the callback; do not store across rows.
  class Row {
   public:
    Row(const char **row, const unsigned long *lengths,
        unsigned int num_columns)
        : row_(row), lengths_(lengths), num_columns_(num_columns) {}

    unsigned int num_columns() const { return num_columns_; }

    std::string_view column_str(unsigned int i) const {
      if (row_ == nullptr || row_[i] == nullptr) return {};
      return {row_[i], lengths_[i]};
    }

    long long column_int(unsigned int i) const {
      if (row_ == nullptr || row_[i] == nullptr) return 0;
      return std::strtoll(row_[i], nullptr, 10);
    }

    double column_real(unsigned int i) const {
      if (row_ == nullptr || row_[i] == nullptr) return 0.0;
      return std::strtod(row_[i], nullptr);
    }

   private:
    const char **row_{nullptr};
    const unsigned long *lengths_{nullptr};
    unsigned int num_columns_{0};
  };

  // Execute the query and invoke fn once per row, without buffering the full
  // result set. fn receives a const Row& valid only for the duration of
  // the call; do not store it across rows.
  //
  // Returns a Result that carries diagnostics (error + warnings) only — it
  // holds no buffered rows. Check result.has_error() / result.warning_count()
  // after for_each returns.
  template <typename F>
  Result for_each(F &&fn) const {
    if (session_.cap().abi_ == nullptr ||
        session_.cap().abi_->for_each_row == nullptr)
      return Result{session_.cap(), nullptr};

    struct Ctx {
      F &fn;
    } ctx{fn};

    vef_sql_result_t *h = session_.cap().abi_->for_each_row(
        session_.handle(), sql_.data(), sql_.size(),
        [](const char **row, const unsigned long *lengths,
           unsigned int num_columns, void *raw) -> bool {
          auto *c = static_cast<Ctx *>(raw);
          Row r{row, lengths, num_columns};
          c->fn(r);
          return true;
        },
        &ctx);
    return Result{session_.cap(), h};
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
