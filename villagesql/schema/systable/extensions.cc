/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#include "villagesql/schema/systable/extensions.h"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <string>
#include <utility>

#include "scope_guard.h"
#include "sql/field.h"
#include "sql/sql_base.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/systable/helpers.h"

#include "my_rapidjson_size_t.h"  // IWYU pragma: keep

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace villagesql {

namespace {

// Single source of truth for the column name carrying a PendingAction.
constexpr const char kPendingActionColumn[] = "pending_action";

// Format the current UTC time as ISO-8601 with microseconds:
// "YYYY-MM-DDTHH:MM:SS.uuuuuuZ".
//
// TODO(villagesql): move to a shared time-formatting utility once a second
// caller wants it.
std::string CurrentTimestampUtc() {
  using clock = std::chrono::system_clock;
  const auto now = clock::now();
  const auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(now);
  const auto micros =
      std::chrono::duration_cast<std::chrono::microseconds>(now - seconds)
          .count();

  std::time_t t = clock::to_time_t(seconds);
  std::tm tm_utc{};
#if defined(_WIN32)
  gmtime_s(&tm_utc, &t);
#else
  gmtime_r(&t, &tm_utc);
#endif

  // Worst-case output is 27 chars + NUL ("YYYY-MM-DDTHH:MM:SS.uuuuuuZ").
  // We size buf far above that to keep GCC's -Wformat-truncation happy:
  // it assumes each %0Nd field can produce its int's full range, summing
  // to ~84 bytes even though every gmtime field is in fact bounded to two
  // digits and micros to six.
  const unsigned micros_u = static_cast<unsigned>(micros);

  char buf[96];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%06uZ",
                tm_utc.tm_year + 1900, tm_utc.tm_mon + 1, tm_utc.tm_mday,
                tm_utc.tm_hour, tm_utc.tm_min, tm_utc.tm_sec, micros_u);
  return buf;
}

}  // namespace

PendingAction PendingAction::CreateVersionUpdate(
    std::string target_version, std::string target_veb_sha256) {
  PendingAction a;
  a.target_version_ = std::move(target_version);
  a.target_veb_sha256_ = std::move(target_veb_sha256);
  a.requested_at_ = CurrentTimestampUtc();
  return a;
}

void PendingAction::MarkFailed(std::string error_message) {
  last_error_ = std::move(error_message);
  last_error_at_ = CurrentTimestampUtc();
}

const std::string &PendingAction::target_version() const {
  return target_version_;
}

const std::string &PendingAction::target_veb_sha256() const {
  return target_veb_sha256_;
}

const std::string &PendingAction::requested_at() const { return requested_at_; }

const std::string &PendingAction::last_error() const { return last_error_; }

const std::string &PendingAction::last_error_at() const {
  return last_error_at_;
}

bool PendingAction::has_failure() const { return !last_error_.empty(); }

std::string PendingAction::Serialize() const {
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> w(sb);

  w.StartObject();
  w.Key("target_version");
  w.String(target_version_.c_str(),
           static_cast<rapidjson::SizeType>(target_version_.size()));
  w.Key("target_veb_sha256");
  w.String(target_veb_sha256_.c_str(),
           static_cast<rapidjson::SizeType>(target_veb_sha256_.size()));
  w.Key("requested_at");
  w.String(requested_at_.c_str(),
           static_cast<rapidjson::SizeType>(requested_at_.size()));
  if (!last_error_.empty()) {
    w.Key("last_error");
    w.String(last_error_.c_str(),
             static_cast<rapidjson::SizeType>(last_error_.size()));
    w.Key("last_error_at");
    w.String(last_error_at_.c_str(),
             static_cast<rapidjson::SizeType>(last_error_at_.size()));
  }
  w.EndObject();

  return sb.GetString();
}

bool PendingAction::ReadFromTable(TABLE &table,
                                  std::optional<PendingAction> &out,
                                  std::string &error_message) {
  Field *f = find_field_in_table_sef(&table, kPendingActionColumn);
  if (f == nullptr) {
    error_message = std::string("column '") + kPendingActionColumn +
                    "' not found in " + SchemaManager::EXTENSIONS_TABLE_NAME +
                    " table";
    return true;
  }
  if (f->is_null()) {
    out.reset();
    return false;
  }
  std::string raw;
  read_string_field(f, raw);
  PendingAction parsed;
  if (Deserialize(raw, parsed, error_message)) return true;
  out = std::move(parsed);
  return false;
}

namespace {
// Build a JSON_UNQUOTE(JSON_EXTRACT(alias.pending_action, '$.field')) SQL
// expression. Shared body for the four *SqlExpr helpers.
std::string BuildSqlExpr(std::string_view table_alias,
                         std::string_view json_path) {
  // Realistic inputs are a 64-char identifier plus a short json path;
  // 256 leaves comfortable margin for the fixed literal wrapper.
  char buf[256];
  const int n = std::snprintf(
      buf, sizeof(buf), "JSON_UNQUOTE(JSON_EXTRACT(%.*s.%s, '$.%.*s'))",
      static_cast<int>(table_alias.size()), table_alias.data(),
      kPendingActionColumn, static_cast<int>(json_path.size()),
      json_path.data());
  if (n < 0 || static_cast<size_t>(n) >= sizeof(buf)) return std::string();
  return std::string(buf, static_cast<size_t>(n));
}
}  // namespace

std::string PendingAction::TargetVersionSqlExpr(std::string_view table_alias) {
  return BuildSqlExpr(table_alias, "target_version");
}

std::string PendingAction::RequestedAtSqlExpr(std::string_view table_alias) {
  return BuildSqlExpr(table_alias, "requested_at");
}

std::string PendingAction::LastErrorSqlExpr(std::string_view table_alias) {
  return BuildSqlExpr(table_alias, "last_error");
}

std::string PendingAction::LastErrorAtSqlExpr(std::string_view table_alias) {
  return BuildSqlExpr(table_alias, "last_error_at");
}

bool PendingAction::StoreToTable(TABLE &table,
                                 const std::optional<PendingAction> &value,
                                 std::string &error_message) {
  Field *f = find_field_in_table_sef(&table, kPendingActionColumn);
  if (f == nullptr) {
    error_message = std::string("column '") + kPendingActionColumn +
                    "' not found in " + SchemaManager::EXTENSIONS_TABLE_NAME +
                    " table";
    return true;
  }
  if (!value.has_value()) {
    f->set_null();
    return false;
  }
  const std::string raw = value->Serialize();
  f->set_notnull();
  f->store(raw.c_str(), raw.length(), &my_charset_utf8mb4_bin);
  return false;
}

bool PendingAction::Deserialize(const std::string &raw, PendingAction &out,
                                std::string &error_message) {
  rapidjson::Document doc;
  if (doc.Parse(raw.c_str(), raw.size()).HasParseError()) {
    error_message = std::string("pending_action JSON parse error: ") +
                    rapidjson::GetParseError_En(doc.GetParseError());
    return true;
  }
  if (!doc.IsObject()) {
    error_message = "pending_action JSON is not an object";
    return true;
  }

  PendingAction tmp;

  const auto tv = doc.FindMember("target_version");
  if (tv == doc.MemberEnd() || !tv->value.IsString()) {
    error_message =
        "pending_action JSON is missing string field 'target_version'";
    return true;
  }
  tmp.target_version_ = tv->value.GetString();

  const auto ts = doc.FindMember("target_veb_sha256");
  if (ts == doc.MemberEnd() || !ts->value.IsString()) {
    error_message =
        "pending_action JSON is missing string field 'target_veb_sha256'";
    return true;
  }
  tmp.target_veb_sha256_ = ts->value.GetString();

  const auto ra = doc.FindMember("requested_at");
  if (ra == doc.MemberEnd() || !ra->value.IsString()) {
    error_message =
        "pending_action JSON is missing string field 'requested_at'";
    return true;
  }
  tmp.requested_at_ = ra->value.GetString();

  // Optional failure record. Both fields must be present together; either
  // both absent (no failure recorded) or both present strings.
  const auto le = doc.FindMember("last_error");
  const auto lea = doc.FindMember("last_error_at");
  const bool le_present = le != doc.MemberEnd();
  const bool lea_present = lea != doc.MemberEnd();
  if (le_present != lea_present) {
    error_message =
        "pending_action JSON has only one of 'last_error' / 'last_error_at'";
    return true;
  }
  if (le_present) {
    if (!le->value.IsString() || !lea->value.IsString()) {
      error_message =
          "pending_action JSON 'last_error' / 'last_error_at' must be strings";
      return true;
    }
    tmp.last_error_ = le->value.GetString();
    tmp.last_error_at_ = lea->value.GetString();
  }

  out = std::move(tmp);
  return false;
}

bool TableTraits<ExtensionEntry>::read_from_table(TABLE &table,
                                                  ExtensionEntry &entry) {
  Field **field = table.field;

  // extension_name (field 0) - read key component
  std::string ext_name;
  read_string_field(field[0], ext_name);
  entry.set_key(ExtensionKey(std::move(ext_name)));

  // extension_version (field 1)
  read_string_field(field[1], entry.extension_version);

  // veb_sha256 (field 2)
  read_string_field(field[2], entry.veb_sha256);

  // Read pending_action. Treat an unreadable cell (malformed JSON,
  // future-shape we don't understand, etc.) the same as a real apply-time
  // failure: surface it to the operator via PENDING_LAST_ERROR rather than
  // blocking the whole extension load. The extension stays at its current
  // version and the operator can clear the bad row via SQL.
  std::string err;
  if (PendingAction::ReadFromTable(table, entry.pending_action, err)) {
    LogVSQL(WARNING_LEVEL,
            "pending action for extension '%s' was unreadable: %s; "
            "treating as failed apply",
            entry.extension_name().c_str(), err.c_str());
    PendingAction stub;
    stub.MarkFailed("Pending action could not be read: " + err);
    entry.pending_action = std::move(stub);
  }

  return false;  // Success
}

bool TableTraits<ExtensionEntry>::write_to_table(TABLE &table,
                                                 const ExtensionEntry &entry) {
  Field **field = table.field;

  // Set all fields for the row
  // extension_name (field 0) - required
  field[0]->store(entry.extension_name().c_str(),
                  entry.extension_name().length(), &my_charset_utf8mb4_bin);

  // extension_version (field 1) - required
  field[1]->store(entry.extension_version.c_str(),
                  entry.extension_version.length(), &my_charset_utf8mb4_bin);

  // veb_sha256 (field 2) - required
  field[2]->store(entry.veb_sha256.c_str(), entry.veb_sha256.length(),
                  &my_charset_utf8mb4_bin);

  {
    std::string err;
    if (PendingAction::StoreToTable(table, entry.pending_action, err)) {
      LogVSQL(ERROR_LEVEL, "extension '%s': %s", entry.extension_name().c_str(),
              err.c_str());
      return true;
    }
  }

  // Write the row - use ha_write_row for INSERT
  int error = table.file->ha_write_row(table.record[0]);
  if (should_assert_if_true(error)) {
    LogVSQL(
        ERROR_LEVEL, "Failed to write extension '%s' version '%s': error %d",
        entry.extension_name().c_str(), entry.extension_version.c_str(), error);
    return true;
  }

  return false;  // Success
}

bool TableTraits<ExtensionEntry>::update_in_table(TABLE &table,
                                                  const ExtensionEntry &entry,
                                                  const ExtensionKey &old_key) {
  const ExtensionKey &lookup_key =
      old_key.str().empty() ? entry.key() : old_key;

  // Build index scan on primary key (extension_name)
  uchar key_buf[MAX_KEY_LENGTH];

  // Set up the key for index scan
  Field **field = table.field;
  field[0]->store(lookup_key.extension_name().c_str(),
                  lookup_key.extension_name().length(),
                  &my_charset_utf8mb4_bin);

  // Copy the key for index read
  key_copy(key_buf, table.record[0], table.key_info,
           table.key_info->key_length);

  // Save old record for update
  store_record(&table, record[1]);

  // Find the row using index read
  int error = table.file->ha_index_init(0, false);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to init index for update: error %d", error);
    return true;
  }

  auto index_end_guard =
      create_scope_guard([&table]() { table.file->ha_index_end(); });

  error = table.file->ha_index_read_map(table.record[0], key_buf, HA_WHOLE_KEY,
                                        HA_READ_KEY_EXACT);
  if (should_assert_if_true(error)) {
    LogVSQL(ERROR_LEVEL, "Failed to find row for update: error %d", error);
    return true;
  }

  // Now update the fields with new values
  field[0]->store(entry.extension_name().c_str(),
                  entry.extension_name().length(), &my_charset_utf8mb4_bin);
  field[1]->store(entry.extension_version.c_str(),
                  entry.extension_version.length(), &my_charset_utf8mb4_bin);
  field[2]->store(entry.veb_sha256.c_str(), entry.veb_sha256.length(),
                  &my_charset_utf8mb4_bin);
  {
    std::string err;
    if (PendingAction::StoreToTable(table, entry.pending_action, err)) {
      LogVSQL(ERROR_LEVEL, "extension '%s': %s", entry.extension_name().c_str(),
              err.c_str());
      return true;
    }
  }

  // Update the row
  error = table.file->ha_update_row(table.record[1], table.record[0]);

  if (should_assert_if_true(error && error != HA_ERR_RECORD_IS_THE_SAME)) {
    LogVSQL(ERROR_LEVEL, "Failed to update row: error %d", error);
    return true;
  }

  return false;  // Success
}

bool TableTraits<ExtensionEntry>::delete_from_table(
    TABLE &table, const ExtensionEntry &entry) {
  // Build index scan on primary key (extension_name)
  uchar key_buf[MAX_KEY_LENGTH];

  // Set up the key for index scan
  Field **field = table.field;
  field[0]->store(entry.extension_name().c_str(),
                  entry.extension_name().length(), &my_charset_utf8mb4_bin);

  // Copy the key for index read
  key_copy(key_buf, table.record[0], table.key_info,
           table.key_info->key_length);

  // Find the row using index read
  int error = table.file->ha_index_init(0, false);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to init index for delete: error %d", error);
    return true;
  }

  auto index_end_guard =
      create_scope_guard([&table]() { table.file->ha_index_end(); });

  error = table.file->ha_index_read_map(table.record[0], key_buf, HA_WHOLE_KEY,
                                        HA_READ_KEY_EXACT);
  if (error) {
    if (error == HA_ERR_KEY_NOT_FOUND) {
      // Row doesn't exist - this is OK for delete
      LogVSQL(WARNING_LEVEL, "Extension row not found for delete: %s",
              entry.extension_name().c_str());
      return false;
    }
    // We expect this to not happen if the caller checked for existence first
    if (should_assert_if_true(error)) {
      LogVSQL(ERROR_LEVEL, "Failed to find row for delete: error %d", error);
      return true;
    }
  }

  // Delete the row
  error = table.file->ha_delete_row(table.record[0]);

  if (should_assert_if_true(error)) {
    LogVSQL(ERROR_LEVEL, "Failed to delete row: error %d", error);
    return true;
  }

  return false;  // Success
}

}  // namespace villagesql
