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

#ifndef VILLAGESQL_INCLUDE_BUILD_INFO_H_
#define VILLAGESQL_INCLUDE_BUILD_INFO_H_

namespace villagesql {

// Metadata describing how this server binary was built. The values are baked
// in at build time by the generated build_info.cc (see common/build_info.cc.in
// and cmake/gen_build_info.cmake).
//
// This API header is hand-written, committed, and stable: it deliberately
// contains no volatile values, so callers that include it do not recompile
// when only the build timestamp or git state change. Only the generated
// build_info.cc is rebuilt on each build.
//
// The git_sha pins the source revision; the three counts describe how far the
// build tree diverged from that revision. A clean (unmodified) tree has
// files_added == files_deleted == files_modified == 0. Version information is
// not captured here; use villagesql::GetBuildVersion() (Semver) for that.
struct BuildInfo {
  const char *git_sha;          // full 40-char commit SHA, or "unknown"
  int files_added;              // added + untracked files (git status A, ??)
  int files_deleted;            // deleted files (git status D)
  int files_modified;           // modified files (git status M)
  const char *build_timestamp;  // ISO-8601 UTC, e.g. "2026-06-17T12:34:56Z"
  const char *build_host;       // hostname of the machine that built it
  const char *build_os;         // host OS, e.g. "Linux-6.8.0"
  const char *build_arch;       // host architecture, e.g. "x86_64"

  // True if the build tree diverged from git_sha, i.e. any of the file-change
  // counts is non-zero.
  bool is_dirty() const;
};

// Returns the build metadata baked into this binary. The reference is to a
// constexpr instance with static storage duration; it is always valid.
const BuildInfo &GetBuildInfo();

}  // namespace villagesql

#endif  // VILLAGESQL_INCLUDE_BUILD_INFO_H_
