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

// Metadata describing how this server binary was built. The values live in a
// magic-marked block (see build_info_raw.h) that a POST_BUILD stamper
// rewrites in the linked mysqld; this header is hand-written, committed, and
// stable, so callers never recompile when build state changes.
//
// Semantics: the values describe the git state, time, and host of the most
// recent build that actually relinked the binary. A no-op build does not
// refresh them (the binary did not change, so its provenance did not either).
// Binaries that are never stamped (unit tests, tools) report the placeholder
// defaults: git_sha "unknown", zero counts, empty strings.
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

// Returns the build metadata parsed from the stamped block. The reference is
// to a function-local static with process lifetime; it is always valid. The
// block is parsed once, on first call.
const BuildInfo &GetBuildInfo();

}  // namespace villagesql

#endif  // VILLAGESQL_INCLUDE_BUILD_INFO_H_
