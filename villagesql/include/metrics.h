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

#ifndef VILLAGESQL_INCLUDE_METRICS_H_
#define VILLAGESQL_INCLUDE_METRICS_H_

#include <atomic>
#include <cstdint>

// VillageSQL's global status variables.
//
// Every exported counter in VillageSQL is declared here and registered from
// one table in metrics.cc. Adding one is a counter below, a row in that table,
// and an increment at the site that knows the event happened.
//
// The naming convention is Villagesql_{subsystem}_{event}. The shared prefix
// is what lets a scraper pick up a new counter without being taught its name,
// so a collector matching 'Villagesql\_%' needs no change when an area starts
// reporting. The subsystem segment keeps the namespace navigable. Keep names
// to [A-Za-z0-9_] since a scraper may rewrite anything outside that set.
//
// These count events, not state. Where a system table already holds the answer
// read it from there instead, so one number has one source rather than two
// that drift.
//
// Counters are monotonic for the life of the process and FLUSH STATUS does not
// reset them: a scraper derives rates from the difference between samples, and
// a reset part way through a series is indistinguishable from a counter wrap.

namespace villagesql {
namespace metrics {

// Expansions rebuilt because the cached .so was missing or unusable. The
// extension loads normally afterwards, so this is not an error count: a
// nonzero rate means a crash or something damaging the expansion cache.
extern std::atomic<uint64_t> g_veb_cache_repaired;

// Publish every counter above through the server's status variable service.
// Called once during VillageSQL initialization, before installed extensions
// are loaded, so the counters are live for events raised during loading.
//
// Returns false on success, true on error (already logged).
bool register_status_variables();

// Counterpart to register_status_variables(), called during shutdown. Safe to
// call when registration failed or never ran.
void unregister_status_variables();

}  // namespace metrics
}  // namespace villagesql

#endif  // VILLAGESQL_INCLUDE_METRICS_H_
