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

#include <sys/types.h>

#include "sql/sys_vars.h"

namespace villagesql {

// Initialize the villagesql extension framework.
bool init_extension_infrastructure();

// VillageSQL extension shutdown runs in two phases, invoked from two different
// points in the server shutdown sequence (sql/mysqld.cc). The split exists
// because the teardown steps have opposing ordering constraints relative to
// InnoDB shutdown:
//
//   Phase 1 - depopulate_extension_capabilities() - runs in
//     close_connections(), before wait_till_no_thd() and plugin_shutdown().
//   Phase 2 - destroy_extension_state() - runs in clean_up(), after
//     plugin_shutdown() (i.e. after innodb_shutdown()).
//
// Why phase 1 must be early:
//   - Capability background threads (thread_worker) register THDs in
//     Global_THD_manager; only depopulation joins them, so it must precede
//     wait_till_no_thd() or that call blocks forever.
//   - sys/status var depopulation calls component services that
//     plugin_shutdown() tears down, so it must precede plugin_shutdown().
//
// Why phase 2 must be late:
//   - InnoDB's dict cache holds Custom_column objects whose destructors call
//     extension-provided storage functions during innodb_shutdown(). The .so
//     files and the VictionaryClient type metadata must outlive InnoDB, so
//     they can only be unloaded/destroyed after plugin_shutdown().
//
// Extending this: two phases suffice only because every current capability
// tears down either "before threads are gone" or "before InnoDB is down", and
// the single phase-1 point happens to satisfy both. If a future capability
// needs a teardown step with a third ordering constraint - for example a
// deinitialization that can only run AFTER wait_till_no_thd(), once every
// connection thread has exited - two fixed phases will not be enough. At that
// point, replace these functions with an explicit per-capability teardown-phase
// mechanism: tag each capability with the phase it must be torn down in (e.g.
// kBeforeThreadsGone, kBeforeInnoDBDown, kAfterInnoDBDown) and drive each phase
// from the corresponding point in the shutdown sequence, rather than
// re-deriving these constraints by hand.

// Phase 1: depopulate the capabilities of all loaded extensions (deregister
// sys/status variables, join capability background threads, drain in-flight
// capability calls). Does NOT unload the .so files or destroy extension state;
// InnoDB still references extension storage interfaces until its dict cache is
// cleared during innodb_shutdown(). See the two-phase note above.
void depopulate_extension_capabilities();

// Phase 2: unload the extension .so files and destroy the VictionaryClient and
// SchemaManager state. Safe only after wait_till_no_thd() (no connection thread
// is mid-rollback holding VictionaryClient locks) AND after
// plugin_shutdown()/innodb_shutdown() (InnoDB no longer calls into
// extension-provided storage interfaces). See the two-phase note above.
void destroy_extension_state();

/**
 * Initializes VillageSQL before running the user's --init-file.  This runs on
 * a bootstrap thread as SYSTEM_THREAD_SERVER_INITIALIZE.
 */
bool bootstrap_for_init_file(THD *thd);

}  // namespace villagesql
