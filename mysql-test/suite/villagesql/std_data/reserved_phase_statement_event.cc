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

// VEF extension whose statement_event capability uses a reserved phase, so
// INSTALL EXTENSION rejects it while populating capabilities.

#include <villagesql/preview/statement_event.h>
#include <villagesql/vsql.h>

using namespace vsql;
namespace se = vsql::preview_statement_event;

static void unused_hook(const se::StatementEventArgs &,
                        se::StatementEventResult &) {}

static se::StatementEventCapability<VEF_STATEMENT_EVENT_PREEXECUTE,
                                    &unused_hook>
    STATEMENT_EVENT;

VEF_GENERATE_ENTRY_POINTS(make_extension().with(STATEMENT_EVENT))
