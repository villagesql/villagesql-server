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

#ifndef VILLAGESQL_PREVIEW_STATEMENT_EVENT_IMPL_H
#define VILLAGESQL_PREVIEW_STATEMENT_EVENT_IMPL_H

#include <villagesql/preview/statement_event.h>

namespace vsql::preview_statement_event {

template <vef_statement_event_phase_t Phase,
          void (*Fn)(const StatementEventArgs &, StatementEventResult &)>
inline StatementEventCapability<Phase,
                                Fn>::StatementEventCapability() noexcept {
  cc.phase = Phase;
  // Adapts the raw vef_statement_event_args_t*/vef_statement_event_result_t*
  // into the typed wrappers the user's Fn expects.
  cc.hook = [](const vef_statement_event_args_t *args,
               vef_statement_event_result_t *result) {
    StatementEventArgs typed_args(args);
    StatementEventResult typed_result(result);
    Fn(typed_args, typed_result);
  };
}

}  // namespace vsql::preview_statement_event

#endif  // VILLAGESQL_PREVIEW_STATEMENT_EVENT_IMPL_H
