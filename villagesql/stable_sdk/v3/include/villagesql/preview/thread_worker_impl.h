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

#ifndef VILLAGESQL_PREVIEW_THREAD_WORKER_IMPL_H
#define VILLAGESQL_PREVIEW_THREAD_WORKER_IMPL_H

#include <villagesql/preview/thread_worker.h>

namespace vsql::preview_thread_worker {

template <auto WorkFn>
inline ThreadWorkerCapability<WorkFn>::ThreadWorkerCapability(
    const char *suffix, const char *var_name) noexcept {
  descriptor.work_fn = WorkFn;
  descriptor.arg = nullptr;
  descriptor.sleep_ms = 0;
  descriptor.suffix = suffix;
  descriptor.var_name = var_name;
}

}  // namespace vsql::preview_thread_worker

#endif  // VILLAGESQL_PREVIEW_THREAD_WORKER_IMPL_H
