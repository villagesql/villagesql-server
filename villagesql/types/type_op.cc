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

#include "villagesql/types/type_op.h"

#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/types/special_vdf_call.h"

namespace villagesql {

EncodeOp::EncodeOp(const EncodeFunction &func, const TypeParameters &params)
    : fn_(func.fn()), vdf_(func.vdf()), params_(params) {}

DecodeOp::DecodeOp(const DecodeFunction &func, const TypeParameters &params)
    : fn_(func.fn()), vdf_(func.vdf()), params_(params) {}

CompareOp::CompareOp(const CompareFunction &func, const TypeParameters &params)
    : fn_(func.fn()), vdf_(func.vdf()), params_(params) {}

int CompareOp::invoke(const unsigned char *data1, size_t len1,
                      const unsigned char *data2, size_t len2) const {
  if (fn_ != nullptr) {
    return fn_(data1, len1, data2, len2);
  }

  TypeParameterSlice tp(params_.count(), params_.key_data(),
                        params_.value_data());
  SpecialVdfCall<IntResult, CustomArg, CustomArg> call(vdf_);
  call.init(NoInitData{}, tp, tp);
  auto result = call.invoke(BinarySlice{data1, len1}, BinarySlice{data2, len2});
  if (!result) {
    LogVSQL(ERROR_LEVEL, "compare VDF '%s' returned error: %s", call.name(),
            call.error_msg());
    return 0;
  }
  return static_cast<int>(*result);
}

HashOp::HashOp(const HashFunction &func, const TypeParameters &params)
    : fn_(func.fn()), vdf_(func.vdf()), params_(params) {}

size_t HashOp::invoke(const unsigned char *data, size_t len) const {
  if (fn_ != nullptr) {
    return fn_(data, len);
  }

  SpecialVdfCall<IntResult, CustomArg> call(vdf_);
  call.init(NoInitData{},
            TypeParameterSlice(params_.count(), params_.key_data(),
                               params_.value_data()));
  auto result = call.invoke(BinarySlice{data, len});
  if (!result) {
    LogVSQL(ERROR_LEVEL, "hash VDF '%s' returned error: %s", call.name(),
            call.error_msg());
    return 0;
  }
  return static_cast<size_t>(*result);
}

}  // namespace villagesql
