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

RealValueOp::RealValueOp(const RealValueFunction &func,
                         const TypeParameters &params)
    : vdf_(func.vdf()), params_(params) {}

double RealValueOp::invoke(const unsigned char *data, size_t len) const {
  vef_context_t ctx{};
  ctx.protocol = vdf_->protocol;

  vef_invalue_t input{};
  input.type = VEF_TYPE_CUSTOM;
  input.is_null = false;
  input.bin_value = data;
  input.bin_len = len;
  input.type_params.count = params_.count();
  input.type_params.keys = params_.key_data();
  input.type_params.values = params_.value_data();

  vef_invalue_t *inputs[] = {&input};
  vef_vdf_args_t args{};
  args.user_data = nullptr;
  args.value_count = 1;
  args.values = inputs;

  char error_msg[VEF_MAX_ERROR_LEN] = {};
  vef_vdf_result_t result{};
  result.error_msg = error_msg;
  result.type = VEF_RESULT_VALUE;
  vdf_->vdf(&ctx, &args, &result);
  if (result.type != VEF_RESULT_VALUE) {
    LogVSQL(ERROR_LEVEL, "real_value VDF '%s' returned %d: %s", vdf_->name,
            static_cast<int>(result.type),
            error_msg[0] != '\0' ? error_msg : "unknown error");
    return 0.0;
  }
  return result.real_value;
}

}  // namespace villagesql
