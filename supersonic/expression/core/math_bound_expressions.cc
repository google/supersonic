// Copyright 2010 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: onufry@google.com (Onufry Wojtaszczyk)
//
// Bound expression accessors for math functions.

#include "supersonic/expression/core/math_bound_expressions.h"

#include <math.h>
#include <stddef.h>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/arithmetic_bound_expressions.h"
#include "supersonic/expression/core/elementary_bound_expressions.h"
#include "supersonic/expression/core/math_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/infrastructure/terminal_bound_expressions.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/bound_expression_factory.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// ---------------------- Exponents, logarithms, powers ------------------------

class BufferAllocator;

FailureOrOwned<BoundExpression> BoundExp(BoundExpression* arg,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_EXP, DOUBLE, DOUBLE>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundLnNulling(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_LN_NULLING, DOUBLE, DOUBLE>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundLnQuiet(BoundExpression* arg,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_LN_QUIET, DOUBLE, DOUBLE>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundLog10Nulling(BoundExpression* arg,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_LOG10_NULLING, DOUBLE,
      DOUBLE>(allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundLog10Quiet(BoundExpression* arg,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_LOG10_QUIET, DOUBLE, DOUBLE>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundLog2Nulling(BoundExpression* arg,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_LOG2_NULLING, DOUBLE, DOUBLE>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundLog2Quiet(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_LOG2_QUIET, DOUBLE, DOUBLE>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundLogNulling(BoundExpression* base_ptr,
                                                BoundExpression* argument_ptr,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> base(base_ptr);
  scoped_ptr<BoundExpression> argument(argument_ptr);
  FailureOrOwned<BoundExpression> ln_argument =
      BoundLnNulling(argument.release(), allocator, max_row_count);
  PROPAGATE_ON_FAILURE(ln_argument);
  FailureOrOwned<BoundExpression> ln_base =
      BoundLnNulling(base.release(), allocator, max_row_count);
  PROPAGATE_ON_FAILURE(ln_base);
  return BoundDivideNulling(ln_argument.release(), ln_base.release(),
                            allocator, max_row_count);
}

FailureOrOwned<BoundExpression> BoundLogQuiet(BoundExpression* base_ptr,
                                              BoundExpression* argument_ptr,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> base(base_ptr);
  scoped_ptr<BoundExpression> argument(argument_ptr);
  FailureOrOwned<BoundExpression> ln_argument =
      BoundLnQuiet(argument.release(), allocator, max_row_count);
  PROPAGATE_ON_FAILURE(ln_argument);
  FailureOrOwned<BoundExpression> ln_base =
      BoundLnQuiet(base.release(), allocator, max_row_count);
  PROPAGATE_ON_FAILURE(ln_base);
  return BoundDivideQuiet(ln_argument.release(), ln_base.release(),
                          allocator, max_row_count);
}

FailureOrOwned<BoundExpression> BoundPowerSignaling(BoundExpression* base,
                                                    BoundExpression* exponent,
                                                    BufferAllocator* allocator,
                                                    rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<OPERATOR_POW_SIGNALING, DOUBLE,
      DOUBLE, DOUBLE>(allocator, max_row_count, base, exponent);
}

FailureOrOwned<BoundExpression> BoundPowerNulling(BoundExpression* base,
                                                  BoundExpression* exponent,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<OPERATOR_POW_NULLING, DOUBLE,
      DOUBLE, DOUBLE>(allocator, max_row_count, base, exponent);
}

FailureOrOwned<BoundExpression> BoundPowerQuiet(BoundExpression* base,
                                                BoundExpression* exponent,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<OPERATOR_POW_QUIET, DOUBLE,
      DOUBLE, DOUBLE>(allocator, max_row_count, base, exponent);
}

FailureOrOwned<BoundExpression> BoundSqrtSignaling(BoundExpression* arg,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_SQRT_SIGNALING, DOUBLE,
      DOUBLE>(allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundSqrtNulling(BoundExpression* arg,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_SQRT_NULLING, DOUBLE,
      DOUBLE>(allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundSqrtQuiet(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_SQRT_QUIET, DOUBLE, DOUBLE>(
      allocator, max_row_count, arg);
}

// ----------------------------- Trigonometry ----------------------------------

FailureOrOwned<BoundExpression> BoundSin(BoundExpression* argument,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_SIN, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundCos(BoundExpression* argument,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_COS, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundTanQuiet(BoundExpression* argument,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_TAN, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

// This is slightly suboptimal, but it avoids adding yet another expression to
// the giant (templated) set.
FailureOrOwned<BoundExpression> BoundCot(BoundExpression* argument,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count) {
  FailureOrOwned<BoundExpression> tangent =
      BoundTanQuiet(argument, allocator, max_row_count);
  PROPAGATE_ON_FAILURE(tangent);

  FailureOrOwned<BoundExpression> one =
      BoundConstDouble(1., allocator, max_row_count);
  PROPAGATE_ON_FAILURE(one);

  return BoundDivideQuiet(one.release(),
                          tangent.release(),
                          allocator,
                          max_row_count);
}

FailureOrOwned<BoundExpression> BoundAsin(BoundExpression* argument,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_ASIN, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundAcos(BoundExpression* argument,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_ACOS, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundAtan(BoundExpression* argument,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_ATAN, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundAtan2(BoundExpression* x,
                                           BoundExpression* y,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<
      OPERATOR_ATAN2, DOUBLE, DOUBLE, DOUBLE>(allocator, max_row_count, x, y);
}

FailureOrOwned<BoundExpression> BoundSinh(BoundExpression* argument,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_SINH, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundCosh(BoundExpression* argument,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_COSH, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundTanh(BoundExpression* argument,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_TANH, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundAsinh(BoundExpression* argument,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_ASINH, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}


FailureOrOwned<BoundExpression> BoundAcosh(BoundExpression* argument,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_ACOSH, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundAtanh(BoundExpression* argument,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_ATANH, DOUBLE, DOUBLE>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundToDegrees(BoundExpression* argument,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> argument_ptr(argument);
  FailureOrOwned<BoundExpression> scale =
      BoundConstDouble(180. / M_PI, allocator, max_row_count);
  PROPAGATE_ON_FAILURE(scale);
  return BoundMultiply(argument_ptr.release(),
                       scale.release(),
                       allocator,
                       max_row_count);
}

FailureOrOwned<BoundExpression> BoundToRadians(BoundExpression* argument,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> argument_ptr(argument);
  FailureOrOwned<BoundExpression> scale =
      BoundConstDouble(M_PI / 180., allocator, max_row_count);
  PROPAGATE_ON_FAILURE(scale);
  return BoundMultiply(argument_ptr.release(),
                       scale.release(),
                       allocator,
                       max_row_count);
}

FailureOrOwned<BoundExpression> BoundPi(BufferAllocator* allocator,
                                        rowcount_t max_row_count) {
  return BoundConstDouble(M_PI, allocator, max_row_count);
}

// ------------------------------------ Rounding -------------------------------

FailureOrOwned<BoundExpression> BoundRound(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  if (GetTypeInfo(GetExpressionType(arg)).is_integer())
    return Success(arg);
  return CreateUnaryFloatingExpression<OPERATOR_ROUND>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundRoundToInt(BoundExpression* arg,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  if (GetTypeInfo(GetExpressionType(arg)).is_integer())
     return Success(arg);
  FailureOrOwned<BoundExpression> bound_round =
      BoundRound(arg, allocator, max_row_count);
  PROPAGATE_ON_FAILURE(bound_round);
  return BoundCeilToInt(bound_round.release(), allocator, max_row_count);
  // TODO(ptab): Revert to the version below, when b/5183960 is fixed.
  //  return CreateUnaryFloatingInputExpression<OPERATOR_ROUND_TO_INT, INT64>(
  //      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundRoundWithPrecision(
    BoundExpression* argument_ptr,
    BoundExpression* precision_ptr,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> argument(argument_ptr);
  scoped_ptr<BoundExpression> precision(precision_ptr);
  // We expect the precision to be an integer.
  PROPAGATE_ON_FAILURE(CheckAttributeCount("ROUND_WITH_PRECISION",
                                           precision->result_schema(), 1));
  if (!GetTypeInfo(GetExpressionType(precision.get())).is_integer()) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StrCat("Wrong type of argument supplied. Precision has to be an "
               "integer; is: ",
               GetTypeInfo(GetExpressionType(precision.get())).name(),
               " in expression: ",
               precision->result_schema().GetHumanReadableSpecification())));
  }
  // The formula is round(arg * 10^precision) / 10^precision. We don't want to
  // calculate 10^precision twice, while the expression contract currently
  // prohibits the same expression being used twice in any expression tree.
  // We solve this problem by having an internal ROUND_WITH_MULTIPLIER
  // expression, which performs round(arg * x) / x, and precalculate x to be
  // POWER(10, precision). This, incidentally, gives another win in the most
  // common case, when the precision is a constant - because then POWER(10,
  // precision) will get calculated by our internal constant folding.
  FailureOrOwned<BoundExpression> ten =
      BoundConstDouble(10., allocator, max_row_count);
  PROPAGATE_ON_FAILURE(ten);

  // The arguments are always correct - ten and an integer - so we can safely
  // use POWER_QUIET for efficiency here.
  FailureOrOwned<BoundExpression> multiplier =
      BoundPowerQuiet(ten.release(), precision.release(),
                      allocator, max_row_count);
  PROPAGATE_ON_FAILURE(multiplier);

  return CreateTypedBoundBinaryExpression<OPERATOR_ROUND_WITH_MULTIPLIER,
         DOUBLE, DOUBLE, DOUBLE>(allocator, max_row_count,
                                 argument.release(), multiplier.release());
}

FailureOrOwned<BoundExpression> BoundFloor(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  if (GetTypeInfo(GetExpressionType(arg)).is_integer())
    return Success(arg);
  return CreateUnaryFloatingExpression<OPERATOR_FLOOR>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundFloorToInt(BoundExpression* arg,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  if (GetTypeInfo(GetExpressionType(arg)).is_integer())
     return Success(arg);
  return CreateUnaryFloatingInputExpression<OPERATOR_FLOOR_TO_INT, INT64>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundCeil(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  if (GetTypeInfo(GetExpressionType(arg)).is_integer())
    return Success(arg);
  return CreateUnaryFloatingExpression<OPERATOR_CEIL>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundCeilToInt(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  if (GetTypeInfo(GetExpressionType(arg)).is_integer())
     return Success(arg);
  return CreateUnaryFloatingInputExpression<OPERATOR_CEIL_TO_INT, INT64>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundTrunc(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  if (GetTypeInfo(GetExpressionType(arg)).is_integer())
    return Success(arg);
  return CreateUnaryFloatingExpression<OPERATOR_TRUNC>(
      allocator, max_row_count, arg);
}

// -------------------------------- IEEE 754 checks ----------------------------

FailureOrOwned<BoundExpression> BoundIsFinite(BoundExpression* arg,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_IS_FINITE, DOUBLE, BOOL>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundIsNormal(BoundExpression* arg,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_IS_NORMAL, DOUBLE, BOOL>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundIsNaN(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_IS_NAN, DOUBLE, BOOL>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundIsInf(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_IS_INF, DOUBLE, BOOL>(
      allocator, max_row_count, arg);
}

// ------------------------- Other ---------------------------------

UnaryExpressionFactory* CreateAbsFactory(DataType type) {
  const OperatorId op = OPERATOR_ABS;

  switch (type) {
    case INT32: return new SpecializedUnaryFactory<op, INT32, UINT32>();
    case INT64: return new SpecializedUnaryFactory<op, INT64, UINT64>();
    case FLOAT: return new SpecializedUnaryFactory<op, FLOAT, FLOAT>();
    case DOUBLE: return new SpecializedUnaryFactory<op, DOUBLE, DOUBLE>();
    default: return NULL;
  }
}

FailureOrOwned<BoundExpression> BoundAbs(BoundExpression* argument,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> argument_ptr(argument);
  PROPAGATE_ON_FAILURE(CheckAttributeCount("ABS",
                                           argument_ptr->result_schema(),
                                           1));
  DataType input_type = GetExpressionType(argument_ptr.get());
  if (input_type == UINT32 || input_type == UINT64) {
    return Success(argument_ptr.release());
  }
  UnaryExpressionFactory* factory =
      CreateAbsFactory(GetExpressionType(argument_ptr.get()));
  return RunUnaryFactory(factory, allocator, max_row_count,
                         argument_ptr.release(), "ABS");
}

FailureOrOwned<BoundExpression> BoundFormatSignaling(BoundExpression* number,
                                                     BoundExpression* precision,
                                                     BufferAllocator* allocator,
                                                     rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<OPERATOR_FORMAT_SIGNALING, DOUBLE,
      INT32, STRING>(allocator, max_row_count, number, precision);
}

}  // namespace supersonic
