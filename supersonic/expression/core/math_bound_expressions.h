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
// Bound expression accessors for math functions.i
// For the descriptions of particular functions see math_expressions.h.
// For the usage of bound expression accessors, see expression.h or kick the
// Supersonic team for documentation.

#ifndef SUPERSONIC_EXPRESSION_CORE_MATH_BOUND_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_MATH_BOUND_EXPRESSIONS_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

// ---------------------- Exponents, logarithms, powers ------------------------

class BoundExpression;
class BufferAllocator;

FailureOrOwned<BoundExpression> BoundExp(BoundExpression* arg,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLnNulling(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLnQuiet(BoundExpression* arg,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLog10Nulling(BoundExpression* arg,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLog10Quiet(BoundExpression* arg,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLogNulling(BoundExpression* base,
                                                BoundExpression* argument,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLogQuiet(BoundExpression* base,
                                              BoundExpression* argument,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLog2Nulling(BoundExpression* arg,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLog2Quiet(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundPowerSignaling(BoundExpression* base,
                                                    BoundExpression* exponent,
                                                    BufferAllocator* allocator,
                                                    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundPowerNulling(BoundExpression* base,
                                                  BoundExpression* exponent,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundPowerQuiet(BoundExpression* base,
                                                BoundExpression* exponent,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundSqrtSignaling(BoundExpression* arg,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundSqrtNulling(BoundExpression* arg,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundSqrtQuiet(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

// ----------------------------- Trigonometry ----------------------------------

FailureOrOwned<BoundExpression> BoundSin(BoundExpression* arg,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundCos(BoundExpression* arg,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundTanQuiet(BoundExpression* arg,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundCot(BoundExpression* arg,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundAsin(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundAcos(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundAtan(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundAtan2(BoundExpression* x,
                                           BoundExpression* y,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundSinh(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundCosh(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundTanh(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundAsinh(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundAcosh(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundAtanh(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundToDegrees(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundToRadians(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundPi(BufferAllocator* allocator,
                                        rowcount_t max_row_count);

// ------------------------------------ Rounding -------------------------------

FailureOrOwned<BoundExpression> BoundRound(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundRoundToInt(BoundExpression* arg,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundRoundWithPrecision(
    BoundExpression* argument,
    BoundExpression* precision,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundFloor(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundFloorToInt(BoundExpression* arg,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundCeil(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundCeilToInt(BoundExpression* arg,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundTrunc(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

// -------------------------------- IEEE 754 checks ----------------------------

FailureOrOwned<BoundExpression> BoundIsFinite(BoundExpression* arg,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundIsNormal(BoundExpression* arg,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundIsNaN(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundIsInf(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

// ------------------------------------- Other ---------------------------------

FailureOrOwned<BoundExpression> BoundAbs(BoundExpression* argument,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundFormatSignaling(BoundExpression* number,
                                                     BoundExpression* precision,
                                                     BufferAllocator* allocator,
                                                     rowcount_t max_row_count);

}  // namespace supersonic


#endif  // SUPERSONIC_EXPRESSION_CORE_MATH_BOUND_EXPRESSIONS_H_
