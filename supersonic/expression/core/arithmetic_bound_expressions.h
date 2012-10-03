// Copyright 2010 Google Inc.  All Rights Reserved
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
//
// Base arithmetic expressions.
// For comments on the semantics see arithmetic_expressions.h.

#ifndef SUPERSONIC_EXPRESSION_CORE_ARITHMETIC_BOUND_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_ARITHMETIC_BOUND_EXPRESSIONS_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

class BoundExpression;
class BufferAllocator;

FailureOrOwned<BoundExpression> BoundNegate(BoundExpression* source,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundPlus(BoundExpression* left,
                                          BoundExpression* right,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundMultiply(BoundExpression* left,
                                              BoundExpression* right,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundMinus(BoundExpression* left,
                                           BoundExpression* right,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundDivideSignaling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundDivideNulling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundDivideQuiet(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundCppDivideSignaling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundCppDivideNulling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundModulusSignaling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundModulusNulling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_ARITHMETIC_BOUND_EXPRESSIONS_H_
