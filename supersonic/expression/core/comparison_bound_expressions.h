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
// For comments on the semantics see comparison_expressions.h.

#ifndef SUPERSONIC_EXPRESSION_CORE_COMPARISON_BOUND_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_COMPARISON_BOUND_EXPRESSIONS_H_

#include <vector>
using std::vector;

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class BoundExpression;
class BoundExpressionList;
class BufferAllocator;

FailureOrOwned<BoundExpression> BoundEqual(BoundExpression* left,
                                           BoundExpression* right,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundNotEqual(BoundExpression* left,
                                              BoundExpression* right,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLess(BoundExpression* left,
                                          BoundExpression* right,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLessOrEqual(BoundExpression* left,
                                                 BoundExpression* right,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);


FailureOrOwned<BoundExpression> BoundGreater(BoundExpression* left,
                                             BoundExpression* right,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundGreaterOrEqual(BoundExpression* left,
                                                    BoundExpression* right,
                                                    BufferAllocator* allocator,
                                                    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundIsOdd(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundIsEven(BoundExpression* arg,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundInSet(
    BoundExpression* needle_expression,
    BoundExpressionList* haystack_arguments,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_COMPARISON_BOUND_EXPRESSIONS_H_
