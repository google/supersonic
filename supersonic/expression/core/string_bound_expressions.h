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

#ifndef SUPERSONIC_EXPRESSION_CORE_STRING_BOUND_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_STRING_BOUND_EXPRESSIONS_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"

// For the descriptions of particular functions, see string_expressions.h.
// For the usage of bound expression accessors, see expression.h or kick the
// Supersonic team for documentation.

namespace supersonic {

class BoundExpression;
class BoundExpressionList;
class BufferAllocator;

FailureOrOwned<BoundExpression> BoundConcat(BoundExpressionList* args,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLength(BoundExpression* arg,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundLtrim(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundRtrim(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundContains(BoundExpression* haystack,
                                              BoundExpression* needle,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundContainsCI(BoundExpression* haystack,
                                                BoundExpression* needle,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundStringOffset(BoundExpression* haystack,
                                                  BoundExpression* needle,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundStringReplace(
    BoundExpression* haystack,
    BoundExpression* needle,
    BoundExpression* substitute,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundSubstring(BoundExpression* str,
                                               BoundExpression* pos,
                                               BoundExpression* length,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundToLower(BoundExpression* arg,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundToString(BoundExpression* arg,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundToUpper(BoundExpression* arg,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundTrim(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundTrailingSubstring(
    BoundExpression* str,
    BoundExpression* pos,
    BufferAllocator* allocator,
    rowcount_t max_row_count);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_STRING_BOUND_EXPRESSIONS_H_
