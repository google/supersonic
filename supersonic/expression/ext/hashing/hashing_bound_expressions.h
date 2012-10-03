// Copyright 2011 Google Inc. All Rights Reserved.
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
// Author: ptab@google.com (Piotr Tabor)

#ifndef SUPERSONIC_EXPRESSION_EXT_HASHING_HASHING_BOUND_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_EXT_HASHING_HASHING_BOUND_EXPRESSIONS_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

class BoundExpression;
class BufferAllocator;

// For the description of particular functions, refer to hashing_expressions.h.

FailureOrOwned<BoundExpression> BoundFingerprint(BoundExpression* child,
                                                 BufferAllocator* allocator,
                                                 rowcount_t row_capacity);

FailureOrOwned<BoundExpression> BoundHash(BoundExpression* child,
                                          BoundExpression* seed,
                                          BufferAllocator* allocator,
                                          rowcount_t row_capacity);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_EXT_HASHING_HASHING_BOUND_EXPRESSIONS_H_
