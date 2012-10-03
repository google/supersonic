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
// Terminal expressions - Null and Sequence.

#ifndef SUPERSONIC_EXPRESSION_INFRASTRUCTURE_TERMINAL_BOUND_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_INFRASTRUCTURE_TERMINAL_BOUND_EXPRESSIONS_H_

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"

class RandomBase;

namespace supersonic {

// A typed null.
class BoundExpression;
class BufferAllocator;

FailureOrOwned<BoundExpression> BoundNull(DataType type,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

// Creates an expression of type INT64 that will produce a monotonically
// increasing sequence when evaluated, starting at 0.
FailureOrOwned<BoundExpression> BoundSequence(BufferAllocator* allocator,
                                              rowcount_t max_row_count);

// Typed constants.
FailureOrOwned<BoundExpression> BoundConstInt32(const int32& value,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstInt64(const int64& value,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstUInt32(const uint32& value,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstUInt64(const uint64& value,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstFloat(const float& value,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstDouble(const double& value,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstBool(const bool& value,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstDate(const int32& value,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstDateTime(const int64& value,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstString(const StringPiece& value,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstBinary(const StringPiece& value,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundConstDataType(const DataType& value,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundRandInt32(RandomBase* random_generator,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);


}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_INFRASTRUCTURE_TERMINAL_BOUND_EXPRESSIONS_H_
