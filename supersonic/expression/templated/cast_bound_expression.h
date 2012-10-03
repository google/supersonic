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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#ifndef SUPERSONIC_EXPRESSION_TEMPLATED_CAST_BOUND_EXPRESSION_H_
#define SUPERSONIC_EXPRESSION_TEMPLATED_CAST_BOUND_EXPRESSION_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// Creates a bound cast expression that casts child_ptr to to_type.
// The is_implicit argument determines (not surprisingly) whether the cast is
// implicit, which will cause the binding to fail if the cast would be a
// narrowing down-cast.
// Type-checking is done, the expression will fail (with a hopefully helpful
// message) if child_ptr cannot be cast to to_type.
// Assumes ownership of child_ptr.

class BoundExpression;
class BufferAllocator;

FailureOrOwned<BoundExpression> BoundInternalCast(
    BufferAllocator* allocator,
    rowcount_t row_capacity,
    BoundExpression* child_ptr,
    DataType to_type,
    bool is_implicit);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_TEMPLATED_CAST_BOUND_EXPRESSION_H_
