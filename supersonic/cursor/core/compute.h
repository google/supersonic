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

#ifndef SUPERSONIC_CURSOR_CORE_COMPUTE_H_
#define SUPERSONIC_CURSOR_CORE_COMPUTE_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

class BoundExpressionTree;
class Expression;
class BufferAllocator;
class Cursor;
class Operation;

// Creates a new compute operation with the given computation specification.
// Takes ownership of the computation and the child.
Operation* Compute(const Expression* computation, Operation* child);

// Creates a compute cursor directly. Takes ownership of the computation
// and the child. Does not take ownership of the allocator.
FailureOrOwned<Cursor> BoundCompute(BoundExpressionTree* computation,
                                    BufferAllocator* allocator,
                                    rowcount_t max_row_count,
                                    Cursor* child);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_COMPUTE_H_
