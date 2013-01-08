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

#ifndef SUPERSONIC_CURSOR_CORE_FILTER_H_
#define SUPERSONIC_CURSOR_CORE_FILTER_H_

#include "supersonic/base/exception/result.h"

namespace supersonic {

class BoundExpressionTree;
class BoundSingleSourceProjector;
class BufferAllocator;
class Cursor;
class Expression;
class Operation;
class SingleSourceProjector;

// Creates new filter operation with the given predicate.
// The projector specifies the attributes to keep after filtering (use
// ProjectAllAttributes() if you don't want to change the schema).
// Takes ownership of predicate, projector and child.
Operation* Filter(const Expression* predicate,
                  const SingleSourceProjector* projector,
                  Operation* child);

// Creates new filter cursor.
// The projector specifies the attributes to keep after filtering (use
// ProjectAllAttributes() if you don't want to change the schema).
// Takes ownership of predicate, projector and child, doesn't take
// ownership of allocator.
FailureOrOwned<Cursor> BoundFilter(BoundExpressionTree* predicate,
                                   const BoundSingleSourceProjector* projector,
                                   BufferAllocator* buffer_allocator,
                                   Cursor* child_cursor);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_FILTER_H_
