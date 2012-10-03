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

#ifndef SUPERSONIC_CURSOR_CORE_MERGE_UNION_ALL_H_
#define SUPERSONIC_CURSOR_CORE_MERGE_UNION_ALL_H_

#include <vector>
using std::vector;

#include "supersonic/base/exception/result.h"

namespace supersonic {

class Operation;
class SortOrder;
class BoundSortOrder;
class BufferAllocator;
class Cursor;

// Creates a new 'Merge Union All' operation that merges the specified inputs.
// The inputs must have type-identical schemas. The inputs must all be sorted
// according to the specified sort_order, or the results are undefined.
// Takes ownership of the sort_order and inputs. If there are no inputs, returns
// an empty operation.
// MergeUnionAll is deterministic. Given the same inputs it will always produce
// results in the same order.
Operation* MergeUnionAll(const SortOrder* sort_order,
                         const vector<Operation*>& inputs);

// Bound version of the above.
FailureOrOwned<Cursor> BoundMergeUnionAll(const BoundSortOrder* sort_order,
                                          vector<Cursor*> inputs,
                                          BufferAllocator* buffer_allocator);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_MERGE_UNION_ALL_H_
