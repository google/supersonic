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

#ifndef SUPERSONIC_CURSOR_CORE_FOREIGN_FILTER_H_
#define SUPERSONIC_CURSOR_CORE_FOREIGN_FILTER_H_

namespace supersonic {

class SingleSourceProjector;
class Cursor;
class Operation;

// Filter projected via filter_key must produce a single INT64 NOT NULL column,
// that will be used to select rows by merging with the foreign key column in
// the input. Key values must be unique and ascending. The foreign key in the
// input is specified via the foreign_key projector, which must resolve to
// INT64 NOT NULL as well, and it must be ascending (but not necessarily
// unique).The resulting stream has the same schema as the input, with the
// values in the foreign_key column recomputed to be equal to row-id of
// matching rows in the 'filter' operation.
//
// As a special case, when filter is the identity (i.e. consists of numbers
// 0, 1, 2, ...), the result is equal to the input.
//
// This operation is intended primarily for implementing the structured filter.
//
// Example:
// input: [0, 'a'], [3, 'b'], [3, 'c'], [4, 'd'], [6, 'e']
// filter: [1], [3], [6]
// result: [0, 'b'], [0, 'c'], [1, 'e']
//
Operation* ForeignFilter(const SingleSourceProjector* filter_key,
                         const SingleSourceProjector* foreign_key,
                         Operation* filter,
                         Operation* input);

// 'Bound' version of the above, when schemas and cursors are already resolved.
// Key columns are specified simply by indicating column indexes in the
// filter and input cursors, respectively.
Cursor* BoundForeignFilter(const int filter_key_column,
                           const int input_foreign_key_column,
                           Cursor* filter,
                           Cursor* input);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_FOREIGN_FILTER_H_
