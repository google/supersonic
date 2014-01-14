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
// Expressions used to select a subset of columns from the input

#ifndef SUPERSONIC_EXPRESSION_CORE_PROJECTING_BOUND_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_PROJECTING_BOUND_EXPRESSIONS_H_

#include <stddef.h>
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

// Creates an expression that will return projected input attributes.
class BoundExpression;
class BoundExpressionList;
class BoundMultiSourceProjector;
class BufferAllocator;
class SingleSourceProjector;
class TupleSchema;

FailureOrOwned<BoundExpression> BoundInputAttributeProjection(
    const TupleSchema& schema,
    const SingleSourceProjector& projector);

// Creates a bound expression that will return the attribute from the selected
// position of the input.
FailureOrOwned<BoundExpression> BoundAttributeAt(const TupleSchema& schema,
                                                 size_t position);

// Creates a bound expression that will return the value of a specified
// attribute in the input view.
FailureOrOwned<BoundExpression> BoundNamedAttribute(const TupleSchema& schema,
                                                    const string& name);

// Creates an expression that takes a single-column-child argument and renames
// it (without changing the data, type or nullability).
FailureOrOwned<BoundExpression> BoundAlias(const string& new_name,
                                           BoundExpression* argument,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

// Creates an expression that will return a projection over its child
// expressions.
FailureOrOwned<BoundExpression> BoundProjection(
    const BoundMultiSourceProjector* projector,
    BoundExpressionList* arguments);

// Creates an expression that is compound by given set of expressions.
FailureOrOwned<BoundExpression> BoundCompoundExpression(
    BoundExpressionList* expressions);

// Creates an expression that is compound by given set of expressions, renames
// all columns to given names.
FailureOrOwned<BoundExpression> BoundRenameCompoundExpression(
    const vector<string>& names,
    BoundExpressionList* expressions);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_PROJECTING_BOUND_EXPRESSIONS_H_
