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
// Author: onufry@google.com (Onufry Wojtaszczyk)
//
// Template constructors for bound string expressions (which we do not want
// to make visible in public).

#ifndef SUPERSONIC_EXPRESSION_CORE_REGEXP_BOUND_EXPRESSIONS_INTERNAL_H_
#define SUPERSONIC_EXPRESSION_CORE_REGEXP_BOUND_EXPRESSIONS_INTERNAL_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

// Creates a Regexp-type expression (taking a StringPiece, which is converted
// to a RegExp pattern, and a single STRING argument). Used to avoid code
// duplication between Full and Partial match expressions.
class BoundExpression;
class BufferAllocator;

template<OperatorId operation_type>
FailureOrOwned<BoundExpression> BoundGeneralRegexp(BoundExpression* str,
                                                   const StringPiece& pattern,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_REGEXP_BOUND_EXPRESSIONS_INTERNAL_H_
