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

#ifndef SUPERSONIC_EXPRESSION_TEMPLATED_CAST_EXPRESSION_H_
#define SUPERSONIC_EXPRESSION_TEMPLATED_CAST_EXPRESSION_H_

#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// Creates an expression that casts source to the prescribed datatype. The
// is_implicit argument determines (not surprisingly) whether the cast is
// implicit or explicit (the consequence being that downcasts will be
// disallowed in the implicit case, while allowed in the explicit case).
//
// Any type-checking will be done at binding-time, the expression creation
// never fails.
class Expression;

const Expression* InternalCast(DataType to_type,
                               const Expression* const source,
                               bool is_implicit);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_TEMPLATED_CAST_EXPRESSION_H_
