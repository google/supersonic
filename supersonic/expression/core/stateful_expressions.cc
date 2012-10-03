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

#include "supersonic/expression/core/stateful_expressions.h"

#include "supersonic/expression/core/stateful_bound_expressions.h"
#include "supersonic/expression/infrastructure/basic_expressions.h"

namespace supersonic {

const Expression* Changed(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundChanged, "CHANGED($0)");
}

const Expression* RunningSum(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundRunningSum, "RUNNING_SUM($0)");
}

const Expression* Smudge(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundSmudge, "SMUDGE($0)");
}

const Expression* RunningMinWithFlush(const Expression* const flush,
                                      const Expression* const input) {
  return CreateExpressionForExistingBoundFactory(
      flush, input, &BoundRunningMinWithFlush,
      "RUNNING_MIN_WITH_FLUSH($0, $1)");
}

const Expression* SmudgeIf(const Expression* const argument,
                           const Expression* const condition) {
  return CreateExpressionForExistingBoundFactory(
      argument, condition, &BoundSmudgeIf,
      "SMUDGE_IF($0, $1)");
}

}  // namespace supersonic
