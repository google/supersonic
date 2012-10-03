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

#include "supersonic/expression/ext/hashing/hashing_expressions.h"

#include "supersonic/expression/ext/hashing/hashing_bound_expressions.h"
#include "supersonic/expression/ext/hashing/hashing_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/infrastructure/basic_expressions.h"

namespace supersonic {

class Expression;

const Expression* SupersonicFingerprint(const Expression* e) {
  return CreateExpressionForExistingBoundFactory(e, &BoundFingerprint,
                                                 "FINGERPRINT($0)");
}

const Expression* SupersonicHash(const Expression* e, const Expression* seed) {
  return CreateExpressionForExistingBoundFactory(e, seed,
                                                 &BoundHash, "HASH($0, $1)");
}

}  // namespace supersonic
