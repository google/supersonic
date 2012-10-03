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
// Author:  onufry@google.com (Jakub Onufry Wojtaszczyk)

#include "supersonic/expression/core/arithmetic_expressions.h"

#include "supersonic/expression/core/arithmetic_bound_expressions.h"
#include "supersonic/expression/infrastructure/basic_expressions.h"

namespace supersonic {

// TODO(onufry): We should add a description<OperatorId>() function that would
// return the appropriate string of the ($0 + $1) type. This should go through
// traits, and in this way we would declare the description in a single place
// only.

// Expressions instantiation:
class Expression;

const Expression* Plus(const Expression* const left,
                       const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundPlus, "($0 + $1)");
}

const Expression* Minus(const Expression* const left,
                        const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundMinus, "($0 - $1)");
}

const Expression* Multiply(const Expression* const left,
                           const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundMultiply, "($0 * $1)");
}

const Expression* DivideSignaling(const Expression* const left,
                                  const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundDivideSignaling, "($0 /. $1)");
}

const Expression* DivideNulling(const Expression* const left,
                                const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundDivideNulling, "($0 /. $1)");
}

const Expression* DivideQuiet(const Expression* const left,
                              const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundDivideQuiet, "($0 /. $1)");
}

const Expression* CppDivideSignaling(const Expression* const left,
                                     const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundCppDivideSignaling, "($0 / $1)");
}

const Expression* CppDivideNulling(const Expression* const left,
                                   const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundCppDivideNulling, "($0 / $1)");
}

const Expression* Negate(const Expression* const child) {
  return CreateExpressionForExistingBoundFactory(
      child, &BoundNegate, "(-$0)");
}

const Expression* ModulusSignaling(const Expression* const left,
                                   const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundModulusSignaling, "$0 % $1");
}

const Expression* ModulusNulling(const Expression* const left,
                                 const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundModulusNulling, "$0 % $1");
}

// TODO(onufry): Delete these, in favor of policy-specific functions, and
// refactor our clients not to use them.
const Expression* Modulus(const Expression* const left,
                          const Expression* const right) {
  return ModulusSignaling(left, right);
}

const Expression* Divide(const Expression* const left,
                         const Expression* const right) {
  return DivideSignaling(left, right);
}

const Expression* CppDivide(const Expression* const left,
                            const Expression* const right) {
  return CppDivideSignaling(left, right);
}

}  // namespace supersonic
