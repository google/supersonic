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
// Basic arithmetic expressions.

#ifndef SUPERSONIC_EXPRESSION_CORE_ARITHMETIC_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_ARITHMETIC_EXPRESSIONS_H_



namespace supersonic {

// Floating point arithmetics is handled according to IEEE 754, i.e.
// infinite and NaN values can result.

// Creates an expression that will return a sum of two subexpressions.
class Expression;

const Expression* Plus(const Expression* const a,
                       const Expression* const b);

// Creates an expression that will return a difference of two subexpressions.
const Expression* Minus(const Expression* const a,
                        const Expression* const b);

// Creates an expression that will return a product of two subexpressions.
const Expression* Multiply(const Expression* const a,
                           const Expression* const b);

// Creates an expression that will return a ratio of two subexpressions.
//
// DEPRECATED. Use one of the policy-conscious types instead.
const Expression* Divide(const Expression* const a,
                         const Expression* const b);

// Creates an expression that will return a ratio of two subexpressions. Will
// fail the evaluation at division by zero.
const Expression* DivideSignaling(const Expression* const a,
                                  const Expression* const b);

// This version assumes x / 0 = NULL.
const Expression* DivideNulling(const Expression* const a,
                                const Expression* const b);

// This version assumes division as in CPP (so x / 0 is an inf, -inf or nan,
// depending on x).
const Expression* DivideQuiet(const Expression* const a,
                              const Expression* const b);

// TODO(onufry): This expression should be removed in favour of an
// IntegerDivide, which will take only integer arguments.
// Creates an expression that will return the division result (rounded in case
// of integers). Examples: 5 / 2 = 2, but 5.0 / 2 = 2.5 and 5 / 2.0 = 2.5.
//
// DEPRECATED! Use one of the policy-concious types instead.
const Expression* CppDivide(const Expression* const a,
                            const Expression* const b);

// This version assumes x / 0 = NULL.
const Expression* CppDivideNulling(const Expression* const a,
                                   const Expression* const b);

// This version assumes x / 0 fails.
const Expression* CppDivideSignaling(const Expression* const a,
                                     const Expression* const b);

// Creates an expression that negates a number.
const Expression* Negate(const Expression* const a);

// Creates an expression that will return a modulus of two integer
// subexpressions.
const Expression* Modulus(const Expression* const a,
                          const Expression* const b);

// This version assumes x % 0 == NULL.
const Expression* ModulusNulling(const Expression* const a,
                                 const Expression* const b);

// This version assumes x % 0 fails.
const Expression* ModulusSignaling(const Expression* const a,
                                   const Expression* const b);

// Creates an expression that will return true if the argument is odd.
// Requires the argument to be integer.
const Expression* IsOdd(const Expression* const arg);

// Creates an expression that will return true if the argument is even.
// Requires the argument to be integer.
const Expression* IsEven(const Expression* const arg);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_ARITHMETIC_EXPRESSIONS_H_
