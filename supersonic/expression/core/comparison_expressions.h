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

#ifndef SUPERSONIC_EXPRESSION_CORE_COMPARISON_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_COMPARISON_EXPRESSIONS_H_

#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include "supersonic/utils/integral_types.h"

namespace supersonic {

// Creates an expression that will compare two subexpressions for equality.
// Returns NULL if any subexpression is NULL. Otherwise, returns a semantic
// equivalent of the "a == b" test.
class Expression;
class ExpressionList;

const Expression* Equal(const Expression* const a,
                        const Expression* const b);

// Creates an expression that will compare two subexpressions for inequality.
// Returns NULL if any subexpression is NULL. Otherwise, returns a semantic
// equivalent of the "a != b" test.
const Expression* NotEqual(const Expression* const a,
                           const Expression* const b);

// Creates an expression that will 'less'-compare two subexpressions.
// Returns NULL if any subexpression is NULL. Otherwise, returns a semantic
// equivalent of the "a < b" test.
const Expression* Less(const Expression* const a,
                       const Expression* const b);

// Creates an expression that will 'less-or-equal'-compare two subexpressions.
// Returns NULL if any subexpression is NULL. Otherwise, returns a semantic
// equivalent of the "a <= b" test.
const Expression* LessOrEqual(const Expression* const a,
                              const Expression* const b);

// Creates an expression that will 'greater'-compare two subexpressions.
// Returns NULL if any subexpression is NULL. Otherwise, returns a semantic
// equivalent of the "a > b" test.
const Expression* Greater(const Expression* const a,
                          const Expression* const b);

// Creates an expression that will 'greater-or-equal'-compare two
// subexpressions. Returns NULL if any subexpression is NULL. Otherwise,
// returns a semantic equivalent of the "a >= b" test.
const Expression* GreaterOrEqual(const Expression* const a,
                                 const Expression* const b);

// Creates an expression that will return true if the argument is odd.
// Requires the argument to be integer.
const Expression* IsOdd(const Expression* const arg);

// Creates an expression that will return true if the argument is even.
// Requires the argument to be integer.
const Expression* IsEven(const Expression* const arg);

// expr IN (value, ...)
//
// Returns true if expr is equal to any of the values in the IN list, else
// returns false. For constant values the search is done using a binary search.
// This means IN is very quick if the IN value list consists entirely of
// constants.
// To comply with the SQL standard, IN returns NULL not only if the expr is
// NULL, but also if no match is found in the list and one of the expressions
// in the list is NULL.
// Example: x in (col0, sqrt(col1), PI/2);

// Creates an IN expression as described above.
// Takes ownership of arguments.
const Expression* In(const Expression* const needle_expression,
                     const ExpressionList* haystack_arguments);
}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_COMPARISON_EXPRESSIONS_H_
