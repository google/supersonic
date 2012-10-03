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
//
// Stateful expressions - that is expressions where the output results depends
// not only on the inputs, but also on the contents of some "state" which is
// kept within the expression and changes while processing. The state is
// persistent across calls to Evaluate/DoEvaluate, but is not persistent across
// different bindings.

#ifndef SUPERSONIC_EXPRESSION_CORE_STATEFUL_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_STATEFUL_EXPRESSIONS_H_

namespace supersonic {

class Expression;

// A boolean expression. Always true for the first (zeroeth) row in the stream.
// For subsequent rows, false if the value is equal to that of the preceding
// row, true otherwise.
//
// mind, and thus works only for not-nullable inputs. The proper semantic is not
// really obvious when the input becomes nullable. If you are interested in
// using this for nullable input, contact the Supersonic team with your
// proposition of semantics. For now, this will fail (binding time) if argument
// is nullable.
const Expression* Changed(const Expression* const argument);

// The argument is a numeric expression. The output is the sum of all the
// argument values so far. If the argument is NULL, it is ignored (treated as
// zero), except for the case where there were no non-null values yet, in which
// case the output is NULL.
const Expression* RunningSum(const Expression* const argument);

// The argument is any expression. If the argument is not null, the output is
// equal to the argument, if the argument is null, then the output is equal to
// the last non-null argument. If no non-null argument appeared yet, the output
// is NULL.
// Note that this is a running aggregation, where the aggregation used is Last.
const Expression* Smudge(const Expression* const argument);

// Takes an integer expression and a boolean expression and returns the minimum
// value observed from the integer expression since the last true value seen
// from the boolean expression. The integer expression may be nullable. The
// output will be null until a non-null value is encountered from the input and
// null input values will be ignored once a non-null input is received. The
// boolean expression may not be nullable. If it is, the expression will fail at
// binding time.
const Expression* RunningMinWithFlush(const Expression* const flush,
                                      const Expression* const input);

// Takes an argument and a condition. The condition must be a non-nullable
// boolean expression while the argument is an arbitrary one-column
// expression. If the condition is false, or if it is the first row of argument,
// the output is equal to the argument. Otherwise, the output is equal to the
// previous output.
const Expression* SmudgeIf(const Expression* const argument,
                           const Expression* const condition);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_STATEFUL_EXPRESSIONS_H_
