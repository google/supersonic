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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)
//
// Exposes functions used to test the behaviour of short circuit for
// expressions.

#ifndef SUPERSONIC_TESTING_SHORT_CIRCUIT_TESTER_H_
#define SUPERSONIC_TESTING_SHORT_CIRCUIT_TESTER_H_

#include "supersonic/expression/infrastructure/bound_expression_creators.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "gtest/gtest.h"

namespace supersonic {

class Block;
class BoundExpression;
class BufferAllocator;
class Expression;

// Testers for short circuit behaviour in expressions.
//
// The input block should contain:
// - a non-nullable boolean column which will be used as the input skip vector;
// - for each input of the expression, two columns. The first contains the
// values in this column, the second contains the expected skip vector coming
// into the child.
// - the expected output.
//
// Sample Usage:
//
// TestShortCircuitBinary(
//     BlockBuilder<BOOL, STRING, BOOL, STRING, BOOL, STRING>()
//     .AddRow(false, "a", false, "b", true,  "a")
//     .AddRow(true,  "a", true,  "a", true,  __)
//     .AddRow(false, __,  false, "a", false, "a")
//     .AddRow(false, __,  false, __,  false, __)
//     .Build(), &IfNull);
//
// This checks that the left input of the IfNull function is evaluated whenever
// the incoming skip vector is not set, while the right input is evaluated if
// the incoming skip vector is not set and the left side is NULL.
void TestShortCircuitUnary(const Block* block, UnaryExpressionCreator factory);
void TestShortCircuitBinary(const Block* block,
                            BinaryExpressionCreator factory);
void TestShortCircuitTernary(const Block* block,
                             TernaryExpressionCreator factory);

// Testing expression creators. Not to be used, exposed for testing only.
const Expression* Skipper(const Expression* skip_vector,
                          const Expression* input);

const Expression* SkipVectorExpectation(const Expression* expected,
                                        const Expression* input);

}  // namespace supersonic

#endif  // SUPERSONIC_TESTING_SHORT_CIRCUIT_TESTER_H_
