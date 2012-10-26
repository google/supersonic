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

#include "supersonic/expression/core/arithmetic_expressions.h"
#include "supersonic/expression/core/elementary_expressions.h"
#include "supersonic/expression/infrastructure/terminal_expressions.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

// Case is a variadic expression. To test it with the evaluation engine, we
// created fixed-arity constructors of Case expressions, as above, and apply the
// testing suite to those expressions.
// This one is SWITCH(X) { CASE(1) : "one"; CASE(2) : "two"; default : "other" }
// where X is the input variable.
class Expression;

const Expression* BasicInt32ToStringCase(const Expression* selector) {
  return Case(MakeExpressionList(util::gtl::Container(
      selector, ConstString("other"),
      ConstInt32(1), ConstString("one"),
      ConstInt32(2), ConstString("two"))));
}

TEST(CaseExpressionTest, BasicInt32ToString) {
  TestEvaluation(BlockBuilder<INT32, STRING>()
      .AddRow(1,  "one")
      .AddRow(2,  "two")
      .AddRow(3,  "other")
      .AddRow(4,  "other")
      .AddRow(5,  "other")
      .AddRow(__, "other")
      .Build(), &BasicInt32ToStringCase);
}

// This is SWITCH(X) { case "C" : 1; case "E" : NULL; default : Y }
// where X and Y are the inputs.
const Expression* StringToUInt32Case(const Expression* selector,
                                     const Expression* otherwise) {
  return Case(MakeExpressionList(util::gtl::Container(
      selector, otherwise,
      ConstString("one"), ConstUint32(1),
      ConstString("null"), Null(UINT32))));
}

TEST(CaseExpressionTest, StringToUInt32) {
  TestEvaluation(BlockBuilder<STRING, UINT32, UINT32>()
      .AddRow("zero", 10, 10)
      .AddRow("nine",  9,  9)
      .AddRow("one",   8,  1)
      .AddRow("NULL",  7,  7)
      .AddRow("null",  6, __)
      .AddRow(__,     __, __)
      .Build(), &StringToUInt32Case);
}

// This is SWITCH(X) { case 3 : 4; case 5.0 : 10.0; deafult: Y }
// where X and Y are the inputs, checks whether the types get reconciled.
const Expression* ForceCastCase(const Expression* selector,
                                const Expression* otherwise) {
  return Case(MakeExpressionList(util::gtl::Container(
      // INT32, UINT32
      selector, otherwise,
      // INT64, UINT64
      ConstInt64(3), ConstUint64(4),
      // FLOAT, DOUBLE
      ConstFloat(5.0), ConstDouble(10.0))));
}

TEST(CaseExpressionTest, ForceCast) {
  TestEvaluation(BlockBuilder<INT32, UINT32, DOUBLE>()
      .AddRow(1,  10, 10.0)
      .AddRow(2,   9,  9.0)
      .AddRow(3,   8,  4.0)
      .AddRow(4,   7,  7.0)
      .AddRow(5,   6, 10.0)
      .AddRow(__, __,   __)
      .Build(), &ForceCastCase);
}

// This is the SWITCH(X) { case true: Y; default: Z }
// It's equivalent to IF X THEN Y ELSE Z; and the latter should be preferred.

const Expression* IfCase(const Expression* condition,
                         const Expression* then,
                         const Expression* otherwise) {
  return Case(MakeExpressionList(util::gtl::Container(
      condition, otherwise, ConstBool(true), then)));
}

TEST(CaseExpressionTest, BasicIfCase) {
  TestEvaluation(BlockBuilder<BOOL, STRING, STRING, STRING>()
      .AddRow(true,  "A",  "B",  "A")
      .AddRow(false, "C",  "D",  "D")
      .AddRow(true,  "E",  "F",  "E")
      .Build(), &IfCase);
}

TEST(CaseExpressionTest, IfCaseWithNullCondition) {
  TestEvaluation(BlockBuilder<BOOL, STRING, STRING, STRING>()
      .AddRow(false, "A", "B", "B")
      .AddRow(__,    "C", "D", "D")
      .AddRow(true,  "E", "F", "E")
      .Build(), &IfCase);
}

TEST(CaseExpressionTest, IfCaseWithNullThen) {
  TestEvaluation(BlockBuilder<BOOL, STRING, STRING, STRING>()
      .AddRow(true,  "A", "B", "A")
      .AddRow(true,  __,  "D", __)
      .AddRow(false, __,  "F", "F")
      .Build(), &IfCase);
}

TEST(CaseExpressionTest, IfCaseWithNullOtherwise) {
  TestEvaluation(BlockBuilder<BOOL, STRING, STRING, STRING>()
      .AddRow(false, "A",  __,  __)
      .AddRow(true,  "C", "D", "C")
      .AddRow(false, "E", "F", "F")
      .Build(), &IfCase);
}

TEST(CaseExpressionTest, IfCaseWithConditionAndThenNullable) {
  TestEvaluation(BlockBuilder<BOOL, STRING, STRING, STRING>()
      .AddRow(true,  "A", "B", "A")
      .AddRow(false, "C", "D", "D")
      .AddRow(true,   __, "F",  __)
      .AddRow(false,  __, "H", "H")
      .AddRow(__,    "I", "J", "J")
      .AddRow(__,     __, "L", "L")
      .AddRow(false, "M", "N", "N")
      .Build(), &IfCase);
}

TEST(CaseExpressionTest, IfCaseWithConditionAndOtherwiseNullable) {
  TestEvaluation(BlockBuilder<BOOL, STRING, STRING, STRING>()
      .AddRow(true,  "A", "B", "A")
      .AddRow(false, "C", "D", "D")
      .AddRow(true,  "E",  __, "E")
      .AddRow(false, "G",  __,  __)
      .AddRow(__,    "I", "J", "J")
      .AddRow(__,    "K",  __,  __)
      .AddRow(false, "M", "N", "N")
      .Build(), &IfCase);
}

TEST(CaseExpressionTest, IfCaseWithThenAndOtherWiseNullable) {
  TestEvaluation(BlockBuilder<BOOL, STRING, STRING, STRING>()
      .AddRow(true,  "A", "B", "A")
      .AddRow(false, "C", "D", "D")
      .AddRow(true,   __, "F",  __)
      .AddRow(false,  __, "H", "H")
      .AddRow(true,  "I",  __, "I")
      .AddRow(false, "K",  __,  __)
      .AddRow(true,   __,  __,  __)
      .AddRow(false,  __,  __,  __)
      .AddRow(false, "M", "N", "N")
      .Build(), &IfCase);
}

TEST(CaseExpressionTest, IfCaseAllNullable) {
  TestEvaluation(BlockBuilder<BOOL, STRING, STRING, STRING>()
      .AddRow(true,  "A", "B", "A")
      .AddRow(false, "C", "D", "D")
      .AddRow(true,   __, "F",  __)
      .AddRow(false,  __, "H", "H")
      .AddRow(true,  "I",  __, "I")
      .AddRow(false, "K",  __,  __)
      .AddRow(true,   __,  __,  __)
      .AddRow(false,  __,  __,  __)
      .AddRow(__,    "M", "N", "N")
      .AddRow(__,     __, "P", "P")
      .AddRow(__,    "R",  __,  __)
      .AddRow(__,     __,  __,  __)
      .AddRow(false, "W", "Y", "Y")
      .Build(), &IfCase);
}

// A short circuit tester expression for Case.
// This is
// A + SWITCH(B) { case (C) : D%0; default : E%0 }
// where A, B, C, D, E are the inputs in the order given.
const Expression* CaseShortCircuit(const Expression* top,
                                   const Expression* selector,
                                   const Expression* when,
                                   const Expression* then,
                                   const Expression* else_expr) {
  return Plus(top,
              Case(MakeExpressionList(
                  util::gtl::Container(
                      selector,
                      ModulusSignaling(
                          ConstInt32(0),
                          else_expr),
                      when,
                      ModulusSignaling(
                          ConstInt32(0),
                          then)))));
}

// The idea here is to check whether the (failing) expressions D%0 and/or E%0
// get evaluated - if they do, we will get a failure.
// We do not evaluate them in the following cases:
// 1) When A is NULL, because then the whole case shouldn't be evaluated, as the
// addition will evaluate to NULL anyway.
// 2) The LHS (D%0) should not be evaluated if B=/=C, and, in particular, if any
// of those two is NULL. It should also not be evaluated if D = NULL (the
// semantics is that a failing expression does not fail if one of the arguments
// NULLs)
// 3) The RHS (E%0) should not be evaluated if B=C, and if E = NULL.
// On the other hand we should get failures if B=C even if E = NULL, and if
// B=/=C, even if D = NULL.
TEST(CaseExpressionTest, CaseShortCircuit) {
  TestEvaluation(BlockBuilder<INT32, INT32, INT32, INT32, INT32, INT32>()
      .AddRow(__,  1,  1,  0,  0, __)
      .AddRow(__,  1,  2,  0,  0, __)
      .AddRow(__, __,  2,  0,  0, __)
      .AddRow(__,  1, __,  0,  0, __)
      .AddRow(__, __, __,  0,  0, __)
      .AddRow(0,   1,  1,  1,  0,  0)
      .AddRow(0,   1,  2,  0,  1,  0)
      .AddRow(0,  __,  1,  0,  1,  0)
      .AddRow(0,  __, __,  0,  1,  0)
      .AddRow(0,   1, __,  0,  1,  0)
      .AddRow(0,   1,  1, __,  0, __)
      .AddRow(0,   1,  2,  0, __, __)
      .AddRow(0,  __,  1,  0, __, __)
      .Build(), &CaseShortCircuit);

  TestEvaluationFailure(BlockBuilder<INT32, INT32, INT32, INT32, INT32>()
      .AddRow(0,  1,  1,  0,  __)
      .AddRow(0,  1,  2, __,   0)
      .AddRow(0, __, __, __,   0)
      .Build(), &CaseShortCircuit);
}

}  // namespace supersonic
