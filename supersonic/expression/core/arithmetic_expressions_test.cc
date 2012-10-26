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

#include "supersonic/expression/core/arithmetic_expressions.h"

#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"

namespace supersonic {

TEST(ArithmeticExpressionTest, NegateBinding) {
  TestUnaryBinding(&Negate,  DOUBLE, "(-$0)",  DOUBLE, false);
  TestUnaryBinding(&Negate,  INT32,  "(-$0)",  INT32,  false);
  TestUnaryBinding(&Negate,  UINT32, "(-$0)",  INT32,  false);

  TestBindingFailure(&Negate,  DATETIME);
  TestBindingFailure(&Negate,  STRING);
}

TEST(ArithmeticExpressionTest, Negate) {
  TestEvaluation(BlockBuilder<FLOAT, FLOAT>()
      .AddRow(3.,     -3.)
      .AddRow(0.,      0.)
      .AddRow(-3.,     3.)
      .AddRow(11.2, -11.2)
      .Build(), &Negate);

  TestEvaluation(BlockBuilder<UINT64, INT64>()
      .AddRow(0LL,     0LL)
      .AddRow(4LL,     -4LL)
      .AddRow(12314LL, -12314LL)
      .Build(), &Negate);

  TestEvaluation(BlockBuilder<UINT32, INT32>()
      .AddRow(13, -13)
      .AddRow(__,  __)
      .AddRow(0,    0)
      .Build(), &Negate);
}

// Note - in these tests the "PLUS" operator was chosen as an archetypical
// binary expression, and a large number of tests are performed only for PLUS.
// Note that these are _not_ unit tests - they test a whole large stack of
// calls, each with it's own unit tests. Thus, one should not be surprised
// that not all expressions possess such a robust testing scheme.
TEST(BinaryExpressionTest, PlusBindingSuccess) {
  TestBinaryBindingNotNullable(&Plus, INT64, INT64, "($0 + $1)", INT64);
  TestBindingWithNull(
      &Plus, INT64, false, INT64, true, "($0 + $1)", INT64, true);
  TestBinaryBindingNotNullable(&Plus, UINT64, FLOAT,
      "(CAST_UINT64_TO_DOUBLE($0) + CAST_FLOAT_TO_DOUBLE($1))", DOUBLE);
}

TEST(BinaryExpressionTest, Plus) {
  TestEvaluation(BlockBuilder<INT64, INT64, INT64>()
      .AddRow(-1, 1, 0)
      .AddRow(-2, 2, 0)
      .AddRow(2,  2, 4)
      .AddRow(13, 1, 14)
      .Build(), &Plus);
}

TEST(BinaryExpressionTest, PlusNullable) {
  TestEvaluation(BlockBuilder<INT64, INT64, INT64>()
      .AddRow(1,  __, __)
      .AddRow(-1,  2, 1)
      .AddRow(__,  2, __)
      .Build(), &Plus);
}

TEST(BinaryExpressionTest, PlusLeftColumnNullable) {
  TestEvaluation(BlockBuilder<INT64, INT64, INT64>()
      .AddRow(-1, 2,  1)
      .AddRow(-1, -2, -3)
      .AddRow(__, 2,  __)
      .Build(), &Plus);
}

TEST(BinaryExpressionTest, PlusDifferentTypes) {
  TestEvaluation(BlockBuilder<INT64, INT32, INT64>()
      .AddRow(-1, 1, 0)
      .AddRow(-2, 2, 0)
      .AddRow(2,  2, 4)
      .AddRow(3,  1, 4)
      .Build(), &Plus);
}

TEST(BinaryExpressionTest, BindingSuccess) {
  TestBinaryBindingNotNullable(
      &Minus,              INT64,  INT64,  "($0 - $1)",   INT64);
  TestBinaryBindingNotNullable(
      &Multiply,           INT64,  INT64,  "($0 * $1)",   INT64);
  TestBinaryBindingNotNullable(
      &DivideSignaling,    DOUBLE, DOUBLE, "($0 /. $1)",  DOUBLE);
  TestBinaryBindingNotNullable(
      &DivideQuiet,        DOUBLE, DOUBLE, "($0 /. $1)",  DOUBLE);
  TestBinaryBindingNotNullable(
      &CppDivideSignaling, INT32,  INT32,  "($0 / $1)",   INT32);
  TestBinaryBindingNotNullable(
      &ModulusSignaling,   UINT32, UINT32, "($0 % $1)",   UINT32);

  TestBinaryBindingNullable(
      &CppDivideNulling,   INT64,  INT64,  "($0 / $1)",   INT64);
  TestBinaryBindingNullable(
      &ModulusNulling,     INT32,  INT32,  "($0 % $1)",   INT32);
  TestBinaryBindingNullable(
      &DivideNulling,      DOUBLE, DOUBLE, "($0 /. $1)",  DOUBLE);

  TestBinaryBindingNullable(
      &ModulusNulling, INT64, INT32, "($0 % CAST_INT32_TO_INT64($1))", INT64);
}

TEST(BinaryExpressionTest, Minus) {
  TestEvaluation(BlockBuilder<INT64, INT32, INT64>()
      .AddRow(1,  __, __)
      .AddRow(2,  -1, 3)
      .AddRow(-1, 2,  -3)
      .AddRow(__, 2,  __)
      .Build(), &Minus);
}

TEST(BinaryExpressionTest, Multiply) {
  TestEvaluation(BlockBuilder<INT64, INT32, INT64>()
      .AddRow(1,  __, __)
      .AddRow(2,  2,  4)
      .AddRow(2,  0,  0)
      .AddRow(20, 20, 400)
      .AddRow(__, 2,  __)
      .Build(), &Multiply);
}

TEST(BinaryExpressionTest, DivideQuiet) {
  TestEvaluation(BlockBuilder<INT64, INT32, DOUBLE>()
      .AddRow(2, 2, 1.)
      .AddRow(3, 1, 3.)
      .AddRow(1, 2, 0.5)
      .AddRow(1, 0, 1./0.)  // inf.
      .AddRow(0, 0, 0./0.)  // nan.
      .Build(), &DivideQuiet);
}

TEST(BinaryExpressionTest, DivideNulling) {
  TestEvaluation(BlockBuilder<INT64, FLOAT, DOUBLE>()
      .AddRow(2, 2, 1.)
      .AddRow(3, 1, 3.)
      .AddRow(1, 2, 0.5)
      .AddRow(1, 0, __)
      .AddRow(0, 0, __)
      .Build(), &DivideNulling);
}

TEST(BinaryExpressionTest, DivideSignaling) {
  TestEvaluation(BlockBuilder<INT64, FLOAT, DOUBLE>()
      .AddRow(2, 2, 1.)
      .AddRow(3, 1, 3.)
      .AddRow(1, 2, 0.5)
      .Build(), &DivideNulling);

  TestEvaluationFailure(BlockBuilder<DOUBLE, INT32>()
      .AddRow(1, 0)
      .AddRow(0, 0)
      .Build(), &DivideSignaling);
}

TEST(BinaryExpressionTest, CppDivideNulling) {
  TestEvaluation(BlockBuilder<INT64, INT32, INT64>()
      .AddRow(5,  2, 2)
      .AddRow(2,  2, 1)
      .AddRow(-3, 1, -3)
      .AddRow(3,  0, __)
      .AddRow(0,  3, 0)
      .Build(), &CppDivideNulling);
}

TEST(BinaryExpressionTest, CppDivideSignaling) {
  TestEvaluation(BlockBuilder<INT32, INT32, INT32>()
      .AddRow(5,  2, 2)
      .AddRow(2,  2, 1)
      .AddRow(-3, 1, -3)
      .AddRow(0,  3, 0)
      .AddRow(__, 0, __)
      .AddRow(0, __, __)
      .Build(), &CppDivideSignaling);

  TestEvaluationFailure(BlockBuilder<INT32, INT32>()
      .AddRow(3, 0)
      .AddRow(0, 0)
      .AddRow(-1, 0)
      .Build(), &CppDivideSignaling);
}

TEST(BinaryExpressionTest, ModulusNulling) {
  TestEvaluation(BlockBuilder<INT32, INT32, INT32>()
      .AddRow(5,  2,  1)
      .AddRow(-1, 5, -1)
      .AddRow(0,  3,  0)
      .AddRow(7,  5,  2)
      .AddRow(__, 4, __)
      .AddRow(4,  0, __)
      .AddRow(4, -3, 1)
      .Build(), &ModulusNulling);
}

TEST(BinaryExpressionTest, ModulusSignaling) {
  TestEvaluation(BlockBuilder<INT32, INT32, INT32>()
      .AddRow(5,  2,  1)
      .AddRow(-1, 5, -1)
      .AddRow(0,  3,  0)
      .AddRow(7,  5,  2)
      .AddRow(__, 4, __)
      .AddRow(-4, -3, -1)
      .Build(), &ModulusSignaling);

  TestEvaluationFailure(BlockBuilder<INT64, INT64>()
      .AddRow(1, 0)
      .AddRow(0, 0)
      .Build(), &ModulusSignaling);
}

}  // namespace supersonic
