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

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/projecting_expressions.h"
#include "supersonic/expression/infrastructure/bound_expression_creators.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/expression_test_helper.h"
#include "supersonic/testing/short_circuit_tester.h"
#include "gtest/gtest.h"

namespace supersonic {

namespace {

TEST(StatefulExpressionsTest, ChangedBinding) {
  TestBindingWithNull(&Changed, INT32, false, "CHANGED($0)", BOOL, false);
  TestBindingWithNull(&Changed, STRING, false, "CHANGED($0)", BOOL, false);
  TestBindingWithNull(&Changed, DATA_TYPE, false, "CHANGED($0)", BOOL, false);

  TestUnaryBindingFailureWithNulls(&Changed, INT32, true);
  TestUnaryBindingFailureWithNulls(&Changed, BOOL, true);
}

TEST(StatefulExpressionsTest, ChangedEvaluation) {
  TestStatefulEvaluation(BlockBuilder<INT32, BOOL>()
      .AddRow(1, true)
      .AddRow(1, false)
      .AddRow(2, true)
      .AddRow(1, true)
      .AddRow(1, false)
      .AddRow(7, true)
      .AddRow(0, true)
      .AddRow(0, false)
      .AddRow(0, false)
      .AddRow(0, false)
      .AddRow(1, true)
      .Build(), &Changed);
}

TEST(StatefulExpressionsTest, ChangedConsistentAcrossBlocksForStrings) {
  TestStatefulEvaluation(BlockBuilder<STRING, BOOL>()
      .AddRow("T", true)
      .AddRow("e", true)
      .AddRow("n", true)
      .AddRow("n", false)
      .AddRow("e", true)
      .AddRow("s", true)
      .AddRow("s", false)
      .AddRow("e", true)
      .AddRow("e", false)
      .Build(), &Changed);
}

TEST(StatefulExpressionsTest, ChangedConsistentAcrossBlocksForLongBinaries) {
  TestStatefulEvaluation(BlockBuilder<BINARY, BOOL>()
      .AddRow("", true)
      .AddRow("a", true)
      .AddRow("Some pretty long string goes here", true)
      .AddRow("Some pretty long string goes here", false)
      .AddRow("Some pretty long string", true)
      .AddRow("", true)
      .AddRow("", false)
      .Build(), &Changed);
}

TEST(StatefulExpressionsTest, ChangedConsistentAcrossBlocksForDataTypes) {
  TestStatefulEvaluation(BlockBuilder<DATA_TYPE, BOOL>()
      .AddRow(INT32, true)
      .AddRow(INT64, true)
      .AddRow(INT64, false)
      .AddRow(DATA_TYPE, true)
      .Build(), &Changed);

  TestStatefulEvaluation(BlockBuilder<DATA_TYPE, BOOL>()
      .AddRow(INT64, true)
      .AddRow(INT64, false)
      .AddRow(INT64, false)
      .Build(), &Changed);
}

TEST(StatefulExpressionsTest, ChangedConsistentAcrossBlocksForBools) {
  TestStatefulEvaluation(BlockBuilder<BOOL, BOOL>()
      .AddRow(true, true)
      .AddRow(true, false)
      .AddRow(false, true)
      .AddRow(false, false)
      .Build(), &Changed);

  TestStatefulEvaluation(BlockBuilder<BOOL, BOOL>()
      .AddRow(false, true)
      .AddRow(false, false)
      .AddRow(true, true)
      .AddRow(true, false)
      .Build(), &Changed);
}

TEST(StatefulExpressionsTest, ChangedShortCircuit) {
  TestShortCircuitUnary(BlockBuilder<BOOL, INT32, BOOL, BOOL>()
      .AddRow(true,  12, false, __)
      .AddRow(false, 12, false, false)
      .AddRow(false, 13, false, true)
      .AddRow(true,  14, false, __)
      .AddRow(false, 13, false, true)
      .AddRow(true,  13, false, __)
      .Build(), &Changed);
}

TEST(StatefulExpressionsTest, RunningSumBinding) {
  TestBindingWithNull(&RunningSum, INT32, false, "RUNNING_SUM($0)",
                      INT32, false);
  TestBindingWithNull(&RunningSum, DOUBLE, true, "RUNNING_SUM($0)",
                      DOUBLE, true);

  TestUnaryBindingFailureWithNulls(&RunningSum, DATE, false);
  TestUnaryBindingFailureWithNulls(&RunningSum, STRING, true);
}

TEST(StatefulExpressionsTest, RunningSum) {
  TestStatefulEvaluation(BlockBuilder<INT32, INT32>()
      .AddRow(__, __)
      .AddRow(3,  3)
      .AddRow(3,  6)
      .AddRow(0,  6)
      .AddRow(-2, 4)
      .AddRow(__, 4)
      .AddRow(4,  8)
      .Build(), &RunningSum);
}

TEST(StatefulExpressionsTest, RunningSumNoNulls) {
  TestStatefulEvaluation(BlockBuilder<UINT32, UINT32>()
      .AddRow(1, 1)
      .AddRow(2, 3)
      .AddRow(3, 6)
      .AddRow(2, 8)
      .AddRow(1, 9)
      .Build(), &RunningSum);
}

TEST(StatefulExpressionsTest, RunningSumAcrossBlocks) {
  TestStatefulEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,  1.)
      .AddRow(2.,  3.)
      .AddRow(__,  3.)
      .AddRow(-3., 0.)
      .AddRow(2.,  2.)
      .Build(), &RunningSum);
}

TEST(StatefulExpressionsTest, RunningSumAcrossBlocksWithNoNulls) {
  TestStatefulEvaluation(BlockBuilder<FLOAT, FLOAT>()
      .AddRow(1., 1.)
      .AddRow(8., 9.)
      .AddRow(0., 9.)
      .Build(), &RunningSum);
}

TEST(StatefulExpressionsTest, RunningSumShortCircuit) {
  TestShortCircuitUnary(BlockBuilder<BOOL, INT32, BOOL, INT32>()
      .AddRow(true,  1, false, __)
      .AddRow(true,  2, false, __)
      .AddRow(false, 3, false, 6)
      .AddRow(true,  4, false, __)
      .AddRow(false, 5, false, 15)
      .Build(), &RunningSum);

  TestShortCircuitUnary(BlockBuilder<BOOL, INT32, BOOL, INT32>()
      .AddRow(false, __, false, __)
      .AddRow(true,  __, false, __)
      .AddRow(false, __, false, __)
      .AddRow(true,   3, false, __)
      .AddRow(true,  __, false, __)
      .AddRow(false, __, false, 3)
      .Build(), &RunningSum);
}

TEST(StatefulExpressionsTest, SmudgeBinding) {
  TestBindingWithNull(&Smudge, INT32, false, "SMUDGE($0)", INT32, false);
  TestBindingWithNull(&Smudge, DOUBLE, true, "SMUDGE($0)", DOUBLE, true);
  TestBindingWithNull(&Smudge, STRING, true, "SMUDGE($0)", STRING, true);
}

TEST(StatefulExpressionsTest, Smudge) {
  TestStatefulEvaluation(BlockBuilder<DATE, DATE>()
      .AddRow(__,    __)
      .AddRow(12,    12)
      .AddRow(__,    12)
      .AddRow(__,    12)
      .AddRow(13,    13)
      .AddRow(__,    13)
      .Build(), &Smudge);
}

TEST(StatefulExpressionsTest, SmudgeWithNoNulls) {
  // This is a pretty useless case, as this is a no-op, but it could happen.
  TestStatefulEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("World",      "World")
      .AddRow("Task",       "Task")
      .AddRow("Force",      "Force")
      .Build(), &Smudge);
}

TEST(StatefulExpressionsTest, SmudgeAcrossBlocksNumbers) {
  TestStatefulEvaluation(BlockBuilder<INT64, INT64>()
      .AddRow(__,       __)
      .AddRow(__,       __)
      .AddRow(1LL,      1LL)
      .AddRow(__,       1LL)
      .AddRow(2LL,      2LL)
      .AddRow(__,       2LL)
      .AddRow(3LL,      3LL)
      .AddRow(4LL,      4LL)
      .Build(), &Smudge);
}

TEST(StatefulExpressionsTest, SmudgeAcrossBlocksBinary) {
  TestStatefulEvaluation(BlockBuilder<BINARY, BINARY>()
      .AddRow("Super",      "Super")
      .AddRow(__,           "Super")
      .AddRow(__,           "Super")
      .AddRow("Sonic",      "Sonic")
      .AddRow(__,           "Sonic")
      .Build(), &Smudge);
}

TEST(StatefulExpressionsTest, SmudgeShortCircuit) {
  TestShortCircuitUnary(BlockBuilder<BOOL, INT64, BOOL, INT64>()
      .AddRow(false,  __, false, __)
      .AddRow(true,   __, false, __)
      .AddRow(true,  1LL, false, __)
      .AddRow(false,  __, false, 1LL)
      .AddRow(true,  2LL, false, __)
      .AddRow(false, 3LL, false, 3LL)
      .AddRow(false, 4LL, false, 4LL)
      .Build(), &Smudge);
}

// TODO(onufry): Add tests for various types of OOM.

TEST(StatefulExpressionsTest, RunningMinWithFlushBinding) {
  // Test combinations of nullable and non-nullable expressions.
  TestBindingWithNull(&RunningMinWithFlush, BOOL, false, INT32, true,
                      "RUNNING_MIN_WITH_FLUSH($0, $1)", INT32, true);
  TestBindingWithNull(&RunningMinWithFlush, BOOL, false, INT32, false,
                      "RUNNING_MIN_WITH_FLUSH($0, $1)", INT32, false);
  TestBinaryBindingFailureWithNulls(&RunningMinWithFlush, BOOL, true,
                                    INT32, false);

  // Test valid expression types. (INT32 has already been tested above.)
  TestBindingWithNull(&RunningMinWithFlush, BOOL, false, INT64, true,
                      "RUNNING_MIN_WITH_FLUSH($0, $1)", INT64, true);
  TestBindingWithNull(&RunningMinWithFlush, BOOL, false, UINT32, true,
                      "RUNNING_MIN_WITH_FLUSH($0, $1)", UINT32, true);
  TestBindingWithNull(&RunningMinWithFlush, BOOL, false, UINT64, true,
                      "RUNNING_MIN_WITH_FLUSH($0, $1)", UINT64, true);

  // Test invalid expression types.
  TestBindingFailure(&RunningMinWithFlush, INT32, INT32);
  TestBindingFailure(&RunningMinWithFlush, BOOL, FLOAT);
  TestBindingFailure(&RunningMinWithFlush, BOOL, DOUBLE);
  TestBindingFailure(&RunningMinWithFlush, BOOL, BOOL);
  TestBindingFailure(&RunningMinWithFlush, BOOL, DATE);
  TestBindingFailure(&RunningMinWithFlush, BOOL, DATETIME);
  TestBindingFailure(&RunningMinWithFlush, BOOL, STRING);
  TestBindingFailure(&RunningMinWithFlush, BOOL, BINARY);
  TestBindingFailure(&RunningMinWithFlush, BOOL, DATA_TYPE);
}

TEST(StatefulExpressionsTest, RunningMinWithFlushEvaluation) {
  TestStatefulEvaluation(BlockBuilder<BOOL, INT32, INT32>()
      .AddRow(true, 1, 1)
      .AddRow(false, 1, 1)
      .AddRow(false, 2, 1)
      .AddRow(true, 0, 0)
      .AddRow(false, __, __)
      .AddRow(true, __, __)
      .AddRow(false, __, __)
      .AddRow(false, 0, 0)
      .AddRow(false, __, 0)
      .AddRow(true, 1, 0)
      .AddRow(true, 2, 2)
      .AddRow(false, 3, 3)
      .AddRow(false, 4, 3)
      .Build(), &RunningMinWithFlush);

  // We also want to test explicitly the non-null input expression case since
  // it is handled by a separate code path from the null input expression case.
  TestStatefulEvaluation(BlockBuilder<BOOL, INT32, INT32>()
      .AddRow(true, 1, 1)
      .AddRow(false, 1, 1)
      .AddRow(false, 2, 1)
      .AddRow(true, 0, 0)
      .AddRow(false, 0, 0)
      .AddRow(true, 1, 0)
      .Build(), &RunningMinWithFlush);
}

TEST(StatefulExpressionsTest, RunningMinWithFlushConsistentAcrossBlocks) {
  TestStatefulEvaluation(BlockBuilder<BOOL, INT32, INT32>()
      .AddRow(true, 1, 1)
      .AddRow(false, 1, 1)
      .AddRow(false, 2, 1)
      .AddRow(true, 0, 0)
      .AddRow(false, __, __)
      .AddRow(true, __, __)
      .AddRow(false, __, __)
      .AddRow(false, 0, 0)
      .AddRow(false, __, 0)
      .AddRow(true, 1, 0)
      .Build(), &RunningMinWithFlush);

  // We also want to test explicitly the non-null input expression case since
  // it is handled by a separate code path from the null input expression case.
  TestStatefulEvaluation(BlockBuilder<BOOL, INT32, INT32>()
      .AddRow(true, 1, 1)
      .AddRow(false, 1, 1)
      .AddRow(false, 2, 1)
      .AddRow(true, 0, 0)
      .AddRow(false, 0, 0)
      .AddRow(true, 1, 0)
      .Build(), &RunningMinWithFlush);
}

TEST(StatefulExpressionsTest, RunningMinWithFlushShortCircuit) {
  TestShortCircuitBinary(BlockBuilder<BOOL, BOOL, BOOL, INT32, BOOL, INT32>()
      .AddRow(false, true,  false, 1,  false, 1)
      .AddRow(true,  false, false, 1,  false, __)
      .AddRow(false, false, false, 2,  false, 1)
      .AddRow(false, true,  false, 0,  false, 0)
      .AddRow(true,  true,  false, __, false, __)
      .AddRow(false, false, false, __, false, __)
      .AddRow(true,  false, false, __, false, __)
      .AddRow(true,  false, false, 0,  false, __)
      .AddRow(false, false, false, __, false, 0)
      .AddRow(false, true,  false, 1,  false, 0)
      .Build(), &RunningMinWithFlush);
}

TEST(StatefulExpressionsTest, SmudgeIfNullabilityCombinationBinding) {
  // Test combinations of nullable and non-nullable expressions.
  TestBindingWithNull(&SmudgeIf, INT32, false, BOOL, false,
                      "SMUDGE_IF($0, $1)", INT32, false);
  TestBindingWithNull(&SmudgeIf, INT32, true, BOOL, false,
                      "SMUDGE_IF($0, $1)", INT32, true);
  TestBinaryBindingFailureWithNulls(&SmudgeIf, INT32, false,
                                    BOOL, true);
  TestBinaryBindingFailureWithNulls(&SmudgeIf, INT32, true,
                                    BOOL, true);
}

TEST(StatefulExpressionsTest, SmudgeIfInvalidConditionTypeBinding) {
  // Test binding with a subset of invalid types.
  TestBindingFailure(&SmudgeIf, INT32, INT32);
  TestBindingFailure(&SmudgeIf, INT32, STRING);
  TestBindingFailure(&SmudgeIf, INT32, BINARY);
  TestBindingFailure(&SmudgeIf, INT32, DATA_TYPE);
}

TEST(StatefulExpressionsTest, SmudgeIfVarietyArgumentDataTypeBinding) {
  // Test binding with a subset of valid types.
  TestBindingWithNull(&SmudgeIf, INT32, false, BOOL, false,
                      "SMUDGE_IF($0, $1)", INT32, false);
  TestBindingWithNull(&SmudgeIf, INT64, false,  BOOL, false,
                      "SMUDGE_IF($0, $1)", INT64, false);
  TestBindingWithNull(&SmudgeIf, BINARY, false,  BOOL, false,
                      "SMUDGE_IF($0, $1)", BINARY, false);
  TestBindingWithNull(&SmudgeIf, STRING, false,  BOOL, false,
                      "SMUDGE_IF($0, $1)", STRING, false);
  TestBindingWithNull(&SmudgeIf, DATA_TYPE, false,  BOOL, false,
                      "SMUDGE_IF($0, $1)", DATA_TYPE, false);
}

bool first_row_condition_values[] = {true, false};

TEST(StatefulExpressionsTest, SmudgeIfNotNullableEvaluation) {
  for (int i = 0; i < 2; ++i) {
    // Test when  argument is not nullable.
    TestStatefulEvaluation(BlockBuilder<INT32, BOOL, INT32>()
        .AddRow(1, first_row_condition_values[i], 1)
        .AddRow(2, true, 1)
        .AddRow(3, true, 1)
        .AddRow(4, false, 4)
        .AddRow(5, true, 4)
        .AddRow(6, false, 6)
        .AddRow(7, false, 7)
        .AddRow(8, true, 7)
        .Build(), &SmudgeIf);
  }
}

TEST(StatefulExpressionsTest, SmudgeIfNullableEvaluation) {
  for (int i = 0; i < 2; ++i) {
    // Test when  argument is nullable and the first element is not NULL.
    TestStatefulEvaluation(BlockBuilder<INT32, BOOL, INT32>()
        .AddRow(1, first_row_condition_values[i], 1)
        .AddRow(__, true, 1)
        .AddRow(3, true, 1)
        .AddRow(__, false, __)
        .AddRow(__, true, __)
        .AddRow(6, false, 6)
        .AddRow(__, false, __)
        .AddRow(7, false, 7)
        .AddRow(8, true, 7)
        .Build(), &SmudgeIf);
  }
}

TEST(StatefulExpressionsTest, SmudgeIfNullableAndFirstElementNullEvaluation) {
  for (int i = 0; i < 2; ++i) {
    // Test when  argument is nullable and the first element is NULL.
    TestStatefulEvaluation(BlockBuilder<INT32, BOOL, INT32>()
          .AddRow(__, first_row_condition_values[i], __)
          .AddRow(1, true, __)
          .AddRow(3, true, __)
          .AddRow(2, false, 2)
          .AddRow(__, true, 2)
          .AddRow(6, false, 6)
          .AddRow(__, false, __)
          .AddRow(7, false, 7)
          .AddRow(8, true, 7)
          .Build(), &SmudgeIf);
  }
}

TEST(StatefulExpressionsTest, SmudgeIfStringEvaluation) {
  for (int i = 0; i < 2; ++i) {
    // Test when the argument is a string.
    TestStatefulEvaluation(BlockBuilder<STRING, BOOL, STRING>()
        .AddRow("i", first_row_condition_values[i], "i")
        .AddRow("r", true, "i")
        .AddRow("v", true, "i")
        .AddRow("a", false, "a")
        .AddRow("n", true, "a")
        .Build(), &SmudgeIf);

    TestStatefulEvaluation(BlockBuilder<STRING, BOOL, STRING>()
        .AddRow("irvan", first_row_condition_values[i], "irvan")
        .AddRow("onufry", true, "irvan")
        .AddRow("", false, "")
        .AddRow("supersonic", true, "")
        .AddRow("foo", true, "")
        .AddRow("foo-bar", false, "foo-bar")
        .Build(), &SmudgeIf);
  }
}

// Regression Test.
TEST(StatefulExpressionsTest, SmudgeIfAllLocalSkipVectorStorageUsedTest) {
  BlockBuilder<STRING, BOOL, STRING> builder;
  string randomly_picked_strings[] = {"i", "r", "v", "a", "n"};
  for (int i = 0; i < Cursor::kDefaultRowCount / 2; ++i) {
    string data1 = "i" + randomly_picked_strings[i % 5];
    string data2 = "a" + randomly_picked_strings[i % 4];
    builder.AddRow(data1, false, data1);
    builder.AddRow(data2, true, data1);
  }
  TestStatefulEvaluation(builder.Build(), &SmudgeIf);
}

TEST(StatefulExpressionsTest, SmudgeIfNullableShortCircuit) {
  TestShortCircuitBinary(BlockBuilder<BOOL, INT32, BOOL, BOOL, BOOL, INT32>()
      .AddRow(false, 0, false, true, false, 0)
      .AddRow(true, __, false, true, false, __)
      .AddRow(false, 2, false, false, false, 2)
      .AddRow(false, 3, false, false, false, 3)
      .AddRow(true, __, false, false, false, __)
      .AddRow(false, 5, false, true, false, __)
      .AddRow(true, __, false, false, false, __)
      .AddRow(true, __, false, true, false, __)
      .AddRow(false, 8, false, false, false, 8)
      .AddRow(false, 9, false, true, false, 8)
      .AddRow(true, __, false, true, false, __)
      .Build(), &SmudgeIf);
}

}  // namespace
}  // namespace supersonic
