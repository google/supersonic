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
// TODO(user): refactor the tests to use OperationTest.

#include "supersonic/cursor/core/hash_join.h"

#include <stddef.h>

#include <memory>

#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

CompoundSingleSourceProjector* column_0_selector() {
  CompoundSingleSourceProjector* selector =
      new CompoundSingleSourceProjector();
  selector->add(ProjectAttributeAt(0));
  return selector;
}

CompoundSingleSourceProjector* column_01_selector() {
  CompoundSingleSourceProjector* selector =
      new CompoundSingleSourceProjector();
  selector->add(ProjectAttributeAt(0));
  selector->add(ProjectAttributeAt(1));
  return selector;
}

CompoundMultiSourceProjector* all_columns_projector() {
  CompoundMultiSourceProjector* projector =
      new CompoundMultiSourceProjector();
  projector->add(0, ProjectAllAttributes("L."));
  projector->add(1, ProjectAllAttributes("R."));
  return projector;
}

Operation* CreateOperation(
    JoinType join_type,
    const SingleSourceProjector* lhs_key_selector,
    const SingleSourceProjector* rhs_key_selector,
    const MultiSourceProjector* result_projector,
    KeyUniqueness rhs_key_uniqueness,
    Operation* lhs, Operation* rhs) {
  return new HashJoinOperation(
      join_type,
      lhs_key_selector, rhs_key_selector, result_projector,
      rhs_key_uniqueness,
      lhs, rhs);
}


class HashJoinTest : public testing::TestWithParam<KeyUniqueness> {
 public:
  void SetUp() {
    builder_1_.AddRow(1, "a");
    builder_2_.AddRow(2, "b");

    builder_12345_.
        AddRow(1, "a").
        AddRow(2, "b").
        AddRow(3, "c").
        AddRow(4, "d").
        AddRow(5, "e");

    builder_654321_.
        AddRow(6, "f").
        AddRow(5, "e").
        AddRow(4, "d").
        AddRow(3, "c").
        AddRow(2, "b").
        AddRow(1, "a");

    builder_2b2b2c_.
        AddRow(2, "b").
        AddRow(2, "b").
        AddRow(2, "c");

    builder_1a1b2a2b_.
        AddRow(1, "a").
        AddRow(1, "b").
        AddRow(2, "a").
        AddRow(2, "b");

    builder_1a1NNaNN_.
        AddRow(1, "a").
        AddRow(1, __).
        AddRow(__, "a").
        AddRow(__, __);

    builder_2b2b2c_x2_output_
        .AddRow(2, "b", 2, "b")
        .AddRow(2, "b", 2, "b")
        .AddRow(2, "b", 2, "c")
        .AddRow(2, "b", 2, "b")
        .AddRow(2, "b", 2, "b")
        .AddRow(2, "b", 2, "c")
        .AddRow(2, "c", 2, "b")
        .AddRow(2, "c", 2, "b")
        .AddRow(2, "c", 2, "c");
  }

  KeyUniqueness rhs_key_uniqueness() { return GetParam(); }

  TestDataBuilder<INT64, STRING>
    builder_1_, builder_2_,
    builder_12345_, builder_654321_,
    builder_2b2b2c_, builder_1a1b2a2b_, builder_1a1NNaNN_;

  TestDataBuilder<INT64, STRING, INT64, STRING> builder_2b2b2c_x2_output_;

 private:
};

INSTANTIATE_TEST_CASE_P(rhs_key_uniqueness,
                        HashJoinTest,
                        testing::Values(UNIQUE, NOT_UNIQUE));

TEST_P(HashJoinTest, _1_InnerJoin_1) {
  OperationTest test;
  test.AddInput(builder_1_.Build());
  test.AddInput(builder_1_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(1, "a", 1, "a")
                         .Build());
  test.Execute(CreateOperation(INNER, column_0_selector(), column_0_selector(),
                               all_columns_projector(), rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

TEST_P(HashJoinTest, _1_LeftOuterJoin_1) {
  OperationTest test;
  test.AddInput(builder_1_.Build());
  test.AddInput(builder_1_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(1, "a", 1, "a")
                         .Build());
  test.Execute(CreateOperation(LEFT_OUTER, column_0_selector(),
                               column_0_selector(),
                               all_columns_projector(), rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

TEST_P(HashJoinTest, _1_InnerJoin_2) {
  OperationTest test;
  test.AddInput(builder_1_.Build());
  test.AddInput(builder_2_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .Build());
  test.Execute(CreateOperation(INNER, column_0_selector(), column_0_selector(),
                               all_columns_projector(), rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

TEST_P(HashJoinTest, _1_LeftOuterJoin_2) {
  OperationTest test;
  test.AddInput(builder_1_.Build());
  test.AddInput(builder_2_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(1, "a", __, __)
                         .Build());
  test.Execute(CreateOperation(LEFT_OUTER, column_0_selector(),
                               column_0_selector(), all_columns_projector(),
                               rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

TEST_P(HashJoinTest, _12345_InnerJoin_654321) {
  OperationTest test;
  test.AddInput(builder_12345_.Build());
  test.AddInput(builder_654321_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(1, "a", 1, "a")
                         .AddRow(2, "b", 2, "b")
                         .AddRow(3, "c", 3, "c")
                         .AddRow(4, "d", 4, "d")
                         .AddRow(5, "e", 5, "e")
                         .Build());
  test.Execute(CreateOperation(INNER, column_0_selector(), column_0_selector(),
                               all_columns_projector(), rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

TEST_P(HashJoinTest, _654321_InnerJoin_12345) {
  OperationTest test;
  test.AddInput(builder_654321_.Build());
  test.AddInput(builder_12345_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(5, "e", 5, "e")
                         .AddRow(4, "d", 4, "d")
                         .AddRow(3, "c", 3, "c")
                         .AddRow(2, "b", 2, "b")
                         .AddRow(1, "a", 1, "a")
                         .Build());
  test.Execute(CreateOperation(INNER, column_0_selector(), column_0_selector(),
                               all_columns_projector(), rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

// TODO(user): Add more tests for left join.
TEST_P(HashJoinTest, _654321_LeftOuterJoin_12345) {
  OperationTest test;
  test.AddInput(builder_654321_.Build());
  test.AddInput(builder_12345_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(6, "f", __, __)
                         .AddRow(5, "e", 5, "e")
                         .AddRow(4, "d", 4, "d")
                         .AddRow(3, "c", 3, "c")
                         .AddRow(2, "b", 2, "b")
                         .AddRow(1, "a", 1, "a")
                         .Build());
  test.Execute(CreateOperation(LEFT_OUTER, column_0_selector(),
                               column_0_selector(), all_columns_projector(),
                               rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

TEST_F(HashJoinTest, _12345_InnerJoin_2b2b2c) {
  OperationTest test;
  test.AddInput(builder_12345_.Build());
  test.AddInput(builder_2b2b2c_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(2, "b", 2, "b")
                         .AddRow(2, "b", 2, "b")
                         .AddRow(2, "b", 2, "c")
                         .Build());
  test.Execute(CreateOperation(INNER, column_0_selector(), column_0_selector(),
                               all_columns_projector(), NOT_UNIQUE,
                               test.input_at(0), test.input_at(1)));
}

TEST_F(HashJoinTest, _12345_LeftOuterJoin_2b2b2c) {
  OperationTest test;
  test.AddInput(builder_12345_.Build());
  test.AddInput(builder_2b2b2c_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(1, "a", __, __)
                         .AddRow(2, "b", 2, "b")
                         .AddRow(2, "b", 2, "b")
                         .AddRow(2, "b", 2, "c")
                         .AddRow(3, "c", __, __)
                         .AddRow(4, "d", __, __)
                         .AddRow(5, "e", __, __)
                         .Build());
  test.Execute(CreateOperation(LEFT_OUTER, column_0_selector(),
                               column_0_selector(), all_columns_projector(),
                               NOT_UNIQUE,
                               test.input_at(0), test.input_at(1)));
}

TEST_F(HashJoinTest, _2b2b2c_InnerJoin_2b2b2c) {
  OperationTest test;
  test.AddInput(builder_2b2b2c_.Build());
  test.AddInput(builder_2b2b2c_.Build());
  test.SetExpectedResult(builder_2b2b2c_x2_output_.Build());
  test.Execute(CreateOperation(INNER, column_0_selector(), column_0_selector(),
                               all_columns_projector(), NOT_UNIQUE,
                               test.input_at(0), test.input_at(1)));
}

TEST_F(HashJoinTest, _2b2b2c_InnerJoin_2b2b2cWithSpyTransform) {
  Operation* lhs = builder_2b2b2c_.Build();
  Operation* rhs = builder_2b2b2c_.Build();

  std::unique_ptr<Cursor> expected_result(
      builder_2b2b2c_x2_output_.BuildCursor());

  std::unique_ptr<Operation> hash_join_operation(
      CreateOperation(INNER, column_0_selector(), column_0_selector(),
                      all_columns_projector(), NOT_UNIQUE, lhs, rhs));

  std::unique_ptr<Cursor> hash_join(
      SucceedOrDie(hash_join_operation->CreateCursor()));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  hash_join->ApplyToChildren(spy_transformer.get());
  hash_join.reset(spy_transformer->Transform(hash_join.release()));

  EXPECT_CURSORS_EQUAL(expected_result.release(), hash_join.release());
}

TEST_P(HashJoinTest, _1a1b2a2b_InnerJoin_1a1b2a2b) {
  OperationTest test;
  test.AddInput(builder_1a1b2a2b_.Build());
  test.AddInput(builder_1a1b2a2b_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(1, "a", 1, "a")
                         .AddRow(1, "b", 1, "b")
                         .AddRow(2, "a", 2, "a")
                         .AddRow(2, "b", 2, "b")
                         .Build());
  test.Execute(CreateOperation(INNER, column_01_selector(),
                               column_01_selector(), all_columns_projector(),
                               rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

TEST_F(HashJoinTest, _12345_InnerJoin_5k_Rows) {
  // The input data is prepared in such a way that each lhs row matches more
  // than Cursor::kDefaultRowCount rhs rows. This way splitting and piecewise
  // returning of big results in fixed-size windows is exercised, both in the
  // inner cursor (returned from LookupIndex) and in HashJoinCursor.
  TestDataBuilder<INT64, STRING> builder_5k_rows_;
  for (int i = 0; i < 1100; i++) {
    builder_5k_rows_.
        AddRow(1, "a").
        AddRow(2, "b").
        AddRow(3, "c").
        AddRow(4, "d").
        AddRow(5, "e");
    }

  OperationTest test;
  test.AddInput(builder_12345_.Build());
  test.AddInput(builder_5k_rows_.Build());
  test.SetInputViewSizes(2000);
  test.SetResultViewSizes(2000);

  TestDataBuilder<INT64, STRING, INT64, STRING> expected_output_builder;
  char buf[2] = {0, 0};
  for (int i = 1; i <= 5; i++) {
    buf[0] = 'a' + i - 1;
    for (int j_unused = 0; j_unused < 1100; j_unused++)
      expected_output_builder.AddRow(i, buf, i, buf);
  }
  test.SetExpectedResult(expected_output_builder.Build());
  test.Execute(CreateOperation(INNER, column_0_selector(), column_0_selector(),
                               all_columns_projector(), NOT_UNIQUE,
                               test.input_at(0), test.input_at(1)));
}

TEST_P(HashJoinTest, _1a1NNaNN_InnerJoin_1a1NNaNN) {
  OperationTest test;
  test.AddInput(builder_1a1NNaNN_.Build());
  test.AddInput(builder_1a1NNaNN_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(1, "a", 1, "a")
                         .Build());
  test.Execute(CreateOperation(INNER, column_01_selector(),
                               column_01_selector(), all_columns_projector(),
                               rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

TEST_P(HashJoinTest, _1a1NNaNN_LeftOuterJoin_1a1NNaNN) {
  OperationTest test;
  test.AddInput(builder_1a1NNaNN_.Build());
  test.AddInput(builder_1a1NNaNN_.Build());
  test.SetExpectedResult(TestDataBuilder<INT64, STRING, INT64, STRING>()
                         .AddRow(1, "a", 1, "a")
                         .AddRow(1, __, __, __)
                         .AddRow(__, "a", __, __)
                         .AddRow(__, __, __, __)
                         .Build());
  test.Execute(CreateOperation(LEFT_OUTER, column_01_selector(),
                               column_01_selector(), all_columns_projector(),
                               rhs_key_uniqueness(),
                               test.input_at(0), test.input_at(1)));
}

TEST_F(HashJoinTest, HashJoinShallowCopiesStrings) {
  const int kSize = 100;
  TestDataBuilder<STRING> builder;
  for (int i = 0; i < kSize; i++) {
    builder.AddRow("supersonic");
  }
  OperationTest test;
  test.AddInput(builder.Build());
  test.AddInput(builder.Build());
  test.SetInputViewSizes(25);
  test.SetResultViewSizes(25);
  TestDataBuilder<STRING, STRING> expected_output_builder;
  for (int i = 0; i < kSize * kSize; i++) {
    expected_output_builder.AddRow("supersonic", "supersonic");
  }
  test.SetExpectedResult(expected_output_builder.Build());
  Operation* operation = CreateOperation(INNER, column_0_selector(),
                                         column_0_selector(),
                                         all_columns_projector(),
                                         NOT_UNIQUE,
                                         test.input_at(0), test.input_at(1));
  // The limit should be big enough for the index, but not enough for (kSize *
  // kSize) copies of the string "supersonic".
  MemoryLimit memory_limit(9 * kSize * kSize);
  operation->SetBufferAllocator(&memory_limit, false);
  test.Execute(operation);
}

TEST_F(HashJoinTest, TransformTest) {
  Operation* lhs = builder_2b2b2c_.Build();
  Operation* rhs = builder_2b2b2c_.Build();

  std::unique_ptr<Operation> hash_join_operation(
      CreateOperation(INNER, column_0_selector(), column_0_selector(),
                      all_columns_projector(), NOT_UNIQUE, lhs, rhs));

  std::unique_ptr<Cursor> hash_join(
      SucceedOrDie(hash_join_operation->CreateCursor()));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  hash_join->ApplyToChildren(spy_transformer.get());

  EXPECT_EQ(2, spy_transformer->GetHistoryLength());
}

TEST_F(HashJoinTest, EmptyLhsSkipsRhs) {
  Operation* lhs = TestDataBuilder<STRING>().Build();
  Operation* rhs = TestDataBuilder<STRING>()
                   .AddRow("foo")
                   .ReturnException(ERROR_EVALUATION_ERROR).Build();
  OperationTest test;
  test.AddInput(lhs);
  test.AddInput(rhs);
  // Expect an empty result with no exception (since the rhs is not
  // supposed to be read at all).
  test.SetExpectedResult(TestDataBuilder<STRING, STRING>().Build());
  Operation* operation =
      CreateOperation(INNER, column_0_selector(), column_0_selector(),
                      all_columns_projector(), NOT_UNIQUE,
                      test.input_at(0), test.input_at(1));
  test.Execute(operation);
}

TEST_F(HashJoinTest, EmptyRhsSkipsLhs) {
  Operation* lhs = TestDataBuilder<STRING>()
                   .AddRow("foo")
                   .ReturnException(ERROR_EVALUATION_ERROR).Build();
  Operation* rhs = TestDataBuilder<STRING>().Build();
  OperationTest test;
  test.AddInput(lhs);
  test.AddInput(rhs);
  // Expect an empty result with no exception (since the lhs is not
  // supposed to be read beyond the first row).
  test.SetExpectedResult(TestDataBuilder<STRING, STRING>().Build());
  Operation* operation =
      CreateOperation(INNER, column_0_selector(), column_0_selector(),
                      all_columns_projector(), NOT_UNIQUE,
                      test.input_at(0), test.input_at(1));
  test.SetInputViewSizes(1);
  test.Execute(operation);
}

}  // namespace supersonic
