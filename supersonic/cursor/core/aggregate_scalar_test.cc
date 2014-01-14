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
#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class ScalarAggregateCursorTest : public testing::Test {
 protected:
  virtual void CreateSampleData() {
    sample_input_builder_.AddRow("f")
                         .AddRow("c")
                         .AddRow("a")
                         .AddRow("b")
                         .AddRow("g")
                         .AddRow("a")
                         .AddRow("d")
                         .AddRow("a")
                         .AddRow(__)
                         .AddRow("e");
    sample_output_builder_.AddRow("g", 10, 9, 7);
  }

  CompoundSingleSourceProjector empty_projector_;

  TestDataBuilder<STRING> sample_input_builder_;
  TestDataBuilder<STRING, UINT64, UINT64, UINT64> sample_output_builder_;
};

TEST_F(ScalarAggregateCursorTest, AggregateIntegers) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32>()
                .AddRow(13)
                .AddRow(3)
                .AddRow(3)
                .AddRow(__)
                .AddRow(7)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, INT32, UINT64, UINT64, UINT64>()
                         .AddRow(13, 26, 5, 4, 3)
                         .Build());
  std::unique_ptr<AggregationSpecification> aggregator(
      new AggregationSpecification);
  aggregator->AddAggregation(MAX, "col0", "max");
  aggregator->AddAggregation(SUM, "col0", "sum");
  aggregator->AddAggregation(COUNT, "", "count(*)");
  aggregator->AddAggregation(COUNT, "col0", "count");
  aggregator->AddDistinctAggregation(COUNT, "col0", "count distinct");
  test.Execute(ScalarAggregate(aggregator.release(), test.input()));
}

TEST_F(ScalarAggregateCursorTest, AggregateEmptyInput) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32>().Build());
  test.SetExpectedResult(TestDataBuilder<INT32, INT32, UINT64, UINT64, UINT64>()
                         .AddRow(__, __, 0, 0, 0)
                         .Build());
  std::unique_ptr<AggregationSpecification> aggregator(
      new AggregationSpecification);
  aggregator->AddAggregation(MAX, "col0", "max");
  aggregator->AddAggregation(SUM, "col0", "sum");
  aggregator->AddAggregation(COUNT, "", "count(*)");
  aggregator->AddAggregation(COUNT, "col0", "count");
  aggregator->AddDistinctAggregation(COUNT, "col0", "count distinct");
  test.Execute(ScalarAggregate(aggregator.release(), test.input()));
}

TEST_F(ScalarAggregateCursorTest, AggregateStrings) {
  OperationTest test;
  CreateSampleData();
  test.SetInput(sample_input_builder_.Build());
  test.SetExpectedResult(sample_output_builder_.Build());

  std::unique_ptr<AggregationSpecification> aggregator(
      new AggregationSpecification);
  aggregator->AddAggregation(MAX, "col0", "max");
  aggregator->AddAggregation(COUNT, "", "count(*)");
  aggregator->AddAggregation(COUNT, "col0", "count");
  aggregator->AddDistinctAggregation(COUNT, "col0", "count distinct");
  test.Execute(ScalarAggregate(aggregator.release(), test.input()));
}

TEST_F(ScalarAggregateCursorTest, AggregateStringsWithSpyTransform) {
  CreateSampleData();
  Cursor* input = sample_input_builder_.BuildCursor();

  std::unique_ptr<Cursor> expected_result(sample_output_builder_.BuildCursor());

  std::unique_ptr<AggregationSpecification> aggregation(
      new AggregationSpecification);
  aggregation->AddAggregation(MAX, "col0", "max");
  aggregation->AddAggregation(COUNT, "", "count(*)");
  aggregation->AddAggregation(COUNT, "col0", "count");
  aggregation->AddDistinctAggregation(COUNT, "col0", "count distinct");

  FailureOrOwned<Aggregator> aggregator = Aggregator::Create(
        *aggregation, input->schema(), HeapBufferAllocator::Get(), 1);
  ASSERT_TRUE(aggregator.is_success());
  std::unique_ptr<Cursor> aggregate(
      BoundScalarAggregate(aggregator.release(), input));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  aggregate->ApplyToChildren(spy_transformer.get());
  aggregate.reset(spy_transformer->Transform(aggregate.release()));

  EXPECT_CURSORS_EQUAL(expected_result.release(), aggregate.release());
}

TEST_F(ScalarAggregateCursorTest, TransformTest) {
  // Empty input cursor.
  Cursor* input = sample_input_builder_.BuildCursor();

  std::unique_ptr<AggregationSpecification> aggregation(
      new AggregationSpecification);

  FailureOrOwned<Aggregator> aggregator = Aggregator::Create(
        *aggregation, input->schema(), HeapBufferAllocator::Get(), 1);
  ASSERT_TRUE(aggregator.is_success());
  std::unique_ptr<Cursor> aggregate(
      BoundScalarAggregate(aggregator.release(), input));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  aggregate->ApplyToChildren(spy_transformer.get());

  ASSERT_EQ(1, spy_transformer->GetHistoryLength());
  EXPECT_EQ(input, spy_transformer->GetEntryAt(0)->original());
}

}  // namespace supersonic
