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

#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class AggregateClustersCursorTest : public testing::Test {
 protected:
  virtual void CreateSampleData() {
    sample_input_builder_.AddRow(0, 13)
                         .AddRow(2, 4)
                         .AddRow(2, 5)
                         .AddRow(2, -4)
                         .AddRow(2, -6)
                         .AddRow(1, 3)
                         .AddRow(1, 4)
                         .AddRow(1, -3);
    sample_output_builder_.AddRow(0, 13)
                          .AddRow(2, -1)
                          .AddRow(1, 4);
  }

  TestDataBuilder<INT32, INT32> sample_input_builder_;
  TestDataBuilder<INT32, INT32> sample_output_builder_;
};

FailureOrOwned<Cursor> CreateAggregateClusters(
    const SingleSourceProjector& group_by,
    const AggregationSpecification& aggregation,
    Cursor* input) {
  std::unique_ptr<Cursor> input_owner(input);
  FailureOrOwned<Aggregator> aggregator = Aggregator::Create(
      aggregation, input_owner->schema(), HeapBufferAllocator::Get(), 1);
  PROPAGATE_ON_FAILURE(aggregator);
  FailureOrOwned<const BoundSingleSourceProjector> bound_group_by =
      group_by.Bind(input_owner->schema());
  PROPAGATE_ON_FAILURE(bound_group_by);
  return BoundAggregateClusters(
      bound_group_by.release(),
      aggregator.release(),
      HeapBufferAllocator::Get(),
      input_owner.release());
}

// Input clustered into 3 clusters of rows with equal key.
TEST_F(AggregateClustersCursorTest, AggregateClusters) {
  CreateSampleData();
  OperationTest test;
  test.SetInput(sample_input_builder_.Build());
  test.SetExpectedResult(sample_output_builder_.Build());
  test.Execute(
      AggregateClustersWithSpecifiedOutputBlockSize(
          ProjectNamedAttribute("col0"),
          (new AggregationSpecification)
              ->AddAggregation(SUM, "col1", "sum"),
          2,
          test.input()));
}

TEST_F(AggregateClustersCursorTest, AggregateClustersWithSpyTransform) {
  CreateSampleData();
  Cursor* input = sample_input_builder_.BuildCursor();
  std::unique_ptr<const SingleSourceProjector> projector(
      ProjectNamedAttribute("col0"));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col1", "sum");

  std::unique_ptr<Cursor> clusters(
      SucceedOrDie(CreateAggregateClusters(*projector, aggregator, input)));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  clusters->ApplyToChildren(spy_transformer.get());
  clusters.reset(spy_transformer->Transform(clusters.release()));

  std::unique_ptr<Cursor> expected_result(sample_output_builder_.BuildCursor());
  EXPECT_CURSORS_EQUAL(expected_result.release(), clusters.release());
}

// Special case. Input is not clustered by any column, but AggregateClusters
// should handle this case anyway.
TEST_F(AggregateClustersCursorTest, AggregateClustersWithoutClusteredColumn) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32>()
                .AddRow(13)
                .AddRow(3)
                .AddRow(7)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32>()
                         .AddRow(23)
                         .Build());
  test.Execute(
      AggregateClustersWithSpecifiedOutputBlockSize(
          new CompoundSingleSourceProjector(),
          (new AggregationSpecification)
              ->AddAggregation(SUM, "col0", "sum"),
          2,
          test.input()));
}

TEST_F(AggregateClustersCursorTest, EmptyInputWithClusteredColumn) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING, INT32>().Build());
  test.SetExpectedResult(TestDataBuilder<STRING, INT32>().Build());
  test.Execute(
      AggregateClustersWithSpecifiedOutputBlockSize(
          ProjectNamedAttribute("col0"),
          (new AggregationSpecification)
              ->AddAggregation(SUM, "col1", "sum"),
          2,
          test.input()));
}

// Empty input that is not clustered by any column.
TEST_F(AggregateClustersCursorTest, EmptyInputWithoutClusteredColumn) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>().Build());
  test.SetExpectedResult(TestDataBuilder<STRING>().Build());
  test.Execute(
      AggregateClustersWithSpecifiedOutputBlockSize(
          new CompoundSingleSourceProjector(),
          (new AggregationSpecification)
              ->AddAggregation(MAX, "col0", "max"),
          2,
          test.input()));
}

// Three column key, common column(col1) in both aggregation and group by.
TEST_F(AggregateClustersCursorTest, MultiColumnAggregateClusters) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING, INT32, STRING, INT32>()
                .AddRow("a", 0, "a", 13)
                .AddRow("a", 2, "a", 4)
                .AddRow("a", 2, "a", 5)
                .AddRow("a", 2, "b", -4)
                .AddRow("a", 2, "b", -6)
                .AddRow("a", 1, "b", 3)
                .AddRow("a", 1, "b", 4)
                .AddRow("a", 1, "bbbbbbbb", -3)
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING, INT32, STRING, INT32, INT32>()
                         .AddRow("a", 0, "a", 0, 13)
                         .AddRow("a", 2, "a", 4, 9)
                         .AddRow("a", 2, "b", 4, -10)
                         .AddRow("a", 1, "b", 2, 7)
                         .AddRow("a", 1, "bbbbbbbb", 1, -3)
                         .Build());
  test.Execute(
      AggregateClustersWithSpecifiedOutputBlockSize(
          (new CompoundSingleSourceProjector())
              ->add(ProjectNamedAttributeAs("col0", "A"))
              ->add(ProjectNamedAttributeAs("col1", "B"))
              ->add(ProjectNamedAttributeAs("col2", "C")),
          (new AggregationSpecification)
              ->AddAggregation(SUM, "col1", "sum1")
              ->AddAggregation(SUM, "col3", "sum3"),
          2,
          test.input()));
}

// We try to do a group by a column that does not exists in input.
TEST_F(AggregateClustersCursorTest, BadGroupBy) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32>()
                .AddRow(13)
                .AddRow(3)
                .AddRow(7)
                .Build());
  test.SetExpectedBindFailure(ERROR_ATTRIBUTE_MISSING);
  test.Execute(
      AggregateClusters(
          ProjectNamedAttributeAs("col1", "B"),
          (new AggregationSpecification)->AddAggregation(MIN, "col0", "min"),
          test.input()));
}

// Checks error code, when it cannot allocate output memory.
TEST_F(AggregateClustersCursorTest,
       OutOfMemoryErrorWhenOutputBlockCanNotBeAllocated) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32>()
                .AddRow(1)
                .AddRow(2)
                .AddRow(3)
                .Build());
  test.SetExpectedBindFailure(ERROR_MEMORY_EXCEEDED);
  MemoryLimit memory_limit(0);
  test.SetBufferAllocator(&memory_limit);
  Operation* op = AggregateClustersWithSpecifiedOutputBlockSize(
      new CompoundSingleSourceProjector(),
      (new AggregationSpecification)->AddAggregation(SUM, "col0", "sum"),
      2,
      test.input());
  test.Execute(op);
}

// Tries to make output with aggregation column having same name
// as output column with the group by key.
TEST_F(AggregateClustersCursorTest, ResultingColumnsNamesConflict) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, INT32>().Build());
  test.SetExpectedBindFailure(ERROR_ATTRIBUTE_EXISTS);
  test.Execute(
      AggregateClusters(
          ProjectNamedAttributeAs("col0", "A"),
          (new AggregationSpecification)->AddAggregation(SUM, "col1", "A"),
          test.input()));
}

TEST_F(AggregateClustersCursorTest, ExceptionFromInputPropagated) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32>()
                .ReturnException(ERROR_GENERAL_IO_ERROR)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32>()
                         .ReturnException(ERROR_GENERAL_IO_ERROR)
                         .Build());
  test.Execute(
      AggregateClusters(
          new CompoundSingleSourceProjector(),
          (new AggregationSpecification)->AddAggregation(SUM, "col0", "sum"),
          test.input()));
}

// Testing default constructor.
TEST_F(AggregateClustersCursorTest, AggregateClustersWithDefaultCreate) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, INT32>()
                .AddRow(0, 13)
                .AddRow(2, 4)
                .AddRow(2, 5)
                .AddRow(2, -4)
                .AddRow(2, -6)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, INT32>()
                         .AddRow(0, 13)
                         .AddRow(2, -1)
                         .Build());
  test.Execute(
      AggregateClusters(
          ProjectNamedAttribute("col0"),
          (new AggregationSpecification)->AddAggregation(SUM, "col1", "sum"),
          test.input()));
}

TEST_F(AggregateClustersCursorTest, TransformTest) {
  // Empty input cursor.
  Cursor* input = sample_input_builder_.BuildCursor();
  std::unique_ptr<const SingleSourceProjector> projector(
      ProjectNamedAttribute("col0"));
  AggregationSpecification aggregator;

  std::unique_ptr<Cursor> clusters(
      SucceedOrDie(CreateAggregateClusters(*projector, aggregator, input)));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  clusters->ApplyToChildren(spy_transformer.get());

  ASSERT_EQ(1, spy_transformer->GetHistoryLength());
  EXPECT_EQ(input, spy_transformer->GetEntryAt(0)->original());
}

}  // namespace supersonic
