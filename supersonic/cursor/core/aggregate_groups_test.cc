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

#include <stddef.h>

#include <limits>
#include "supersonic/utils/std_namespace.h"
#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/aggregator.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/cursor/core/sort.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

// Creates a non-deterministic, non best-effort GroupAggregate Cursor for
// testing purposes. Memory limits are not enforced.
// TODO(user): switch to OperationTest (but need to add support for
// comparing nullability)
FailureOrOwned<Cursor> CreateGroupAggregate(
    const SingleSourceProjector& group_by,
    const AggregationSpecification& aggregation,
    Cursor* input,
    const int64 max_unique_rows = 20000) {
  std::unique_ptr<Cursor> input_owner(input);
  FailureOrOwned<Aggregator> aggregator = Aggregator::Create(
      aggregation, input_owner->schema(), HeapBufferAllocator::Get(), 1);
  PROPAGATE_ON_FAILURE(aggregator);
  FailureOrOwned<const BoundSingleSourceProjector> bound_group_by =
      group_by.Bind(input_owner->schema());
  PROPAGATE_ON_FAILURE(bound_group_by);
  return BoundGroupAggregateWithLimit(
      bound_group_by.release(), aggregator.release(),
      new MemoryLimit(std::numeric_limits<size_t>::max(),
                      false,
                      HeapBufferAllocator::Get()),
      HeapBufferAllocator::Get(),
      false,
      max_unique_rows,
      input_owner.release());
}

// Sorts cursor according to all columns in an ascending order. This function is
// needed because group provides no guarantees over the order of returned rows,
// so output needs to be sorted before it is compared against expected output.
static Cursor* Sort(Cursor* input) {
  SortOrder sort_order;
  sort_order.add(ProjectAllAttributes(), ASCENDING);
  std::unique_ptr<const BoundSortOrder> bound_sort_order(
      SucceedOrDie(sort_order.Bind(input->schema())));
  std::unique_ptr<const SingleSourceProjector> result_projector(
      ProjectAllAttributes());
  std::unique_ptr<const BoundSingleSourceProjector> bound_result_projector(
      SucceedOrDie(result_projector->Bind(input->schema())));
  return SucceedOrDie(BoundSort(
      bound_sort_order.release(),
      bound_result_projector.release(),
      1 << 20,
      "",
      HeapBufferAllocator::Get(),
      input));
}

class AggregateCursorTest : public testing::Test {
 protected:
  CompoundSingleSourceProjector empty_projector_;
};

class AggregateCursorSpyTest : public ::testing::TestWithParam<bool> {};

TEST_F(AggregateCursorTest, SimpleAggregation) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(1)
      .AddRow(3)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col0", "sum");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32>().AddRow(4).BuildCursor());
  EXPECT_EQ("sum", aggregate->schema().attribute(0).name());
  EXPECT_TRUE(aggregate->schema().attribute(0).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, CountWithInputColumn) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(1)
      .AddRow(3)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(COUNT, "col0", "count");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<UINT64>().AddRow(2).BuildCursor());
  EXPECT_FALSE(aggregate->schema().attribute(0).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, CountWithNullableInputColumn) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(1)
      .AddRow(__)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(COUNT, "col0", "count");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<UINT64>().AddRow(1).BuildCursor());
  EXPECT_FALSE(aggregate->schema().attribute(0).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, CountAll) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(1)
      .AddRow(__)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(COUNT, "", "count");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<UINT64>().AddRow(2).BuildCursor());
  EXPECT_FALSE(aggregate->schema().attribute(0).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, AggregationWithOnlyNullInputs) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(__)
      .AddRow(__)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col0", "sum");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32>().AddRow(__).BuildCursor());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, AggregationWithOutputTypeDifferentFromInputType) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(1)
      .AddRow(3)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregationWithDefinedOutputType(SUM, "col0", "sum", INT64);
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT64>().AddRow(4).BuildCursor());
  EXPECT_EQ("sum", aggregate->schema().attribute(0).name());
  EXPECT_TRUE(aggregate->schema().attribute(0).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, MultipleAggregations) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(1)
      .AddRow(2)
      .AddRow(3)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col0", "sum");
  aggregator.AddAggregation(MAX, "col0", "max");
  aggregator.AddAggregation(MIN, "col0", "min");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32, INT32, INT32>().AddRow(6, 3, 1).BuildCursor());
  EXPECT_EQ("sum", aggregate->schema().attribute(0).name());
  EXPECT_EQ("max", aggregate->schema().attribute(1).name());
  EXPECT_EQ("min", aggregate->schema().attribute(2).name());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, DistinctAggregation) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(3)
      .AddRow(4)
      .AddRow(4)
      .AddRow(3)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddDistinctAggregation(SUM, "col0", "sum");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  // Only distinct values are summed (3 + 4).
  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32>().AddRow(7).BuildCursor());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, DistinctCountAggregation) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(3)
      .AddRow(4)
      .AddRow(4)
      .AddRow(3)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddDistinctAggregationWithDefinedOutputType(
      COUNT, "col0", "count", INT32);
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  // There are two distinct values (3, 4).
  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32>().AddRow(2).BuildCursor());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, DistinctCountAggregationNeedsInputColumn) {
  Cursor* input = TestDataBuilder<INT32>()
      .AddRow(3)
      .BuildCursor();

  AggregationSpecification aggregator;
  // This should not work, there is no way to find distinct values if input
  // column is not specified.
  aggregator.AddDistinctAggregationWithDefinedOutputType(
      COUNT, "", "count", INT32);
  FailureOrOwned<Cursor> result(
      CreateGroupAggregate(empty_projector_, aggregator, input));
  EXPECT_TRUE(result.is_failure());
}

TEST_F(AggregateCursorTest, AggregationWithGroupBy) {
  Cursor* input = TestDataBuilder<INT32, INT32>()
      .AddRow(1, 3)
      .AddRow(3, -3)
      .AddRow(1, 4)
      .AddRow(3, -5)
      .BuildCursor();
  std::unique_ptr<const SingleSourceProjector> group_by_column(
      ProjectNamedAttribute("col0"));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col1", "sum");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(*group_by_column, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32, INT32>().AddRow(1, 7).AddRow(3, -8).BuildCursor());

  EXPECT_EQ("col0", aggregate->schema().attribute(0).name());
  EXPECT_EQ("sum", aggregate->schema().attribute(1).name());
  EXPECT_FALSE(aggregate->schema().attribute(0).is_nullable());
  EXPECT_TRUE(aggregate->schema().attribute(1).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, AggregationWithGroupBy_UniqueRowLimit) {
  Cursor* input = TestDataBuilder<INT32, INT32>()
      .AddRow(1, 3)
      .AddRow(3, -3)
      .AddRow(1, 4)
      .AddRow(3, -5)
      .AddRow(4, 5)
      .AddRow(3, -1)
      .AddRow(5, 1)
      .AddRow(4, 3)
      .AddRow(1, -2)
      .BuildCursor();
  std::unique_ptr<const SingleSourceProjector> group_by_column(
      ProjectNamedAttribute("col0"));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col1", "sum");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(
          *group_by_column, aggregator, input, 2)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32, INT32>()
          .AddRow(1, 5)
          .AddRow(3, -9)
          .AddRow(4, 9)
          .BuildCursor());

  EXPECT_EQ("col0", aggregate->schema().attribute(0).name());
  EXPECT_EQ("sum", aggregate->schema().attribute(1).name());
  EXPECT_FALSE(aggregate->schema().attribute(0).is_nullable());
  EXPECT_TRUE(aggregate->schema().attribute(1).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, AggregationWithGroupByNullableColumn) {
  Cursor* input = TestDataBuilder<INT32, INT32>()
      .AddRow(3, -3)
      .AddRow(__, 4)
      .AddRow(3, -5)
      .AddRow(__, 1)
      .BuildCursor();
  std::unique_ptr<const SingleSourceProjector> group_by_column(
      ProjectNamedAttribute("col0"));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col1", "sum");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(*group_by_column, aggregator, input)));
  std::unique_ptr<Cursor> expected_output(TestDataBuilder<INT32, INT32>()
                                              .AddRow(3, -8)
                                              .AddRow(__, 5)
                                              .BuildCursor());
  EXPECT_EQ("col0", aggregate->schema().attribute(0).name());
  EXPECT_EQ("sum", aggregate->schema().attribute(1).name());
  EXPECT_TRUE(aggregate->schema().attribute(0).is_nullable());
  EXPECT_TRUE(aggregate->schema().attribute(1).is_nullable());
  EXPECT_CURSORS_EQUAL(Sort(expected_output.release()),
                       Sort(aggregate.release()));
}

TEST_F(AggregateCursorTest, GroupBySecondColumn) {
  Cursor* input = TestDataBuilder<INT32, STRING>()
      .AddRow(-3, "foo")
      .AddRow(2, "bar")
      .AddRow(3, "bar")
      .AddRow(-2, "foo")
      .BuildCursor();
  std::unique_ptr<const SingleSourceProjector> group_by_column(
      ProjectNamedAttribute("col1"));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col0", "sum");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(*group_by_column, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(TestDataBuilder<STRING, INT32>()
                                              .AddRow("foo", -5)
                                              .AddRow("bar", 5)
                                              .BuildCursor());
  EXPECT_EQ("col1", aggregate->schema().attribute(0).name());
  EXPECT_EQ("sum", aggregate->schema().attribute(1).name());
  EXPECT_CURSORS_EQUAL(Sort(expected_output.release()),
                       Sort(aggregate.release()));
}

TEST_F(AggregateCursorTest, GroupByTwoColumns) {
  Cursor* input = TestDataBuilder<STRING, INT32, INT32>()
      .AddRow("foo", 1, 3)
      .AddRow("bar", 2, -3)
      .AddRow("foo", 1, 4)
      .AddRow("bar", 3, -5)
      .BuildCursor();
  std::unique_ptr<const SingleSourceProjector> group_by_columns(
      ProjectNamedAttributes(util::gtl::Container("col0", "col1")));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col2", "sum");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(*group_by_columns, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<STRING, INT32, INT32>()
          .AddRow("foo", 1, 7)
          .AddRow("bar", 2, -3)
          .AddRow("bar", 3, -5)
          .BuildCursor());
  EXPECT_CURSORS_EQUAL(Sort(expected_output.release()),
                       Sort(aggregate.release()));
}

TEST_F(AggregateCursorTest, GroupByTwoColumnsWithMultipleAggregations) {
  Cursor* input = TestDataBuilder<STRING, INT32, INT32>()
      .AddRow("foo", 1, 3)
      .AddRow("bar", 2, -3)
      .AddRow("foo", 1, 4)
      .AddRow("bar", 3, -5)
      .BuildCursor();
  std::unique_ptr<const SingleSourceProjector> group_by_columns(
      ProjectNamedAttributes(util::gtl::Container("col0", "col1")));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col2", "sum");
  aggregator.AddAggregation(MIN, "col2", "min");
  aggregator.AddAggregation(COUNT, "", "count");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(*group_by_columns, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<STRING, INT32, INT32, INT32, UINT64>()
      // Group by col, group by col, SUM col, MIN col, COUNT col
          .AddRow("foo", 1, 7, 3, 2)
          .AddRow("bar", 2, -3, -3, 1)
          .AddRow("bar", 3, -5, -5, 1)
          .BuildCursor());
  EXPECT_CURSORS_EQUAL(Sort(expected_output.release()),
                       Sort(aggregate.release()));
}

TEST_F(AggregateCursorTest, GroupByWithoutAggregateFunctions) {
  Cursor* input = TestDataBuilder<STRING>()
      .AddRow("foo")
      .AddRow("bar")
      .AddRow("foo")
      .AddRow("bar")
      .BuildCursor();
  std::unique_ptr<const SingleSourceProjector> group_by_column(
      ProjectNamedAttribute("col0"));
  AggregationSpecification empty_aggregator;
  std::unique_ptr<Cursor> aggregate(SucceedOrDie(
      CreateGroupAggregate(*group_by_column, empty_aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<STRING>().AddRow("foo").AddRow("bar").BuildCursor());
  EXPECT_CURSORS_EQUAL(Sort(expected_output.release()),
                       Sort(aggregate.release()));
}

// Aggregation on empty input with empty key should return empty result.
TEST_F(AggregateCursorTest, AggregationOnEmptyInput) {
  Cursor* input = TestDataBuilder<DATETIME>().BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(MIN, "col0", "min");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<DATETIME>().BuildCursor());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

// Aggregation on empty input with group by columns should return empty result.
TEST_F(AggregateCursorTest, AggregationOnEmptyInputWithGroupByColumn) {
  Cursor* input = TestDataBuilder<STRING, DATETIME>().BuildCursor();
  std::unique_ptr<const SingleSourceProjector> group_by_column(
      ProjectNamedAttribute("col0"));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(MIN, "col1", "min");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(*group_by_column, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<STRING, DATETIME>().BuildCursor());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

// Count on empty input with empty key should return empty result.
TEST_F(AggregateCursorTest, CountOnEmptyInput) {
  Cursor* input = TestDataBuilder<DATETIME>().BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(COUNT, "col0", "count");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<UINT64>().BuildCursor());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

// Count on empty input with group by columns should return empty result.
TEST_F(AggregateCursorTest, CountOnEmptyInputWithGroupByColumn) {
  Cursor* input = TestDataBuilder<STRING, DATETIME>()
      .BuildCursor();
  std::unique_ptr<const SingleSourceProjector> group_by_column(
      ProjectNamedAttribute("col0"));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(COUNT, "col1", "count");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(*group_by_column, aggregator, input)));
  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<STRING, UINT64>().BuildCursor());
  EXPECT_CURSORS_EQUAL(expected_output.release(), aggregate.release());
}

TEST_F(AggregateCursorTest, AggregationInputColumnMissingError) {
  Cursor* input = TestDataBuilder<INT32>().BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "NotExistingCol", "sum");
  FailureOrOwned<Cursor> result(
      CreateGroupAggregate(empty_projector_, aggregator, input));
  ASSERT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_ATTRIBUTE_MISSING, result.exception().return_code());
}

TEST_F(AggregateCursorTest, AggregationResultColumnExistsError) {
  Cursor* input = TestDataBuilder<INT32, INT32>().BuildCursor();
  AggregationSpecification aggregator;
  // Two results can not be stored in the same column.
  aggregator.AddAggregation(SUM, "col0", "result_col");
  aggregator.AddAggregation(MIN, "col1", "result_col");
  FailureOrOwned<Cursor> result(
      CreateGroupAggregate(empty_projector_, aggregator, input));
  ASSERT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_ATTRIBUTE_EXISTS, result.exception().return_code());
}

TEST_F(AggregateCursorTest, NotSupportedAggregationError) {
  Cursor* input = TestDataBuilder<BINARY>().BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col0", "sum");
  FailureOrOwned<Cursor> result(
      CreateGroupAggregate(empty_projector_, aggregator, input));
  ASSERT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_INVALID_ARGUMENT_TYPE, result.exception().return_code());
}

TEST_F(AggregateCursorTest, OutOfMemoryErrorWhenOutputBlockCanNotBeAllocated) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32>().Build());
  test.SetExpectedBindFailure(ERROR_MEMORY_EXCEEDED);
  MemoryLimit memory_limit(0);
  test.SetBufferAllocator(&memory_limit);
  Operation* op =
      GroupAggregate(
          new CompoundSingleSourceProjector(),
          (new AggregationSpecification)->AddAggregation(SUM, "col0", "sum"),
          NULL,
          test.input());
  test.Execute(op);
}

TEST_F(AggregateCursorTest, OutOfMemoryErrorWhenOutputBlockIsTooSmall) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, INT32>()
                .AddRow(1, 3)
                .AddRow(3, -3)
                .AddRow(1, 4)
                .AddRow(3, -5)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, INT32>()
                         .ReturnException(ERROR_MEMORY_EXCEEDED)
                         .Build());
  test.Execute(
      // Allocate space for only one output row (too few to hold two rows that
      // are the result of group by).
      GroupAggregate(
          ProjectNamedAttribute("col0"),
          (new AggregationSpecification)->AddAggregation(SUM, "col1", "sum"),
          (new GroupAggregateOptions)
              ->set_memory_quota(1)
              ->set_estimated_result_row_count(1),
          test.input()));
}

TEST_F(AggregateCursorTest, Reallocation) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, INT32>()
                .AddRow(1, 3)
                .AddRow(1, 4)
                .AddRow(3, -3)
                .AddRow(2, 4)
                .AddRow(3, -5)
                .AddRow(4, __)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, INT32>()
                         .AddRow(1, 7)
                         .AddRow(2, 4)
                         .AddRow(3, -8)
                         .AddRow(4, __)
                         .Build());
  test.SetIgnoreRowOrder(true);
  test.Execute(
      GroupAggregate(
          ProjectNamedAttribute("col0"),
          (new AggregationSpecification)->AddAggregation(SUM, "col1", "sum"),
          (new GroupAggregateOptions)
              ->set_estimated_result_row_count(2),  // Needs to grow to 4.
          test.input()));
}

// When BestEffortGroupAggregate is used, no matter how large is the input, it
// will be always processed, but result may not be accurate.
TEST_F(AggregateCursorTest, BestEffortGroupAggregate) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, INT32>()
                .AddRow(1, 3)
                .AddRow(1, 4)
                .AddRow(3, -3)
                .AddRow(2, 4)
                .AddRow(3, -5)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, INT32>()
                         .AddRow(1, 7)
                         .AddRow(3, -3)
                         .AddRow(2, 4)
                         .AddRow(3, -5)
                         .Build());
  test.SetIgnoreRowOrder(true);
  test.Execute(
      // At 20 bytes quota, the buffer is filled after processing 3 rows.
      BestEffortGroupAggregate(
          ProjectNamedAttribute("col0"),
          (new AggregationSpecification)->AddAggregation(SUM, "col1", "sum"),
          (new GroupAggregateOptions)
              ->set_memory_quota(20)
              ->set_estimated_result_row_count(2),
          test.input()));
}

// It doesn't make much sense to use BestEffortGroupAggregate when there are no
// group by columns (the result occupies 1 row, so normal aggregate would always
// succeed), but such special case needs to be handled anyway.
TEST_F(AggregateCursorTest, BestEffortGroupAggregateWithoutGroupByColumns) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32>()
                .AddRow(1)
                .AddRow(2)
                .AddRow(3)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32>()
                         .AddRow(6)
                         .Build());
  test.Execute(
      BestEffortGroupAggregate(
          new CompoundSingleSourceProjector(),
          (new AggregationSpecification)->AddAggregation(SUM, "col0", "sum"),
          (new GroupAggregateOptions)
              ->set_memory_quota(100)
              ->set_estimated_result_row_count(2),
          test.input()));
}

TEST_F(AggregateCursorTest, ExceptionFromInputPropagated) {
  Cursor* input = TestDataBuilder<INT32>()
      .ReturnException(ERROR_GENERAL_IO_ERROR)
      .BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(COUNT, "col0", "count");
  FailureOrOwned<Cursor> cursor(
      CreateGroupAggregate(empty_projector_, aggregator, input));
  ASSERT_TRUE(cursor.is_success());
  ResultView result = cursor->Next(100);
  ASSERT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_GENERAL_IO_ERROR, result.exception().return_code());
}

INSTANTIATE_TEST_CASE_P(SpyUse, AggregateCursorSpyTest, testing::Bool());

TEST_P(AggregateCursorSpyTest, LargeInput) {
  TestDataBuilder<INT64, STRING> cursor_builder;
  for (int i = 0; i < 3 * Cursor::kDefaultRowCount + 1; ++i) {
    cursor_builder.AddRow(13, "foo")
        .AddRow(17, "bar")
        .AddRow(13, "foo");
  }
  Cursor* input = cursor_builder.BuildCursor();

  std::unique_ptr<const SingleSourceProjector> group_by_columns(
      ProjectNamedAttributes(util::gtl::Container("col0", "col1")));
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col0", "sum");

  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(*group_by_columns, aggregator, input)));

  if (GetParam()) {
    std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
        PrintingSpyTransformer());
    aggregate->ApplyToChildren(spy_transformer.get());
    aggregate.reset(spy_transformer->Transform(aggregate.release()));
  }

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT64, STRING, INT64>()
          .AddRow(13, "foo", 2 * 13 * (3 * Cursor::kDefaultRowCount + 1))
          .AddRow(17, "bar", 17 * (3 * Cursor::kDefaultRowCount + 1))
          .BuildCursor());
  EXPECT_CURSORS_EQUAL(Sort(expected_output.release()),
                       Sort(aggregate.release()));
}

TEST_F(AggregateCursorTest, NoGroupByColumns) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32>()
                .AddRow(1)
                .AddRow(1)
                .AddRow(3)
                .AddRow(3)
                .AddRow(2)
                .AddRow(3)
                .AddRow(1)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, UINT64>()
                         .AddRow(14, 7)
                         .Build());
  std::unique_ptr<AggregationSpecification> aggregation(
      new AggregationSpecification);
  aggregation->AddAggregation(SUM, "col0", "sum");
  aggregation->AddAggregation(COUNT, "col0", "cnt");
  test.SetIgnoreRowOrder(true);
  // TODO(user): Make it work with set_memory_quota(0).
  test.Execute(GroupAggregate(
      new CompoundSingleSourceProjector,
      aggregation.release(),
      (new GroupAggregateOptions)->set_memory_quota(100)
                                 ->set_estimated_result_row_count(2),
      test.input()));
}

class MemoryUsageTracker : public MemoryStatisticsCollectingBufferAllocator {
 public:
  explicit MemoryUsageTracker(BufferAllocator* delegate)
      : MemoryStatisticsCollectingBufferAllocator(
          delegate, stats_collector_ = new Collector()) {}

  size_t GetMaxUsage() const {
    return stats_collector_->GetMaxUsage();
  }

 private:
  class Collector : public MemoryStatisticsCollectorInterface {
   public:
    Collector()
        : current_usage_(0), max_usage_(0) {}

    ~Collector() { CHECK_EQ(0, current_usage_); }

    virtual void AllocatedMemoryBytes(size_t bytes) {
      max_usage_ = max(max_usage_, current_usage_ += bytes);
    }

    size_t GetMaxUsage() const { return max_usage_; }

    virtual void RefusedMemoryBytes(size_t bytes) {}
    virtual void FreedMemoryBytes(size_t bytes) { current_usage_ -= bytes; }
   private:
    size_t current_usage_;
    size_t max_usage_;
  };

  Collector* stats_collector_;
  DISALLOW_COPY_AND_ASSIGN(MemoryUsageTracker);
};

// Helper class to test best-effort group by determinism.
class BestEffortGroupAggregateDeterminismHelper {
 public:
  BestEffortGroupAggregateDeterminismHelper() {
    // Build input data.
    for (int i = 0; i < kNumInputRows; ++i) {
      input_builder.AddRow(i, i);
    }
    aggregation_specification_.AddAggregation(SUM, "col1", "sum");
  }

  Operation* CreateInput() {
    return input_builder.Build();
  }

  // Helper method to create best-effort GroupBy operations.
  Operation* CreateBestEffortGroupAggregateOperation(
      size_t memory_quota, bool enforce_quota, Operation* input_operation) {
    std::unique_ptr<GroupAggregateOptions> options(
        (new GroupAggregateOptions)
            ->set_memory_quota(memory_quota)
            ->set_enforce_quota(enforce_quota));
    return BestEffortGroupAggregate(
            ProjectNamedAttribute("col0"),
            new AggregationSpecification(aggregation_specification_),
            options.release(),
            input_operation);
  }

  void ExpectCursorSucceeds(Cursor* cursor) {
    do {
      ResultView result = cursor->Next(100);
      ASSERT_FALSE(result.is_failure());
      if (result.is_eos()) return;
    } while (true);
  }

  void ExpectCursorFails(Cursor* cursor, ReturnCode error_code) {
    do {
      ResultView result = cursor->Next(100);
      if (result.is_failure()) {
        ASSERT_EQ(error_code, result.exception().return_code());
        // Cursor failed with OOM, return.
        break;
      }
      ASSERT_FALSE(result.is_eos());
    } while (true);
  }

 private:
  static const size_t kNumInputRows = 10000;

  AggregationSpecification aggregation_specification_;
  TestDataBuilder<INT32, INT32> input_builder;
  DISALLOW_COPY_AND_ASSIGN(BestEffortGroupAggregateDeterminismHelper);
};

TEST_F(AggregateCursorTest, BestEffortGroupByRespectsMemoryLimits) {
  static const size_t kMemory = 20000;

  BestEffortGroupAggregateDeterminismHelper helper;
  {
    std::unique_ptr<Operation> group_by(
        helper.CreateBestEffortGroupAggregateOperation(kMemory, true,
                                                       helper.CreateInput()));
    MemoryUsageTracker tracker(HeapBufferAllocator::Get());
    group_by->SetBufferAllocator(&tracker, false);
    std::unique_ptr<Cursor> cursor(SucceedOrDie(group_by->CreateCursor()));
    helper.ExpectCursorSucceeds(cursor.get());
    EXPECT_LE(tracker.GetMaxUsage(), kMemory);
  }
  // Now don't enforce memory.
  {
    std::unique_ptr<Operation> group_by(
        helper.CreateBestEffortGroupAggregateOperation(kMemory, false,
                                                       helper.CreateInput()));
    MemoryUsageTracker tracker(HeapBufferAllocator::Get());
    group_by->SetBufferAllocator(&tracker, false);
    std::unique_ptr<Cursor> cursor(SucceedOrDie(group_by->CreateCursor()));
    helper.ExpectCursorSucceeds(cursor.get());
    EXPECT_GT(tracker.GetMaxUsage(), kMemory);
  }
}

TEST_F(AggregateCursorTest, BestEffortGroupAggregateFailsOnOOM) {
  static const size_t kMemory = 20000;

  // BufferAllocators should be destroyed last, so let's create them first.
  MemoryLimit half_memory(kMemory / 2);
  MemoryLimit enough_memory(kMemory);

  BestEffortGroupAggregateDeterminismHelper helper;
  std::unique_ptr<Operation> group_by(
      helper.CreateBestEffortGroupAggregateOperation(kMemory, true,
                                                     helper.CreateInput()));

  // Given enough memory, cursor shouldn't fail.
  group_by->SetBufferAllocator(&enough_memory, false);
  std::unique_ptr<Cursor> cursor(SucceedOrDie(group_by->CreateCursor()));
  helper.ExpectCursorSucceeds(cursor.get());

  // If best-effort group-by with enforcing quota has less memory available
  // than needed, the cursor should fail with OOM.
  group_by->SetBufferAllocator(&half_memory, false);
  cursor.reset(SucceedOrDie(group_by->CreateCursor()));
  helper.ExpectCursorFails(cursor.get(), ERROR_MEMORY_EXCEEDED);
}

// Cursor transform test
TEST_F(AggregateCursorTest, TransformTest) {
  Cursor* input = TestDataBuilder<INT32>().BuildCursor();
  AggregationSpecification aggregator;
  aggregator.AddAggregation(SUM, "col0", "sum");
  std::unique_ptr<Cursor> aggregate(
      SucceedOrDie(CreateGroupAggregate(empty_projector_, aggregator, input)));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  aggregate->ApplyToChildren(spy_transformer.get());

  // One child - input.
  ASSERT_EQ(1, spy_transformer->GetHistoryLength());

  // Test if the input child is actually stored in history.
  EXPECT_EQ(input, spy_transformer->GetEntryAt(0)->original());
}

}  // namespace supersonic
