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

#include "supersonic/cursor/core/column_aggregator.h"

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "gtest/gtest.h"

namespace supersonic {
namespace aggregations {

class AggregatorsTest : public testing::Test {};

static Block* EmptyBlockWithSingleNullableColumn(DataType column_type,
                                                 rowcount_t capacity) {
  Attribute column_attribute("col0", column_type, NULLABLE);
  TupleSchema schema;
  schema.add_attribute(column_attribute);
  Block* block = new Block(schema, HeapBufferAllocator::Get());
  CHECK(block->Reallocate(capacity));
  return block;
}

static Block* EmptyBlockWithSingleNotNullableColumn(DataType column_type,
                                                    rowcount_t capacity) {
  Attribute column_attribute("col0", column_type, NOT_NULLABLE);
  TupleSchema schema;
  schema.add_attribute(column_attribute);
  Block* block = new Block(schema, HeapBufferAllocator::Get());
  CHECK(block->Reallocate(capacity));
  return block;
}

TEST_F(AggregatorsTest, ComputeSimpleAggregation) {
  scoped_ptr<Block> result_block(EmptyBlockWithSingleNullableColumn(INT64, 4));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          MIN, INT64, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 1, 2, 3 };
  const int64 input1[] = { -5, 0, 4, 4 };
  View view(TupleSchema::Singleton("", INT64, NOT_NULLABLE));
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());
  const int64 input2[] = { -2, 3, 1, -1 };
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  scoped_ptr<Block> expected_output(BlockBuilder<INT64>()
                                    .AddRow(-5)
                                    .AddRow(0)
                                    .AddRow(1)
                                    .AddRow(-1)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest,
       StoreResultOfAggregationInSecondColumnOfResultBlock) {
  // Create result block with two columns, store aggregation result in the
  // second column.
  scoped_ptr<Block> result_block(BlockBuilder<INT64, INT64>()
                                 .AddRow(__, __)
                                 .AddRow(__, __)
                                 .AddRow(__, __)
                                 .AddRow(__, __)
                                 .Build());
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          SUM, INT32, result_block.get(), 1)));

  const rowid_t result_index[] = { 0, 1, 2, 3 };
  View view(TupleSchema::Singleton("", INT64, NULLABLE));
  const int32 input1[] = { -5, 0, 4, 4 };
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());
  const int32 input2[] = { -2, 3, 1, -1 };
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());


  // First column should be left unchanged.
  scoped_ptr<Block> expected_output(BlockBuilder<INT64, INT64>()
                                    .AddRow(__, -7)
                                    .AddRow(__, 3)
                                    .AddRow(__, 5)
                                    .AddRow(__, 3)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest,
       StoreResultOfAggregationInColumnOfDifferentTypeThenInputColumn) {
  scoped_ptr<Block> result_block(EmptyBlockWithSingleNullableColumn(INT64, 4));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          SUM, UINT32, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 1, 2, 3 };
  View view(TupleSchema::Singleton("", UINT32, NULLABLE));
  const uint32 input1[] = { 2, 3, 1, 0xFFFFFFFF };
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());
  const uint32 input2[] = { 5, 0, 4, 4 };
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  scoped_ptr<Block> expected_output(BlockBuilder<INT64>()
                                    .AddRow(7)
                                    .AddRow(3)
                                    .AddRow(5)
                                    .AddRow(0xFFFFFFFFLL + 4)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeAggregationOfValuesWithNulls) {
  scoped_ptr<Block> result_block(EmptyBlockWithSingleNullableColumn(INT32, 4));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          SUM, INT32, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 1, 2, 3 };
  int32 input1[] = { -2, 3, 1, 0};
  small_bool_array input1_is_null;
  bool input1_is_null_data[] = { false, true, false, true};
  bit_pointer::FillFrom(input1_is_null.mutable_data(), input1_is_null_data, 4);
  View view(TupleSchema::Singleton("", INT32, NULLABLE));
  view.mutable_column(0)->Reset(input1, input1_is_null.mutable_data());
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  const int32 input2[] = { -5, 0, 4, 4};
  small_bool_array input2_is_null;
  const bool input2_is_null_data[] = { true, true, false, false};
  bit_pointer::FillFrom(input2_is_null.mutable_data(), input2_is_null_data, 4);
  view.mutable_column(0)->Reset(input2, input2_is_null.mutable_data());
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());


  scoped_ptr<Block> expected_output(
      BlockBuilder<INT32>()
      // Taken from inpu1[0] because input2[0] is null.
      .AddRow(-2)

      // Null because both input1[1] and input2[1] are null.
      .AddRow(__)
      .AddRow(5)

      // Taken from inpu2[3] because input1[3] is null.
      .AddRow(4)
      .Build());

  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeAggregationWithResultStoredAsString) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNullableColumn(STRING, 4));

  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          MIN, STRING, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 1, 2, 3 };
  View view(TupleSchema::Singleton("", STRING, NULLABLE));
  const StringPiece input1[] = { "baba", "baba", "dada",  "oda"};
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());
  const StringPiece input2[] = { "abakus", "baba", "ada", "wada"};
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  scoped_ptr<Block> expected_output(BlockBuilder<STRING>()
                                    .AddRow("abakus")
                                    .AddRow("baba")
                                    .AddRow("ada")
                                    .AddRow("oda")
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ResultIndexRespectedWhileUpdatingAggregation) {
  scoped_ptr<Block> result_block(EmptyBlockWithSingleNullableColumn(INT32, 4));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          SUM, INT32, result_block.get(), 0)));

  View view(TupleSchema::Singleton("", INT32, NULLABLE));
  const int32 input[] = { 1, 1, 1, 1 };
  view.mutable_column(0)->Reset(input, bool_ptr(NULL));
  // Agregate all results in 3rd element of result table.
  const rowid_t result_index[] = { 2, 2, 2, 2 };
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  scoped_ptr<Block> expected_output(BlockBuilder<INT32>()
                                    .AddRow(__)
                                    .AddRow(__)
                                    .AddRow(4)
                                    .AddRow(__)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeCount) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNotNullableColumn(INT64, 1));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateCountAggregator(
          result_block.get(), 0)));

  View view(TupleSchema::Singleton("", INT64, NULLABLE));
  const int64 input[] = { -5, 0, 4, 4 };
  view.mutable_column(0)->Reset(input, bool_ptr(NULL));
  const rowid_t result_index[] = { 0, 0, 0, 0 };
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  scoped_ptr<Block> expected_output(BlockBuilder<INT64>()
                                    .AddRow(4)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeCountWithoutInputColumn) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNotNullableColumn(INT64, 1));

  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateCountAggregator(
          result_block.get(), 0)));
  const rowid_t result_index[] = { 0, 0, 0, 0 };
  ASSERT_TRUE(aggregator->UpdateAggregation(NULL, 4, result_index)
              .is_success());

  scoped_ptr<Block> expected_output(BlockBuilder<INT64>()
                                    .AddRow(4)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeCountOfValuesWithNulls) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNotNullableColumn(INT32, 1));

  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateCountAggregator(
          result_block.get(), 0)));

  View view(TupleSchema::Singleton("", INT32, NULLABLE));
  const int64 input[] = { -5, 0, 4, 4 };
  small_bool_array input_is_null;
  const bool input_is_null_data[] = { true, false, true, false};
  bit_pointer::FillFrom(input_is_null.mutable_data(), input_is_null_data, 4);
  view.mutable_column(0)->Reset(input, input_is_null.mutable_data());
  const rowid_t result_index[] = { 0, 0, 0, 0 };
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  // NULL values should not be counted.
  scoped_ptr<Block> expected_output(BlockBuilder<INT32>()
                                    .AddRow(2)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeDistinctCountOfIntegers) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNotNullableColumn(INT64, 1));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateDistinctCountAggregator(
          INT64, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 0, 0, 0 };
  // Only distinct values are counted, so result should be 2.
  const int64 input1[] = { -5, 2, -5, -5 };
  View view(TupleSchema::Singleton("", INT64, NULLABLE));
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());
  scoped_ptr<Block> expected_output(BlockBuilder<INT64>()
                                    .AddRow(2)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeDistinctCountOfStrings) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNotNullableColumn(INT64, 1));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateDistinctCountAggregator(
          STRING, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 0, 0, 0 };
  const StringPiece input1[] = { "baba", "baba", "dada",  "oda"};
  View view(TupleSchema::Singleton("", INT64, NULLABLE));
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  const StringPiece input2[] = { "zzzzzzzz", "oda", "baba"};
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 3, result_index)
              .is_success());

  // Overall 4 distinct strings in both input columns.
  scoped_ptr<Block> expected_output(BlockBuilder<INT64>()
                                    .AddRow(4)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeDistinctConcatOfStrings) {
  scoped_ptr<Block> result_block(EmptyBlockWithSingleNullableColumn(STRING, 2));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateDistinctAggregator(
          CONCAT, STRING, result_block.get(), 0)));

  // Concatenate distinct strings from rows 0 and 1 into the first result row
  // and from rows 2 and 3 into the second result row.
  const rowid_t result_index[] = { 0, 0, 1, 1 };
  const StringPiece input1[] = { "baba", "aba", "baba",  "baba"};
  View view(TupleSchema::Singleton("", STRING, NULLABLE));
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  const StringPiece input2[] = { "aba", "oda", "rada", "baba"};
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  scoped_ptr<Block> expected_output(BlockBuilder<STRING>()
                                    // DISTINCT(baba,aba,aba,oda)=baba,aba,oda
                                    .AddRow("baba,aba,oda")
                                    // DISTINCT(baba,baba,raba,baba)=baba,rada
                                    .AddRow("baba,rada")
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeConcatOfInts) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNullableColumn(STRING, 1));

  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          CONCAT, INT32, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 0, 0, 0 };
  View view(TupleSchema::Singleton("", STRING, NULLABLE));
  const int32 input1[] = { -5, 0, 345, 2 };
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());
  const int32 input2[] = { -2, 3, 1};
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 3, result_index)
              .is_success());

  scoped_ptr<Block> expected_output(BlockBuilder<STRING>()
                                    .AddRow("-5,0,345,2,-2,3,1")
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ComputeConcatOfStrings) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNullableColumn(STRING, 1));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          CONCAT, STRING, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 0, 0, 0 };
  View view(TupleSchema::Singleton("", STRING, NULLABLE));
  const StringPiece input1[] = { "baba", "baba", "dada"};
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 3, result_index)
              .is_success());
  const StringPiece input2[] = { "aba", "wada"};
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 2, result_index)
              .is_success());

  scoped_ptr<Block> expected_output(BlockBuilder<STRING>()
                                    .AddRow("baba,baba,dada,aba,wada")
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ResetSetsAllResultsToNulls) {
  scoped_ptr<Block> result_block(EmptyBlockWithSingleNullableColumn(INT64, 4));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          MIN, INT64, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 1, 2, 3 };
  const int64 input1[] = { -5, 0, 4, 4 };
  View view(TupleSchema::Singleton("", INT64, NULLABLE));
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());
  aggregator->Reset();

  scoped_ptr<Block> expected_output(BlockBuilder<INT64>()
                                    .AddRow(__)
                                    .AddRow(__)
                                    .AddRow(__)
                                    .AddRow(__)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ResetSetsAllCountResultsToZero) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNotNullableColumn(INT64, 1));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateCountAggregator(
          result_block.get(), 0)));

  View view(TupleSchema::Singleton("", INT64, NULLABLE));
  const int64 input[] = { -5, 0, 4, 4 };
  view.mutable_column(0)->Reset(input, bool_ptr(NULL));
  const rowid_t result_index[] = { 0, 0, 0, 0 };
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());
  aggregator->Reset();

  scoped_ptr<Block> expected_output(BlockBuilder<INT64>()
                                    .AddRow(0)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ResetWorksOnStringResultColumn) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNullableColumn(STRING, 4));

  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          MAX, STRING, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 1, 2, 3 };
  View view(TupleSchema::Singleton("", STRING, NULLABLE));
  const StringPiece input1[] = { "baba", "baba", "dada",  "oda"};
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  // Reset aggregator to discard all results from the first update.
  aggregator->Reset();

  const StringPiece input2[] = { "aba", "baba", "ada", "wada"};
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  // All results are from the second update.
  scoped_ptr<Block> expected_output(BlockBuilder<STRING>()
                                    .AddRow("aba")
                                    .AddRow("baba")
                                    .AddRow("ada")
                                    .AddRow("wada")
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, ResetDistinctAggregationDiscardsOldDistinctValues) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNotNullableColumn(INT64, 1));
  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateDistinctCountAggregator(
          STRING, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 0, 0, 0 };
  const StringPiece input1[] = { "baba", "baba", "dada",  "oda"};
  View view(TupleSchema::Singleton("", STRING, NULLABLE));
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 4, result_index)
              .is_success());

  // Reset aggregator to discard all values from the first input column.
  aggregator->Reset();

  const StringPiece input2[] = { "zzzzzzzz", "oda", "baba", "oda"};
  view.mutable_column(0)->Reset(input2, bool_ptr(NULL));
  ASSERT_TRUE(aggregator->UpdateAggregation(&view.column(0), 3, result_index)
              .is_success());

  // Overall 3 distinct strings in the second input column.
  scoped_ptr<Block> expected_output(BlockBuilder<INT64>()
                                    .AddRow(3)
                                    .Build());
  EXPECT_VIEWS_EQUAL(expected_output->view(), result_block->view());
}

TEST_F(AggregatorsTest, NotSupportedAggregationDetected) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNullableColumn(STRING, 1));

  FailureOrOwned<ColumnAggregator> status =
      ColumnAggregatorFactory().CreateAggregator(
          SUM, STRING, result_block.get(), 0);
  EXPECT_TRUE(status.is_failure());
}

TEST_F(AggregatorsTest, NotSupportedCountOutputTypeDetected) {
  scoped_ptr<Block> result_block(
      EmptyBlockWithSingleNotNullableColumn(DATETIME, 1));
  FailureOrOwned<ColumnAggregator> status =
      ColumnAggregatorFactory().CreateCountAggregator(result_block.get(), 0);
  EXPECT_TRUE(status.is_failure());
}

TEST_F(AggregatorsTest, UpdateStringAggregationReturnsErrorWhenOutOfMemory) {
  Attribute column_attribute("col0", STRING, NULLABLE);
  TupleSchema schema;
  schema.add_attribute(column_attribute);
  // Pass enough memory to allocate a block, but not enough to update
  // aggregation.
  MemoryLimit memory_limit(32);
  scoped_ptr<Block> result_block(new Block(schema, &memory_limit));
  CHECK(result_block->Reallocate(1));

  scoped_ptr<ColumnAggregator> aggregator(
      SucceedOrDie(ColumnAggregatorFactory().CreateAggregator(
          MAX, STRING, result_block.get(), 0)));

  const rowid_t result_index[] = { 0, 0, 0, 0 };
  View view(TupleSchema::Singleton("", STRING, NULLABLE));
  // String of length 33 shouldn't fit in what is left from 32 bytes.
  const StringPiece input1[] = { "foooooooooooooooooooooooooooooooo"};
  view.mutable_column(0)->Reset(input1, bool_ptr(NULL));
  ASSERT_FALSE(aggregator->UpdateAggregation(&view.column(0), 1, result_index)
               .is_success());
}

}  // namespace aggregations
}  // namespace supersonic
