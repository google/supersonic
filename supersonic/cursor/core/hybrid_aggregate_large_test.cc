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

#include <map>
using std::map;
#include <memory>
#include <set>
#include "supersonic/utils/std_namespace.h"

#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/sort.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"
#include "supersonic/testing/repeating_block.h"

#include "gtest/gtest.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

namespace {

class HybridAggregateLargeTest : public testing::Test {
 protected:
  CompoundSingleSourceProjector empty_projector_;
};

// A lot of tests just copied from aggregate_groups_test.cc. Maybe it would be
// better to share this code somehow and avoid "copy & paste".

FailureOrOwned<Cursor> CreateHybridGroupAggregate(
    const SingleSourceProjector& group_by,
    const AggregationSpecification& aggregation,
    Cursor* input) {
  std::unique_ptr<Cursor> input_owner(input);
  return BoundHybridGroupAggregate(
      group_by.Clone(),
      aggregation,
      "",
      HeapBufferAllocator::Get(),
      16,
      NULL,
      input_owner.release());
}

TEST_F(HybridAggregateLargeTest, LargeInputWithManyDistinctKeys) {
  OperationTest test;
  test.SetInputViewSizes(1024);
  test.SetResultViewSizes(1024);
  // Skipping barrier checks because hybrid group, as a blocking operation, in
  // the worst case will produce about (8 * reps * (1 / 0.99)) rows, and even
  // for reps == 2000 it's 1600000 > kMaxBarrierRetries.
  test.SkipBarrierHandlingChecks(true);
  const rowcount_t input_size = 1000000;
  TestDataBuilder<INT32, INT32> input_builder;
  TestDataBuilder<INT32, INT32, UINT64> expected_result_builder;
  MTRandom random(0);
  map<int32, set<int32> > values_for_key;
  map<int32, int32> sum_for_key;
  for (int i = 0; i < input_size; ++i) {
    int32 key = random.Rand32() % 20000;
    int32 value = random.Rand32() % 1000;
    input_builder.AddRow(key, value);
    values_for_key[key].insert(value);
    sum_for_key[key] += value;
  }
  for (map<int32, set<int32> >::const_iterator mi = values_for_key.begin();
       mi != values_for_key.end(); ++mi) {
    expected_result_builder.AddRow(mi->first,
                                   sum_for_key[mi->first],
                                   mi->second.size());
  }
  test.SetInput(input_builder.Build());
  test.SetExpectedResult(expected_result_builder.Build());
  test.SetIgnoreRowOrder(true);
  std::unique_ptr<const SingleSourceProjector> group_by_columns(
      ProjectNamedAttribute("col0"));
  std::unique_ptr<AggregationSpecification> aggregation(
      new AggregationSpecification);
  aggregation->AddAggregation(SUM, "col1", "sum");
  aggregation->AddDistinctAggregation(COUNT, "col1", "cnt");
  test.Execute(HybridGroupAggregate(
      group_by_columns.release(),
      aggregation.release(),
      320000,
      "",
      test.input()));
}

// Large input, but small amount of unique keys and distinct aggregated values.
TEST_F(HybridAggregateLargeTest, NonDistinctAndDistinctAggregationsLargeInput) {
  OperationTest test;
  test.SetInputViewSizes(1024);
  test.SetResultViewSizes(1024);
  // Skipping barrier checks because hybrid group, as a blocking operation, in
  // the worst case will produce about (8 * reps * (1 / 0.99)) rows, and even
  // for reps == 2000 it's 1600000 > kMaxBarrierRetries.
  test.SkipBarrierHandlingChecks(true);
  const int reps = 4000;
  test.SetInput(
      new RepeatingBlockOperation(
        BlockBuilder<INT32, INT32>()
        .AddRow(1, 3)
        .AddRow(1, 4)
        .AddRow(3, -1)
        .AddRow(3, -2)
        .AddRow(2, 4)
        .AddRow(3, -3)
        .AddRow(1, 3)
        .AddRow(1, __)
        .Build(),
      8 * reps));
  test.SetExpectedResult(TestDataBuilder<INT32, INT32, UINT64, INT32, UINT64,
                                         UINT64>()
                         .AddRow(1, 7, 2, 10 * reps, 3 * reps, 4 * reps)
                         .AddRow(2, 4, 1, 4 * reps, 1 * reps, 1 * reps)
                         .AddRow(3, -6, 3, -6 * reps, 3 * reps, 3 * reps)
                         .Build());
  std::unique_ptr<const SingleSourceProjector> group_by_columns(
      ProjectNamedAttribute("col0"));
  std::unique_ptr<AggregationSpecification> aggregation(
      new AggregationSpecification);
  aggregation->AddDistinctAggregation(SUM, "col1", "sum");
  aggregation->AddDistinctAggregation(COUNT, "col1", "cnt");
  aggregation->AddAggregation(SUM, "col1", "sum2");
  aggregation->AddAggregation(COUNT, "col1", "cnt2");
  aggregation->AddAggregation(COUNT, "", "cnt3");

  test.Execute(HybridGroupAggregate(
      group_by_columns.release(),
      aggregation.release(),
      8000,
      "",
      test.input()));
}

}  // namespace

}  // namespace supersonic
