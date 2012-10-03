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
//
// Aggregations that are working on columns of input values and are updating
// column of result values.

#ifndef SUPERSONIC_CURSOR_CORE_COLUMN_AGGREGATOR_H_
#define SUPERSONIC_CURSOR_CORE_COLUMN_AGGREGATOR_H_

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class Block;
class Column;

namespace aggregations {

// From the SQL Reference Manual:
// "All aggregate functions except COUNT(*) and GROUPING ignore nulls.
// COUNT never returns null, but returns either a number or zero.
// For all the remaining aggregate functions, if the data set contains
// no rows, or contains only rows with nulls as arguments to the aggregate
// function, then the function returns null".

// Interface for updating and reseting result of an aggregation. The interface
// does not specify how result column is passed to concrete implementation of
// ColumnAggregator.
class ColumnAggregator {
 public:
  ColumnAggregator();
  // Frees memory allocated for storing state of an aggregation.
  virtual ~ColumnAggregator();

  // Updates aggregation result. All values from input column all aggregated
  // into result column.
  // Input column can be null if aggregation can work on input without any
  // columns (currently only COUNT).
  // result_index_map[i] specifies to which result value ith value from
  // input column needs to be aggregated. All values in result_index_map must
  // be smaller then size of the result block.
  // Returns failure when there is not enough memory to update aggregation
  // result.
  virtual FailureOrVoid UpdateAggregation(const Column* input,
                                          rowcount_t input_row_count,
                                          const rowid_t result_index_map[]) = 0;

  // Called after the block that this column aggregator refers to got
  // reallocated. Gives the aggregator an opportunity to re-fetch column
  // pointers, that may have changed as the result of reallocation. The
  // parameters specify the previous and the new row capacity of the
  // reallocated block.
  virtual void Rebind(rowcount_t previous_capacity,
                      rowcount_t new_capacity) = 0;

  // Resets state of an aggregator, sets all results to NULL or 0 (for
  // COUNT). Frees memory allocated for storing previous state of the
  // aggregation. Reset() does not need to be called before destructor.
  virtual void Reset() = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(ColumnAggregator);
};

class ColumnAggregatorFactoryImpl;

// Creates concrete instances of column aggregators for
// computing specific aggregations.
class ColumnAggregatorFactory {
 public:
  ColumnAggregatorFactory();
  ~ColumnAggregatorFactory();
  // Four separate factory methods are needed here because COUNT does not need
  // an input column.

  // Creates aggregator for any aggregation operator with the exception of
  // COUNT.
  // aggregation_operator - type of aggregation.
  // input_type - type of values in input columns.
  //
  // result_block - block with a column to which result of the aggregation will
  // be stored. It is responsibility of a caller to allocate result block and to
  // make sure that UpdateAggregation calls do not try to write at index larger
  // then capacity of the result block.
  //
  // result_column_index - index of a column in the result block to which result
  // of the aggregation will be stored. The result column needs to be
  // nullable. Caller does not need to initialize content of the result column
  // and its nullability table, ColumnAggregator does it automatically.
  FailureOrOwned<ColumnAggregator> CreateAggregator(
      Aggregation aggregation_operator,
      DataType input_type,
      Block* result_block,
      int result_column_index);

  // The same as CreateAggregator() but aggregation function will be applied
  // only to distinct input values. For example DISTINCT SUM(1,1,2) == 3.
  FailureOrOwned<ColumnAggregator> CreateDistinctAggregator(
      Aggregation aggregation_operator,
      DataType input_type,
      Block* result_block,
      int result_column_index);

  // Creates aggregator for COUNT operation. The same as CreateAggregator() but
  // input_type is not needed because COUNT does not access values from input
  // column, result column can not be nullable, because result of COUNT is never
  // null.
  FailureOrOwned<ColumnAggregator> CreateCountAggregator(
      Block* result_block,
      int result_column_index);

  // The same as CreateCountAggregator but only distinct input values are
  // counted. Input type is needed here because DISTINCT COUNT needs to access
  // input values. For example DISTINCT COUNT(4, 5, 4) == 2.
  FailureOrOwned<ColumnAggregator> CreateDistinctCountAggregator(
      DataType input_type,
      Block* result_block,
      int result_column_index);

 private:
  scoped_ptr<ColumnAggregatorFactoryImpl> pimpl_;

  DISALLOW_COPY_AND_ASSIGN(ColumnAggregatorFactory);
};

}  // namespace aggregations
}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_COLUMN_AGGREGATOR_H_
