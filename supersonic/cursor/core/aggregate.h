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
// Cursors that allow to group together rows with equal key columns
// (possibly empty in which case all rows are grouped together) and compute
// aggregate functions on groups of rows.

#ifndef SUPERSONIC_CURSOR_CORE_AGGREGATE_H_
#define SUPERSONIC_CURSOR_CORE_AGGREGATE_H_

#include <vector>
using std::vector;
#include <string>
using std::string;

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/core/aggregator.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class HybridGroupDebugOptions;

// Represents a collection of (symbolic) aggregation operations to perform on
// a group of attributes.
class AggregationSpecification {
 public:
  // Describes a single attribute in a multi-attribute aggregator.
  class Element {
   public:
    Element(const Aggregation aggregation,
            const StringPiece& input_name,
            const StringPiece& output_name,
            bool distinct)
        : aggregation_(aggregation),
          input_name_(input_name.as_string()),
          output_name_(output_name.as_string()),
          output_type_specified_(false),
          distinct_(distinct) {}

    Element(const Aggregation aggregation,
            const StringPiece& input_name,
            const StringPiece& output_name,
            DataType output_type,
            bool distinct)
        : aggregation_(aggregation),
          input_name_(input_name.as_string()),
          output_name_(output_name.as_string()),
          output_type_(output_type),
          output_type_specified_(true),
          distinct_(distinct) {}

    const Aggregation& aggregation_operator() const { return aggregation_; }
    const string& output() const { return output_name_; }
    const string& input() const { return input_name_; }
    bool output_type_specified() const { return output_type_specified_; }

    DataType output_type() const {
      CHECK(output_type_specified_);
      return output_type_;
    }

    void set_input(StringPiece name) { input_name_ = name.as_string(); }
    void set_output(StringPiece name) { output_name_ = name.as_string(); }
    void set_aggregation_operator(Aggregation aggregation) {
      aggregation_ = aggregation;
    }

    bool is_distinct() const {
      return distinct_;
    }

   private:
    Aggregation aggregation_;
    string input_name_;
    string output_name_;
    DataType output_type_;
    bool output_type_specified_;
    bool distinct_;
    // Copyable.
  };

  AggregationSpecification() {}

  // Defines aggregation function to be computed with given input and output
  // columns. Output column will have a default type (UINT64 for COUNT, and type
  // of input column for all other aggregations).
  AggregationSpecification* AddAggregation(Aggregation aggregation,
                                           const StringPiece& input_name,
                                           const StringPiece& output_name) {
    return add(Element(aggregation, input_name, output_name, false));
  }

  // Like add aggregation but aggregation function will be applied only to
  // distinct input values (for example DISTINCT COUNT(4, 5, 4) is 2).
  AggregationSpecification*  AddDistinctAggregation(
      Aggregation aggregation,
      const StringPiece& input_name,
      const StringPiece& output_name) {
    return add(Element(aggregation, input_name, output_name, true));
  }

  // Defines aggregation function to be computed with given input and output
  // columns. Output column will have a specifed output type.
  AggregationSpecification* AddAggregationWithDefinedOutputType(
      Aggregation aggregation,
      const StringPiece& input_name,
      const StringPiece& output_name,
      DataType output_type) {
    return add(Element(aggregation, input_name, output_name, output_type,
                       false));
  }

  // Like AddAggregationWithDefinedOutputType but aggregation function will be
  // applied only to distinct input values.
  AggregationSpecification* AddDistinctAggregationWithDefinedOutputType(
      Aggregation aggregation,
      const StringPiece& input_name,
      const StringPiece& output_name,
      DataType output_type) {
    return add(Element(aggregation, input_name, output_name, output_type,
                       true));
  }

  // Takes ownership of element.
  AggregationSpecification* add(const Element& element) {
    aggregations_.push_back(element);
    return this;
  }

  const int size() const { return aggregations_.size(); }
  const Element& aggregation(int pos) const { return aggregations_[pos]; }

 private:
  vector<Element> aggregations_;
};

class GroupAggregateOptions {
 public:
  static const int64 kDefaultResultEstimatedGroupCount = 16;
  GroupAggregateOptions()
      : memory_quota_(std::numeric_limits<size_t>::max()),
        enforce_quota_(false),
        estimated_result_row_count_(kDefaultResultEstimatedGroupCount) {}
  size_t memory_quota() const { return memory_quota_; }
  bool enforce_quota() const { return enforce_quota_; }
  int64 estimated_result_row_count() const {
    return estimated_result_row_count_;
  }

  GroupAggregateOptions* set_memory_quota(size_t memory_quota) {
    memory_quota_ = memory_quota;
    return this;
  }
  GroupAggregateOptions* set_enforce_quota(bool enforce_quota) {
    enforce_quota_ = enforce_quota;
    return this;
  }
  GroupAggregateOptions* set_estimated_result_row_count(int64 estimate) {
    estimated_result_row_count_ = estimate;
    return this;
  }

 private:
  size_t memory_quota_;
  bool enforce_quota_;
  int64 estimated_result_row_count_;
  DISALLOW_COPY_AND_ASSIGN(GroupAggregateOptions);
};

// Creates an operation to group and aggregate rows with the same key. The
// cursor allocates a block to store its output. Initially, the block is equal
// to result_estimated_group_count rows, and it grows as needed.
// The group_by parameter describes which columns are a key for grouping. All
// columns specified in group_by must exist in the child's schema. The
// aggregation_specification parameter specifies aggregations to be applied to
// the grouped rows. When the result of the aggregation is larger than
// result_maximum_row_capacity, ERROR_MEMORY_EXCEEDED is returned.
// The cursor's schema consists of group-by columns and aggregation output
// columns.
//
// The result row count is guaranteed not to exceed the input row count. In
// particular, the output will always be empty if the input is empty, regardless
// of the group_by.
//
// NOTE(user): if you want a total aggregation that always (even for empty
// input) returns exactly 1 row, then use ScalarAggregation instead.
Operation* GroupAggregate(
    const SingleSourceProjector* group_by,
    const AggregationSpecification* aggregation,
    GroupAggregateOptions* options,
    Operation* child);

// Creates an operation to group and aggregate as many rows as possible within
// the available memory. The cursor does not guarantee to produce fully
// aggregated results. It aggregates as many rows as can fit in output block
// and returns them. When all these rows are read, aggregation is repeated until
// the input is exhausted. This algorithm does NOT guarantee that all the result
// rows are key-unique on the group-by key. It only guarantees that result rows
// are key-unique on group-by key within each returned result view.
// BestEffortGroupAggregateCursor is useful mainly as the first step in
// aggregating of very large amount of rows. Rows pregrouped this way can be
// then shuffled by the group-by key and serialized to disk or sent over the
// network. Shuffled output can be then processed with the standard
// AggregateCursor to produce final, fully aggregated result.
//
// The arguments are like in GroupAggregate(). The only difference is that
// ERROR_MEMORY_EXCEEDED is not returned when the input is too large, because
// this Cursor can process input of any size.
Operation* BestEffortGroupAggregate(
    const SingleSourceProjector* group_by,
    const AggregationSpecification* aggregation,
    GroupAggregateOptions* options,
    Operation* child);

// Bound version of GroupAggregate*. <original_allocator> is for the original
// allocator if it was wrapped in MemoryLimit or GuaranteeMemory in <allocator>.
FailureOrOwned<Cursor>
BoundGroupAggregate(
    const BoundSingleSourceProjector* group_by,
    Aggregator* aggregator,
    BufferAllocator* allocator,             // Takes ownership.
    BufferAllocator* original_allocator,  // Doesn't take ownership.
                                            // Can be equal allocator or NULL.
    bool best_effort,
    Cursor* child);

// Creates a cursor to aggregate input that is already clustered by
// clustered_by_columns (i.e., rows with the same key must be consecutive).
// This condition is trivially satisfied if the input is sorted by
// 'clustered_by_columns' (in any order). Unlike GroupAggregate,
// AggregateClusters is a streaming cursor; i.e. it does not need to
// buffer the input before it can start producing the result. Hence,
// memory requirements are small.
Operation* AggregateClusters(
    const SingleSourceProjector* clustered_by_columns,
    const AggregationSpecification* aggregation,
    Operation* child);

// Bound version of AggregateClusters.
FailureOrOwned<Cursor> BoundAggregateClusters(
    const BoundSingleSourceProjector* group_by,
    Aggregator* aggregator,
    BufferAllocator* allocator,
    Cursor* child);

// Normaly it doesn't make sense to change AggregateClusters' output block
// capacity. But the default size of 2 * Cursor::kDefaultRowCount rows is too
// large for unit testing the logic that handles inputs that do not fit into
// output block.
//
// The smallest possible value for block_size is 2.
Operation* AggregateClustersWithSpecifiedOutputBlockSize(
    const SingleSourceProjector* clustered_by_columns,
    const AggregationSpecification* aggregation,
    rowcount_t block_size,
    Operation* child);

// Creates an operation to group and aggregate rows with the same key. Uses disk
// if there's not enough memory to aggregate input data.
// TODO(user): (Same as in sort.h) in ptab's words: Instead of directory prefix
// I wish we use something like:
//
// class TemporaryFileFactory {
//   File* CreateTemporaryFile();
// }
//
// This way transition to d-server or other storage would be easier. Even for
// testing it might be helpful.
Operation* HybridGroupAggregate(
    const SingleSourceProjector* group_by_columns,
    const AggregationSpecification* aggregation_specification,
    size_t memory_quota,
    StringPiece temporary_directory_prefix,
    Operation* child);

// Bound version of HybridGroupAggregate. Takes ownership of group_by_columns,
// debug_options and child. For normal use debug_options should be NULL.
FailureOrOwned<Cursor> BoundHybridGroupAggregate(
    const SingleSourceProjector* group_by_columns,
    const AggregationSpecification& aggregation_specification,
    StringPiece temporary_directory_prefix,
    BufferAllocator* allocator,
    size_t memory_quota,
    const HybridGroupDebugOptions* debug_options,
    Cursor* child);

// Aggregates the entire input. Result is always a single row (even if the
// input is empty). Takes ownership of the aggregation_specification and
// the child.
Operation* ScalarAggregate(AggregationSpecification* aggregation_specification,
                           Operation* child);

// Bound version of the above.
Cursor* BoundScalarAggregate(Aggregator* aggregator, Cursor* child);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_AGGREGATE_H_
