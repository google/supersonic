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

#ifndef SUPERSONIC_CURSOR_CORE_AGGREGATOR_H_
#define SUPERSONIC_CURSOR_CORE_AGGREGATOR_H_

#include <utility>
using std::make_pair;
using std::pair;
#include <vector>
using std::vector;

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/core/column_aggregator.h"
#include "supersonic/utils/linked_ptr.h"

namespace supersonic {

class AggregationSpecification;
class BufferAllocator;

// Creates and updates block that holds results of aggregations.
class Aggregator {
 public:
  // Creates an Aggregator. The aggregation_specification describes aggregations
  // to be performed. The aggregator initially stores
  // result_initial_row_capacity rows, and can grow as needed.
  static FailureOrOwned<Aggregator> Create(
      const AggregationSpecification& aggregation_specification,
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t result_initial_row_capacity);

  const TupleSchema& schema() const { return schema_; }

  const View& data() const { return data_->view(); }

  // Makes room for row_capacity rows. Returns true on success; false on OOM.
  bool Reallocate(rowcount_t row_capacity);

  rowcount_t capacity() const { return data_->row_capacity(); }

  // Aggregates each row from an input view to the aggregation result block at
  // specified indexes. result_index_map must be of size
  // view.row_count(). Caller must ensure that all values from result_index_map
  // are lower then aggregation block capacity.
  // Update can fail if there is not enough memory to hold a result of an
  // aggregation.
  FailureOrVoid UpdateAggregations(const View& view,
                                   const rowid_t result_index_map[]);

  // Resets aggregation result block to hold default values.
  void Reset();

 private:
  Aggregator() {}

  FailureOrVoid Init(const AggregationSpecification& aggregation_specification,
                     const TupleSchema& result_schema,
                     BufferAllocator* allocator,
                     rowcount_t result_initial_row_capacity);

  TupleSchema schema_;
  scoped_ptr<Block> data_;

  // Pairs of input column position (can be -1 for COUNT) and aggregators that
  // should be used to compute aggregation on given inputs.
  vector<pair<int, linked_ptr<aggregations::ColumnAggregator> > >
      column_aggregator_;

  DISALLOW_COPY_AND_ASSIGN(Aggregator);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_AGGREGATOR_H_
