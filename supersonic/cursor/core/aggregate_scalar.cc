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

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/aggregator.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/iterators.h"

namespace supersonic {

class ScalarAggregateCursor : public BasicCursor {
 public:
  // Takes ownership of the aggregator and the child.
  ScalarAggregateCursor(Aggregator* aggregator, Cursor* child)
      : BasicCursor(aggregator->schema()),
        child_(child),
        eos_(false),
        aggregator_(aggregator) {
    std::fill(&zeros_[0], &zeros_[arraysize(zeros_)], 0);
    my_view()->ResetFrom(aggregator_->data());
  }

  virtual ResultView Next(rowcount_t max_row_count) {
    if (eos_) return ResultView::EOS();
    while (child_.Next(arraysize(zeros_), false)) {
      PROPAGATE_ON_FAILURE(
          aggregator_->UpdateAggregations(child_.view(), zeros_));
    }
    PROPAGATE_ON_FAILURE(child_);
    if (!child_.is_done()) {
      CHECK(child_.result().is_waiting_on_barrier());
      return child_.result();
    }
    CHECK(child_.is_eos());
    eos_ = true;
    my_view()->ResetFrom(aggregator_->data());
    my_view()->set_row_count(1);
    return ResultView::Success(my_view());
  }

  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual void Interrupt() { child_.Interrupt(); }

  virtual void ApplyToChildren(CursorTransformer* transformer) {
    child_.ApplyToCursor(transformer);
  }

  virtual CursorId GetCursorId() const { return SCALAR_AGGREGATE; }

 private:
  CursorIterator child_;
  bool eos_;  // Set after the result row has been returned.
  std::unique_ptr<Aggregator> aggregator_;

  // We aggregate all the results to a single row. Aggregators operate on a
  // block-by-block basis, and expect to be informed to which output row should
  // the particular input row be aggregated, so we pre-fill an array with zeros
  // and use it for all aggregations.
  rowid_t zeros_[Cursor::kDefaultRowCount];

  DISALLOW_COPY_AND_ASSIGN(ScalarAggregateCursor);
};

class ScalarAggregateOperation : public BasicOperation {
 public:
  // Passes ownership of AggregationSpecification and child_operation to
  // the operation.
  ScalarAggregateOperation(AggregationSpecification* aggregation_specification,
                           Operation* child_operation)
      : BasicOperation(child_operation),
        aggregation_specification_(aggregation_specification) {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> child_cursor = child()->CreateCursor();
    PROPAGATE_ON_FAILURE(child_cursor);
    FailureOrOwned<Aggregator> aggregator = Aggregator::Create(
        *aggregation_specification_, child_cursor->schema(),
        buffer_allocator(), 1);
    PROPAGATE_ON_FAILURE(aggregator);
    return Success(
        BoundScalarAggregate(aggregator.release(), child_cursor.release()));
  }

 private:
  std::unique_ptr<AggregationSpecification> aggregation_specification_;
  DISALLOW_COPY_AND_ASSIGN(ScalarAggregateOperation);
};

Operation* ScalarAggregate(AggregationSpecification* aggregation_specification,
                           Operation* child) {
  return new ScalarAggregateOperation(aggregation_specification, child);
}

Cursor* BoundScalarAggregate(Aggregator* aggregator, Cursor* child) {
  return new ScalarAggregateCursor(aggregator, child);
}

}  // namespace supersonic
