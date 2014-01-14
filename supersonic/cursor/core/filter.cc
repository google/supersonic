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

#include "supersonic/cursor/core/filter.h"

#include <stddef.h>

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class BufferAllocator;

namespace {

static const int kMinimumFillPercent = 25;

// TODO(user): perhaps this class should be broken in two: a simple
// shallow-copy filter, and an (optional) compactor. To make this work w/o
// imposing additional copying, we'd need to add support for selection vectors
// to views.
//
// TODO(onufry): At the moment the filter calculates the predicate for all the
// rows in the input at one go (at PrepareInputRowIds). This means the input
// size has to be bounded by the row_capacity of the predicate. In the future
// it would be better to hold additional pointers to show for what area of the
// input the predicate has been calculated, and refrain from restricting the
// input block size by the predicate capacity (to allow the child to process
// more rows at one go).
class FilterCursor : public BasicCursor {
 public:
  // Creates new FilterCursor.
  // Takes ownership of the predicate and child_cursor, doens't take
  // ownership of buffer_allocator.
  static FailureOrOwned<Cursor> Create(
      BoundExpressionTree* predicate,
      const BoundSingleSourceProjector* projector,
      Cursor* child_cursor,
      BufferAllocator* buffer_allocator) {
    std::unique_ptr<BoundExpressionTree> predicate_holder(predicate);
    std::unique_ptr<const BoundSingleSourceProjector> projector_holder(
        projector);
    std::unique_ptr<Cursor> child_holder(child_cursor);
    const TupleSchema& predicate_schema = predicate->result_schema();
    if (predicate_schema.attribute_count() != 1 ||
        predicate_schema.attribute(0).type() != BOOL) {
      THROW(new Exception(
          predicate_schema.attribute_count() != 1 ?
              ERROR_ATTRIBUTE_COUNT_MISMATCH :
              ERROR_ATTRIBUTE_TYPE_MISMATCH,
          "Predicate has to return exactly one column of type BOOL"));
    }
    std::unique_ptr<FilterCursor> cursor(
        new FilterCursor(buffer_allocator, predicate_holder.release(),
                         projector_holder.release(), child_holder.release()));
    PROPAGATE_ON_FAILURE(cursor->Init(std::min(Cursor::kDefaultRowCount,
                                               predicate->row_capacity())));
    return Success(cursor.release());
  }

  virtual ResultView Next(rowcount_t max_row_count) {
    rowcount_t effective_max_row_count =
        min(result_block_.row_capacity(), max_row_count);
    result_block_.ResetArenas();
    write_pointer_ = 0;
    while (true) {
      // TODO(user): rewrite to use CursorIterator.
      if (read_pointer_ < input_row_ids_count_) {
        // Still have some data to read from current view.
        if (CopyDataToResultAndSeeIfDone(effective_max_row_count)) break;
      } else {
        // We're not supposed to call Next() once we saw EOS or failure from
        // a previous Next().
        if (eos_) return ResultView::EOS();
        ResultView result = child()->Next(std::min(result_block_.row_capacity(),
                                                   predicate_capacity_));
        PROPAGATE_ON_FAILURE(result);
        if (result.is_eos()) {
          // We might be returning some data (in case write_pointer_ != 0)
          // so we need to mark that we've seen EOS, to avoid a subsequent call
          // to Next().
          eos_ = true;
          if (write_pointer_ != 0) break;
        }
        if (!result.has_data()) return result;
        current_view_ = &result.view();
        PROPAGATE_ON_FAILURE(PrepareInputRowIds());
        read_pointer_ = 0;
      }
    }
    my_view()->ResetFromSubRange(result_block_.view(), 0, write_pointer_);
    return ResultView::Success(my_view());
  }

  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual CursorId GetCursorId() const { return FILTER; }

 private:
  FilterCursor(BufferAllocator* allocator,
               BoundExpressionTree* predicate,
               const BoundSingleSourceProjector* projector,
               Cursor* child_cursor)
      : BasicCursor(projector->result_schema(), child_cursor),
        predicate_(predicate),
        predicate_capacity_(predicate->row_capacity()),
        deep_copier_(projector, true),
        shallow_copier_(projector, false),
        input_row_ids_(
            TupleSchema::Singleton("row ids", kRowidDatatype, NOT_NULLABLE),
            allocator),
        input_row_ids_count_(0),
        eos_(false),
        result_block_(projector->result_schema(), allocator),
        current_view_(NULL),
        read_pointer_(0),
        write_pointer_(0),
        projector_(projector) {}

  // Allocates memory for internal buffers.
  FailureOrVoid Init(rowcount_t capacity) {
    if (!result_block_.Reallocate(capacity)) {
      THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                          "Couldn't allocate block for filter's result block"));
    }
    if (!input_row_ids_.Reallocate(capacity)) {
      THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                          "Couldn't allocate block for filter's ids block"));
    }
    return Success();
  }

  // Evaluates predicate on rows from current_view_ and puts indices
  // of rows that pass into input_row_ids_.
  FailureOrVoid PrepareInputRowIds() {
    CHECK_NOTNULL(current_view_);
    EvaluationResult eval_result = predicate_->Evaluate(*current_view_);
    PROPAGATE_ON_FAILURE(eval_result);
    const Column& result_column = eval_result.get().column(0);
    const bool* predicate_column_data =result_column.typed_data<BOOL>();
    rowcount_t last_empty = 0;
    const rowcount_t row_count = current_view_->row_count();
    rowid_t* ids_pointer =
        input_row_ids_.mutable_column(0)->mutable_typed_data<kRowidDatatype>();
    if (result_column.is_null() != NULL) {
      bool_const_ptr predicate_column_is_null = result_column.is_null();
      for (rowid_t i = 0;
           i < row_count;
           ++i, ++predicate_column_data, ++predicate_column_is_null) {
        // Only rows that evaluate to not null, TRUE should pass.
        if (!*predicate_column_is_null && *predicate_column_data) {
          ids_pointer[last_empty++] = i;
        }
      }
    } else {
      for (rowid_t i = 0; i < row_count; i++, predicate_column_data++) {
        if (*predicate_column_data) {
          ids_pointer[last_empty++] = i;
        }
      }
    }
    input_row_ids_count_ = last_empty;
    return Success();
  }

  bool CopyImpl(rowcount_t max_row_count, rowcount_t rows_to_copy) {
    const rowid_t* ids_pointer =
        input_row_ids_.mutable_column(0)->content().
            typed_data<kRowidDatatype>() + read_pointer_;

    // Consider this input view as the final one if one of the following
    // conditions occur:
    // (1) the number of resulting output rows will reach X% of max_row_count.
    // (2) deep-copy of string arenas doesn't succeed.
    // If this input view is final, shallow-copy underlying strings
    // (which is significantly faster than deep-copy).
    // TODO(user): should add one more condition: (3) the input contains
    // some very long strings. For that, need to add methods to query the
    // input view for arena sizes.
    bool is_input_view_final =
        (100 * (rows_to_copy + write_pointer_)) >=
        kMinimumFillPercent * max_row_count;

    if (!is_input_view_final) {
      if (deep_copier_.Copy(rows_to_copy, *current_view_, ids_pointer,
                            write_pointer_, &result_block_) == rows_to_copy) {
        return false;
      }  // Otherwise, fall back.
    }

    CHECK_EQ(rows_to_copy, shallow_copier_.Copy(rows_to_copy, *current_view_,
                                                ids_pointer, write_pointer_,
                                                &result_block_));
    return true;
  }

  // Copies data from current_view_ to result_block_.
  // Advances read and write pointers appropriately.
  // Returns true when it concludes that it should be the last piece of
  // data copied, and that Next() should now return.
  bool CopyDataToResultAndSeeIfDone(int max_row_count) {
    DCHECK_LE(max_row_count, result_block_.row_capacity());
    CHECK_NOTNULL(current_view_);
    rowcount_t rows_to_copy = std::min(input_row_ids_count_ - read_pointer_,
                                       max_row_count - write_pointer_);
    bool is_input_view_final = CopyImpl(max_row_count, rows_to_copy);
    read_pointer_ += rows_to_copy;
    write_pointer_ += rows_to_copy;
    return is_input_view_final;
  }

  // Predicate to evaluate on the data.
  std::unique_ptr<BoundExpressionTree> predicate_;
  // Cached capacity of the predicate, to avoid recalculation at each call to
  // Next().
  rowcount_t predicate_capacity_;
  // Perform actual row copying.
  SelectiveViewCopier deep_copier_;
  SelectiveViewCopier shallow_copier_;

  // Holds input_row_ids_count_ identifiers of rows that should be copied to the
  // result in a single rowid_t column.
  Block input_row_ids_;
  rowcount_t input_row_ids_count_;
  bool eos_;  // Set after reaching EOS.

  // Block for result data.
  Block result_block_;
  // Currently processed view, result of child.Next.
  const View* current_view_;
  // Index of first id in input_row_ids_ that needs processing.
  rowcount_t read_pointer_;
  // Index of first free space in the result_block_.
  rowcount_t write_pointer_;

  // Not used, only to keep it around (and destroy properly).
  std::unique_ptr<const BoundSingleSourceProjector> projector_;
};

class FilterOperation : public BasicOperation {
 public:
  // Takes ownership of predicate and projector.
  FilterOperation(const Expression* predicate,
                  const SingleSourceProjector* projector,
                  Operation* child)
      : BasicOperation(child),
        predicate_(CHECK_NOTNULL(predicate)),
        projector_(CHECK_NOTNULL(projector)) {}

  virtual ~FilterOperation() {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> child_cursor = child()->CreateCursor();
    PROPAGATE_ON_FAILURE(child_cursor);
    FailureOrOwned<BoundExpressionTree> predicate =
        predicate_->Bind(child_cursor->schema(),
                         buffer_allocator(),
                         Cursor::kDefaultRowCount);
    PROPAGATE_ON_FAILURE(predicate);
    FailureOrOwned<const BoundSingleSourceProjector> projector =
        projector_->Bind(child_cursor->schema());
    PROPAGATE_ON_FAILURE(projector);
    return FilterCursor::Create(predicate.release(),
                                projector.release(),
                                child_cursor.release(),
                                buffer_allocator());
  }

 private:
  std::unique_ptr<const Expression> predicate_;
  std::unique_ptr<const SingleSourceProjector> projector_;
};

}  // namespace

Operation* Filter(const Expression* predicate,
                  const SingleSourceProjector* projector,
                  Operation* child) {
  return new FilterOperation(predicate, projector, child);
}

FailureOrOwned<Cursor> BoundFilter(BoundExpressionTree* predicate,
                                   const BoundSingleSourceProjector* projector,
                                   BufferAllocator* buffer_allocator,
                                   Cursor* child_cursor) {
  return FilterCursor::Create(predicate,
                              projector,
                              child_cursor,
                              buffer_allocator);
}

}  // namespace supersonic
