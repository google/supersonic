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

#include "supersonic/cursor/core/rowid_merge_join.h"

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <string>
namespace supersonic {using std::string; }
#include <utility>
#include "supersonic/utils/std_namespace.h"
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/port.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

namespace {

TupleSchema KeySelectorSchema() {
  return TupleSchema::Singleton("KeySelector", kRowidDatatype, NOT_NULLABLE);
}

class RowidMergeJoinCursor : public BasicCursor {
 public:
  RowidMergeJoinCursor(
      const BoundSingleSourceProjector* left_key,
      const BoundSingleSourceProjector* canonical_right_projector,
      const BoundMultiSourceProjector* decomposed_result_projector,
      Cursor* left,
      Cursor* right,
      BufferAllocator* allocator)
      : BasicCursor(decomposed_result_projector->result_schema()),
        left_key_(left_key),
        canonical_right_projector_(canonical_right_projector),
        result_projector_(decomposed_result_projector),
        right_flattener_(canonical_right_projector_.get(), false),
        left_(left),
        right_(right),
        indirector_(KeySelectorSchema(), allocator),
        canonical_right_flattened_block_(
            canonical_right_projector_->result_schema(),
            HeapBufferAllocator::Get()),
        canonical_right_flattened_(canonical_right_projector_->result_schema()),
        key_view_(KeySelectorSchema()) {
    CHECK(indirector_.Reallocate(kDefaultRowCount));
    CHECK(canonical_right_flattened_block_.Reallocate(kDefaultRowCount));
  }

  virtual ResultView Next(rowcount_t max_row_count) {
    max_row_count = std::min(max_row_count, indirector_.row_capacity());
    // Counts the number of left-side rows we'll consume.
    rowcount_t row_count = 0;
    rowid_t* indirector =
        indirector_.mutable_column(0)->mutable_typed_data<kRowidDatatype>();
    do {
      if (!left_.Next(max_row_count, false)) return left_.result();
      if (!right_.EagerNext()) {
        CHECK(!right_.has_data());
        if (right_.is_eos()) {
          THROW(new Exception(
              ERROR_FOREIGN_KEY_INVALID,
              StrCat("No parent record with ID > ",
                     right_.current_row_index())));
        }
        if (right_.is_waiting_on_barrier()) left_.truncate(0);
        return right_.result();
      }
      const rowid_t right_rowid_begin = right_.current_row_index();
      const rowid_t right_rowid_end = right_.current_row_index() +
                                      right_.view().row_count();
      left_key_->Project(left_.view(), &key_view_);
      const rowid_t* key_data = key_view_.column(0).
          typed_data<kRowidDatatype>();
      DCHECK(!key_view_.column(0).attribute().is_nullable());
      rowid_t key = -1;
      // Convert the foreign key to the indirection vector (relative to the
      // current view positions).
      for (; row_count < left_.view().row_count(); ++row_count) {
        // Ensure the FK is sorted.
        DCHECK_GE(key_data[row_count], key);
        key = key_data[row_count];
        if (PREDICT_FALSE(key >= right_rowid_end)) break;
        // TODO(user): use SIMD (via ColumnComputer) to calculate this
        // subtraction.
        indirector[row_count] = key - right_rowid_begin;
      }

      // Now, the indirector table contains row_count entries indicating
      // matching offsets into right_.view(). Flatten out the right columns.
      rowcount_t copied = right_flattener_.Copy(
          row_count,
          right_.view(), indirector, 0,
          &canonical_right_flattened_block_);
      DCHECK_EQ(row_count, copied);
      canonical_right_flattened_.ResetFromSubRange(
          canonical_right_flattened_block_.view(), 0, row_count);

      // Align right and left sides, so that we can later safely continue
      // with another Next(). Note: FK in the next left view might be equal to
      // the last FK we've seen in this view; hence, we push back the last
      // matched right row, so it can be re-matched.
      left_.truncate(row_count);
      DCHECK_GE(key, right_rowid_begin);
      right_.truncate(key - right_rowid_begin);
    } while (row_count == 0);

    // Project expects an iterator range of View*. We use a plain array,
    // passing pointers to the first element and one-past-end.
    const View* intermediate_sources[] = {
      &left_.view(),
      &canonical_right_flattened_
    };
    result_projector_->Project(&intermediate_sources[0],
                               &intermediate_sources[2],
                               my_view());
    my_view()->set_row_count(left_.view().row_count());
    return ResultView::Success(my_view());
  }

  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual void Interrupt() {
    left_.Interrupt();
    right_.Interrupt();
  }

  virtual void ApplyToChildren(CursorTransformer* transformer) {
    left_.ApplyToCursor(transformer);
    right_.ApplyToCursor(transformer);
  }

  virtual CursorId GetCursorId() const { return ROWID_MERGE_JOIN; }

 private:
  std::unique_ptr<const BoundSingleSourceProjector> left_key_;
  std::unique_ptr<const BoundSingleSourceProjector> canonical_right_projector_;
  std::unique_ptr<const BoundMultiSourceProjector> result_projector_;
  SelectiveViewCopier right_flattener_;
  CursorIterator left_;
  CursorIterator right_;
  Block indirector_;
  Block canonical_right_flattened_block_;
  View canonical_right_flattened_;
  View key_view_;
};

FailureOrVoid EnsureSingleColumnRowidTypeNotNull(const TupleSchema& schema) {
  if (schema.attribute_count() != 1) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_COUNT_MISMATCH,
        StrCat("Expected exactly 1 attribute for the key; found: ",
               schema.attribute_count())));
  }
  if (schema.attribute(0).type() != kRowidDatatype) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StringPrintf(
            "Column %s designated as a key has type %s; expected %s",
            schema.attribute(0).name().c_str(),
            GetTypeInfo(schema.attribute(0).type()).name().c_str(),
            TypeTraits<kRowidDatatype>::name())));
  }
  if (schema.attribute(0).is_nullable()) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_IS_NULLABLE,
        StringPrintf(
            "Column %s designated as a key is nullable. Please "
            "remove nullability explicitly (by casting, or using NVL)",
            schema.attribute(0).name().c_str())));
  }
  return Success();
}

class RowidMergeJoinOperation : public BasicOperation {
 public:
  RowidMergeJoinOperation(const SingleSourceProjector* left_key_selector,
                          const MultiSourceProjector* result_projector,
                          Operation* left,
                          Operation* right)
      : BasicOperation(left, right),
        left_key_selector_(left_key_selector),
        result_projector_(result_projector) {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> left = child_at(0)->CreateCursor();
    PROPAGATE_ON_FAILURE(left);
    FailureOrOwned<Cursor> right = child_at(1)->CreateCursor();
    PROPAGATE_ON_FAILURE(right);
    FailureOrOwned<const BoundSingleSourceProjector> left_key =
        left_key_selector_->Bind(left->schema());
    PROPAGATE_ON_FAILURE(left_key);
    PROPAGATE_ON_FAILURE(
        EnsureSingleColumnRowidTypeNotNull(left_key->result_schema()));

    vector<const TupleSchema*> schemas;
    schemas.push_back(&left->schema());
    schemas.push_back(&right->schema());
    FailureOrOwned<const BoundMultiSourceProjector> result =
        result_projector_->Bind(schemas);
    PROPAGATE_ON_FAILURE(result);
    return Success(
      BoundRowidMergeJoin(left_key.release(), result.release(),
                          left.release(), right.release(),
                          buffer_allocator()));
  }

 private:
  std::unique_ptr<const SingleSourceProjector> left_key_selector_;
  std::unique_ptr<const MultiSourceProjector> result_projector_;
  DISALLOW_COPY_AND_ASSIGN(RowidMergeJoinOperation);
};

}  // namespace

Operation* RowidMergeJoin(const SingleSourceProjector* left_key_selector,
                          const MultiSourceProjector* result_projector,
                          Operation* left,
                          Operation* right) {
  return new RowidMergeJoinOperation(left_key_selector, result_projector,
                                     left, right);
}

Cursor* BoundRowidMergeJoin(
    const BoundSingleSourceProjector* left_key,
    const BoundMultiSourceProjector* result_projector,
    Cursor* left,
    Cursor* right,
    BufferAllocator* allocator) {
  CHECK_LE(result_projector->source_count(), 2);
  std::unique_ptr<const BoundMultiSourceProjector> result_projector_deleter(
      result_projector);
  pair<BoundMultiSourceProjector*, BoundSingleSourceProjector*> decomposed =
      DecomposeNth(1, *result_projector);
  return new RowidMergeJoinCursor(left_key, decomposed.second,
                                  decomposed.first, left, right, allocator);
}

}  // namespace supersonic
