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
// Author: onufry@google.com (Jakub Wojtaszczyk)

#include "supersonic/cursor/infrastructure/view_cursor.h"

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

class BufferAllocator;

namespace {

class ViewCursor : public BasicCursor {
 public:
  static Cursor* Create(const View& view) { return new ViewCursor(view); }

  virtual ResultView Next(rowcount_t max_row_count) {
    PROPAGATE_ON_FAILURE(ThrowIfInterrupted());
    if (!iterator_.next(max_row_count)) return ResultView::EOS();
    return ResultView::Success(&iterator_.view());
  }

  // No WaitingOnBarrier possible here, as no children present.
  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual CursorId GetCursorId() const { return VIEW; }

  virtual void AppendDebugDescription(string* output) const {
    output->append("ViewCursor(");
    output->append(schema().GetHumanReadableSpecification());
    output->append(")");
  }

 private:
  explicit ViewCursor(const View& view)
      : BasicCursor(view.schema()),
        iterator_(view) {}

  ViewIterator iterator_;
  DISALLOW_COPY_AND_ASSIGN(ViewCursor);
};

class ViewCursorWithSelectionVector : public BasicCursor {
 public:
  static FailureOrOwned<Cursor> Create(const View& view,
                                       const rowcount_t row_count,
                                       const rowid_t* const selection_vector,
                                       BufferAllocator* buffer_allocator,
                                       const rowcount_t buffer_row_capacity) {
    std::unique_ptr<ViewCursorWithSelectionVector> cursor(
        new ViewCursorWithSelectionVector(view, row_count, selection_vector,
                                          buffer_allocator));
    if (!cursor->Allocate(buffer_row_capacity)) {
      THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                          "Failed to preallocate buffer"));
    }
    return Success(cursor.release());
  }

  virtual ResultView Next(rowcount_t max_row_count) {
    PROPAGATE_ON_FAILURE(ThrowIfInterrupted());
    if (read_pointer_ == row_count_) {
      return ResultView::EOS();
    }
    if (max_row_count > my_block_.row_capacity()) {
      max_row_count = my_block_.row_capacity();
    }
    rowcount_t current_row_count = std::min(max_row_count,
                                            row_count_ - read_pointer_);
    my_block_.ResetArenas();
    if (copier_.Copy(current_row_count, source_view_,
                     selection_vector_ + read_pointer_,
                     0, &my_block_) < current_row_count) {
      THROW(new Exception(
          ERROR_MEMORY_EXCEEDED,
          "Failed to copy a block of data"));
    }
    my_view()->ResetFromSubRange(my_block_.view(), 0, current_row_count);
    read_pointer_ += current_row_count;
    return ResultView::Success(my_view());
  }

  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual CursorId GetCursorId() const { return SELECTION_VECTOR_VIEW; }

 private:
  ViewCursorWithSelectionVector(const View& view,
                                const rowcount_t row_count,
                                const rowid_t* const selection_vector,
                                BufferAllocator* buffer_allocator)
      : BasicCursor(view.schema()),
        source_view_(view.schema()),
        my_block_(view.schema(), buffer_allocator),
        selection_vector_(selection_vector),
        row_count_(row_count),
        read_pointer_(0),
        copier_(view.schema(), /* deep_copy = */ false) {
    source_view_.ResetFrom(view);
  }

  virtual string DebugDescription() const {
    return StrCat("ViewCursorWithSelectionVector(",
                  source_view_.schema().GetHumanReadableSpecification(),
                  ")");
  }

  bool Allocate(rowcount_t row_capacity) {
    return my_block_.Reallocate(row_capacity);
  }

  View source_view_;
  Block my_block_;
  const rowid_t* const selection_vector_;
  const rowcount_t row_count_;
  rowcount_t read_pointer_;
  SelectiveViewCopier copier_;

  DISALLOW_COPY_AND_ASSIGN(ViewCursorWithSelectionVector);
};

}  // namespace

Cursor* CreateCursorOverView(const View& view) {
  return ViewCursor::Create(view);
}

FailureOrOwned<Cursor> CreateCursorOverViewWithSelection(
    const View& view,
    const rowcount_t row_count,
    const rowid_t* selection_vector,
    BufferAllocator* buffer_allocator,
    rowcount_t buffer_row_capacity) {
  return ViewCursorWithSelectionVector::Create(
      view, row_count, selection_vector, buffer_allocator, buffer_row_capacity);
}

}  // namespace supersonic
