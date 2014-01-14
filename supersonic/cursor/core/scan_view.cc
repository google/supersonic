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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/cursor/core/scan_view.h"

#include <stddef.h>

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/view_cursor.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

class BufferAllocator;
class Operation;

namespace {
// An operation class that can be bound to a cursor over the given view, with
// the given selection vector.
// If the selection_vector is NULL, the row_count and buffer_row_capacity
// arguments are ignored, and the operation simply scans the view.
class ScanViewOperation : public BasicOperation {
 public:
  ScanViewOperation(const View& view,
                    const size_t row_count,
                    const int64* selection_vector,
                    size_t buffer_row_capacity)
      : BasicOperation(),
        view_(view),
        row_count_(row_count),
        selection_vector_(selection_vector),
        buffer_row_capacity_(buffer_row_capacity) {}

  virtual ~ScanViewOperation() {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    if (selection_vector_ != NULL) {
      return BoundScanViewWithSelection(view_,
                                        row_count_,
                                        selection_vector_,
                                        buffer_allocator(),
                                        buffer_row_capacity_);
    } else {
      return Success(BoundScanView(view_));
    }
  }

  virtual string DebugDescription() const {
    string name = (selection_vector_ != NULL) ? "ScanViewWithSelection"
                                              : "ScanView";
    return StrCat(name, "(", view_.schema().GetHumanReadableSpecification(),
                  ")");
  }

 private:
  // The view over which the operation is.
  const View& view_;
  // The row_count of the output (equal to the number of entries in the
  // selection vector).
  size_t row_count_;
  // The selection vector. The i-th row of the output is equal to the
  // selection_vector_[i]-th row of the input view.
  const int64* selection_vector_;
  // The buffer capacity we expect from the created cursor (this is the
  // maximum size of a returned block).
  size_t buffer_row_capacity_;

  DISALLOW_COPY_AND_ASSIGN(ScanViewOperation);
};

}  // namespace

// The actual implementation of cursors over views is in
// cursor/infrastructure/view_cursor.{cc,h}.
Cursor* BoundScanView(const View& view) {
  return CreateCursorOverView(view);
}

FailureOrOwned<Cursor> BoundScanViewWithSelection(
    const View& view,
    const rowcount_t row_count,
    const rowid_t* selection_vector,
    BufferAllocator* allocator,
    rowcount_t buffer_row_capacity) {
  return CreateCursorOverViewWithSelection(
      view, row_count, selection_vector, allocator, buffer_row_capacity);
}

Operation* ScanView(const View& view) {
  return new ScanViewOperation(view, 0, NULL, 0);
}

Operation* ScanViewWithSelection(
    const View& view,
    const rowcount_t row_count,
    const rowid_t* selection_vector,
    rowcount_t buffer_row_capacity) {
  CHECK_NOTNULL(selection_vector);
  return new ScanViewOperation(
      view, row_count, selection_vector, buffer_row_capacity);
}

}  // namespace supersonic
