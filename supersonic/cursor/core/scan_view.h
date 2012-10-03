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
//
// Exposes an Operation and a Cursor that scans over the given view.

#ifndef SUPERSONIC_CURSOR_CORE_SCAN_VIEW_H_
#define SUPERSONIC_CURSOR_CORE_SCAN_VIEW_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

class BufferAllocator;
class Cursor;
class Operation;
class View;

// Creates an operation that produces a cursor able to iterate over a specified
// view. The view must outlive the operation and any cursors created from the
// operation.
Operation* ScanView(const View& view);

// Creates an operation that produces a cursor over the input view that iterates
// over it using a selection vector. Does not take ownership of the selection
// vector. Does not take ownership of the selection_vector.
// Note: row_count specifies the size of the selection_vector table. Because
// rows may be selected zero or multiple times, it might be different than
// view.row_count().
Operation* ScanViewWithSelection(const View& view,
                                 const rowcount_t row_count,
                                 const rowid_t* selection_vector,
                                 rowcount_t buffer_row_capacity);

// Creates a cursor that iterates over a specified view. The view must outlive
// the cursor.
Cursor* BoundScanView(const View& view);

// Creates a cursor that iterates over a specified view, using the supplied
// selection vector for indirection. The view must outlive the cursor.
// Note: row_count specifies the size of the selection_vector table. Because
// rows may be selected zero or multiple times, it might be different than
// view.row_count().
// Does not take ownership of either the selection vector or the
// BufferAllocator.
FailureOrOwned<Cursor> BoundScanViewWithSelection(
    const View& view,
    const rowcount_t row_count,
    const rowid_t* selection_vector,
    BufferAllocator* allocator,
    rowcount_t buffer_row_capacity);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_SCAN_VIEW_H_
