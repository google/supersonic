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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_VIEW_CURSOR_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_VIEW_CURSOR_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

class BufferAllocator;
class Cursor;
class View;

// Creates a cursor that iterates over a specified view. The view must outlive
// the cursor.
Cursor* CreateCursorOverView(const View& view);

// Creates a cursor that iterates over a specified view, using the supplied
// selection vector for indirection. The view must outlive the cursor.
// Note: row_count specifies the size of the selection_vector table. Because
// rows may be selected zero or multiple times, it might be different than
// view.row_count().
FailureOrOwned<Cursor> CreateCursorOverViewWithSelection(
    const View& view,
    const rowcount_t row_count,
    const rowid_t* selection_vector,
    BufferAllocator* buffer_allocator,
    rowcount_t buffer_row_capacity);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_VIEW_CURSOR_H_
