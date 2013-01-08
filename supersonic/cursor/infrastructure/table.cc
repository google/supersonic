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

#include "supersonic/cursor/infrastructure/table.h"

#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/infrastructure/view_cursor.h"

namespace supersonic {

Table::Table(const TupleSchema& schema, BufferAllocator* buffer_allocator)
    : BasicOperation(),
      block_(new Block(schema, buffer_allocator)),
      view_(schema),
      view_copier_(schema, true) {
  view_.ResetFrom(block_->view());
}

Table::Table(Block* block)
    : BasicOperation(),
      block_(block),
      view_(block->schema()),
      view_copier_(block_->schema(), true) {
  view_.ResetFrom(block_->view());
}

// Needs to be here as long as ViewCopier is only forward-declared in *.h.
Table::~Table() {}

rowid_t Table::AddRow() {
  if (row_count() == block_->row_capacity()) {
    if (!Reallocate(block_->row_capacity() == 0 ? Cursor::kDefaultRowCount
                                                : block_->row_capacity() * 2)) {
      return -1;
    }
  }
  rowid_t row_index = row_count();
  view_.set_row_count(row_index + 1);
  return row_index;
}

void Table::Clear() {
  view_.set_row_count(0);
  block_->ResetArenas();
}

bool Table::SetRowCapacity(rowcount_t row_capacity) {
  CHECK_GE(row_capacity, view_.row_count())
      << "New table capacity must be equal to or exceed current row count";
  return Reallocate(row_capacity);
}

bool Table::ReserveRowCapacity(rowcount_t needed_capacity) {
  if (block_->row_capacity() < needed_capacity) {
    // TODO(user): Consider having a conservative allocation also when
    // Available() is small (but more than zero). Needs enhancement of
    // Block::Reallocate.
    rowcount_t new_capacity =
        std::max(2 * block_->row_capacity(), needed_capacity);
    if (block_->allocator()->Available() <= 0) {
      // If there's not soft quota left, reserve only up to kDefaultRowCount.
      // TODO(user): This hack makes it difficult to test (on small data) that
      // Sort works properly when the space in the Table runs out (so Sort needs
      // to write to disk).
      new_capacity = min(needed_capacity,
                         max(block_->row_capacity(), Cursor::kDefaultRowCount));
    }
    CHECK_LE(block_->row_capacity(), new_capacity);
    // If unsuccessful, we'll end up copying less rows (and returning non-zero).
    Reallocate(new_capacity);
    return row_capacity() >= needed_capacity;
  } else {
    return true;
  }
}

void Table::SetNull(int col_index, rowid_t row_index) {
  DCHECK_GE(row_index, 0);
  DCHECK_LT(row_index, row_count());
  DCHECK_LT(col_index, schema().attribute_count());
  bool_ptr is_null = block_->mutable_column(col_index)->mutable_is_null();
  DCHECK(is_null != NULL) << "Column is not nullable";
  is_null[row_index] = true;
}

rowcount_t Table::AppendView(const View& view) {
  // Ensure that the block has sufficient capacity.
  // If unsuccessful, we'll end up copying less rows (and returning non-zero).
  ReserveRowCapacity(row_count() + view.row_count());
  rowcount_t rows_to_copy =
      std::min(view.row_count(), block_->row_capacity() - row_count());
  rowcount_t rows_copied =
      view_copier_.Copy(rows_to_copy, view, row_count(), block_.get());
  view_.set_row_count(row_count() + rows_copied);
  return rows_copied;
}

FailureOrOwned<Cursor> Table::CreateCursor() const {
  return Success(CreateCursorOverView(view()));
}

FailureOrOwned<Table> MaterializeTable(BufferAllocator* allocator,
                                       Cursor* cursor) {
  scoped_ptr<Table> table(new Table(cursor->schema(), allocator));
  TableSink sink(table.get());
  PROPAGATE_ON_FAILURE(WriteCursor(cursor, &sink));
  return Success(table.release());
}

}  // namespace supersonic
