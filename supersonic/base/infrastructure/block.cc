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

#include "supersonic/base/infrastructure/block.h"

namespace supersonic {

bool OwnedColumn::Reallocate(rowcount_t row_capacity,
                             BufferAllocator* allocator) {
  const size_t buffer_size = row_capacity << column_->type_info().log2_size();
  if (!allocator->Reallocate(buffer_size, data_buffer_.get())) {
    return false;
  }

  if (is_nullable()) {
    if (!is_null_array_.Reallocate(row_capacity, allocator)) {
      // We have reallocated the data buffer; thus need to reset anyway.
      column_->Reset(mutable_data(), mutable_is_null());
      return false;
    }
  }

  column_->Reset(mutable_data(), mutable_is_null());
  return true;
}

void OwnedColumn::Init(BufferAllocator* allocator, Column* column) {
  column_ = column;
  if (column_->type_info().is_variable_length()) {
    // NOTE: the Arena constructor makes it OK for the initial arena
    // buffer to be zero, so that creation of the arena never fails.
    arena_.reset(new Arena(allocator, 0, kMaxArenaBufferSize));
  }
  // NOTE: per BufferAllocator contract, request for zero bytes must succeed.
  data_buffer_.reset(allocator->Allocate(0));
  CHECK_NOTNULL(data_buffer_.get());
  if (column_->attribute().is_nullable()) {
    is_null_array_.Reallocate(0, allocator);
    CHECK(!(is_null_array_.mutable_data() == NULL));
  }
  column_->Reset(mutable_data(), mutable_is_null());
}

bool Block::Reallocate(rowcount_t new_row_capacity) {
  for (int i = 0; i < column_count(); i++) {
    if (!columns_[i].Reallocate(new_row_capacity, allocator_)) {
      if (new_row_capacity < row_capacity()) {
        set_row_capacity(new_row_capacity);
      }
      return false;
    }
  }
  set_row_capacity(new_row_capacity);
  return true;
}

}  // namespace supersonic
