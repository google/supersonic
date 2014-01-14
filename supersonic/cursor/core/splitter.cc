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

#include "supersonic/cursor/core/splitter.h"

#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/port.h"

namespace supersonic {

// =================== Barrier Splitter implementations =======================

ResultView BarrierSplitReaderCursor::Next(rowcount_t max_row_count) {
  if (PREDICT_TRUE(!at_barrier_)) {
    if (PREDICT_TRUE(input_.next(max_row_count))) {
      return ResultView::Success(&input_.view());
    } else {
      enter_barrier();
    }
  }
  if (!splitter_->all_at_barrier()) return ResultView::WaitingOnBarrier();
  DCHECK(at_barrier_);
  DCHECK(splitter_->all_at_barrier());
  if (splitter_->try_crossing_barrier()) {
    // We managed to cross the barrier; at least 1 row must be now available.
    bool success = input_.next(max_row_count);
    DCHECK(success);
    return ResultView::Success(&input_.view());
  } else {
    // EOS, Waiting, or Failure from the source. Propagate directly.
    return splitter_->result();
  }
}

void BarrierSplitReaderCursor::AppendDebugDescription(string* target) const {
  splitter_->AppendDebugDescription(target);
}

Cursor* BarrierSplitter::AddReader() {
  // First reader takes ownership; subsequent readers share it.
  shared_ptr<BarrierSplitter> splitter = readers_.empty()
      ? shared_ptr<BarrierSplitter>(this)
      : readers_[0]->splitter();
  BarrierSplitReaderCursor* reader = new BarrierSplitReaderCursor(
      splitter, readers_.size());
  // The new reader is positioned at the barrier.
  readers_.push_back(reader);
  return reader;
}

void BarrierSplitter::AppendDebugDescription(string* target) const {
  target->append("BARRIER_SPLIT.");
  input_.AppendDebugDescription(target);
}

// ================== Buffered Splitter implementations =======================

Cursor* BufferedSplitter::AddReader() {
  CHECK(!has_next_called_);
  // First reader takes ownership; subsequent readers share it.
  shared_ptr<BufferedSplitter> splitter = readers_.empty()
      ? shared_ptr<BufferedSplitter>(this)
      : readers_[0]->splitter();
  BufferedSplitReaderCursor* reader = new BufferedSplitReaderCursor(
      splitter, readers_.size());
  readers_.push_back(reader);
  views_.push_back(new View(schema()));
  block_read_.push_back(storages_.end());
  next_row_position_.push_back(0);
  return reader;
}

bool BufferedSplitter::IsDone(size_t position) const {
  return next_row_position_[position] == rows_fetched_ &&
      input_.is_done();
}

FailureOrVoid BufferedSplitter::AllocateNewBlock() {
  VLOG(2) << "Allocating a new block. Previously we have "
      << owned_storages_.size() << " blocks";
  owned_storages_.push_back(
      new Block(input_.schema(), buffer_allocator_));
  // Current implementation supports changing this following block size to
  // any number, should a better candidate be found.
  if (!owned_storages_.back()->Reallocate(max_row_count_)) {
    THROW(new Exception(
        ERROR_MEMORY_EXCEEDED,
        "Couldn't allocate block for BufferedSplitter's storage"));
  }

  // We simulate as if this block is full and all readers have finished reading
  // this block - so that it can get reused.
  BlockWithMetadata new_block(owned_storages_.back().get(),
                              /* row_offset = */ 0,  // any number is fine.
                              owned_storages_.back()->row_capacity(),
                              /* readers done reading = */ readers_.size());
  if (storages_.empty()) {
    storages_.push_back(new_block);
    last_block_ = storages_.begin();
    for (size_t i = 0; i < readers_.size(); ++i) {
      block_read_[i] = storages_.begin();
    }
  } else {
    list<BlockWithMetadata>::iterator next_block = last_block_;
    ++next_block;
    storages_.insert(next_block, new_block);
    // We do not update last_block_.
  }
  return Success();
}

// Here's how the following code works. First it checks if the current reader
// already read all data buffered - in which case Next() is called on the
// underlying cursor and data is copied to the storage (the reason that we
// need to copy data right now is because otherwise it can get stale - for
// example after some other cursor calls NextView() twice). Then, it simply
// checks whether the current block read by the reader still have some data,
// in which case that data is given instead. Otherwise, the reader moves to the
// next block and output the data there.
FailureOr<const View*> BufferedSplitter::NextView(size_t position) {
  DVLOG(7) << "Requesting next view...";
  if (next_row_position_[position] == rows_fetched_) {
    DCHECK(!IsDone(position));
    // We need to fetch the next result view.
    if (!input_.Next(max_row_count_, /* limit cursor input = */ false)) {
      DVLOG(7) << "Failed to fetch next view, returning NULL";
      return Success(static_cast<const View*>(NULL));
    }
    PROPAGATE_ON_FAILURE(input_.result());
    DCHECK(input_.has_data());

    rowcount_t before_next_row_index = rows_fetched_;
    rows_fetched_ += input_.view().row_count();
    has_next_called_ = true;

    rowcount_t next_row_offset = 0;  // Iterates over the rows in the new view.
    View view_to_be_copied(input_.view().schema());
    view_to_be_copied.ResetFrom(input_.view());
    while (next_row_offset < input_.view().row_count()) {
      if (storages_.size() == 0) {
        PROPAGATE_ON_FAILURE(AllocateNewBlock());
      }
      if (last_block_->filled_row_count_ <
          last_block_->block_->row_capacity()) {
        // Some free slot are still available in this location, so we copy a row
        // to this block.
        DCHECK_EQ(before_next_row_index + next_row_offset,
                  last_block_->first_row_offset_ +
                      last_block_->filled_row_count_);
        rowcount_t copy_count = min(
            view_to_be_copied.row_count(),
            last_block_->block_->row_capacity() -
                last_block_->filled_row_count_);
        if (copier.Copy(
                copy_count,
                view_to_be_copied,
                last_block_->filled_row_count_,
                last_block_->block_) < copy_count) {
          THROW(new Exception(
                ERROR_MEMORY_EXCEEDED,
                "Failed to do a deep copying of row in a schema. Probably"
                " memory exceeded."));
        }
        next_row_offset += copy_count;
        last_block_->filled_row_count_ += copy_count;
        view_to_be_copied.ResetFromSubRange(
            view_to_be_copied,
            copy_count,
            view_to_be_copied.row_count() - copy_count);
      } else {
        // No available slot is present here so we have to move block.
        list<BlockWithMetadata>::iterator next_block = last_block_;
        ++next_block;
        if (next_block == storages_.end()) next_block = storages_.begin();
        if (next_block->readers_done_reading_ < readers_.size()) {
          // Some readers are still using this block, so we have to allocate
          // a new block instead of reusing an old one.
          PROPAGATE_ON_FAILURE(AllocateNewBlock());
          next_block = last_block_;
          ++next_block;
          DCHECK(next_block != storages_.end());
        }
        DCHECK(next_block->readers_done_reading_ == readers_.size());

        next_block->readers_done_reading_ = 0;
        next_block->first_row_offset_ = before_next_row_index +
            next_row_offset;
        next_block->filled_row_count_ = 0;
        last_block_ = next_block;
      }
    }
    DVLOG(9) << "Success! Copied to storage";
  }

  DVLOG(9) << "Updating block read...";
  DCHECK_LT(next_row_position_[position], rows_fetched_);

  if (next_row_position_[position] == block_read_[position]->first_row_offset_ +
          block_read_[position]->filled_row_count_) {
    DCHECK(block_read_[position] != last_block_);
    DCHECK_LT(block_read_[position]->readers_done_reading_, readers_.size());

    ++(block_read_[position]->readers_done_reading_);
    ++block_read_[position];
    if (block_read_[position] == storages_.end()) {
      block_read_[position] = storages_.begin();
    }

    DCHECK_LT(block_read_[position]->readers_done_reading_, readers_.size());
    DCHECK_EQ(next_row_position_[position],
              block_read_[position]->first_row_offset_);
  }

  DCHECK_LT(next_row_position_[position],
            block_read_[position]->first_row_offset_ +
                block_read_[position]->filled_row_count_);
  DCHECK_GE(next_row_position_[position],
            block_read_[position]->first_row_offset_);

  rowcount_t offset =
      next_row_position_[position] - block_read_[position]->first_row_offset_;
  rowcount_t row_count =
      block_read_[position]->filled_row_count_ - offset;
  next_row_position_[position] += row_count;

  if (row_count == 0) {
    views_[position]->set_row_count(0);
  } else {
    views_[position]->ResetFromSubRange(
        block_read_[position]->block_->view(), offset, row_count);
  }

  DVLOG(7) << "Success! Returning view with " << row_count << " rows.";
  return Success(views_[position].get());
}

void BufferedSplitter::AppendDebugDescription(string* target) const {
  target->append("BUFFERED_SPLIT.");
  input_.AppendDebugDescription(target);
}

ResultView BufferedSplitReaderCursor::Next(rowcount_t max_row_count) {
  if (input_.next(max_row_count)) {
    return ResultView::Success(&(input_.view()));
  } else {
    if (PREDICT_FALSE(splitter_->IsDone(position_))) {
      return splitter_->result();
    }
    // We try to ask for the next view.
    FailureOr<const View*> unowned_view = splitter_->NextView(position_);
    PROPAGATE_ON_FAILURE(unowned_view);
    if (unowned_view.get() == NULL) {
      return splitter_->result();
    }
    input_.reset(*(unowned_view.get()));
    return Next(max_row_count);
  }
}

void BufferedSplitReaderCursor::AppendDebugDescription(string* target) const {
  splitter_->AppendDebugDescription(target);
}

}  // namespace supersonic
