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

#ifndef SUPERSONIC_CURSOR_CORE_SPLITTER_H_
#define SUPERSONIC_CURSOR_CORE_SPLITTER_H_

#include <stddef.h>

#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;
#include <list>
#include "supersonic/utils/std_namespace.h"

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/cursor/infrastructure/row.h"
#include "supersonic/utils/pointer_vector.h"
#include <memory>

namespace supersonic {

class BarrierSplitReaderCursor;
class BufferedSplitReaderCursor;
class TupleSchema;
class View;

// Abstract cursor that is able to split a cursor into several cursors as if
// the resulting cursors are copies of the input cursor. The exact
// implementation and behavior depends on each implementation (see below).
class SplitterInterface {
 public:
  // Creates a new splitter for the specified cursor. The new splitter has no
  // readers - you need to add them via AddReader.
  SplitterInterface() {}
  virtual ~SplitterInterface() {}

  // Adds a new reader of that splitter. Passes the ownership of this splitter
  // to the reader (to be shared with all other readers added, if any).
  // The new reader joins the stream at the nearest barrier.
  //
  // Supports early departure: It is OK to destroy some readers early and keep
  // using others, as long as there is always at least one reader remaining.
  //
  // This should not be called after any of the readers calls Next().
  virtual Cursor* AddReader() = 0;

  // Returns the schema of the cursor behind this splitter.
  virtual const TupleSchema& schema() const = 0;

  virtual void Interrupt() = 0;

  virtual void ApplyToChildren(CursorTransformer* transformer) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(SplitterInterface);
};

// Implementation of SplitterInterface. This buffers all views that it reads.
// Simply put, calling AddReader() virtually creates a perfect copy of the
// cursor.
//
// If a block no longer gets read by any reader(s), its memory will be reused.
// Warning: If this class has only one child, this will still buffer the views!
//
// This class serves the same purpose as BarrierSplitter, but works for
// aggregation operations that are in parallel with projections or other
// streaming operations. The main difference is that since this class buffers
// the views it receives, it may buffer several views at a time due to some
// reader still reading from an old view and another reader reading from the
// newer views. Hence, the memory cost of this splitter is approximately
// the maximum difference between the row read by a reader with the row
// read by another reader.
//
// Please note that this splitter does not make use of WAITING_ON_BARRIER,
// but supports if its child returns WAITING_ON_BARRIER.
//
// The class is NOT thread-safe; the envisaged use case is to execute the entire
// plan in a single thread.
class BufferedSplitter : public SplitterInterface {
 public:
  // Creates a new splitter for the specified cursor. The new splitter has no
  // readers - you need to add them via AddReader.
  explicit BufferedSplitter(Cursor* input,
                            BufferAllocator* buffer_allocator,
                            rowcount_t max_row_count)
      : input_(input),
        copier(input->schema(), /* deep copy = */ true),
        rows_fetched_(0), has_next_called_(false),
        buffer_allocator_(buffer_allocator), max_row_count_(max_row_count) {}
  virtual ~BufferedSplitter() {}
  virtual Cursor* AddReader();
  virtual const TupleSchema& schema() const { return input_.schema(); }
  virtual void Interrupt() { input_.Interrupt(); }

  virtual void ApplyToChildren(CursorTransformer* transformer) {
    input_.ApplyToCursor(transformer);
  }

 private:
  void unregister(int position) {
    CHECK(readers_[position] != NULL);
    readers_[position] = NULL;
  }

  void AppendDebugDescription(string* target) const;
  const ResultView result() const { return input_.result(); }

  // Returns true if all rows have been read (in which case the reader should
  // return splitter_.result().
  bool IsDone(size_t position) const;

  // Retrieves the next view for one of the readers.
  // Returns NULL if it's blocked - either waiting on
  // barrier or end of stream or propagated failure.
  FailureOr<const View*> NextView(size_t position);

  // Allocates a new block. Also works if no block was allocated yet in which
  // case it sets the last block to the newly allocated block.
  FailureOrVoid AllocateNewBlock();

  // Friends are bad - but I guess it's better than exposing all methods above.
  // TODO(user): Consider making it a nested class instead.
  friend class BufferedSplitReaderCursor;

  CursorIterator input_;

  // Buffers the data. The blocks will be reused if it's no longer used similar
  // to how circular queue works. When we need to allocate more block, we
  // insert it at the right position and update all the readers. Note that this
  // is quick because insertions are O(1) in lists.
  // TODO(user): Also consider deallocating it. The problem is that this may
  // be very inefficient if given a sequence of records containing alternating
  // records, one with huge number of rows, one with a tiny number of rows.
  util::gtl::PointerVector<Block> owned_storages_;

  struct BlockWithMetadata {
    BlockWithMetadata(Block* block,
                      rowcount_t first_row_offset,
                      rowcount_t filled_row_count,
                      int readers_done_reading)
        : block_(block),
          first_row_offset_(first_row_offset),
          filled_row_count_(filled_row_count),
          readers_done_reading_(readers_done_reading) {}

    Block* block_;
    // The first row in this block corresponds to the first_row_offset_-th row
    // in the cursor.
    rowcount_t first_row_offset_;
    // This block has the first filled_row_count_ rows filled with data.
    rowcount_t filled_row_count_;
    // Number of readers that will no longer read from this block.
    int readers_done_reading_;
  };

  // Store pointers to the block in the cyclic queue order.
  list<BlockWithMetadata> storages_;

  // Points to the block that's last used to store data.
  list<BlockWithMetadata>::iterator last_block_;

  // All readers that stems from this splitter.
  vector<BufferedSplitReaderCursor*> readers_;

  // Responsible for deep copying the data (deep copy = Arena).
  ViewCopier copier;

  // Here's the relationship between all the following containers. Readers_
  // keeps track of all readers reading this splitter. readers_[i] has been
  // feed with views so that the next view it requests should begin with
  // row next_row_position_[i]. It's currently reading block
  // block_read_[i].

  // === Reader related metadata ===
  vector<list<BlockWithMetadata>::iterator> block_read_;
  vector<rowcount_t> next_row_position_;

  // Views responsible for viewing data. Each reader has its own view.
  util::gtl::PointerVector<View> views_;

  // === Input cursor related metadata ===
  rowcount_t rows_fetched_;
  // Flags once input_.Next() have been called at least once.
  bool has_next_called_;

  // === For allocating blocks ===
  BufferAllocator* buffer_allocator_;
  // It's a bit evil, but max_row_count_ currently signals how big a block
  // should be. Note that current implementation actually support this value
  // to be lower or higher than the maximum value passed to a call to Next().
  rowcount_t max_row_count_;

  DISALLOW_COPY_AND_ASSIGN(BufferedSplitter);
};

// Implementation of SplitterInterface. The data is not
// copied or buffered; reader cursors iterate over views returned by the
// original cursor. Once a reader reaches the end of a view, its subseqent
// call to Next() return WAITING_ON_BARRIER until all other readers also reach
// the end of that view. The caller is responsible for handling
// WAITING_ON_BARRIER responses and iterating in the way that ensures that
// all readers advance through the barriers.
//
// The primary purpose of this class is to support structured operations, but
// it can be also used in non-structural queries - e.g. to split a common
// intermediate stream into further, independent post-processing branches.
//
// The class is NOT thread-safe; the envisaged use case is to execute the entire
// plan in a single thread.
class BarrierSplitter : public SplitterInterface {
 public:
  // Creates a new splitter for the specified cursor. The new splitter has no
  // readers - you need to add them via AddReader.
  explicit BarrierSplitter(Cursor* input)
      : input_(input), readers_(), barrier_(0) {}
  virtual ~BarrierSplitter() {}

  virtual Cursor* AddReader();

  virtual const TupleSchema& schema() const { return input_.schema(); }

  virtual void Interrupt() { input_.Interrupt(); }

  virtual void ApplyToChildren(CursorTransformer* transformer) {
    input_.ApplyToCursor(transformer);
  }

 private:
  bool all_at_barrier() const { return barrier_ == 0; }
  void enter_barrier() {
    DCHECK(!all_at_barrier());
    --barrier_;
  }
  inline bool try_crossing_barrier();
  void unregister(int position) {
    CHECK(readers_[position] != NULL);
    readers_[position] = NULL;
  }
  const ResultView result() const { return input_.result(); }
  void AppendDebugDescription(string* target) const;

  friend class BarrierSplitReaderCursor;

  CursorIterator input_;
  vector<BarrierSplitReaderCursor*> readers_;
  int barrier_;
  DISALLOW_COPY_AND_ASSIGN(BarrierSplitter);
};

// Implementation (inline methods and auxiliary definitions).
class BufferedSplitReaderCursor : public Cursor {
 public:
  BufferedSplitReaderCursor(shared_ptr<BufferedSplitter> const splitter,
                            const int position)
      : splitter_(splitter),
        position_(position),
        input_(splitter->schema()) {}

  ~BufferedSplitReaderCursor() {
    splitter_->unregister(position_);
  }

  virtual const TupleSchema& schema() const { return input_.schema(); }
  virtual ResultView Next(rowcount_t max_row_count);
  // TODO(user): One day, we might want to not push interrupt past the
  // splitter until all consumers are interrupted. Not needed for now though,
  // and more complex to implement.
  void Interrupt() { splitter_->Interrupt(); }
  // Call transform on the underlying splitter only if we own it.
  virtual void ApplyToChildren(CursorTransformer* transformer) {
    if (position_ == 0) {
      splitter_->ApplyToChildren(transformer);
    }
  }
  virtual void AppendDebugDescription(string* target) const;
  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  inline void reset(const View& view) {
    input_.reset(view);
  }

  virtual CursorId GetCursorId() const { return BUFFERED_SPLIT_READER; }

  shared_ptr<BufferedSplitter> const splitter() const { return splitter_; }

 private:
  shared_ptr<BufferedSplitter> const splitter_;  // shared by all Readers.
  size_t position_;  // Index of this reader in the splitter.
  ViewIterator input_;
  DISALLOW_COPY_AND_ASSIGN(BufferedSplitReaderCursor);
};

// Implementation (inline methods and auxiliary definitions).
class BarrierSplitReaderCursor : public Cursor {
 public:
  BarrierSplitReaderCursor(shared_ptr<BarrierSplitter> const splitter,
                           const int position)
      : splitter_(splitter),
        position_(position),
        at_barrier_(true),
        input_(splitter->schema()) {}

  ~BarrierSplitReaderCursor() {
    if (!at_barrier_) enter_barrier();
    splitter_->unregister(position_);
  }

  virtual const TupleSchema& schema() const { return input_.schema(); }
  virtual ResultView Next(rowcount_t max_row_count);
  // TODO(user): One day, we might want to not push interrupt past the
  // splitter until all consumers are interrupted. Not needed for now though,
  // and more complex to implement.
  void Interrupt() { splitter_->Interrupt(); }
  // Call transform on the underlying splitter only if we own it.
  virtual void ApplyToChildren(CursorTransformer* transformer) {
    if (position_ == 0) {
      splitter_->ApplyToChildren(transformer);
    }
  }
  virtual void AppendDebugDescription(string* target) const;
  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  inline void enter_barrier() {
    at_barrier_ = true;
    splitter_->enter_barrier();
  }

  inline void reset(const View& view) {
    input_.reset(view);
    at_barrier_ = false;
  }

  virtual CursorId GetCursorId() const { return BARRIER_SPLIT_READER; }

  shared_ptr<BarrierSplitter> const splitter() const { return splitter_; }

 private:
  shared_ptr<BarrierSplitter> const splitter_;  // shared by all Readers.
  size_t position_;  // Index of this reader in the splitter.
  bool at_barrier_;
  ViewIterator input_;
  DISALLOW_COPY_AND_ASSIGN(BarrierSplitReaderCursor);
};

bool BarrierSplitter::try_crossing_barrier() {
  DCHECK(all_at_barrier());
  if (input_.EagerNext()) {
    for (int i = 0; i < readers_.size(); ++i) {
      if (readers_[i] != NULL) {
        readers_[i]->reset(input_.view());
        ++barrier_;
      }
    }
    return true;
  }
  // If we saw EOS or WAITING_ON_BARRIER from the input, we leave all
  // readers at the barrier (they'll use is_eos() to decide between
  // returning one or the other).
  return false;
}

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_SPLITTER_H_
