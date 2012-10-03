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

#ifndef SUPERSONIC_CURSOR_CORE_SORT_H_
#define SUPERSONIC_CURSOR_CORE_SORT_H_

#include <stddef.h>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/cursor/infrastructure/writer.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

class BoundSingleSourceProjector;
class BoundSortOrder;
class BufferAllocator;
class Cursor;
class ExtendedSortSpecification;
class Operation;
class Permutation;
class SingleSourceProjector;
class SortOrder;
class View;

// Sorts the specified input view according to the sort_order, and writes
// results to the specified permutation. The permutation (in ordering.h) must
// be preallocated with permutation->size() == input.row_count().
// The sort is NOT stable.
//
// Example usage:
//
// View view(...);
// Permutation permutation(view.row_count());
// scoped_ptr<SortOrder> sort_order(...);
// FailureOrOwned<BoundSortOder> sort_order_result =
//     sort_order->Bind(view.schema());
// if (sort_order_result.failure()) {
// .. handle
// } else {
//   scoped_ptr<BoundSortOrder> bound_sort_order(sort_order_result->get());
//
//   SortPermutation(*bound_sort_order, view, &permutation);
//
//   scoped_ptr<Cursor> cursor1(CreateCursorOverViewWithSelection(
//       view, view.row_count(), permutation.permutation(), ...);
//   scoped_ptr<Cursor> cursor2(CreateCursorOverViewWithSelection(
//       view, view.row_count(), permutation.permutation(), ...);
//
//   Now, use the two cursors.
// }
void SortPermutation(const BoundSortOrder& sort_order,
                     const View& input,
                     Permutation* permutation);

// Creates a new sort operation. The memory_limit allows to constrain memory
// usage. Currently, the implementation only has a "soft" guarantee that the
// upper limit of memory usage is of similar order of magnitude that the
// memory_limit parameter. In other words, the usage may be up to 2 times more
// than memory_limit value, and momentary usage even more than that. When
// there's too much data to sort in this amount of memory, partial sorted
// results are saved to temporary files, to be merged at the end.
// temporary_directory_prefix is passed to TempFile::Create (use empty string
// for default).
// TODO(user): Achieve tighter guarantees on memory usage.
// TODO(user): Introduce SortOptions and remove SortWithTempDirPrefix.
// TODO(user): (same as in aggregate.h) in ptab's words: Instead of directory
// prefix I wish we use something like:
//
// class TemporaryFileFactory {
//   File* CreateTemporaryFile();
// }
//
// This way transition to d-server or other storage would be easier. Even for
// testing it might be helpful.
Operation* Sort(const SortOrder* sort_order,
                const SingleSourceProjector* result_projector,
                size_t memory_limit,  // in bytes
                Operation* child);

Operation* SortWithTempDirPrefix(const SortOrder* sort_order,
                                 const SingleSourceProjector* result_projector,
                                 size_t memory_limit,
                                 StringPiece temporary_directory_prefix,
                                 Operation* child);

// Identical with Sort, but supports case insensitivity and limit on the number
// of returned elements. Also, unlike sort, its parameter is serializable.
// Takes ownership of all input.
Operation* ExtendedSort(const ExtendedSortSpecification* specification,
                        const SingleSourceProjector* result_projector,
                        size_t memory_limit,
                        Operation* child);

// Creates a new sort cursor. It will emit data from the child cursor, ordered
// according to the sort_order, and projected via result_projector. Takes
// ownership of the sort_order and the result_projector.
// If BoundSort exceeds memory_limit, it will try to complete the operation by
// storing sorted parts of the data in temporary files and merging them all at
// the end.
FailureOrOwned<Cursor> BoundSort(
    const BoundSortOrder* sort_order,
    const BoundSingleSourceProjector* result_projector,
    size_t memory_limit,  // in bytes
    StringPiece temporary_directory_prefix,
    BufferAllocator* allocator,
    Cursor* child_cursor);

// Similar to above, but supports case insensitive sorting and sorting with
// limit.
FailureOrOwned<Cursor> BoundExtendedSort(
    const ExtendedSortSpecification* sort_specification,
    const BoundSingleSourceProjector* result_projector,
    size_t memory_quota,
    StringPiece temporary_directory_prefix,
    BufferAllocator* allocator,
    rowcount_t max_row_count,
    Cursor* child);

// An interface for storing sorted parts of data for later merging.
class Merger {
 public:
  virtual ~Merger() {}

  // Takes ownership of cursor, reads all the data and deletes cursor before
  // returning (this is important, for example, when the cursor iterates over a
  // block it does not own, and the block is changed later). This method does
  // not support the cursor returning WAITING_ON_BARRIER.
  virtual FailureOrVoid AddSorted(Cursor* cursor) = 0;

  // Final merge of the sorted parts. The "additional" cursor, if not null, is
  // merged together with the stored data. It should be ordered according to
  // sort_order too. Takes ownership of sort_order and additional.
  virtual FailureOrOwned<Cursor> Merge(const BoundSortOrder* sort_order,
                                       Cursor* additional) = 0;

  // Returns true iff there was no data added to Merger. Adding an empty cursor
  // counts as not empty.
  virtual bool empty() const = 0;

 protected:
  Merger() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(Merger);
};

// Create a Merger instance that stores the data in files, using Supersonic's
// FileInput/FileOutput, in the specified directory.
Merger* CreateMerger(TupleSchema schema,
                     StringPiece temporary_directory_prefix,
                     BufferAllocator* allocator);

// Sorter accepts an arbitrary(*) amount of unordered data and produces a sorted
// Cursor with the same rows. The implementation buffers data into chunks that
// are sorted and written to disk. The final GetResultCursor() call simply
// MergeUnionAlls all the sorted files.
// (*) The amount of data can be limited by disk quota or the maximal number of
// files that can be read at once.
class Sorter {
 public:
  virtual ~Sorter() {}

  // Writes a new chunk of data to the Sorter. Returns the number of rows
  // successfully written (or exception). Write() shouldn't be called after
  // GetResultCursor() was called.
  virtual FailureOr<rowcount_t> Write(const View& data) = 0;

  // Returns all the data written to the Sorter sorted. Write() shouldn't be
  // called after GetResultCursor() was called.
  virtual FailureOrOwned<Cursor> GetResultCursor() = 0;

 protected:
  Sorter() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(Sorter);
};

// Creates an unbuffered Sorter object with a given schema, sort order and a
// location for temporary files. Every View passed to Write is separately sorted
// and written to a file.
Sorter* CreateUnbufferedSorter(const TupleSchema& schema,
                               const BoundSortOrder* sort_order,
                               StringPiece temporary_directory_prefix,
                               BufferAllocator* allocator);

// Creates a buffering Sorter object with a given schema, sort order, memory
// limit and a location for temporary files.
Sorter* CreateBufferingSorter(const TupleSchema& schema,
                              const BoundSortOrder* sort_order,
                              size_t memory_quota,
                              StringPiece temporary_directory_prefix,
                              BufferAllocator* allocator);

// Sink class for Sorter. Allows using Writer with Sorter.
class SorterSink : public Sink {
 public:
  explicit SorterSink(Sorter* sorter) : sorter_(sorter) {}
  virtual FailureOr<rowcount_t> Write(const View& data) {
    return sorter_->Write(data);
  }
  virtual FailureOrVoid Finalize() {
    return Success();
  }

 private:
  Sorter* sorter_;
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_SORT_H_
