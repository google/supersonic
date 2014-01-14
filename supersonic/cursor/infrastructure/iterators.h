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
//
// Helpers to simplify iterating over views and cursors, either block-by-block
// or row-by-row.

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_ITERATORS_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_ITERATORS_H_

#include <stddef.h>

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <limits>
#include "supersonic/utils/std_namespace.h"
#include <memory>

#include <gflags/gflags.h>
#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/proto/supersonic.pb.h"

// TODO(user): remove after a successful launch.
DECLARE_bool(supersonic_release_cursors_aggressively);

namespace supersonic {

class Exception;

// An iterator interface to read from views in smaller chunks.
// An example usage pattern (iterating over views, in chunks no larger than
// max_chunk_row_count):
//
// ViewIterator iterator(schema);
// while (has next view to iterate over) {
//   iterator.reset(view);
//   while (iterator.next(max_chunk_row_count)) {
//     DoSomethingWith(iterator.view());
//   }
// }
//
class ViewIterator {
 public:
  // Creates a new view iterator for views with the specified schema.
  explicit ViewIterator(const TupleSchema& schema)
      : schema_(schema),
        view_(schema),
        rows_remaining_(0) {}

  // Convenience constructor that resets to the specified view right away.
  explicit ViewIterator(const View& view)
      : schema_(view.schema()),
        view_(view.schema()) {
    reset(view);
  }

  // Resets the iterator to point to the specified view. Right after this
  // method is called, the iterator state is 'before the beginning of the
  // view'; you must call next() to actually fill it with data:
  //
  // iterator.reset(view);
  // EXPECT_EQ(0, iterator.row_count());
  // ASSERT_TRUE(iterator.next());
  // EXPECT_EQ(view.row_count(), iterator.row_count());
  void reset(const View& view) {
    view_.ResetFrom(view);  // DCHECKs the schema internally.
    view_.set_row_count(0);
    rows_remaining_ = view.row_count();
  }

  // Advances the iterator, returning up to max_row_count subsequent rows.
  // The max_row_count must be > 0. Returns true on success; false on EOS.
  // If successful, a following call to view().row_count() will yield a
  // positive number.
  bool next(rowcount_t max_row_count) {
    rows_remaining_ -= row_count();
    view_.Advance(row_count());
    rowcount_t result_row_count = std::min(max_row_count, rows_remaining_);
    view_.set_row_count(result_row_count);
    return result_row_count > 0;
  }

  // Possibly reduces the size of the view() up to max_row_count. Returns true
  // if the reduction actually took place; false if view() already contained
  // max_row_count or less rows. The truncated rows are 'pushed back' to the
  // source; they will be returned again after a subsequent call to next():
  //
  // LOG(INFO) << view.row_count();  // Say, 20
  // ViewIterator(view);
  // ASSERT_TRUE(iterator.next(15));
  // EXPECT_EQ(15, iterator.row_count());
  // ASSERT_TRUE(iterator.truncate(5));
  // EXPECT_EQ(5, iterator.row_count());
  // ASSERT_TRUE(iterator.next(20));
  // EXPECT_EQ(15, iterator.row_count());
  bool truncate(rowcount_t max_row_count) {
    if (max_row_count < view_.row_count()) {
      view_.set_row_count(max_row_count);
      return true;
    } else {
      return false;
    }
  }

  const TupleSchema& schema() const { return schema_; }

  const View& view() const { return view_; }

  // Equivalent to view().row_count().
  const rowcount_t row_count() const { return view_.row_count(); }

  // Equivalent to schema().attribute_count().
  const int column_count() const { return schema().attribute_count(); }

  // Returns the number of rows remaining in ViewIterator, including the rows in
  // current view().
  rowcount_t rows_remaining() const {
    return rows_remaining_;
  }

  const Column& column(size_t column_index) const {
    return view_.column(column_index);
  }

 private:
  TupleSchema schema_;
  View view_;
  rowcount_t rows_remaining_;  // Includes rows in the view_.
  DISALLOW_COPY_AND_ASSIGN(ViewIterator);
};

// Keeping here to make CursorProxy inlinable (in an attempt to obsessively
// optimize).
namespace internal {

// A helper class, existing to factor out common parts of CursorIterator
// and CursorRowIterator. This class hides a cursor, remembers its last
// result, and allows the caller to cause the cursor to be safely destroyed
// (and resources released).
class CursorProxy {
 public:
  explicit CursorProxy(Cursor* cursor);

  void Next(rowcount_t max_row_count) {
    DCHECK(!cursor_status_.is_done())
        << "Cursor already iterated to completion.";
    cursor_status_ = cursor_->Next(max_row_count);
    if (cursor_status_.is_done() &&
        FLAGS_supersonic_release_cursors_aggressively) {
      // Dispose of the cursor to release resources ASAP.
      Terminate();
    }
  }

  void Interrupt() { if (cursor_ != NULL) cursor_->Interrupt(); }

  // Replaces the underlying cursor with the result of the transformation.
  void ApplyToCursor(CursorTransformer* transformer) {
    cursor_.reset(transformer->Transform(cursor_.release()));
  }

  const ResultView& status() const { return cursor_status_; }

  // Destroys the underlying cursor. If the iterator was not done, the proxy
  // will then behave as if it reached an exception.
  void Terminate();

  Exception* release_exception() { return cursor_status_.release_exception(); }

  bool is_waiting_on_barrier_supported() const {
    return is_waiting_on_barrier_supported_;
  }

  void AppendDebugDescription(string* target) const;

 private:
  std::unique_ptr<Cursor> cursor_;
  const bool is_waiting_on_barrier_supported_;
  string terminal_debug_description_;
  ResultView cursor_status_;
  DISALLOW_COPY_AND_ASSIGN(CursorProxy);
};

}  // namespace internal

// An iterator interface to read from cursors in smaller chunks.
class CursorIterator {
 public:
  explicit CursorIterator(Cursor* cursor)
      : proxy_(cursor),
        view_iterator_(cursor->schema()),
        row_index_(0) {}

  // Advances the iterator, to return a view with as many rows as possible.
  // Returns true on success; false on EOS or exception. If successful, a
  // following call to has_data() will yield true, and a call to
  // view().row_count() will yield a positive number. You can
  // call truncate(...) to further reduce the number of rows in the view.
  //
  // If Next() or EagerNext() return false:
  // * if is_eos() or is_failure(), any subsequent call will always return false
  //   and do nothing.
  // * if is_waiting_on_barrier(), an immediately subsequent call will return
  //   false and do nothing. Intervening operations on other cursors, however,
  //   may cause the iterator to get 'unstuck' from the barrier.
  bool EagerNext() {
    return Next(std::numeric_limits<rowcount_t>::max(), false);
  }

  // Advances the iterator, to return a view with up-to max_row_count rows.
  // Returns true on success; false on EOS or exception. If successful, a
  // following call to has_data() will yield true, and a call to
  // view().row_count() will yield a positive number. You can
  // call truncate(...) to further reduce the number of rows in the view.
  //
  // If limit_cursor_input is false, the underlying cursor is asked for
  // as many rows as it can provide. Otherwise, it is asked for up to
  // max_row_count rows. If unsure, use 'false'. This way, even if you iterate
  // row-by-row, the underlying cursor stack still operates in a block-based
  // fashion.
  //
  // See the discussion in EagerNext() for semantics after Next() or
  // EagerNext() return false.
  bool Next(rowcount_t max_row_count, bool limit_cursor_input) {
    row_index_ += view_iterator_.row_count();
    while (!view_iterator_.next(max_row_count)) {
      if (is_done()) return false;
      proxy_.Next(
          limit_cursor_input ? max_row_count
                             : std::numeric_limits<rowcount_t>::max());
      if (!has_data()) {
        // Leaves the view_iterator empty (row_count() == 0).
        return false;
      }
      view_iterator_.reset(proxy_.status().view());
    }
    return true;
  }

  // Destroys the underlying cursor. If the iterator was not done, it will
  // then behave as if it reached an exception.
  void Terminate() {
    row_index_ += view_iterator_.row_count();
    view_iterator_.next(std::numeric_limits<rowcount_t>::max());
    proxy_.Terminate();
  }

  // Propagates the interruption request to the underlying cursor.
  void Interrupt() { proxy_.Interrupt(); }

  // Runs transformation on the underlying cursor proxy.
  void ApplyToCursor(CursorTransformer* transformer) {
    proxy_.ApplyToCursor(transformer);
  }

  // Possibly reduces the size of the view() up to max_row_count. Returns true
  // if the reduction actually took place; false if view() already contained
  // max_row_count or less rows. The truncated rows are 'pushed back' to the
  // source; a subsequent call to next() will re-fetch them:
  //
  // CursorIterator iterator(...);
  // CHECK(iterator.EagerNext());
  // LOG(INFO) << iterator.row_count();  // say, 20
  // ASSERT_TRUE(iterator.truncate(5));
  // EXPECT_EQ(5, iterator.row_count());
  // ASSERT_TRUE(iterator.EagerNext());
  // EXPECT_EQ(15, iterator.row_count());
  bool truncate(rowcount_t max_row_count) {
    DCHECK(has_data());
    return view_iterator_.truncate(max_row_count);
  }

  const TupleSchema& schema() const {
    return view_iterator_.schema();  // Valid even after cursor termination.
  }

  // Returns true if the iteration has succeeded with retrieving new piece of
  // data. If so, it is safe to call view().
  // Returns false if the iterator state is one of: failure, EOS, BOS.
  bool has_data() const { return proxy_.status().has_data(); }

  // Returns true if the cursor is in a terminal state (EOS or failure).
  bool is_done() const { return proxy_.status().is_done(); }

  // Returns the current view. The result is undefined if the iterator is
  // in state where has_data() == false.
  const View& view() const {
    DCHECK(has_data());
    return view_iterator_.view();
  }

  // Returns true if the iteration resulted in an exception.
  // Usage pattern:
  // if (!iterator.next()) {
  //   if (iterator.is_failure() { ... handle the exception ... }
  //   else { CHECK(iterator.is_eos()); /* ... handle the EOS ... */ }
  // }
  const bool is_failure() const { return proxy_.status().is_failure(); }

  // Returns true if the iteration has reached EOS.
  const bool is_eos() const { return proxy_.status().is_eos(); }

  // Returns true if the iteration is at BOS (beginning of stream, i.e. before
  // the first call to Next).
  const bool is_bos() const { return proxy_.status().is_bos(); }

  // Returns true if the iteration is waiting on a barrier, caused by other
  // cursors reading from the same stream.
  const bool is_waiting_on_barrier() const {
    return proxy_.status().is_waiting_on_barrier();
  }

  // Returns the exception that caused this iteration to fail. The result
  // is undefined if the iteration did not fail.
  const Exception& exception() const { return proxy_.status().exception(); }

  // Adapter to cursor APIs, wrapping the iteration state in a ResultView.
  ResultView result() const {
    return has_data() ? ResultView::Success(&view()) : proxy_.status();
  }

  // After a successful Next(), returns the absolute row offset of the current
  // view in the underlying cursor. When the iterator is in one of the 'special'
  // states (EOS, BOS, error), the method behaves as follows:
  // BOS: the result is undefined.
  // EOS: returns the total row count.
  // Error: returns the row index one past the last successfully returned view.
  // TODO(user): we might need to refine BOS behavior if we ever support
  // mark/reset.
  const rowcount_t current_row_index() const {
    DCHECK(!is_bos());
    return row_index_;
  }

  // Returns the exception raised by the failing iterator.next(), if any.
  // Ownership is passed to the caller. This function exists mainly to
  // support PROPAGATE_ON_FAILURE(iterator). Can be called only once.
  // (Subsequent calls will crash).
  Exception* release_exception() {
    return proxy_.release_exception();
  }

  bool is_waiting_on_barrier_supported() const {
    return proxy_.is_waiting_on_barrier_supported();
  }

  void AppendDebugDescription(string* target) const {
    proxy_.AppendDebugDescription(target);
  }

 private:
  internal::CursorProxy proxy_;
  ViewIterator view_iterator_;
  rowcount_t row_index_;
  DISALLOW_COPY_AND_ASSIGN(CursorIterator);
};

// ViewRowIterator and CursorRowIterator, below, conform to the ConstRow
// contract defined in the internal/row.h.

// A row-oriented interface for accessing views. Indented to be as fast as
// row-oriented accessor can be (so that it makes sense to use it for row-
// oriented cursor implementations).
class ViewRowIterator {
 public:
  // Creates a new view iterator for views with the specified schema. If you
  // try to iterate it before resetting, it will behave as-if it was reset
  // to an empty view.
  explicit ViewRowIterator(const TupleSchema& schema)
      : schema_(schema),
        view_(schema),
        row_index_(-1) {}

  // A convenience constructor that resets the iterator to the specified view
  // right away.
  explicit ViewRowIterator(const View& view)
      : schema_(view.schema()),
        view_(view),
        row_index_(-1) {}

  // Resets the iterator to the specified view. The view must have a compatible
  // schema.
  void reset(const View& view) {
    view_.ResetFrom(view);  // DCHECKs the schema internally.
    row_index_ = -1;
  }

  // Resets the iterator to the initial state, i.e. as-if it was reset to an
  // empty view.
  void clear() {
    view_.set_row_count(0);
    row_index_ = -1;
  }

  // Advances the iterator to the next row. Returns true if successful, i.e.
  // if the row existed; false if the iterator has just moved past the last row.
  // Once next() returns false, it continues returning false.
  bool next() {
    DCHECK_LE(row_index_, total_row_count());
    // The following code is the more efficient version of this:
    // if (row_index_ < total_row_count()) ++row_index_;
    // return row_index_ < total_row_count();
    int increment = (row_index_ < total_row_count());
    return (row_index_ += increment) < total_row_count();
  }

  // Returns the iterator's schema.
  const TupleSchema& schema() const { return schema_; }

  // Returns the absolute index of the current row in the iterated view.
  // Right after reset, it is -1. After the cursor reaches EOS, it reflects the
  // total row count.
  const int64 current_row_index() const { return row_index_; }

  // Returns the total row count in the iterated view.
  const int64 total_row_count() const { return view_.row_count(); }

  // Returns the type_info for the specified column.
  const TypeInfo& type_info(const int column_index) const {
    return view_.column(column_index).type_info();
  }

  // Returns true if the item at the specified column, in the current row,
  // is null; false otherwise.
  // The result is undefined if next() has not been called since last reset,
  // or if it returned false.
  bool is_null(const int column_index) const {
    DCHECK_LE(current_row_index(), total_row_count());
    bool_const_ptr is_null = view_.column(column_index).is_null();
    return is_null != NULL && is_null[row_index_];
  }

  // Returns a pointer to the item at the specified column, in the current row.
  // The result is undefined if next() has not been called since last reset,
  // or if it returned false.
  VariantConstPointer data(const int column_index) const {
    DCHECK_LE(current_row_index(), total_row_count());
    return view_.column(column_index).data_plus_offset(row_index_);
  }

  // Returns a typed pointer to the item at the specified column, in the
  // current row.
  // The result is undefined if next() has not been called since last reset,
  // or if it returned false.
  template<DataType type>
  const typename TypeTraits<type>::cpp_type* typed_data(
      const int column_index) const {
    DCHECK_LE(current_row_index(), total_row_count());
    return view_.column(column_index).typed_data<type>() + row_index_;
  }

  // Returns a typed const reference to the item at the specified column, in
  // the current row.
  // The result is undefined if next() has not been called since last reset,
  // if it returned false, or if the item is NULL.
  template<DataType type>
  const typename TypeTraits<type>::cpp_type& typed_notnull_data(
      const int column_index) const {
    DCHECK_LE(current_row_index(), total_row_count());
    DCHECK(!is_null(column_index));
    return view_.column(column_index).typed_data<type>()[row_index_];
  }

 private:
  TupleSchema schema_;
  View view_;
  int64 row_index_;
  DISALLOW_COPY_AND_ASSIGN(ViewRowIterator);
};

// A row-oriented interface for accessing cursors.
class CursorRowIterator {
 public:
  // Creates a new cursor iterator. Takes ownership of the cursor.
  explicit CursorRowIterator(Cursor* cursor)
      : proxy_(cursor),
        view_iterator_(cursor->schema()),
        row_index_base_(0) {}

  // Advances the iterator to the next row. Returns true if successful;
  // false on EOS or exception.
  // Once Next() returns false:
  // * if is_eos() or is_failure(), any subsequent call will always return
  //   false and do nothing.
  // * if is_waiting_on_barrier(), an immediately subsequent call will return
  //   false and do nothing. Intervening operations on other cursors, however,
  //   may cause the iterator to get 'unstuck' from the barrier, and return
  //   true from further calls.
  bool Next() {
    while (!view_iterator_.next()) {
      if (is_done()) {
        return false;
      }
      row_index_base_ += view_iterator_.total_row_count();
      proxy_.Next(std::numeric_limits<rowcount_t>::max());
      if (proxy_.status().has_data()) {
        view_iterator_.reset(proxy_.status().view());
      } else {
        // Make sure that a subsequent call to current_row_index() returns
        // the total number of rows in the cursor so far.
        view_iterator_.clear();
        CHECK(!view_iterator_.next());
        return false;
      }
    }
    return true;
  }

  // Same with above, but the call to Next() will not advance block and in the
  // event it needs to do so, it returns false.
  bool NextWithinBlock() {
    return view_iterator_.next();
  }

  // Destroys the underlying cursor. If the iterator was not done, it will
  // then behave as if it reached an exception.
  void Terminate() {
    view_iterator_.clear();
    CHECK(!view_iterator_.next());
    proxy_.Terminate();
  }

  // Propagates the cancellation request to the underlying cursor.
  void Interrupt() { proxy_.Interrupt(); }

  // Runs transformation on the underlying cursor proxy.
  void Transform(CursorTransformer* transformer) {
    proxy_.ApplyToCursor(transformer);
  }

  // Returns the iterator's (== the cursor's) schema.
  const TupleSchema& schema() const {
    return view_iterator_.schema();  // Valid even after cursor termination.
  }

  // Returns the type_info for the specified column.
  const TypeInfo& type_info(const int column_index) const {
    return view_iterator_.type_info(column_index);
  }

  // Returns true if the item at the specified column, in the current row,
  // is null; false otherwise.
  // The result is undefined if next() has not been called since last reset,
  // or if it returned false.
  bool is_null(const int column_index) const {
    return view_iterator_.is_null(column_index);
  }

  // Returns a pointer to the item at the specified column, in the current row.
  // The result is undefined if next() has not been called since last reset,
  // or if it returned false.
  VariantConstPointer data(const int column_index) const {
    return view_iterator_.data(column_index);
  }

  // Returns a typed pointer to the item at the specified column, in the
  // current row.
  // The result is undefined if next() has not been called since last reset,
  // or if it returned false.
  template<DataType type>
  const typename TypeTraits<type>::cpp_type* typed_data(
      const int column_index) const {
    return view_iterator_.typed_data<type>(column_index);
  }

  // Returns a typed const reference to the item at the specified column, in
  // the current row.
  // The result is undefined if next() has not been called since last reset,
  // if it returned false, or if the item is NULL.
  template<DataType type>
  const typename TypeTraits<type>::cpp_type& typed_notnull_data(
      const int column_index) const {
    return view_iterator_.typed_notnull_data<type>(column_index);
  }

  // Returns the index of the current row, in the original cursor.
  const rowid_t current_row_index() const {
    return row_index_base_ + view_iterator_.current_row_index();
  }

  // Returns true if the last iterator.next() failed.
  const bool is_failure() const { return proxy_.status().is_failure(); }

  // Returns true if the iteration has reached EOS.
  const bool is_eos() const { return proxy_.status().is_eos(); }

  // Returns true if the cursor is in a terminal state (EOS or failure).
  const bool is_done() const { return proxy_.status().is_done(); }

  // Returns true if the iteration is waiting on a barrier, caused by a
  // synchronous iteration of the same stream by some other cursor.
  const bool is_waiting_on_barrier() const {
    return proxy_.status().is_waiting_on_barrier();
  }

  // Returns the exception raised by the failing iterator.next(). The result
  // is undefined if the last iterator.next() did not fail.
  const Exception& exception() const { return proxy_.status().exception(); }

  // Returns the exception raised by the failing iterator.next(), if any.
  // Ownership is passed to the caller. This function exists mainly to
  // support PROPAGATE_ON_FAILURE(iterator). Can be called only once.
  // (Subsequent calls will crash).
  Exception* release_exception() {
    return proxy_.release_exception();
  }

  bool is_waiting_on_barrier_supported() const {
    return proxy_.is_waiting_on_barrier_supported();
  }

  void AppendDebugDescription(string* target) const {
    proxy_.AppendDebugDescription(target);
  }

 private:
  internal::CursorProxy proxy_;
  ViewRowIterator view_iterator_;
  rowid_t row_index_base_;
  DISALLOW_COPY_AND_ASSIGN(CursorRowIterator);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_ITERATORS_H_
