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

#ifndef SUPERSONIC_CURSOR_BASE_LOOKUP_INDEX_H_
#define SUPERSONIC_CURSOR_BASE_LOOKUP_INDEX_H_

#include "supersonic/utils/macros.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

class BoundSingleSourceProjector;
class LookupIndexCursor;
class Exception;
class TupleSchema;
class CursorTransformer;

class LookupIndex {
 public:
  virtual ~LookupIndex() {}
  // Performs in a single invocation a series of individual lookups, each of
  // them stored in the query block as a single row. Query blocks should only
  // hold key columns, meaning they should have identical schema as key_schema.
  // The returned cursor packs matches in LookupIndexViews, one match per row,
  // limited to value columns, and with an additioal query_ids() side vector.
  // Because a query row can match 0, 1 or more index rows, the total number
  // of rows in block(s) returned by the result cursor (the total number
  // of matches) can be different than the number of query rows. Also, the
  // result cursor can return 0, 1 or more blocks.
  // The query block must outlive the result cursor.
  virtual FailureOrOwned<LookupIndexCursor> MultiLookup(
      const View* query) const = 0;

  // Internal index's full logical schema, includes key and non-key columns.
  virtual const TupleSchema& schema() const = 0;

  // Key columns schema.
  virtual const BoundSingleSourceProjector& key_selector() const = 0;

  // Creates a projector from schema() to non-key columns. The default
  // implementation does so by complementing key_selector().
  virtual BoundSingleSourceProjector value_selector() const;

  // Returns true if the index has no items, thus MultiLookup is guaranteed
  // to always return an empty cursor.
  virtual bool empty() const = 0;
};

class LookupIndexBuilder {
 public:
  LookupIndexBuilder() {}
  virtual const TupleSchema& schema() const = 0;

  // Attempts to build the index. May fail (return an Exception), or return
  // NULL indicating that a barrier was encountered, and Build needs to be
  // retried after the barrier is crossed.
  virtual FailureOrOwned<LookupIndex> Build() = 0;

  // Notifies the builder that the index is no longer needed. The builder
  // should propagate this notification to the underlying cursors, if any.
  virtual void Interrupt() = 0;

  // Runs the cursor transformer on the input cursor (if it has one).
  virtual void ApplyToChildren(CursorTransformer* transformer) = 0;

  virtual ~LookupIndexBuilder() {}
 private:
  DISALLOW_COPY_AND_ASSIGN(LookupIndexBuilder);
};


// A subclass of View with an additional query_ids side vector. The vector holds
// ids of query rows that match index rows at corresponding offsets in the view:
// i-th returned index row matches query row at id query_ids[i].
class LookupIndexView : public View {
 public:
  // query_ids is a placeholder allocated by the caller.
  LookupIndexView(const TupleSchema& schema, rowid_t* query_ids)
      : View(schema), query_ids_(query_ids) { }

  const rowid_t* query_ids() const { return query_ids_; }
  rowid_t* mutable_query_ids() { return query_ids_; }

 private:
  rowid_t* query_ids_;  // Not owned.
};


// Result wrapper around LookupIndexView, result of LookupIndexCursor::Next().
// TODO(user): Avoid code duplication between this class and ResultView.
class ResultLookupIndexView {
 public:
  // A result that indicates END_OF_INPUT.
  static ResultLookupIndexView EOS() {
    return ResultLookupIndexView(NULL, true);
  }

  // A result that indicates a success, with a valid value.
  static ResultLookupIndexView Success(const LookupIndexView* view) {
    return ResultLookupIndexView(view, false);
  }

  // A result that indicates a failure, with a given exception.
  static ResultLookupIndexView Failure(Exception* exception) {
    return ResultLookupIndexView(exception);
  }

  bool has_data() const { return !is_failure() && !is_eos(); }
  bool is_eos() const { return is_eos_; }
  bool is_failure() const { return result_.is_failure(); }

  // Obtains the view. Can be called only if there was no exception.
  const LookupIndexView& view() const { return *result_.get(); }

  // Obtains the exception. Can be called only if there was one.
  const Exception& exception() const { return result_.exception(); }

  // Obtains and releases the exception. Can be called only if there was one.
  // Ownership is passed to the caller.
  Exception* release_exception() { return result_.release_exception(); }

 private:
  ResultLookupIndexView(const LookupIndexView* view, bool is_eos)
      : result_(supersonic::Success(view)),
        is_eos_(is_eos) {}

  explicit ResultLookupIndexView(Exception* exception)
      : result_(supersonic::Failure(exception)),
        is_eos_(false) {}

  FailureOr<const LookupIndexView*> result_;
  bool is_eos_;
  // Copyable.
};


// A stream of LookupIndexView results modeled after Cursor.
class LookupIndexCursor {
 public:
  virtual ~LookupIndexCursor() {}

  virtual const TupleSchema& schema() const = 0;
  virtual ResultLookupIndexView Next(rowcount_t max_row_count) = 0;

 protected:
  // To allow instantiation in subclasses.
  LookupIndexCursor() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(LookupIndexCursor);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_BASE_LOOKUP_INDEX_H_
