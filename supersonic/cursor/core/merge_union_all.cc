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

#include "supersonic/cursor/core/merge_union_all.h"

#include <stdint.h>

#include <memory>
#include <queue>
#include "supersonic/utils/std_namespace.h"
#include <stack>
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/generate.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/pointer_vector.h"
#include "supersonic/utils/stl_util.h"

// This implementation is row-oriented. A previous attempt at block-oriented
// implementation resulted in 2x slower code; see cl/14371571.

namespace supersonic {

class BufferAllocator;

using util::gtl::PointerVector;

namespace {

// A row comparator used in the priority queue of rows below.
struct RowComparator {
 public:
  explicit RowComparator(const BoundSortOrder& sort_order) {
    const TupleSchema& key_schema = sort_order.schema();
    for (int i = 0; i < key_schema.attribute_count(); i++) {
      key_column_ids_.push_back(
          sort_order.projector().source_attribute_position(i));
      const Attribute& attribute = key_schema.attribute(i);
      value_comparators_.push_back(
          GetSortComparator(attribute.type(),
                            sort_order.column_order(i) == DESCENDING,
                            !attribute.is_nullable(),
                            i + 1 == key_schema.attribute_count()));
    }
  }

  // Returns true if a should come after b in the sorted output.
  // Comparison of two rows is done as a series of comparisons of individual
  // values in order given by key_selector. For each key attribute
  // RowComparator holds an appropriate ValueComparator specialization.
  // Together they are stored in a vector of virtual comparators.
  bool operator()(const CursorRowIterator& a,
                  const CursorRowIterator& b) const {
    for (int i = 0; i < key_column_ids_.size(); i++) {
      const int key_column_id = key_column_ids_[i];
      const VariantConstPointer value_a =
          a.is_null(key_column_id) ? NULL : a.data(key_column_id);
      const VariantConstPointer value_b =
          b.is_null(key_column_id) ? NULL : b.data(key_column_id);
      // Using "b comparison a", as this is more convenient in the presence of
      // RESULT_GREATER_OR_EQUAL.
      const ComparisonResult b_cmp_a = value_comparators_[i](value_b, value_a);
      if (b_cmp_a == RESULT_LESS) return true;
      // If b_cmp_a is RESULT_GREATER_OR_EQUAL, then this is the last compared
      // column and we will correctly return false after finishing the loop.
      if (b_cmp_a == RESULT_GREATER) return false;
    }
    return false;
  }

 private:
  vector<InequalityComparator> value_comparators_;
  vector<int> key_column_ids_;
  DISALLOW_COPY_AND_ASSIGN(RowComparator);
};

// A copyable wrapper around RowComparator that delegates to it. This is
// necessary to share an instance of RowComparator between MergeSortedCursor
// and PriorityQueue objects because the queue makes a copy of the comparator
// it is instantiated with.
struct RowComparatorPointer {
 public:
  explicit RowComparatorPointer(const RowComparator* const row_comparator)
      : row_comparator_(row_comparator) { }
  bool operator()(const CursorRowIterator* a,
                  const CursorRowIterator* b) const {
    return row_comparator_->operator()(*a, *b);
  }
 private:
  const RowComparator* const row_comparator_;
};

class MergeUnionAllCursor : public Cursor {
 public:
  // Takes ownership of key_selector. key_selector is an ordered sequence of
  // columns that make up the (possibly multi-column) key that rows are sorted
  // based on.
  MergeUnionAllCursor(const BoundSortOrder* sort_order,
                      const vector<Cursor*>& inputs,
                      BufferAllocator* allocator);

  // Allocates the internal block that acts as a placeholder for one view worth
  // of result rows. Returns success iff the allocation is successful.
  FailureOrVoid Init();

  virtual const TupleSchema& schema() const;
  virtual ResultView Next(rowcount_t max_row_count);
  virtual void Interrupt();
  virtual void ApplyToChildren(CursorTransformer* transformer);
  virtual void AppendDebugDescription(string* target) const;
  virtual bool IsWaitingOnBarrierSupported() const { return true; }
  virtual CursorId GetCursorId() const { return MERGE_UNION_ALL; }

  // Calculates a common schema from all input schemas and specifically, the
  // nullability: an output column is nullable if any in the set of
  // corresponding input columns is nullable.
  static TupleSchema ResultSchema(const vector<Cursor*>& inputs);

 private:
  typedef priority_queue<CursorRowIterator*, vector<CursorRowIterator*>,
                         RowComparatorPointer> PriorityQueue;

  // Equals to true if result_ was returned to caller and we can clear it.
  bool clear_result_;

  std::unique_ptr<const BoundSortOrder> sort_order_;
  PointerVector<CursorRowIterator> inputs_;

  // Internal placeholder for result rows.
  Table result_;

  // Used by priority_queue_.
  RowComparator row_comparator_;

  // For appending rows to the result.
  TableRowAppender<DirectRowSourceReader<CursorRowIterator> > row_appender_;

  // Used to merge sorted rows from many inputs in order.
  PriorityQueue priority_queue_;

  // Contains inputs that have not been inserted to the queue. Initially, these
  // are all inputs. If any input blocks on 'waiting on barrier' it will be
  // pushed back to the top of the stack. This ensures that we will always
  // process inputs in the same order, not skipping any of them.
  // Later a single input may get there if it cannot be advanced because of
  // 'waiting on barrier'. No other inputs will be read until reading of that
  // particular input succeeds. This ensures that rows will always be pushed
  // into priority_queue_ in the same order and guarantee operation determinism
  // assuming std::priority_queue is deterministic (not necessarily stable).
  std::stack<CursorRowIterator*> pending_inputs_;
};

MergeUnionAllCursor::MergeUnionAllCursor(const BoundSortOrder* sort_order,
                                         const vector<Cursor*>& inputs,
                                         BufferAllocator* allocator)
    : clear_result_(true),
      sort_order_(sort_order),
      result_(ResultSchema(inputs), allocator),
      row_comparator_(*sort_order),
      // TODO(user): Avoid deep copy, by controlling view iterations.
      row_appender_(&result_, true),
      priority_queue_(RowComparatorPointer(&row_comparator_)) {
  for (int i = 0; i < inputs.size(); i++) {
    CursorRowIterator* input = new CursorRowIterator(inputs[i]);
    inputs_.push_back(input);
    pending_inputs_.push(input);
  }
}

FailureOrVoid MergeUnionAllCursor::Init() {
  result_.ReserveRowCapacity(Cursor::kDefaultRowCount);
  if (result_.row_capacity() == 0) {
    THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                        "Memory exceeded in Init(): can't allocate Table."));
  }
  return Success();
}

const TupleSchema& MergeUnionAllCursor::schema() const {
  return result_.schema();
}

void MergeUnionAllCursor::AppendDebugDescription(string* target) const {
  target->append("MergeSortedCursor");
}

ResultView MergeUnionAllCursor::Next(rowcount_t max_row_count) {
  // The priority queue is initialized on the first call to Next(). From then on
  // the queue holds exactly one row from each input which has not been
  // exhausted yet - the smallest value. In case a cursor that needs to be
  // advanced hits WaitingOnBarrier, it is moved back to the pending queue,
  // and retried on next Next().
  if (clear_result_) {
    result_.Clear();
    clear_result_ = false;
  }

  while (!pending_inputs_.empty()) {
    CursorRowIterator* input = pending_inputs_.top();
    pending_inputs_.pop();
    if (input->Next()) {
      priority_queue_.push(input);
    } else {
      PROPAGATE_ON_FAILURE(*input);
      if (input->is_waiting_on_barrier()) {
        pending_inputs_.push(input);
        return ResultView::WaitingOnBarrier();
      }
      CHECK(input->is_eos());
    }
  }

  if (max_row_count > result_.row_capacity())
    max_row_count = result_.row_capacity();

  // The main loop iterates over rows from all inputs combined. In each
  // iteration, the smallest row is popped from the queue and appended to the
  // final result block. The popped row is replaced with the next smallest row
  // from that input, unless there are no more rows from that input. The loop
  // terminates if the queue is empty or if max_row_count rows is processed.

  while ((result_.row_count() < max_row_count) && !priority_queue_.empty()) {
    // The id of the next unread row from each input is stored uniquely in the
    // queue.
    CursorRowIterator* input = priority_queue_.top();
    if (!row_appender_.AppendRow(*input)) {
      THROW(new Exception(
          ERROR_MEMORY_EXCEEDED, "Memory exceeded when copying data"));
    }
    priority_queue_.pop();
    if (input->Next()) {
      priority_queue_.push(input);
    } else {
      PROPAGATE_ON_FAILURE(*input);
      if (input->is_waiting_on_barrier()) {
        pending_inputs_.push(input);
        return ResultView::WaitingOnBarrier();
      }
      CHECK(input->is_eos());
    }
  }

  if (result_.row_count() > 0) {
    clear_result_ = true;
    return ResultView::Success(&result_.view());
  } else {
    return ResultView::EOS();
  }
}

void MergeUnionAllCursor::Interrupt() {
  for (size_t i = 0; i < inputs_.size(); i++) {
    inputs_[i]->Interrupt();
  }
}

void MergeUnionAllCursor::ApplyToChildren(CursorTransformer* callback) {
  // Using CursorRowIterator's Transform() method which will transform
  // its internal cursor.
  for (size_t i = 0; i < inputs_.size(); i++) {
    inputs_[i]->Transform(callback);
  }
}

TupleSchema MergeUnionAllCursor::ResultSchema(const vector<Cursor*>& inputs) {
  for (int i = 0; i < inputs.size() - 1; i++)
    CHECK(inputs[i + 1]->schema().EqualByType(inputs[i]->schema()));
  TupleSchema result;
  for (int i = 0; i < inputs[0]->schema().attribute_count(); i++) {
    const Attribute& attr = inputs[0]->schema().attribute(i);
    Nullability nullability = attr.nullability();
    for (int j = 1; j < inputs.size() && nullability == NOT_NULLABLE; j++) {
      if (inputs[j]->schema().attribute(i).is_nullable())
        nullability = NULLABLE;
    }
    result.add_attribute(Attribute(attr.name(), attr.type(), nullability));
  }
  return result;
}

class MergeUnionAllOperation : public BasicOperation {
 public:
  MergeUnionAllOperation(const SortOrder* sort_order,
                         const vector<Operation*>& inputs)
    : BasicOperation(inputs),
      sort_order_(sort_order) {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    vector<Cursor*> inputs;
    ElementDeleter deleter(&inputs);
    for (int i = 0; i < children_count(); i++) {
      FailureOrOwned<Cursor> input_cursor = child_at(i)->CreateCursor();
      PROPAGATE_ON_FAILURE(input_cursor);
      inputs.push_back(input_cursor.release());
    }

    // BoundSortOrder should allow for differences in column nullability in
    // different inputs, so we compute a common schema with
    // MergeUnionAllCursor::ResultSchema().
    FailureOrOwned<const BoundSortOrder> bound_sort_order =
        sort_order_->Bind(MergeUnionAllCursor::ResultSchema(inputs));
    PROPAGATE_ON_FAILURE(bound_sort_order);
    FailureOrOwned<Cursor> result =
        BoundMergeUnionAll(bound_sort_order.release(), inputs,
                           buffer_allocator());
    inputs.clear();  // Disengages the deleter.
    return result;
  }

 private:
  std::unique_ptr<const SortOrder> sort_order_;
};

}  // namespace

Operation* MergeUnionAll(const SortOrder* sort_order_raw,
                         const vector<Operation*>& inputs) {
  // If there are no inputs, there'll be nowhere to take the schema from.
  std::unique_ptr<const SortOrder> sort_order(sort_order_raw);
  if (inputs.empty()) {
    return Generate(0);
  }
  if (inputs.size() == 1) {
    return inputs.at(0);
  }
  return new MergeUnionAllOperation(sort_order.release(), inputs);
}

FailureOrOwned<Cursor> BoundMergeUnionAll(const BoundSortOrder* sort_order,
                                          vector<Cursor*> inputs,
                                          BufferAllocator* buffer_allocator) {
  std::unique_ptr<MergeUnionAllCursor> cursor(
      new MergeUnionAllCursor(sort_order, inputs, buffer_allocator));
  PROPAGATE_ON_FAILURE(cursor->Init());
  return Success(cursor.release());
}

}  // namespace supersonic
