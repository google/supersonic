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

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <vector>
using std::vector;

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/port.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/operators.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/cursor/infrastructure/row.h"
#include "supersonic/cursor/infrastructure/row_copier.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/aggregator.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

// A (View*, RowId) pair, denotes a row.
class BufferAllocator;

struct Row {
  Row(const View* key_columns_, rowid_t row_id_)
  : key_columns(key_columns_), row_id(row_id_) {}
  const View* key_columns;
  rowid_t row_id;
};

// Abstract interface of classes that compare particular values (table cells)
// in corresponding columns for equality.
struct ValueComparatorInterface {
  virtual ~ValueComparatorInterface() {}
  virtual bool Equal(
      const Column& column_a, rowid_t row_id_a,
      const Column& column_b, rowid_t row_id_b) const = 0;

  virtual void ColumnEqual(const Column& column,
                           rowcount_t row_count,
                           bool *diff)
      const = 0;
};

// A concrete implementation for an arbitrary data type.
template <DataType type>
class ValueComparator : public ValueComparatorInterface {
 public:
  inline bool Equal(const Column& column_a, rowid_t row_id_a,
                    const Column& column_b, rowid_t row_id_b) const {
    operators::Equal equal;
    const bool is_null_a = (column_a.is_null() != NULL)
        && column_a.is_null()[row_id_a];
    const bool is_null_b = (column_b.is_null() != NULL)
        && column_b.is_null()[row_id_b];
    if (is_null_a || is_null_b) return is_null_a == is_null_b;
    return equal(column_a.typed_data<type>()[row_id_a],
                 column_b.typed_data<type>()[row_id_b]);
  }

  // Calculates whether consecutive entries in the column differ.
  inline void ColumnEqual(const Column& column, rowcount_t row_count,
                          bool *diff) const {
    typedef typename TypeTraits<type>::cpp_type cpp_type;
    operators::Equal equal;
    const cpp_type* data = column.typed_data<type>();
    const cpp_type* end = data + (row_count - 1);
    ++diff;

    if (column.is_null() != NULL) {
      bool is_null_a;
      bool is_null_b = column.is_null()[0];

      for (rowid_t i = 0; i < row_count - 1; ++i, ++data, ++diff) {
        is_null_a = is_null_b;
        is_null_b = column.is_null()[i + 1];
        if (!is_null_a && !is_null_b) {
          *diff |= (!equal(*data, *(data + 1)));
        } else {
          *diff |= (is_null_a != is_null_b);
        }
      }
    } else {
       for (; data != end; ++data, ++diff)
         *diff |= !equal(*data, *(data + 1));
    }
  }
};

// Helper struct used by CreateValueComparator.
struct ValueComparatorFactory {
  template <DataType type>
  ValueComparatorInterface* operator()() const {
    return new ValueComparator<type>();
  }
};

// Instantiates a particular specialization of ValueComparator<type>.
ValueComparatorInterface* CreateValueComparator(DataType type) {
  return TypeSpecialization<ValueComparatorInterface*, ValueComparatorFactory>(
      type);
}

// Holds and updates a set of unique keys on which input is clustered.
class AggregateClustersKeySet {
 public:
  // Creates a AggregateClustersKeySet. group_by_columns describes which
  // columns constitute a key and should be grouped together, it can be empty,
  // in which case all rows are considered equal and are grouped together.
  // Input data has to be clustered (rows with equal key need to be in
  // continuous block).
  // All columns specified in group_by_columns must exist in child's schema.
  // Set can store only output_block_capacity unique keys.
  static FailureOrOwned<AggregateClustersKeySet> Create(
      const BoundSingleSourceProjector* group_by,
      BufferAllocator* allocator,
      rowcount_t output_block_capacity) {
    std::unique_ptr<AggregateClustersKeySet> key_set(
        new AggregateClustersKeySet(group_by, allocator));
    PROPAGATE_ON_FAILURE(key_set->Init(output_block_capacity));
    return Success(key_set.release());
  }

  const TupleSchema& key_schema() const {
    return key_projector_->result_schema();
  }

  // View on a block that keeps unique keys. Can be called only when key is not
  // empty.
  const View& key_view() const {
    return indexed_block_.view();
  }

  // How many rows can a view passed in the next call to Insert() have. If 0, no
  // more calls to Insert are allowed.
  rowcount_t max_view_row_count_to_insert() const {
    // Does not accept views larger then cursor::kDefaultRowCount because this
    // is the size of preallocated table that holds result of Insert().
    return std::min(
        Cursor::kDefaultRowCount, capacity() - indexed_block_row_count_);
  }

  // Count of unique keys in the set. If key is empty, size is 1, even if no
  // rows were inserted to the set (this is in line with the group semantic,
  // which when there are no group by column returns always single row, even
  // when input is empty).
  rowcount_t size() const {
    return indexed_block_row_count_;
  }

  // How many unique keys the set can hold.
  rowcount_t capacity() const {
    return indexed_block_.row_capacity();
  }

  // Inserts all unique keys from the view to the key block. For each row
  // from input view finds an index of its key in the key block and returns
  // these indexes in a table of size view.row_count().
  // Input view can not have more rows then max_view_row_count_to_insert().
  const rowid_t* Insert(const View& view, bool all_equal);

  void Reset() {
    DCHECK_GT(indexed_block_.row_capacity(), 0)
        << "Init() must be called first";
    indexed_block_.ResetArenas();
    indexed_block_row_count_ = 0;
    last_added_.reset(NULL);
  }

  ~AggregateClustersKeySet();

 private:
  AggregateClustersKeySet(const BoundSingleSourceProjector* group_by,
                          BufferAllocator* allocator)
      : child_key_view_(group_by->result_schema()),
        key_projector_(group_by),
        indexed_block_(group_by->result_schema(), allocator),
        indexed_block_row_count_(0),
        copier_(indexed_block_.schema(), true) {
    for (int i = 0; i < key_schema().attribute_count(); i++) {
      comparators_.push_back(
          CreateValueComparator(key_schema().attribute(i).type()));
    }
  }

  FailureOrVoid Init(rowcount_t output_block_capacity) {
    if (!indexed_block_.Reallocate(output_block_capacity)) {
      THROW(new Exception(
          ERROR_MEMORY_EXCEEDED,
          StrCat("Block allocation failed. Key block with capacity ",
                 output_block_capacity, "not allocated.")));
    }
    return Success();
  }

  // Column-oriented comparison, result is put into diff.
  void column_compare(const View& view, Row* last_added, int col_id,
                      bool all_equal, bool* diff);

  // Calculates actual positions, where to aggregate rows from view.
  bool GetIndexTable(const View& view, bool all_equal, rowid_t* result);

  // Result of Insert.
  rowid_t key_result_index_map_[Cursor::kDefaultRowCount];

  // Bitmask indicating whether consecutive rows are different.
  bool row_diff_[Cursor::kDefaultRowCount];

  // View over an input view from child but with only key columns.
  View child_key_view_;
  std::unique_ptr<const BoundSingleSourceProjector> key_projector_;

  // Contains all the inserted rows.
  Block indexed_block_;

  // Number of rows inserted so far.
  rowcount_t indexed_block_row_count_;

  // Pointer to last added row. Is NULL when indexed_block_ is empty.
  std::unique_ptr<Row> last_added_;

  // comparator_[index] is used for index-th column comparison
  vector<const ValueComparatorInterface*> comparators_;

  RowCopier<DirectRowSourceReader<RowSourceAdapter>,
            DirectRowSourceWriter<RowSinkAdapter> > copier_;

  DISALLOW_COPY_AND_ASSIGN(AggregateClustersKeySet);
};

// Performs comparisons on consecutive columns of view. If there is
// a difference it puts "true" into diff table.
void AggregateClustersKeySet::column_compare(const View& view,
                                             Row* last_added,
                                             int col_id,
                                             bool all_equal,
                                             bool* diff) {
    const Column& key_column_a = view.column(col_id);
    if (!diff[0] && view.row_count() > 0) {
      const Column& key_column_b = last_added->key_columns->column(col_id);
      if (!comparators_[col_id]->Equal(
      key_column_a, 0, key_column_b, last_added->row_id)) diff[0] = 1;
    }
    if (!all_equal) {
      comparators_[col_id]->ColumnEqual(key_column_a, view.row_count(), diff);
    }
}

// GetIndexTable calculates result which contains positions,
// where to insert new rows.
bool AggregateClustersKeySet::GetIndexTable(const View& query,
                                            bool all_equal,
                                            rowid_t* result) {
  DCHECK_GT(indexed_block_.row_capacity(), 0) << "Init() must be called first";
  DCHECK(query.schema().EqualByType(indexed_block_.schema()));
  for (rowid_t i = 0; i < query.row_count(); ++i) row_diff_[i] = false;
  if (last_added_.get() == NULL) row_diff_[0] = true;
  for (int i = 0; i < query.column_count(); ++i)
    column_compare(query, last_added_.get(), i, all_equal, row_diff_);

  for (rowid_t row_id = 0; row_id < query.row_count(); row_id++) {
    if (row_diff_[row_id]) {
      // Copy query row into the index.
      DirectRowSourceWriter<RowSinkAdapter> writer;
      RowSinkAdapter sink(&indexed_block_, indexed_block_row_count_);
      DirectRowSourceReader<RowSourceAdapter> reader;
      RowSourceAdapter source(query, row_id);
      if (!copier_.Copy(reader, source, writer, &sink))
        return false;
      if (last_added_.get() == NULL) {
        last_added_.reset(new Row(&indexed_block_.view(), row_id));
      } else {
        ++last_added_->row_id;
      }
      indexed_block_row_count_++;
    }
    result[row_id] = last_added_->row_id;
  }
  return true;
}

const rowid_t* AggregateClustersKeySet::Insert(const View& view,
                                               bool all_equal) {
  CHECK_LE(view.row_count(), max_view_row_count_to_insert());

  // Set the key view.
  key_projector_->Project(view, &child_key_view_);
  child_key_view_.set_row_count(view.row_count());

  if (!GetIndexTable(child_key_view_, all_equal, key_result_index_map_)) {
    return NULL;
  }
  return key_result_index_map_;
}

AggregateClustersKeySet::~AggregateClustersKeySet() {
    for (int i = 0; i < comparators_.size(); i++)
      delete comparators_[i];
}

// Processes input during iteration in small chunks, but because input is
// clustered, final result is always fully aggregated.
class AggregateClustersCursor : public BasicCursor {
 public:
  // Creates cursor.
  static FailureOrOwned<Cursor> Create(
      const BoundSingleSourceProjector* group_by,
      Aggregator* aggregator,
      BufferAllocator* allocator,
      rowcount_t block_size,
      Cursor* child);

  virtual ResultView Next(rowcount_t max_row_count);

  virtual bool IsWaitingOnBarrierSupported() const {
    return child_.is_waiting_on_barrier_supported();
  }

  virtual void Interrupt() { child_.Interrupt(); }

  virtual void ApplyToChildren(CursorTransformer* transformer) {
    child_.ApplyToCursor(transformer);
  }

  virtual CursorId GetCursorId() const { return AGGREGATE_CLUSTERS; }

 private:
  // Takes ownership of key_set, aggregator and child.
  AggregateClustersCursor(const TupleSchema& result_schema,
                          AggregateClustersKeySet* key_set,
                          Aggregator* aggregator,
                          Cursor* child);

  // Aggregates part of the input. Sets result_ to iterate over the
  // aggregation result.
  FailureOrVoid ProcessInput();

  // The input.
  CursorIterator child_;

  std::unique_ptr<AggregateClustersKeySet> key_set_;
  std::unique_ptr<Aggregator> aggregator_;

  // Iterates over a result of last call to ProcessInput. When result_.next()
  // returns false and result_.is_eos() is true and input_exhausted() is false,
  // ProcessInput needs to be called again to prepare next part of a result and
  // re-initialize result_ to iterate over it.
  ViewIterator result_;

  bool input_exhausted_;
  // Stores input rows that were discarded in the previous call to ProcessInput
  // and need to be reprocessed in the next call.
  View view_to_process_;
  rowcount_t result_block_half_capacity_;
  DISALLOW_COPY_AND_ASSIGN(AggregateClustersCursor);
};

AggregateClustersCursor::AggregateClustersCursor(
    const TupleSchema& result_schema,
    AggregateClustersKeySet* key_set,
    Aggregator* aggregator,
    Cursor* child)
    : BasicCursor(result_schema),
      child_(child),
      key_set_(key_set),
      aggregator_(aggregator),
      result_(result_schema),
      input_exhausted_(false),
      view_to_process_(child->schema()),
      result_block_half_capacity_(
          key_set->max_view_row_count_to_insert() / 2) {
  // If aggregator reallocation is needed, we have to do it once and before
  // attaching columns to my_view().
  if (aggregator_->capacity() < key_set->capacity()) {
    aggregator_->Reallocate(key_set->capacity());
  }
  // Set my_view to point to aggregation result.
  int key_size = key_set_->key_schema().attribute_count();
  for (int i = 0; i < result_schema.attribute_count(); ++i) {
    bool_const_ptr is_null;
    VariantConstPointer block_data;

    if (i < key_size) {
      const Column& key_column(key_set_->key_view().column(i));
      is_null = key_column.is_null();
      block_data = key_column.data();
    } else {
      const Column& aggregated_column(
          aggregator_->data().column(i - key_size));
      is_null = aggregated_column.is_null();
      block_data = aggregated_column.data();
    }
    my_view()->mutable_column(i)->Reset(block_data, is_null);
  }
}

// Processes input until at least half of a small output block is filled
// (default size of the output block is 2 * Cursor::kDefaultRowCount). Because
// input is clustered, the result of a processing of a part of the input is
// always correct with the exception of the last result row. The last row might
// not be fully aggregated (part of the input that is not yet processed may
// still contain rows with equal key). Because of this, the processor always
// discards the last result row and reprocesses a part of a view that produces
// this row in the next call to ProcessInput().
FailureOrVoid AggregateClustersCursor::ProcessInput() {
  bool result_block_half_full = false;
  // Skip resets if last ProcessInput() ended prematurely due to a barrier
  // in the input.
  if (!child_.is_waiting_on_barrier()) {
    key_set_->Reset();
    aggregator_->Reset();
  }

  // Input is processed until the result block has at least
  // result_block_half_capacity_ rows or eos was reached or error occured.
  while (true) {
    bool all_equal = false;
    if (view_to_process_.row_count() == 0) {
      if (!PREDICT_TRUE(child_.Next(result_block_half_capacity_, false))) {
        // It seems safer to Clone() when propagating a stored exception.
        // (OTOH, if Cursor::Next() results in failure, it probably shouldn't be
        // called again.)
        if (child_.is_failure()) {
          DCHECK(!child_.is_waiting_on_barrier());
          THROW(child_.exception().Clone());
        }
        if (!child_.has_data()) {
          if (child_.is_eos()) {
            input_exhausted_ = true;
          } else {
            DCHECK(child_.is_waiting_on_barrier());
            return Success();
          }
        }
        break;
      }
      view_to_process_.ResetFrom(child_.view());
    } else {
      all_equal = true;
    }

    const rowid_t* result_index_map =
        key_set_->Insert(view_to_process_, all_equal);
    if (result_index_map == NULL) {
      THROW(new Exception(
          ERROR_MEMORY_EXCEEDED,
          "AggregateClustersCursor memory exceeded. Not enough memory to "
          "group variable-length type elements."));
    }
    const rowcount_t original_view_size = view_to_process_.row_count();
    rowcount_t rows_to_reprocess = 0;

    // If the last result row position in the output block is larger then
    // result_block_half_capacity_ the row needs to be discarded. Part of the
    // input view that produced it is saved for reprocessing in the next call to
    // ProcessInput().
    if (result_index_map[original_view_size - 1]
        >= result_block_half_capacity_) {
      result_block_half_full = true;
      // Find all rows in the input view that produced last result row.
      do {
        ++rows_to_reprocess;
      } while (
          rows_to_reprocess != original_view_size &&
          result_index_map[original_view_size - 1] ==
          result_index_map[original_view_size - rows_to_reprocess - 1]);
      view_to_process_.set_row_count(original_view_size - rows_to_reprocess);
    }

    if (view_to_process_.row_count() != 0) {
      DCHECK_GT(aggregator_->capacity(),
                result_index_map[view_to_process_.row_count() - 1]);
      PROPAGATE_ON_FAILURE(aggregator_->UpdateAggregations(view_to_process_,
                                                           result_index_map));
    }

    if (result_block_half_full) {
      // Save rows that need to be reprocessed and terminate.
      view_to_process_.Advance(original_view_size - rows_to_reprocess);
      view_to_process_.set_row_count(rows_to_reprocess);
      break;
    } else {
      view_to_process_.set_row_count(0);
    }
  }

  if (result_block_half_full) {
    // Discard the last row.
    my_view()->set_row_count(key_set_->size() - 1);
  } else {
    my_view()->set_row_count(key_set_->size());
  }
  result_.reset(*my_view());
  return Success();
}

ResultView AggregateClustersCursor::Next(rowcount_t max_row_count) {
  while (true) {
    if (result_.next(max_row_count)) {
      return ResultView::Success(&result_.view());
    }
    if (input_exhausted_) return ResultView::EOS();
    // No rows from this call, yet input not exhausted. Retry.
    PROPAGATE_ON_FAILURE(ProcessInput());
    if (child_.is_waiting_on_barrier()) return ResultView::WaitingOnBarrier();
  }
}

FailureOrOwned<Cursor> AggregateClustersCursor::Create(
    const BoundSingleSourceProjector* group_by,
    Aggregator* aggregator,
    BufferAllocator* allocator,
    rowcount_t block_size,
    Cursor* child) {
  CHECK(allocator != NULL);
  CHECK_GT(block_size, 0);
  std::unique_ptr<Cursor> child_owner(child);
  std::unique_ptr<Aggregator> aggregator_owner(aggregator);

  FailureOrOwned<AggregateClustersKeySet> key_row_set =
      AggregateClustersKeySet::Create(group_by, allocator, block_size);
  PROPAGATE_ON_FAILURE(key_row_set);
  // Make sure that the allocator buffer has enough rows.
  if (aggregator->capacity() < block_size) {
    if (!aggregator->Reallocate(block_size)) {
      THROW(new Exception(
          ERROR_MEMORY_EXCEEDED,
          StrCat("Can't allocate the aggregator buffer of size ",
                 key_row_set->capacity())));
    }
  }
  if (!TupleSchema::CanMerge(key_row_set->key_schema(),
                             aggregator_owner->schema())) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_EXISTS,
        "Incorrect aggregation specification. Result of aggregation can not "
        "be stored in column on which grouping is done."));
  }
  TupleSchema result_schema = TupleSchema::Merge(
      key_row_set->key_schema(),
      aggregator_owner->schema());

  return Success(new AggregateClustersCursor(
      result_schema, key_row_set.release(),
      aggregator_owner.release(), child_owner.release()));
}

namespace {

class AggregateClustersOperation : public BasicOperation {
 public:
  // Takes ownership of SingleSourceProjector, AggregationSpecification and
  // child_operation.
  AggregateClustersOperation(
      const SingleSourceProjector* clustered_by_columns,
      const AggregationSpecification* aggregation_specification,
      int64 block_size,
      Operation* child)
      : BasicOperation(child),
        group_by_(clustered_by_columns),
        aggregation_specification_(aggregation_specification),
        block_size_(block_size) {}

  virtual ~AggregateClustersOperation() {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> child_cursor = child()->CreateCursor();
    PROPAGATE_ON_FAILURE(child_cursor);
    FailureOrOwned<Aggregator> aggregator = Aggregator::Create(
        *aggregation_specification_, child_cursor->schema(),
        buffer_allocator(), block_size_);
    PROPAGATE_ON_FAILURE(aggregator);
    FailureOrOwned<const BoundSingleSourceProjector> bound_group_by =
        group_by_->Bind(child_cursor->schema());
    PROPAGATE_ON_FAILURE(bound_group_by);
    return AggregateClustersCursor::Create(bound_group_by.release(),
                                           aggregator.release(),
                                           buffer_allocator(),
                                           block_size_,
                                           child_cursor.release());
  }

 private:
  std::unique_ptr<const SingleSourceProjector> group_by_;
  std::unique_ptr<const AggregationSpecification> aggregation_specification_;
  rowcount_t block_size_;
  DISALLOW_COPY_AND_ASSIGN(AggregateClustersOperation);
};

}  // namespace

Operation* AggregateClusters(
    const SingleSourceProjector* group_by,
    const AggregationSpecification* aggregation,
    Operation* child) {
  return new AggregateClustersOperation(group_by, aggregation,
                                        2 * Cursor::kDefaultRowCount,
                                        child);
}

Operation* AggregateClustersWithSpecifiedOutputBlockSize(
    const SingleSourceProjector* group_by,
    const AggregationSpecification* aggregation,
    rowcount_t block_size,
    Operation* child) {
  return new AggregateClustersOperation(group_by, aggregation,
                                        block_size,
                                        child);
}

FailureOrOwned<Cursor> BoundAggregateClusters(
    const BoundSingleSourceProjector* group_by,
    Aggregator* aggregator,
    BufferAllocator* allocator,
    Cursor* child) {
  return AggregateClustersCursor::Create(
      group_by,
      aggregator,
      allocator,
      2 * Cursor::kDefaultRowCount,
      child);
}

}  // namespace supersonic
