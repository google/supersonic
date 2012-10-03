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

#include <stddef.h>
#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
#include <list>
using std::list;
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/port.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/aggregator.h"
#include "supersonic/cursor/core/hybrid_group_utils.h"
#include "supersonic/cursor/core/sort.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/cursor/infrastructure/row_hash_set.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/container_literal.h"
#include "supersonic/utils/map-util.h"
#include "supersonic/utils/pointer_vector.h"
#include "supersonic/utils/stl_util.h"

namespace supersonic {

class TupleSchema;

namespace {

// Creates and updates a block of unique keys that are the result of grouping.
class GroupKeySet {
 public:
  typedef row_hash_set::FindResult FindResult;  // Re-exporting.

  // Creates a GroupKeySet. group_by_columns describes which columns constitute
  // a key and should be grouped together, it can be empty, in which case all
  // rows are considered equal and are grouped together. Set is pre-allocated
  // to store initial_row_capacity unique keys, and it can grow as needed.
  static FailureOrOwned<GroupKeySet> Create(
      const BoundSingleSourceProjector* group_by,
      BufferAllocator* allocator,
      rowcount_t initial_row_capacity) {
    scoped_ptr<GroupKeySet> group_key_set(new GroupKeySet(group_by, allocator));
    PROPAGATE_ON_FAILURE(group_key_set->Init(initial_row_capacity));
    return Success(group_key_set.release());
  }

  const TupleSchema& key_schema() const {
    return key_projector_->result_schema();
  }

  // View on a block that keeps unique keys. Can be called only when key is not
  // empty.
  const View& key_view() const {
    return key_row_set_.indexed_view();
  }

  // How many rows can a view passed in the next call to Insert() have.
  // TODO(user): Remove this limitation (the row hash set could use a loop
  // to insert more items that it can process in a single shot).
  rowcount_t max_view_row_count_to_insert() const {
    // Does not accept views larger then cursor::kDefaultRowCount because this
    // is the size of preallocated table that holds result of Insert().
    return Cursor::kDefaultRowCount;
  }

  // Count of unique keys in the set.
  rowcount_t size() const { return key_row_set_.size(); }

  // Inserts all unique keys from the view to the key block. For each row
  // from input view finds an index of its key in the key block and puts
  // these indexes in the result table.
  // Input view can not have more rows then max_view_row_count_to_insert().
  const rowid_t Insert(const View& view, FindResult* result) {
    CHECK_LE(view.row_count(), max_view_row_count_to_insert());
    key_projector_->Project(view, &child_key_view_);
    child_key_view_.set_row_count(view.row_count());
    return key_row_set_.Insert(child_key_view_, result);
  }

  void Reset() { key_row_set_.Clear(); }

  void Compact() { key_row_set_.Compact(); }

 private:
  GroupKeySet(const BoundSingleSourceProjector* group_by,
              BufferAllocator* allocator)
      : key_projector_(group_by),
        child_key_view_(key_projector_->result_schema()),
        key_row_set_(key_projector_->result_schema(), allocator) {}

  FailureOrVoid Init(rowcount_t reserved_capacity) {
    if (!key_row_set_.ReserveRowCapacity(reserved_capacity)) {
      THROW(new Exception(
          ERROR_MEMORY_EXCEEDED,
          StrCat("Block allocation failed. Key block with capacity ",
                 reserved_capacity, " not allocated.")));
    }
    return Success();
  }

  scoped_ptr<const BoundSingleSourceProjector> key_projector_;

  // View over an input view from child but with only key columns.
  View child_key_view_;

  row_hash_set::RowHashSet key_row_set_;

  DISALLOW_COPY_AND_ASSIGN(GroupKeySet);
};

// Cursor that is used for handling the standard GroupAggregate mode and
// BestEffortGroupAggregate mode. The difference between these two modes is that
// GroupAggregate needs to process the whole input during creation (returns out
// of memory error if aggregation result is too large) and
// BestEffortGroupAggregate does not process anything during creation and
// processes as large chunks as possible during iteration phase, but does not
// guarantee that the final result will be fully aggregated (i.e. there can be
// more than one output for a given key). To make BestEffortGroupAggregate
// deterministic (always producing the same output), pass GuaranteeMemory as its
// allocator.
class GroupAggregateCursor : public BasicCursor {
 public:
  // Creates the cursor. Returns immediately (w/o processing any input).
  static FailureOrOwned<GroupAggregateCursor> Create(
      const BoundSingleSourceProjector* group_by,
      Aggregator* aggregator,
      BufferAllocator* allocator,           // Takes ownership
      BufferAllocator* original_allocator,  // Doesn't take ownership.
      bool best_effort,
      Cursor* child) {
    scoped_ptr<BufferAllocator> allocator_owned(CHECK_NOTNULL(allocator));
    scoped_ptr<Cursor> child_owner(child);
    scoped_ptr<Aggregator> aggregator_owner(aggregator);
    FailureOrOwned<GroupKeySet> key = GroupKeySet::Create(
         group_by, allocator_owned.get(), aggregator_owner->capacity());
    PROPAGATE_ON_FAILURE(key);
    vector<const TupleSchema*> input_schemas(2);
    input_schemas[0] = &key->key_schema();
    input_schemas[1] = &aggregator->schema();
    scoped_ptr<MultiSourceProjector> result_projector(
        (new CompoundMultiSourceProjector())
            ->add(0, ProjectAllAttributes())
            ->add(1, ProjectAllAttributes()));
    FailureOrOwned<const BoundMultiSourceProjector> bound_result_projector(
        result_projector->Bind(input_schemas));
    PROPAGATE_ON_FAILURE(bound_result_projector);
    TupleSchema result_schema = bound_result_projector->result_schema();
    return Success(
        new GroupAggregateCursor(result_schema,
                                 allocator_owned.release(),  // Takes ownership.
                                 original_allocator,
                                 key.release(),
                                 aggregator_owner.release(),
                                 bound_result_projector.release(),
                                 best_effort,
                                 child_owner.release()));
  }

  virtual ResultView Next(rowcount_t max_row_count) {
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

  // If false, the Cursor will not return more data above what was already
  // returned from Next() calls (unless TruncateResultView is called). This
  // method can be used to determine if best-effort group managed to do full
  // grouping:
  // - Call .Next(numeric_limits<rowcount_t>::max())
  // - Now if CanReturnMoreData() == false, we know that all the results of
  // best-effort group are in a single view, which means that the data was fully
  // aggregated.
  // - TruncateResultView can be used to rewind the cursor to the beginning.
  bool CanReturnMoreData() const {
    return !input_exhausted_ ||
        (result_.rows_remaining() > result_.row_count());
  }

  // Truncates the current result ViewIterator. If we only called Next() once,
  // this rewinds the Cursor to the beginning.
  bool TruncateResultView() {
    return result_.truncate(0);
  }

  virtual bool IsWaitingOnBarrierSupported() const {
    return child_.is_waiting_on_barrier_supported();
  }

  virtual void Interrupt() { child_.Interrupt(); }

  virtual void ApplyToChildren(CursorTransformer* transformer) {
    child_.ApplyToCursor(transformer);
  }

  virtual CursorId GetCursorId() const {
    return best_effort_
           ? BEST_EFFORT_GROUP_AGGREGATE
           : GROUP_AGGREGATE;
  }

 private:
  // Takes ownership of the allocator, key, aggregator, and child.
  GroupAggregateCursor(const TupleSchema& result_schema,
                       BufferAllocator* allocator,
                       BufferAllocator* original_allocator,
                       GroupKeySet* key,
                       Aggregator* aggregator,
                       const BoundMultiSourceProjector* result_projector,
                       bool best_effort,
                       Cursor* child)
      : BasicCursor(result_schema),
        allocator_(allocator),
        original_allocator_(CHECK_NOTNULL(original_allocator)),
        child_(child),
        key_(key),
        aggregator_(aggregator),
        result_(result_schema),
        result_projector_(result_projector),
        inserted_keys_(Cursor::kDefaultRowCount),
        best_effort_(best_effort),
        input_exhausted_(false),
        reset_aggregator_in_processinput_(false) {}

  // Process as many rows from input as can fit into result block. If after the
  // first call to ProcessInput() input_exhausted_ is true, the result is fully
  // aggregated (there are no rows with equal group by columns).
  // Initializes the result_ to iterate over the aggregation result.
  FailureOrVoid ProcessInput();

  // Owned allocator used to allocate the memory.
  // NOTE: it is used by other member objects created by GroupAggregateCursor so
  // it has to be destroyed last. Keep it as the first class member.
  scoped_ptr<const BufferAllocator> allocator_;
  // Non-owned allocator used to check whether we can allocate more memory or
  // not.
  const BufferAllocator* original_allocator_;

  // The input.
  CursorIterator child_;

  // Holds key columns of the result. Wrapper around RowHashSet.
  scoped_ptr<GroupKeySet> key_;

  // Holds 'aggregated' columns of the result.
  scoped_ptr<Aggregator> aggregator_;

  // Iterates over a result of last call to ProcessInput. If
  // cursor_over_result_->Next() returns EOS and input_exhausted() is false,
  // ProcessInput needs to be called again to prepare next part of a result and
  // set cursor_over_result_ to iterate over it.
  ViewIterator result_;

  // Projector to combine key & aggregated columns into the result.
  scoped_ptr<const BoundMultiSourceProjector> result_projector_;

  GroupKeySet::FindResult inserted_keys_;

  // If true, OOM is not fatal; the data aggregated up-to OOM are emitted,
  // and the aggregation starts anew.
  bool best_effort_;

  // Set when EOS reached in the input stream.
  bool input_exhausted_;

  // To track when ProcessInput() should reset key_ and aggregator_. It
  // shouldn't be done after exiting with WAITING_ON_BARRIER - some data might
  // lost. Reset is also not needed in first ProcessInput() call.
  bool reset_aggregator_in_processinput_;

  DISALLOW_COPY_AND_ASSIGN(GroupAggregateCursor);
};

FailureOrVoid GroupAggregateCursor::ProcessInput() {
  if (reset_aggregator_in_processinput_) {
    reset_aggregator_in_processinput_ = false;
    key_->Reset();
    // Compacting GroupKeySet to release more memory. This is a workaround for
    // having (allocator_->Available() == 0) constantly, which would allow to
    // only process one block of input data per call to ProcessInput(). However,
    // freeing the underlying datastructures and building them from scratch many
    // times can be inefficient.
    // TODO(user): Implement a less aggressive solution to this problem.
    key_->Compact();
    aggregator_->Reset();
  }
  rowcount_t row_count = key_->size();

  // Process the input while not exhausted and memory quota not exceeded.
  // (But, test the condition at the end of the loop, to guarantee that we
  // process at least one chunk of the input data).
  do {
    // Fetch next block from the input.
    if (!PREDICT_TRUE(child_.Next(Cursor::kDefaultRowCount, false))) {
      PROPAGATE_ON_FAILURE(child_);
      if (!child_.has_data()) {
        if (child_.is_eos()) {
          input_exhausted_ = true;
        } else {
          DCHECK(child_.is_waiting_on_barrier());
          DCHECK(!reset_aggregator_in_processinput_);
          return Success();
        }
      }
      break;
    }
    // Add new rows to the key set.
    child_.truncate(key_->Insert(child_.view(), &inserted_keys_));
    if (child_.view().row_count() == 0) {
      // Failed to add any new keys.
      break;
    }
    row_count = key_->size();
    if (aggregator_->capacity() < row_count) {
      // Not enough space to hold the aggregate columns; need to reallocate.
      // But, if already over quota, give up.
      if (allocator_->Available() == 0) {
        // Rewind the input and ignore trailing rows.
        row_count = aggregator_->capacity();
        for (rowid_t i = 0; i < child_.view().row_count(); ++i) {
          if (inserted_keys_.row_ids()[i] >= row_count) {
            child_.truncate(i);
            break;
          }
        }
      } else {
        if (original_allocator_->Available() < allocator_->Available()) {
          THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                              "Underlying allocator ran out of memory."));
        }
        // Still have spare memory; reallocate.
        rowcount_t requested_capacity = std::max(2 * aggregator_->capacity(),
                                                 row_count);
        if (!aggregator_->Reallocate(requested_capacity)) {
          // OOM when trying to make more room for aggregate columns. Rewind the
          // last input block, so that it is re-fetched by the next
          // ProcessInput, and break out of the loop, returning what we have up
          // to now.
          row_count = aggregator_->capacity();
          child_.truncate(0);
          break;
        }
      }
    }
    // Update the aggregate columns w/ new rows.
    PROPAGATE_ON_FAILURE(
        aggregator_->UpdateAggregations(child_.view(),
                                        inserted_keys_.row_ids()));
  } while (allocator_->Available() > 0);
  if (!input_exhausted_) {
    if (best_effort_) {
      if (row_count == 0) {
        THROW(new Exception(
            ERROR_MEMORY_EXCEEDED,
            StringPrintf(
                "In best-effort mode, failed to process even a single row. "
                "Memory free: %zd",
                allocator_->Available())));
      }
    } else {  // Non-best-effort.
      THROW(new Exception(
          ERROR_MEMORY_EXCEEDED,
          StringPrintf(
              "Failed to process the entire input. "
              "Memory free: %zd",
              allocator_->Available())));
    }
  }
  const View* views[] = { &key_->key_view(), &aggregator_->data() };
  result_projector_->Project(&views[0], &views[2], my_view());
  my_view()->set_row_count(row_count);
  result_.reset(*my_view());
  reset_aggregator_in_processinput_ = true;
  return Success();
}

class GroupAggregateOperation : public BasicOperation {
 public:
  // Takes ownership of SingleSourceProjector, AggregationSpecification and
  // child_operation.
  GroupAggregateOperation(const SingleSourceProjector* group_by,
                          const AggregationSpecification* aggregation,
                          GroupAggregateOptions* options,
                          bool best_effort,
                          Operation* child)
      : BasicOperation(child),
        group_by_(group_by),
        aggregation_specification_(aggregation),
        best_effort_(best_effort),
        options_(options != NULL ? options : new GroupAggregateOptions()) {}

  virtual ~GroupAggregateOperation() {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> child_cursor = child()->CreateCursor();
    PROPAGATE_ON_FAILURE(child_cursor);

    BufferAllocator* original_allocator = buffer_allocator();
    scoped_ptr<BufferAllocator> allocator;
    if (options_->enforce_quota()) {
      allocator.reset(new GuaranteeMemory(options_->memory_quota(),
                                          original_allocator));
    } else {
      allocator.reset(new MemoryLimit(
          options_->memory_quota(), false, original_allocator));
    }
    FailureOrOwned<Aggregator> aggregator = Aggregator::Create(
        *aggregation_specification_, child_cursor->schema(),
        allocator.get(),
        options_->estimated_result_row_count());
    PROPAGATE_ON_FAILURE(aggregator);
    FailureOrOwned<const BoundSingleSourceProjector> bound_group_by =
        group_by_->Bind(child_cursor->schema());
    PROPAGATE_ON_FAILURE(bound_group_by);
    return BoundGroupAggregate(
        bound_group_by.release(), aggregator.release(),
        allocator.release(),
        original_allocator,
        best_effort_,
        child_cursor.release());
  }

 private:
  scoped_ptr<const SingleSourceProjector> group_by_;
  scoped_ptr<const AggregationSpecification> aggregation_specification_;
  const bool best_effort_;
  scoped_ptr<GroupAggregateOptions> options_;
  DISALLOW_COPY_AND_ASSIGN(GroupAggregateOperation);
};

// Hybrid group implementation classes and functions.

// Hybrid group aggregate uses disk-based sorting to allow processing more data
// than would fit in memory.
//
// The key steps of the algorithm are:
// - DISTINCT aggregation elimination
// - preaggregation with best-effort GroupAggregate
// - sorting the preaggregated data
// - combining preaggregated rows on duplicate keys
// - final aggregation using AggregateClusters.
//
// The motivation for eliminating DISINCT aggregations is threefold:
// - partial results of DISTINCT aggregations can't be easily combined,
// - building a whole set of unique values of an attribute for a single key can
//   take arbitrary amount of memory,
// - existing code for computing DISTINCT aggregations doesn't track the memory
//   needed for tracking sets of unique values,
// Here DISTINCT aggregations are eliminated by removing duplicate values in
// DISTINCT aggregated columns, so these aggregations can then be computed with
// ordinary, non-DISTINCT aggregating functions. This is done by adding DISTINCT
// aggregated columns to the preaggregation key. To get unique values for each
// DISTINCT aggregated column, each column needs to be processed separately. It
// is achieved by transforming the input in such a way that each DISTINCT
// aggregated column gets its copy of rows, where all other distinct aggregated
// columns are set to NULL. This is the job of BoundHybridGroupTransform. The
// approach used here relies heavily on the way NULLs are handled in
// aggregation, most notably:
// - NULLs in grouping key is treated as a distinct value,
// - except for COUNT(*) aggregating functions ignore NULL input values.
//
// The next step is to preaggregate the (possibly transformed) input data using
// best-effort GroupAggregate on a key extended with any DISTINCT aggregated
// columns. This is done to reduce the amount of data that will need to be
// sorted before final aggregation. Also, when there are no DISTINCT
// aggregations best-effort preaggregation may manage to fully aggregate the
// input - in this case sorting and final aggregation is not needed (which
// improves performance).
//
// The preaggregated data is then sorted on the extended key and
// AggregateClusters is performed to combine rows with equal keys.  In the last
// step final aggregation is performed with AggregateClusters on the original
// key.

// Analyzes grouping specification and sets up parameters for various stages of
// hybrid group implementation.
class HybridGroupSetup {
 public:
  // Takes ownership of input.
  static FailureOrOwned<const HybridGroupSetup> Create(
      const SingleSourceProjector& group_by_columns,
      const AggregationSpecification& aggregation_specification,
      const TupleSchema& input_schema) {
    scoped_ptr<HybridGroupSetup> setup(new HybridGroupSetup);
    PROPAGATE_ON_FAILURE(setup->Init(group_by_columns,
                                     aggregation_specification,
                                     input_schema));
    return Success(setup.release());
  }

  // For restoring non-nullability of COUNT columns in hybrid group by. We
  // combine partial COUNT results using SUM and SUM's result is nullable.
  // TODO(user): This would be unneccesary if we could specify that SUM's result
  // should be not nullable.
  FailureOrOwned<Cursor> MakeCountColumnsNotNullable(
      Cursor* cursor,
      BufferAllocator* allocator) const {
    CHECK(initialized_);
    vector<string> column_names(columns_to_make_not_nullable_.begin(),
                                columns_to_make_not_nullable_.end());
    return column_names.empty()
        ? Success(cursor)
        : MakeSelectedColumnsNotNullable(ProjectNamedAttributes(column_names),
                                         allocator, cursor);
  }

  // Computes the result schema for HybridGroupFinalAggregationCursor.
  FailureOr<TupleSchema> ComputeResultSchema(
      const TupleSchema& group_by_columns_schema,
      const TupleSchema& aggregator_schema) const {
    CHECK(initialized_);
    FailureOr<TupleSchema> result_schema_bad_nullability =
        TupleSchema::TryMerge(group_by_columns_schema, aggregator_schema);
    PROPAGATE_ON_FAILURE(result_schema_bad_nullability);
    // Fixing nullability of COUNT columns. They may be wrong in
    // final_aggregator->schema() because final_aggregation uses SUM to compute
    // final results of COUNT(column) and SUMS's result is nullable.
    TupleSchema result_schema;
    for (int i = 0; i < result_schema_bad_nullability.get().attribute_count();
         ++i) {
      const Attribute& attribute =
          result_schema_bad_nullability.get().attribute(i);
      result_schema.add_attribute(
          Attribute(attribute.name(),
                    attribute.type(),
                    ContainsKey(columns_to_make_not_nullable_, attribute.name())
                        ? NOT_NULLABLE
                        : attribute.nullability()));
    }
    return Success(result_schema);
  }

  // Returns a cursor with input transformed for hybrid group.
  FailureOrOwned<Cursor> TransformInput(BufferAllocator* allocator,
                                        Cursor* input) const {
    CHECK(initialized_);
    scoped_ptr<Cursor> input_owned(input);
    if (count_star_present_) {
      // Add a column with non-null values for implementing COUNT(*).
      FailureOrOwned<Cursor> input_with_count_star(
          ExtendByConstantColumn(count_star_column_name_, allocator,
                                 input_owned.release()));
      PROPAGATE_ON_FAILURE(input_with_count_star);
      input_owned.reset(input_with_count_star.release());
    }
    return BoundHybridGroupTransform(
        group_by_columns_->Clone(),
        column_group_projectors_,
        allocator,
        input_owned.release());
  }

  const AggregationSpecification& pregroup_aggregation() const {
    CHECK(initialized_);
    return pregroup_aggregation_;
  }
  const AggregationSpecification& pregroup_combine_aggregation() const {
    CHECK(initialized_);
    return pregroup_combine_aggregation_;
  }
  const AggregationSpecification& final_aggregation() const {
    CHECK(initialized_);
    return final_aggregation_;
  }
  const SingleSourceProjector& pregroup_group_by_columns() const {
    CHECK(initialized_);
    return pregroup_group_by_columns_;
  }

  bool has_distinct_aggregations() const {
    CHECK(initialized_);
    return has_distinct_aggregations_;
  }
  const SingleSourceProjector& group_by_columns_by_name() const {
    CHECK(initialized_);
    return *group_by_columns_by_name_;
  }

 private:
  HybridGroupSetup()
      : initialized_(false),
        count_star_column_name_("$hybrid_group_count_star$") {}

  FailureOrVoid Init(
      const SingleSourceProjector& group_by_columns,
      const AggregationSpecification& aggregation_specification,
      const TupleSchema& input_schema) {
    CHECK(!initialized_);
    group_by_columns_.reset(group_by_columns.Clone());
    has_distinct_aggregations_ = false;
    count_star_present_ = false;
    // Hybrid group-by may need to project group-by columns multiple times
    // over transformed inputs, and it wouldn't work if the projector was
    // renaming columns, selecting columns by position or taking all input
    // columns. Because of this, in later stages of processing we use a
    // projector based on result column names of the original projector.
    FailureOrOwned<const SingleSourceProjector> group_by_columns_by_name(
        ProjectUsingProjectorResultNames(group_by_columns, input_schema));
    PROPAGATE_ON_FAILURE(group_by_columns_by_name);
    group_by_columns_by_name_.reset(group_by_columns_by_name.release());
    // pregroup_group_by_columns_ will contain all the original group-by columns
    // plus all the distinct aggregated columns.
    pregroup_group_by_columns_.add(group_by_columns_by_name_->Clone());
    set<string> distinct_columns_set;
    set<string> nondistinct_columns_set;
    CompoundSingleSourceProjector nondistinct_columns;
    for (int i = 0; i < aggregation_specification.size(); ++i) {
      const AggregationSpecification::Element& elem =
          aggregation_specification.aggregation(i);
      const string pregroup_column_prefix =
          elem.is_distinct()
            ? "$hybrid_group_pregroup_column_d$"
            : "$hybrid_group_pregroup_column_nd$";
      const string pregroup_column_name = StrCat(pregroup_column_prefix,
                                                 elem.input());
      AggregationSpecification::Element final_elem(elem);
      if (elem.is_distinct()) {
        has_distinct_aggregations_ = true;
        // <aggregate-function>(DISTINCT col).
        if (elem.input() == "") {
          THROW(new Exception(
              ERROR_ATTRIBUTE_MISSING,
              StringPrintf("Incorrect aggregation specification. Distinct "
                           "aggregation needs input column.")));
        }
        if (InsertIfNotPresent(&distinct_columns_set, elem.input())) {
          column_group_projectors_.push_back(
              ProjectNamedAttributeAs(elem.input(),
                                      pregroup_column_name));
          pregroup_group_by_columns_.add(
              ProjectNamedAttributeAs(pregroup_column_name,
                                      pregroup_column_name));
        }
        // No need to add anything to pregroup_aggregation_.
        final_elem.set_input(pregroup_column_name);
      } else {
        AggregationSpecification::Element pregroup_elem(elem);
        // <aggregate-function>(col) or COUNT(*).
        if (elem.input() != "") {
          if (InsertIfNotPresent(&nondistinct_columns_set, elem.input())) {
            nondistinct_columns.add(
                ProjectNamedAttributeAs(elem.input(),
                                        pregroup_column_name));
          }
          pregroup_elem.set_input(pregroup_column_name);
        } else {
          count_star_present_ = true;
          pregroup_elem.set_input(count_star_column_name_);
        }
        final_elem.set_input(pregroup_elem.output());
        if (elem.aggregation_operator() == COUNT) {
          columns_to_make_not_nullable_.insert(final_elem.output());
          final_elem.set_aggregation_operator(SUM);
        }
        AggregationSpecification::Element pregroup_combine_elem(final_elem);
        pregroup_combine_elem.set_output(pregroup_combine_elem.input());
        pregroup_aggregation_.add(pregroup_elem);
        pregroup_combine_aggregation_.add(pregroup_combine_elem);
      }
      final_aggregation_.add(final_elem);
    }
    if (count_star_present_) {
      // Add a column with non-null values for implementing COUNT(*).
      nondistinct_columns.add(
          ProjectNamedAttributeAs(count_star_column_name_,
                                  count_star_column_name_));
    }
    if (!nondistinct_columns_set.empty() || count_star_present_ ||
        column_group_projectors_.empty()) {
      column_group_projectors_.push_back(nondistinct_columns.Clone());
    }
    initialized_ = true;
    return Success();
  }

  static FailureOrOwned<const SingleSourceProjector>
      ProjectUsingProjectorResultNames(const SingleSourceProjector& projector,
                                       const TupleSchema& schema) {
    FailureOrOwned<const BoundSingleSourceProjector> bound_projector(
        projector.Bind(schema));
    PROPAGATE_ON_FAILURE(bound_projector);
    const TupleSchema& result_schema = bound_projector->result_schema();
    vector<string> result_names;
    for (int i = 0; i < result_schema.attribute_count(); ++i) {
      result_names.push_back(result_schema.attribute(i).name());
    }
    return Success(ProjectNamedAttributes(result_names));
  }

 private:
  // Was the object initialized successfully with Init(...).
  bool initialized_;

  // Does the AggregationSpecification contain DISTINCT aggregations?
  bool has_distinct_aggregations_;

  // Does the AggregationSpecification contain COUNT(*)?
  bool count_star_present_;

  // The name of the column added for COUNT(*) implementation.
  const string count_star_column_name_;

  // The original group by columns projector.
  scoped_ptr<const SingleSourceProjector> group_by_columns_;

  // A safe projector for use in later processing, based on the result
  // names of the original group_by_columns_ projector.
  scoped_ptr<const SingleSourceProjector> group_by_columns_by_name_;

  // A set of COUNT columns (names) that will need nullability fixing because
  // the implementation uses SUM on later (post pregroup) stages.
  set<string> columns_to_make_not_nullable_;

  // A set of projectors that define column groups for the
  // HybridGroupTransformCursor. There's a separate projector for each column
  // that has DISTINCT aggregations on it, and a shared projector for all
  // columns that have non-DISTINCT aggregations on them.
  util::gtl::PointerVector<const SingleSourceProjector>
      column_group_projectors_;

  // Group-by columns for the pregroup aggregation (will additionally contain
  // distinct aggregated columns).
  CompoundSingleSourceProjector pregroup_group_by_columns_;

  // The initial (pregroup) aggregation.
  AggregationSpecification pregroup_aggregation_;

  // Aggregation for combining results of pregroup for duplicated keys.
  AggregationSpecification pregroup_combine_aggregation_;

  // Final aggregation combining results of pregroup aggregation for
  // non-DISTINCT aggregations and using ordinary, non-DISTINCT aggregations in
  // place of original DISTINCT aggregations.
  AggregationSpecification final_aggregation_;

  DISALLOW_COPY_AND_ASSIGN(HybridGroupSetup);
};

// Combines preaggregated results on repeated (extended) keys and the final
// aggregation to compute the results for DISTINCT aggregations.
class HybridGroupFinalAggregationCursor : public BasicCursor {
 public:
  // Takes ownership of hybrid_group_setup, final_aggregator,
  // final_group_by_columns and pregroup_cursor.
  HybridGroupFinalAggregationCursor(
      const TupleSchema& result_schema,
      const HybridGroupSetup* hybrid_group_setup,
      Aggregator* final_aggregator,
      const BoundSingleSourceProjector* final_group_by_columns,
      StringPiece temporary_directory_prefix,
      BufferAllocator* allocator,
      GroupAggregateCursor* pregroup_cursor)
      : BasicCursor(result_schema),
        is_waiting_on_barrier_supported_(
            pregroup_cursor->IsWaitingOnBarrierSupported()),
        hybrid_group_setup_(hybrid_group_setup),
        final_aggregator_(final_aggregator),
        final_group_by_columns_(final_group_by_columns),
        temporary_directory_prefix_(temporary_directory_prefix.ToString()),
        allocator_(allocator),
        pregroup_cursor_(pregroup_cursor) {
  }

  virtual ResultView Next(rowcount_t max_row_count) {
    if (result_cursor_ == NULL && sorter_ == NULL) {
      ResultView first_result = pregroup_cursor_->Next(
          numeric_limits<rowcount_t>::max());
      if (first_result.is_eos() || !first_result.has_data()) {
        return first_result;
      }
      if (!pregroup_cursor_->CanReturnMoreData() &&
          !hybrid_group_setup_->has_distinct_aggregations()) {
        pregroup_cursor_->TruncateResultView();
        // There are no distinct aggregations, so if pregroup fully aggregated
        // all the data (a single output block), its result can be returned as
        // final result.
        LOG(INFO) << "HybridGroupAggregate not using disk.";
        PROPAGATE_ON_FAILURE(SetResultCursor(pregroup_cursor_.release()));
      } else {
        // Pregroup best-effort didn't fully aggregate the data. Sorting data to
        // combine duplicate pregroup keys. The final aggregation will be
        // performed using AggregateClusters, as the data will also be sorted on
        // the original key.
        if (first_result.has_data()) {
          pregroup_cursor_->TruncateResultView();
        }
        LOG(INFO) << "HybridGroupAggregate using disk ("
                  << (hybrid_group_setup_->has_distinct_aggregations()
                      ? "distinct aggregations" : "best-effort failed")
                  << ").";
        FailureOrOwned<const BoundSingleSourceProjector> bound_group_by_columns(
            hybrid_group_setup_->pregroup_group_by_columns().Bind(
                pregroup_cursor_->schema()));
        PROPAGATE_ON_FAILURE(bound_group_by_columns);
        sorter_.reset(CreateUnbufferedSorter(
            pregroup_cursor_->schema(),
            new BoundSortOrder(bound_group_by_columns.release()),
            temporary_directory_prefix_,
            allocator_));
      }
    }
    if (result_cursor_ == NULL && sorter_ != NULL) {
      while (true) {
        ResultView result = pregroup_cursor_->Next(
            numeric_limits<rowcount_t>::max());
        PROPAGATE_ON_FAILURE(result);
        if (result.is_waiting_on_barrier()) {
          return ResultView::WaitingOnBarrier();
        }
        if (result.is_eos()) {
          break;
        }
        VLOG(1) << "Writing " << result.view().row_count()
                << " rows to UnbufferedSorter.";
        FailureOr<rowcount_t> written = sorter_->Write(result.view());
        PROPAGATE_ON_FAILURE(written);
        CHECK_EQ(written.get(), result.view().row_count());
      }
      // Aggregate to combine duplicated pregroup keys.
      FailureOrOwned<Cursor> sorted = sorter_->GetResultCursor();
      PROPAGATE_ON_FAILURE(sorted);
      sorter_.reset();
      FailureOrOwned<Aggregator> aggregator = Aggregator::Create(
          hybrid_group_setup_->pregroup_combine_aggregation(),
          sorted.get()->schema(),
          allocator_,
          1024);
      PROPAGATE_ON_FAILURE(aggregator);
      FailureOrOwned<const BoundSingleSourceProjector> sorted_group_by_columns =
          hybrid_group_setup_->pregroup_group_by_columns().Bind(
              sorted.get()->schema());
      PROPAGATE_ON_FAILURE(sorted_group_by_columns);
      FailureOrOwned<Cursor> combine_cursor =
          BoundAggregateClusters(
              sorted_group_by_columns.release(),
              aggregator.release(),
              allocator_,
              sorted.release());
      PROPAGATE_ON_FAILURE(combine_cursor);
      // Restoring COUNT column nullability, so we have exactly the same schema
      // that final_aggregator was built for.
      FailureOrOwned<Cursor> combine_cursor_count_nonnullable =
          hybrid_group_setup_->MakeCountColumnsNotNullable(
              combine_cursor.release(), allocator_);
      PROPAGATE_ON_FAILURE(combine_cursor_count_nonnullable);
      // Final aggregation on the original key using AggregateClusters.
      FailureOrOwned<Cursor> aggregated = BoundAggregateClusters(
          final_group_by_columns_.release(),
          final_aggregator_.release(),
          allocator_,
          combine_cursor_count_nonnullable.release());
      PROPAGATE_ON_FAILURE(aggregated);
      PROPAGATE_ON_FAILURE(SetResultCursor(aggregated.release()));
    }
    CHECK(sorter_ == NULL);
    CHECK(result_cursor_ != NULL);
    ResultView result = result_cursor_->Next(max_row_count);
    PROPAGATE_ON_FAILURE(result);
    return result;
  }

  virtual bool IsWaitingOnBarrierSupported() const {
    return is_waiting_on_barrier_supported_;
  }

  virtual void Interrupt() {
    Cursor* pregroup_cursor = pregroup_cursor_.get();
    if (pregroup_cursor != NULL) {
      pregroup_cursor->Interrupt();
    }
    Cursor* result_cursor = result_cursor_.get();
    if (result_cursor != NULL) {
      result_cursor->Interrupt();
    }
  }

  virtual CursorId GetCursorId() const {
    return HYBRID_GROUP_FINAL_AGGREGATION;
  }

  // Runs the transformer recursively on the pregrouping aggregate cursor.
  //
  // This solution effectively omits the transformation of the pregrouping
  // cursor, as it conflicts with the return type of Transform().
  // The hybrid aggregate and pregroup aggregate cursor are treated as one
  // by cursor transformers.
  virtual void ApplyToChildren(CursorTransformer* transformer) {
    pregroup_cursor_->ApplyToChildren(transformer);
  }

 private:
  FailureOrVoid SetResultCursor(Cursor* result_cursor) {
    FailureOrOwned<Cursor> result_cursor_fixed_nullability =
        hybrid_group_setup_->MakeCountColumnsNotNullable(
            result_cursor, allocator_);
    PROPAGATE_ON_FAILURE(result_cursor_fixed_nullability);
    result_cursor_.reset(result_cursor_fixed_nullability.release());
    CHECK(TupleSchema::AreEqual(schema(), result_cursor_->schema(), true))
        << "schema(): "
        << schema().GetHumanReadableSpecification()
        << "\naggregated_owned->schema(): "
        << result_cursor_->schema().GetHumanReadableSpecification();
    return Success();
  }

  bool is_waiting_on_barrier_supported_;
  scoped_ptr<const HybridGroupSetup> hybrid_group_setup_;
  scoped_ptr<Aggregator> final_aggregator_;
  scoped_ptr<const BoundSingleSourceProjector> final_group_by_columns_;
  size_t memory_quota_;
  string temporary_directory_prefix_;
  BufferAllocator* allocator_;
  scoped_ptr<GroupAggregateCursor> pregroup_cursor_;
  scoped_ptr<Sorter> sorter_;
  scoped_ptr<Cursor> result_cursor_;
  DISALLOW_COPY_AND_ASSIGN(HybridGroupFinalAggregationCursor);
};

}  // namespace

Operation* GroupAggregate(
    const SingleSourceProjector* group_by,
    const AggregationSpecification* aggregation,
    GroupAggregateOptions* options,
    Operation* child) {
  return new GroupAggregateOperation(group_by, aggregation, options, false,
                                     child);
}

Operation* BestEffortGroupAggregate(
    const SingleSourceProjector* group_by,
    const AggregationSpecification* aggregation,
    GroupAggregateOptions* options,
    Operation* child) {
  return new GroupAggregateOperation(group_by, aggregation, options, true,
                                     child);
}

FailureOrOwned<Cursor> BoundGroupAggregate(
    const BoundSingleSourceProjector* group_by,
        Aggregator* aggregator,
        BufferAllocator* allocator,
        BufferAllocator* original_allocator,
        bool best_effort,
        Cursor* child) {
  FailureOrOwned<GroupAggregateCursor> result =
      GroupAggregateCursor::Create(
          group_by, aggregator, allocator,
          original_allocator == NULL ? allocator : original_allocator,
          best_effort, child);
  PROPAGATE_ON_FAILURE(result);
  return Success(result.release());
}

FailureOrOwned<Cursor> BoundHybridGroupAggregate(
    const SingleSourceProjector* group_by_columns,
    const AggregationSpecification& aggregation_specification,
    StringPiece temporary_directory_prefix,
    BufferAllocator* allocator,
    size_t memory_quota,
    const HybridGroupDebugOptions* debug_options,
    Cursor* child) {
  scoped_ptr<Cursor> child_owner(child);
  scoped_ptr<const SingleSourceProjector> group_by_columns_owner(
      group_by_columns);
  scoped_ptr<const HybridGroupDebugOptions> debug_options_owned(debug_options);
  FailureOrOwned<const HybridGroupSetup> hybrid_group_setup(
      HybridGroupSetup::Create(*group_by_columns_owner,
                               aggregation_specification,
                               child_owner->schema()));
  PROPAGATE_ON_FAILURE(hybrid_group_setup);
  FailureOrOwned<Cursor> transformed_input(
      hybrid_group_setup->TransformInput(allocator, child_owner.release()));
  PROPAGATE_ON_FAILURE(transformed_input);
  if (debug_options_owned != NULL &&
      debug_options_owned->return_transformed_input()) {
    return transformed_input;
  }
  // Use best-effort group aggregate to pregroup the data.
  FailureOrOwned<const BoundSingleSourceProjector> bound_pregroup_columns(
      hybrid_group_setup->pregroup_group_by_columns().Bind(
          transformed_input->schema()));
  PROPAGATE_ON_FAILURE(bound_pregroup_columns);
  // limit_allocator will be owned by 'pregroup_cursor'.
  scoped_ptr<BufferAllocator> limit_allocator(
      new MemoryLimit(memory_quota, false, allocator));

  FailureOrOwned<Aggregator> pregroup_aggregator = Aggregator::Create(
      hybrid_group_setup->pregroup_aggregation(), transformed_input->schema(),
      limit_allocator.get(),
      1024);
  PROPAGATE_ON_FAILURE(pregroup_aggregator);
  FailureOrOwned<GroupAggregateCursor> pregroup_cursor =
      GroupAggregateCursor::Create(
          bound_pregroup_columns.release(),
          pregroup_aggregator.release(),
          limit_allocator.release(),
          allocator,
          true,  // best effort.
          transformed_input.release());
  PROPAGATE_ON_FAILURE(pregroup_cursor);
  // Building final_aggregator and final_group_by_columns to compute the
  // result_schema for HybridGroupFinalAggregationCursor.
  FailureOrOwned<Aggregator> final_aggregator = Aggregator::Create(
      hybrid_group_setup->final_aggregation(),
      pregroup_cursor->schema(),
      allocator,
      1024);
  PROPAGATE_ON_FAILURE(final_aggregator);
  FailureOrOwned<const BoundSingleSourceProjector> final_group_by_columns(
      hybrid_group_setup->group_by_columns_by_name().Bind(
          pregroup_cursor->schema()));
  PROPAGATE_ON_FAILURE(final_group_by_columns);
  // Calculate the schema of the final cursor.
  FailureOr<TupleSchema> result_schema =
      hybrid_group_setup->ComputeResultSchema(
          final_group_by_columns->result_schema(),
          final_aggregator->schema());
  PROPAGATE_ON_FAILURE(result_schema);
  return Success(new HybridGroupFinalAggregationCursor(
      result_schema.get(),
      hybrid_group_setup.release(),
      final_aggregator.release(),
      final_group_by_columns.release(),
      temporary_directory_prefix,
      allocator,
      pregroup_cursor.release()));
}

class HybridGroupAggregateOperation : public BasicOperation {
 public:
  HybridGroupAggregateOperation(
      const SingleSourceProjector* group_by_columns,
      const AggregationSpecification* aggregation_specification,
      size_t memory_quota,
      StringPiece temporary_directory_prefix,
      Operation* child)
      : BasicOperation(child),
        group_by_columns_(group_by_columns),
        aggregation_specification_(aggregation_specification),
        memory_quota_(memory_quota),
        temporary_directory_prefix_(temporary_directory_prefix.ToString()) {}

  virtual ~HybridGroupAggregateOperation() {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> child_cursor = child()->CreateCursor();
    PROPAGATE_ON_FAILURE(child_cursor);
    return BoundHybridGroupAggregate(
        group_by_columns_->Clone(),
        *aggregation_specification_,
        temporary_directory_prefix_,
        buffer_allocator(),
        memory_quota_,
        NULL,
        child_cursor.release());
  }

 private:
  scoped_ptr<const SingleSourceProjector> group_by_columns_;
  scoped_ptr<const AggregationSpecification> aggregation_specification_;
  size_t memory_quota_;
  string temporary_directory_prefix_;
  DISALLOW_COPY_AND_ASSIGN(HybridGroupAggregateOperation);
};

Operation* HybridGroupAggregate(
    const SingleSourceProjector* group_by_columns,
    const AggregationSpecification* aggregation_specification,
    size_t memory_quota,
    StringPiece temporary_directory_prefix,
    Operation* child) {
  return new HybridGroupAggregateOperation(group_by_columns,
                                           aggregation_specification,
                                           memory_quota,
                                           temporary_directory_prefix,
                                           child);
}

}  // namespace supersonic
