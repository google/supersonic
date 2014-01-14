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

#include "supersonic/cursor/core/hash_join.h"

#include <memory>
#include <string>
namespace supersonic {using std::string; }
#include <utility>
#include "supersonic/utils/std_namespace.h"
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/template_util.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/base/lookup_index.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/row_hash_set.h"
#include "supersonic/expression/vector/vector_logic.h"
#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/container_literal.h"
#include "supersonic/utils/iterator_adaptors.h"

// TODO(onufry): Explore optimization opprotunities, eg. not copying key column
// from lookup index twice.
namespace supersonic {

class BufferAllocator;

using row_hash_set::RowHashMultiSet;
using row_hash_set::RowHashSet;

namespace {

// Calculates the combined is_not_null vector, which is the logical AND of the
// negation of the is_null vectors of the individual columns. is_not_null must
// be allocated to hold view.row_count() values.
// TODO(user): Move to a static view method?
void FindNotNullKeys(const View& view, bool_ptr is_not_null) {
  bit_pointer::FillWithTrue(is_not_null, view.row_count());
  for (int c = 0; c < view.column_count(); ++c) {
    bool_const_ptr column_is_null = view.column(c).is_null();
    if (column_is_null != NULL) {
      vector_logic::AndNot(column_is_null, is_not_null,
                           view.row_count(), is_not_null);
    }
  }
}

TupleSchema WithAllColumnsNullable(const TupleSchema& schema) {
  TupleSchema output;
  for (int i = 0; i < schema.attribute_count(); ++i) {
    const Attribute& input_attr(schema.attribute(i));
    output.add_attribute(Attribute(input_attr.name(),
                                   input_attr.type(),
                                   NULLABLE));
  }
  return output;
}

}  // namespace

// A specific implementation of LookupIndex that adapts a Cursor to this
// interface. It does so by materializing the entire cursor in memory and
// building a hash index on top of it.
template <KeyUniqueness key_uniqueness>
class HashIndexOnMaterializedCursor : public LookupIndex {
 public:
  HashIndexOnMaterializedCursor(
      JoinType join_type,
      BufferAllocator* const allocator,
      const BoundSingleSourceProjector* key_selector,
      const TupleSchema& schema);
  FailureOrVoid Init();

  // Not thread-safe as there is only one instance of storage blocks for result.
  virtual FailureOrOwned<LookupIndexCursor> MultiLookup(
      const View* query) const;
  virtual const TupleSchema& schema() const {
    return schema_;
  }
  virtual const BoundSingleSourceProjector& key_selector() const {
    return *key_selector_;
  }

  FailureOr<bool> MaterializeInputAndBuildIndex(Cursor* input);

  bool empty() const { return index_.size() == 0; }

 private:
  // Subclass of LookupIndexCursor returned by MultiLookup; implements
  // matching logic.
  template <JoinType join_type>
  class ResultCursor;

  JoinType join_type_;

  TupleSchema schema_;

  // Selects key columns from input's schema. Ownership is transferred to index.
  const BoundSingleSourceProjector* key_selector_;

  // if key_uniqueness == UNIQUE:
  //     RowHashSetType = row_hash_set::RowHashSet
  // if key_uniqueness == NOT_UNIQUE:
  //     RowHashSetType = row_hash_set::RowHashMultiSet
  typedef typename base::if_<key_uniqueness == UNIQUE,
      RowHashSet, RowHashMultiSet>::type RowHashSetType;

  // Stores materialized input rows and allows row lookups by key.
  RowHashSetType index_;

  // Set to NULL once the index is materialized.
  std::unique_ptr<Cursor> input_;

  // Placeholders for ResultCursor's data (see its doc). Scratchpads allocated
  // here to avoid repeated allocation in each call to MultiLookup().
  mutable Block result_cursor_block_;
  mutable std::unique_ptr<rowid_t[]> result_cursor_query_ids_;

  template <JoinType join_type>
  friend class ResultCursor;
};

template <KeyUniqueness key_uniqueness>
class HashIndexMaterializer : public LookupIndexBuilder {
 public:
  static FailureOrOwned<LookupIndexBuilder> Create(
      Cursor* input,
      JoinType join_type,
      BufferAllocator* const allocator,
      const BoundSingleSourceProjector* key_selector) {
    std::unique_ptr<HashIndexMaterializer> materializer(
        new HashIndexMaterializer(input, join_type, allocator, key_selector));
    PROPAGATE_ON_FAILURE(materializer->Init());
    return Success(materializer.release());
  }

  virtual const TupleSchema& schema() const { return index_->schema(); }

  FailureOrOwned<LookupIndex> Build() {
    CHECK(input_.get() != NULL) << "Already materialized.";
    FailureOr<bool> materialized =
        index_->MaterializeInputAndBuildIndex(input_.get());
    PROPAGATE_ON_FAILURE(materialized);
    if (materialized.get()) {
      input_.reset(NULL);  // Releases resources held by the input.
      return Success(index_.release());
    } else {
      return Success(static_cast<LookupIndex*>(NULL));
    }
  }

  void Interrupt() {
    if (input_.get() != NULL) input_->Interrupt();
  }

  void ApplyToChildren(CursorTransformer* transformer) {
    input_.reset(transformer->Transform(input_.release()));
  }

 private:
  HashIndexMaterializer(
      Cursor* input,
      JoinType join_type,
      BufferAllocator* const allocator,
      const BoundSingleSourceProjector* key_selector)
      : input_(input),
        index_(new HashIndexOnMaterializedCursor<key_uniqueness>(
            join_type, allocator, key_selector, input->schema())) {}

  FailureOrVoid Init() {
    PROPAGATE_ON_FAILURE(index_->Init());
    return Success();
  }

  std::unique_ptr<Cursor> input_;
  std::unique_ptr<HashIndexOnMaterializedCursor<key_uniqueness> > index_;
};

// The actual hash join implementation.
class HashJoinCursor : public Cursor {
 public:
  // Takes ownership of lhs_key_selector.
  HashJoinCursor(
      JoinType join_type,
      BufferAllocator* const allocator,
      const BoundSingleSourceProjector* lhs_key_selector,
      const BoundMultiSourceProjector& result_projector,
      LookupIndexBuilder* rhs,
      Cursor* lhs);
  virtual ~HashJoinCursor() { }

  // Allocates index' internal block and reads input into it.
  FailureOrVoid Init();

  virtual const TupleSchema& schema() const;
  virtual ResultView Next(rowcount_t max_row_count);
  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual void Interrupt() {
    lhs_->Interrupt();
    if (rhs_builder_ != NULL) rhs_builder_->Interrupt();
  }

  virtual void ApplyToChildren(CursorTransformer* transformer) {
    if (rhs_builder_ == NULL) {
      string description;
      AppendDebugDescription(&description);
      LOG(ERROR) << "Operation run by "
                 << description
                 << " is in progress, lookup builder has been "
                 << "destroyed. Aborting transformation.";
      return;
    }
    lhs_.reset(transformer->Transform(lhs_.release()));
    rhs_builder_->ApplyToChildren(transformer);
  }

  virtual void AppendDebugDescription(string* target) const;

  virtual CursorId GetCursorId() const { return HASH_JOIN; }

 private:
  // Helper method that initializes final_result_projector.
  void SetUpFinalResultProjector(
      JoinType join_type,
      const BoundMultiSourceProjector& result_projector);

  JoinType join_type_;
  std::unique_ptr<Cursor> lhs_;
  std::unique_ptr<LookupIndexBuilder> rhs_builder_;
  std::unique_ptr<const LookupIndex> rhs_;
  std::unique_ptr<const BoundSingleSourceProjector> lhs_key_selector_;

  // Projector from lhs_ to lhs_result_, see below.
  const BoundSingleSourceProjector lhs_result_projector_;

  // Placeholder for lhs columns projected into the final result.
  Block lhs_result_;

  // Copies matched rows from lhs input into lhs_result_ wrt its projector.
  SelectiveViewCopier lhs_result_copier_;

  // Projector that sets result_view over final result data from lhs and rhs
  // input - over (lhs_result_, rhs_result.view()) pair; see below.
  std::unique_ptr<BoundMultiSourceProjector> final_result_projector_;

  // Combined view over the final result, that is over
  //    (1) projected lhs columns in lhs_result_ above.
  //    (2) projected rhs columns obtained from the lookup index.
  View result_view_;

  // Placeholder for a reference to currently processed view from lhs input.
  const View* lhs_view_;

  // View over key columns of lhs input rows, used as queries to lookup index.
  View lookup_query_;

  // Cursor returned by rhs_.MultiLookup(). Declared in the member variables'
  // scope as its presence / absence is a part of the Next() iteration state.
  // Specifically, a single call to HashJoinCursor::Next() might not exhaust
  // the cursor of matches and hence the subsequent call should resume where
  // the last left off.
  std::unique_ptr<LookupIndexCursor> matches_;
};


HashJoinOperation::HashJoinOperation(
    JoinType join_type,
    const SingleSourceProjector* lhs_key_selector,
    const SingleSourceProjector* rhs_key_selector,
    const MultiSourceProjector* result_projector,
    KeyUniqueness rhs_key_uniqueness,
    Operation* lhs_child, Operation* rhs_child)
    : BasicOperation(lhs_child, rhs_child),
      join_type_(join_type),
      lhs_key_selector_(lhs_key_selector),
      rhs_key_selector_(rhs_key_selector),
      result_projector_(result_projector),
      rhs_key_uniqueness_(rhs_key_uniqueness) {}

FailureOrOwned<Cursor> HashJoinOperation::CreateCursor() const {
  Operation* const lhs_operation = child_at(0);
  Operation* const rhs_operation = child_at(1);
  std::unique_ptr<LookupIndex> rhs_lookup_index;

  FailureOrOwned<Cursor> provided_lhs_cursor = lhs_operation->CreateCursor();
  PROPAGATE_ON_FAILURE(provided_lhs_cursor);

  FailureOrOwned<Cursor> provided_rhs_cursor = rhs_operation->CreateCursor();
  PROPAGATE_ON_FAILURE(provided_rhs_cursor);

  DCHECK(rhs_key_selector_.get() != NULL);
  FailureOrOwned<const BoundSingleSourceProjector>
      bound_rhs_key_selector_result =
      rhs_key_selector_->Bind(provided_rhs_cursor->schema());
  PROPAGATE_ON_FAILURE(bound_rhs_key_selector_result);

  FailureOrOwned<LookupIndexBuilder> rhs_builder(
      (rhs_key_uniqueness_ == UNIQUE) ?
          CreateHashIndexMaterializer<UNIQUE>(
              join_type_, bound_rhs_key_selector_result.release(),
              provided_rhs_cursor.release()) :
          CreateHashIndexMaterializer<NOT_UNIQUE>(
              join_type_, bound_rhs_key_selector_result.release(),
              provided_rhs_cursor.release()));
  PROPAGATE_ON_FAILURE(rhs_builder);

  FailureOrOwned<const BoundSingleSourceProjector> bound_lhs_key_selector =
      lhs_key_selector_->Bind(provided_lhs_cursor->schema());
  PROPAGATE_ON_FAILURE(bound_lhs_key_selector);

  FailureOrOwned<const BoundMultiSourceProjector> bound_result_projector =
      result_projector_->Bind(util::gtl::Container(
          &provided_lhs_cursor->schema(), &rhs_builder->schema()));
  PROPAGATE_ON_FAILURE(bound_result_projector);

  std::unique_ptr<HashJoinCursor> cursor(new HashJoinCursor(
      join_type_, buffer_allocator(), bound_lhs_key_selector.release(),
      *bound_result_projector, rhs_builder.release(),
      provided_lhs_cursor.release()));
  PROPAGATE_ON_FAILURE(cursor->Init());
  return Success(cursor.release());
}

// TODO(user): Only index columns that are needed in the final join result.
template <KeyUniqueness key_uniqueness>
FailureOrOwned<LookupIndexBuilder>
HashJoinOperation::CreateHashIndexMaterializer(
    JoinType join_type,
    const BoundSingleSourceProjector* bound_rhs_key_selector,
    Cursor* rhs_cursor) const {
  return HashIndexMaterializer<key_uniqueness>::Create(
      rhs_cursor, join_type, buffer_allocator(), bound_rhs_key_selector);
}

HashJoinCursor::HashJoinCursor(
    JoinType join_type,
    BufferAllocator* const allocator,
    const BoundSingleSourceProjector* lhs_key_selector,
    const BoundMultiSourceProjector& result_projector,
    LookupIndexBuilder* rhs,
    Cursor* lhs)
    : join_type_(join_type),
      lhs_(lhs),
      rhs_builder_(rhs),
      lhs_key_selector_(lhs_key_selector),
      lhs_result_projector_(result_projector.GetSingleSourceProjector(0)),
      lhs_result_(lhs_result_projector_.result_schema(), allocator),
      lhs_result_copier_(&lhs_result_projector_, false),
      result_view_(result_projector.result_schema()),
      lhs_view_(NULL),
      lookup_query_(lhs_key_selector_->result_schema()) {
  DCHECK(lhs_key_selector_->source_schema().EqualByType(lhs->schema()));

  DCHECK_EQ(2, result_projector.source_count());
  DCHECK(result_projector.source_schema(0).EqualByType(lhs->schema()));
  DCHECK(result_projector.source_schema(1).EqualByType(rhs->schema()));

  SetUpFinalResultProjector(join_type, result_projector);
}

FailureOrVoid HashJoinCursor::Init() {
  if (!lhs_result_.Reallocate(Cursor::kDefaultRowCount)) {
    // TODO(user): Specify the amount of memory requested as details.
    THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                        "Memory exceeded in HashJoinCursor::Init()"));
  }
  result_view_.set_row_count(lhs_result_.row_capacity());
  return Success();
}

const TupleSchema& HashJoinCursor::schema() const {
  return result_view_.schema();
}

ResultView HashJoinCursor::Next(rowcount_t max_row_count) {
  // The main loop iterates over views of data read from the lhs input until
  // one that yields a non-empty view of matches from an index lookup is found.
  // In that case rhs rows from the view of matches are merged with matching
  // rows from the lhs view. The merged lhs rows are are written into result.
  // Rhs rows are projected into result (without actual copying). The matching
  // is done based on query_ids().
  for ( ;; ) {
    if (matches_.get() != NULL) {
      // A case where there are (possibly) some leftover views of matches.
      // This happens every time MultiLookup() returns a non-NULL cursor, as
      // after the first Next() on that cursor it's impossible to tell whether
      // the cursor has been exhausted without calling Next() again.
      ResultLookupIndexView rhs_result = matches_->Next(max_row_count);
      PROPAGATE_ON_FAILURE(rhs_result);
      if (rhs_result.has_data()) {
        // Merge lhs columns (1) with rhs columns (2) from lookup in
        // result_view. The rhs columns are taken as-is from the view returned
        // by the lookup (they are simply linked from rhs_result to
        // result_view). Relevant rows from lhs input are written into
        // lhs_result_ based on query_ids().
        // This method returns a view to the caller every time a lookup yields
        // a non-empty view of matches.
        // TODO(user): Fill the result block with multiple MultiLookups if one
        // is not enough before returning it to the caller.

        // Step 1: Project lhs rows selected by lookup result's query_ids() into
        // lhs_result_.
        const rowcount_t copy_result = lhs_result_copier_.Copy(
            rhs_result.view().row_count(), *lhs_view_,
            rhs_result.view().query_ids(), 0,
            &lhs_result_);
        if (copy_result < rhs_result.view().row_count()) {
          return ResultView::Failure(new Exception(
              ERROR_MEMORY_EXCEEDED, "Memory exceeded when copying lhs input"));
        }

        // Step 2: Set up result_view over combined results.
        // TODO(user): Partial Project (only for a selected source) to avoid
        // repetitive copying of fixed Column pointers from lhs_result_ into
        // result_view.
        const View* combined_results[] = {
            &lhs_result_.view(), &rhs_result.view() };
        final_result_projector_->Project(
            combined_results, combined_results + arraysize(combined_results),
            &result_view_);
        result_view_.set_row_count(rhs_result.view().row_count());

        return ResultView::Success(&result_view_);
      } else if (rhs_result.is_eos()) {
        // The cursor of matches is exhausted. The main loop will advance to
        // the next lhs view.
        matches_.reset(NULL);
        lhs_view_ = NULL;
      } else {
        // TODO(user): handle WAITING_ON_BARRIER.
        LOG(FATAL) << "Unexpected iteration result";
      }
    }

    if (lhs_view_ == NULL) {
      ResultView lhs_result = lhs_->Next(Cursor::kDefaultRowCount);
      PROPAGATE_ON_FAILURE(lhs_result);
      // Early termination if lhs is empty.
      if (lhs_result.is_eos()) {
        return ResultView::EOS();
      } else if (lhs_result.is_waiting_on_barrier()) {
        return ResultView::WaitingOnBarrier();
      } else {
        CHECK(lhs_result.has_data());
        lhs_view_ = &lhs_result.view();
      }
    }

    if (rhs_.get() == NULL) {
      // Right-hand side not yet materialized.
      FailureOrOwned<LookupIndex> result = rhs_builder_->Build();
      PROPAGATE_ON_FAILURE(result);
      rhs_.reset(result.release());
      if (rhs_.get() == NULL) {
        // Still not materialized.
        return ResultView::WaitingOnBarrier();
      } else {
        rhs_builder_.reset(NULL);
        DCHECK(lhs_key_selector_->result_schema().EqualByType(
            rhs_->key_selector().result_schema()));
        // Early termination if rhs is empty in the INNER join.
        if (rhs_->empty() && join_type_ == INNER) {
          return ResultView::EOS();
        }
      }
    }

    // If we got here, we have new data in lhs_view_, and the rhs is
    // materialized.
    DCHECK(lhs_view_ != NULL);
    DCHECK_GT(lhs_view_->row_count(), 0);
    // Set up lookup_query_ with lhs key columns.
    lhs_key_selector_->Project(*lhs_view_, &lookup_query_);
    lookup_query_.set_row_count(lhs_view_->row_count());
    // Pass lookup_query to LookupIndex.
    FailureOrOwned<LookupIndexCursor> multi_lookup_result =
        rhs_->MultiLookup(&lookup_query_);
    PROPAGATE_ON_FAILURE(multi_lookup_result);
    matches_.reset(multi_lookup_result.release());
    if (matches_.get() == NULL) {
      // Right-hand-side encountered a barrier during materialization.
      return ResultView::WaitingOnBarrier();
    }
    // Otherwise, continue the loop, and it'll see new matches.
  }
}

void HashJoinCursor::AppendDebugDescription(string* target) const {
  target->append("HashJoinCursor");
}


namespace {

// Helper function to find a source attribute's position in a projector's result
// schema. Assumes that the attribute is projected to exactly one position.
int ProjectedAttributePosition(
    const BoundSingleSourceProjector& projector, int source_position) {
  pair<PositionIterator, PositionIterator> positions =
      projector.ProjectedAttributePositions(source_position);
  DCHECK(--positions.second == positions.first)
      << "Source attribute should be projected to exactly one position";
  return *positions.first;
}

}  // namespace

// Sets up final_result_projector_ that projects lhs and rhs columns indicated
// by the user in result_projector into result_projector's schema. This extra
// projector is needed because final lhs data is taken from temporary lhs_result
// block, which is of different schema not compatible with result_projector's
// source schema. In the algebra of projectors,
// final_result_projector = result_projector * (-lhs_result_projector)
void HashJoinCursor::SetUpFinalResultProjector(
    JoinType join_type,
    const BoundMultiSourceProjector& result_projector) {
  final_result_projector_.reset(new BoundMultiSourceProjector(
      util::gtl::Container(&lhs_result_.schema(), &rhs_builder_->schema()).
      As<vector<const TupleSchema*> >()));
  const TupleSchema& result_schema = result_projector.result_schema();
  // Iterate over result schema attributes.
  for (int i = 0; i < result_schema.attribute_count(); i++) {
    const int source_index = result_projector.source_index(i);
    const int source_position =
        result_projector.source_attribute_position(i);
    const string& result_attribute_name = result_schema.attribute(i).name();
    if (source_index == 0) {
      // Attribute in result schema comes from the left-hand side input.
      // Find the attribute's position in lhs_result_projector's result schema
      // and project the attribute from that position in that schema.
      const int position_in_lhs_result =
          ProjectedAttributePosition(lhs_result_projector_, source_position);
      final_result_projector_->AddAs(
          0, position_in_lhs_result, result_attribute_name);
    } else {
      // Attribute in result schema comes from the right-hand side input.
      DCHECK(join_type != LEFT_OUTER ||
             result_schema.attribute(i).nullability() == NULLABLE);
      final_result_projector_->AddAs(1, source_position, result_attribute_name);
    }
  }
}

template <KeyUniqueness key_uniqueness>
HashIndexOnMaterializedCursor<key_uniqueness>::HashIndexOnMaterializedCursor(
    JoinType join_type,
    BufferAllocator* allocator,
    const BoundSingleSourceProjector* key_selector,
    const TupleSchema& schema)
    : join_type_(join_type),
      schema_(join_type == LEFT_OUTER
                ? WithAllColumnsNullable(schema)
                : schema),
      key_selector_(key_selector),
      index_(schema, allocator, key_selector),
      result_cursor_block_(schema_, allocator) {
  DCHECK(key_selector_->source_schema().EqualByType(schema_));
}

template <KeyUniqueness key_uniqueness>
FailureOrVoid HashIndexOnMaterializedCursor<key_uniqueness>::Init() {
  if (!result_cursor_block_.Reallocate(Cursor::kDefaultRowCount)) {
    THROW(new Exception(
        ERROR_MEMORY_EXCEEDED,
        "Cannot allocate memory to store index lookup results"));
  }
  result_cursor_query_ids_.reset(
      new rowid_t[result_cursor_block_.row_capacity()]);
  return Success();
}

// Materializes the entire rhs input cursor into index_.
template <KeyUniqueness key_uniqueness>
FailureOr<bool> HashIndexOnMaterializedCursor<key_uniqueness>::
MaterializeInputAndBuildIndex(Cursor* input) {
  // TODO(onufry): rethink the usage of the kDefaultRowCount constant here.
  large_bool_array is_not_null_array;
  DCHECK(large_bool_array::capacity() >= Cursor::kDefaultRowCount);
  bool_ptr is_not_null = is_not_null_array.mutable_data();
  View input_key_columns(key_selector_->result_schema());
  ResultView result = ResultView::EOS();
  while ((result = input->Next(Cursor::kDefaultRowCount)).has_data()) {
    key_selector_->Project(result.view(), &input_key_columns);
    input_key_columns.set_row_count(result.view().row_count());
    FindNotNullKeys(input_key_columns, is_not_null);
    if (!index_.Insert(result.view(), is_not_null)) {
      THROW(new Exception(
          ERROR_MEMORY_EXCEEDED,
          "Memory exceeded at input materialization"));
    }
  }
  PROPAGATE_ON_FAILURE(result);
  return Success(result.is_eos());
}

// ResultCursor encapsulates the state of a single MultiLookup to
// HashIndexOnMaterializedCursor.
template <KeyUniqueness key_uniqueness>
template <JoinType join_type>
class HashIndexOnMaterializedCursor<key_uniqueness>::ResultCursor
    : public LookupIndexCursor {
  typedef typename HashIndexOnMaterializedCursor<key_uniqueness>::RowHashSetType
    RowHashSetType;

 public:
  // result_block and query_ids are placeholders allocated by the caller.
  ResultCursor(
      const RowHashSetType& index, const View& query,
      Block* result_block, rowid_t* query_ids);

  const TupleSchema& schema() const { return result_block_->schema(); }
  ResultLookupIndexView Next(rowcount_t max_row_count);

  void AppendDebugDescription(string* target) const;

 private:
  // The number of (query-, index-) row pairs matched so far.
  rowcount_t MatchingRowCount() const { return index_matches_.size(); }

  // TODO(user): The following workaround for impossibility of partially
  // specializing methods is rather ugly. Refactor this code.
  inline void ProcessNextResultNonUniqueKey();
  inline void ProcessNextResultUniqueKey();

  struct NonUnique {
    inline void operator()(ResultCursor* cursor) {
      cursor->ProcessNextResultNonUniqueKey();
    }
  };

  struct Unique {
    inline void operator()(ResultCursor* cursor) {
      cursor->ProcessNextResultUniqueKey();
    }
  };

  // Template method that encapsulates the differences in processing of
  // find_result_ depending on its type. Copies match information from
  // find_result_ into query_matches_ and index_matches_ (see below).
  inline void ProcessNextResult() {
    typename base::if_<key_uniqueness == UNIQUE, Unique, NonUnique>::type
        dispatch;
    dispatch(this);
  }

  // Reference to parent's index (not specific to a particular lookup).
  const RowHashSetType& index_;

  // Placeholder for result rows, not owned.
  Block* result_block_;
  // A view over the result returned to the caller.
  LookupIndexView result_view_;
  // Copies matched rows from index_ into result.
  SelectiveViewCopier index_copier_;

  // The result of a lookup to index_. It contains ids of all rows in the index
  // that match query_.
  typename RowHashSetType::ResultType find_result_;
  // Iterator over the last processed row id set in find_result_.
  // Only used if find_result_ is of RowHashMultiSet type.
  row_hash_set::RowIdSetIterator it_find_result_;

  rowcount_t query_row_count_;
  // Id of the current query row to be processed.
  rowid_t query_row_id_;

  // Pair of intermediate result vectors; first one is a ref to
  // result_view_.query_ids.
  // (query_matches[n], index_matches[n]) = (q, i)  indicates that a query row q
  // matches an index row i. This representation of matches using two vectors
  // makes the final merging of rows in the result_ block very efficient.
  rowid_t* query_matches_;
  vector<rowid_t> index_matches_;
};

template <KeyUniqueness key_uniqueness>
FailureOrOwned<LookupIndexCursor> HashIndexOnMaterializedCursor<key_uniqueness>
::MultiLookup(const View* query) const {
  DCHECK(key_selector_->result_schema().EqualByType(query->schema()))
      << "Query should only contain key columns";
  // Work is delegated to LookupIndexCursor.
  switch (join_type_) {
    case INNER:
      return Success(new ResultCursor<INNER>(
          index_, *query, &result_cursor_block_,
          result_cursor_query_ids_.get()));
    case LEFT_OUTER:
      return Success(new ResultCursor<LEFT_OUTER>(
          index_, *query, &result_cursor_block_,
          result_cursor_query_ids_.get()));
    default:
      THROW(new Exception(ERROR_NOT_IMPLEMENTED,
                          StrCat("Unsupported join_type in hash_join: ",
                                 JoinType_Name(join_type_))));
  }
}

template <KeyUniqueness key_uniqueness>
template <JoinType join_type>
HashIndexOnMaterializedCursor<key_uniqueness>::ResultCursor<join_type>::
ResultCursor(
    const RowHashSetType& index, const View& query,
    Block* result_block, rowid_t* query_ids)
    : index_(index),
      result_block_(result_block),
      result_view_(result_block->schema(), query_ids),
      // Shallow copy is safe because index_ outlives ResultCursor.
      index_copier_(index_.indexed_view().schema(),
                    result_view_.schema(),
                    false),
      find_result_(Cursor::kDefaultRowCount),
      query_row_id_(0) {
  DCHECK(result_block_->schema().EqualByType(index_.indexed_view().schema()));
  result_view_.ResetFrom(result_block_->view());
  query_row_count_ = query.row_count();

  index_matches_.reserve(result_block_->row_capacity());

  // Look up query rows in the index.
  // TODO(onufry): rethink this design with a hard-coded constant here.
  CHECK_GE(Cursor::kDefaultRowCount, query.row_count());
  large_bool_array is_not_null;
  DCHECK(large_bool_array::capacity() >= Cursor::kDefaultRowCount);
  FindNotNullKeys(query, is_not_null.mutable_data());
  index_.Find(query, is_not_null.const_data(), &find_result_);
}

template <KeyUniqueness key_uniqueness>
template <JoinType join_type>
ResultLookupIndexView HashIndexOnMaterializedCursor<key_uniqueness>::
ResultCursor<join_type>::Next(rowcount_t max_row_count) {
  // Ensure that no more rows than the placeholder block can hold are returned.
  if (max_row_count > result_block_->row_capacity())
    max_row_count = result_block_->row_capacity();

  query_matches_ = result_view_.mutable_query_ids();
  index_matches_.clear();

  // First step of the algorithm: gather info about matches as pairs of ints
  // until max_row_count matches are found or the query is fully answered.
  while ((query_row_id_ < query_row_count_) &&
         (MatchingRowCount() < max_row_count))
    ProcessNextResult();

  // Second step of the algorithm: copy data from value columns of matched
  // index rows.
  if (MatchingRowCount() != 0) {
    const rowcount_t copy_result = index_copier_.Copy(
        index_matches_.size(), index_.indexed_view(), index_matches_.data(), 0,
        result_block_);
    if (copy_result < index_matches_.size()) {
      return ResultLookupIndexView::Failure(new Exception(
          ERROR_MEMORY_EXCEEDED, "Memory exceeded when copying rhs input"));
    }
    result_view_.set_row_count(MatchingRowCount());
    return ResultLookupIndexView::Success(&result_view_);
  }

  return ResultLookupIndexView::EOS();
}

template <KeyUniqueness key_uniqueness>
template <JoinType join_type>
void HashIndexOnMaterializedCursor<key_uniqueness>::ResultCursor<join_type>::
ProcessNextResultNonUniqueKey() {
  // TODO(user): Try to decrease the number of AtEnd() checks performed.
  if (it_find_result_.AtEnd()) {
    // First iteration with this query_row_id; retrieve matching rows if any.
    it_find_result_ = find_result_.Result(query_row_id_);
    if (join_type == LEFT_OUTER && it_find_result_.AtEnd()) {
      // There were no results for this query_row_id - when join_type ==
      // LEFT_OUTER we output NULLs for the right side of join.
      index_matches_.push_back(-1);
      *(query_matches_++) = query_row_id_;
    }
  }
  if (!it_find_result_.AtEnd()) {
    const rowid_t index_row_id = it_find_result_.Get();
    index_matches_.push_back(index_row_id);
    *(query_matches_++) = query_row_id_;
    it_find_result_.Next();
  }
  if (it_find_result_.AtEnd())
    query_row_id_++;
}

template <KeyUniqueness key_uniqueness>
template <JoinType join_type>
void HashIndexOnMaterializedCursor<key_uniqueness>::ResultCursor<join_type>::
ProcessNextResultUniqueKey() {
  const rowid_t index_row_id = find_result_.Result(query_row_id_);
  if (index_row_id != row_hash_set::kInvalidRowId) {
    index_matches_.push_back(index_row_id);
    *(query_matches_++) = query_row_id_;
  } else if (join_type == LEFT_OUTER) {
    index_matches_.push_back(-1);
    *(query_matches_++) = query_row_id_;
  }
  query_row_id_++;
}

}  // namespace supersonic
