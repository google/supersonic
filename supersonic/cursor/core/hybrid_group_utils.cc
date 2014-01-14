// Copyright 2011 Google Inc. All Rights Reserved.
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

#include "supersonic/cursor/core/hybrid_group_utils.h"

#include <list>
#include "supersonic/utils/std_namespace.h"
#include <memory>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/core/compute.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/elementary_bound_expressions.h"
#include "supersonic/expression/core/projecting_bound_expressions.h"
#include "supersonic/expression/infrastructure/terminal_bound_expressions.h"
#include "supersonic/utils/pointer_vector.h"

using util::gtl::PointerVector;

namespace supersonic {

namespace {

class HybridGroupTransformCursor : public BasicCursor {
 public:
  virtual ~HybridGroupTransformCursor() {}

  // Doesn't take ownership of allocator.
  // Takes ownership of input.
  static FailureOrOwned<Cursor> Create(
      const SingleSourceProjector& group_by_columns,
      const PointerVector<const SingleSourceProjector>& column_group_projectors,
      BufferAllocator* allocator,
      Cursor* input);

  virtual ResultView Next(rowcount_t max_row_count);

  virtual bool IsWaitingOnBarrierSupported() const {
    return child()->IsWaitingOnBarrierSupported();
  }

  virtual CursorId GetCursorId() const { return HYBRID_GROUP_TRANSFORM; }

 private:
  typedef PointerVector<const BoundMultiSourceProjector> ProjectorVector;

  // Doesn't take ownership of allocator.
  // Takes ownership of input.
  HybridGroupTransformCursor(
      const TupleSchema& transformed_schema,
      const TupleSchema& nulls_block_schema,
      PointerVector<const BoundMultiSourceProjector>* transformation_projectors,
      Cursor* input,
      BufferAllocator* allocator);

  // Builds a BoundMultiSourceProjector that can be used to select data for the
  // given column group (selected_group). Columns other then the selected column
  // group or group_by_columns are filled with NULLs (by applying the projector
  // to input 1). If selected_group is -1, no column group is selected
  // (everything is taken from the nulls block). The schemas vector should
  // contain two elements: input schema and nulls block schema.
  static FailureOrOwned<const BoundMultiSourceProjector> SelectColumnGroup(
      const SingleSourceProjector& group_by_columns,
      const PointerVector<const SingleSourceProjector>& column_group_projectors,
      const vector<const TupleSchema*>& schemas,
      int selected_group);

  // Ensures that nulls_block_ has at least row_count rows. If the block needs
  // to be reallocated, fill the new rows with NULLs.
  FailureOrVoid EnsureNullsBlockCapacity(rowcount_t row_count);

  // A block with schema compatible with input Cursor's schema that has all
  // columns filled with NULLs. This block is used to replace selected input
  // column data with NULLs. The nulls_block_ is resized on demand with
  // EnsureNullsBlockCapacity.
  Block nulls_block_;

  // A series of multi-source projectors that define the transformation of the
  // input. They all have the same output schema. The difference is that some
  // take some columns from the second input, which is nulls_block_, which
  // basically puts NULLs in the given output column.
  std::unique_ptr<ProjectorVector> transformation_projectors_;

  // Points to the next transformation projector to be used. When it gets to
  // .end(), read new block from the child and rewind to the first projector.
  ProjectorVector::const_iterator next_projector_;

  // The view received from the child.
  const View* child_view_;

  // The target view for transformation projectors.
  View projection_result_;

  // View iterator on projection_result_. It is necessary because for one child
  // result view this cursor may need to return more than one view, and
  // max_row_count may change between Next() calls.
  ViewIterator view_iterator_;

  DISALLOW_COPY_AND_ASSIGN(HybridGroupTransformCursor);
};

FailureOrOwned<Cursor> HybridGroupTransformCursor::Create(
    const SingleSourceProjector& group_by_columns,
    const PointerVector<const SingleSourceProjector>& column_group_projectors,
    BufferAllocator* allocator,
    Cursor* input) {
  std::unique_ptr<Cursor> input_owned(input);
  // Prepare the schema for nulls_block. It is the same as input schema, but
  // with all columns NULLABLE (because we want to put NULLs in them).
  TupleSchema nulls_block_schema;
  for (int i = 0; i < input_owned->schema().attribute_count(); ++i) {
    const Attribute& attribute(input_owned->schema().attribute(i));
    nulls_block_schema.add_attribute(
        Attribute(attribute.name(),
                  attribute.type(),
                  NULLABLE));
  }
  // We need a vector with both schemas - input schema and nulls_block_schema -
  // to create the BoundMultiSourceProjectors.
  vector<const TupleSchema*> schemas(2);
  schemas[0] = &input_owned->schema();
  schemas[1] = &nulls_block_schema;
  // Calculate transformed_schema by applying all the column_group_projectors
  // to nulls_block_schema (which is in schemas[1]).
  TupleSchema transformed_schema;
  {
    FailureOrOwned<const BoundMultiSourceProjector> bound_projector(
        SelectColumnGroup(group_by_columns, column_group_projectors, schemas,
                          -1));  // Using -1 to select no column group.
    PROPAGATE_ON_FAILURE(bound_projector);
    transformed_schema = bound_projector->result_schema();
  }
  // Prepare multi-source projectors implementing the transformation - one for
  // each column group.
  std::unique_ptr<PointerVector<const BoundMultiSourceProjector> >
      transformation_projectors(
          new PointerVector<const BoundMultiSourceProjector>);

  // We create O(N*N) new projector objects here, where N is the size of
  // column_group_projectors vector. This would be a problem if
  // column_group_projectors was big, but in reality it will rarely have more
  // than several elements - its size is the number of columns that are being
  // DISTINCT aggregated + 1. Also, this is only done once, when initializing a
  // HybridGroupAggregate cursor. Actually, a bigger problem is that the input
  // data is going to be replicated N times.
  for (int selected_group = 0;
       selected_group < column_group_projectors.size(); ++selected_group) {
    FailureOrOwned<const BoundMultiSourceProjector> bound_projector(
        SelectColumnGroup(group_by_columns, column_group_projectors, schemas,
                          selected_group));
    PROPAGATE_ON_FAILURE(bound_projector);
    transformation_projectors->push_back(bound_projector.release());
  }
  // Build the actual cursor.
  return Success(new HybridGroupTransformCursor(
      transformed_schema,
      nulls_block_schema,
      transformation_projectors.release(),
      input_owned.release(),
      allocator));
}

ResultView HybridGroupTransformCursor::Next(rowcount_t max_row_count) {
  while (!view_iterator_.next(max_row_count)) {
    PROPAGATE_ON_FAILURE(ThrowIfInterrupted());
    if (next_projector_ != transformation_projectors_->end()) {
      vector<const View*> input_views(2);
      input_views[0] = child_view_;
      input_views[1] = &nulls_block_.view();
      (*next_projector_)->Project(input_views.begin(),
                                  input_views.end(),
                                  &projection_result_);
      projection_result_.set_row_count(child_view_->row_count());
      view_iterator_.reset(projection_result_);
      ++next_projector_;
    } else {
      ResultView child_result = child()->Next(max_row_count);
      if (!child_result.has_data()) {
        return child_result;
      }
      child_view_ = &child_result.view();
      PROPAGATE_ON_FAILURE(EnsureNullsBlockCapacity(
          child_view_->row_count()));
      next_projector_ = transformation_projectors_->begin();
    }
  }
  return ResultView::Success(&view_iterator_.view());
}

HybridGroupTransformCursor::HybridGroupTransformCursor(
    const TupleSchema& transformed_schema,
    const TupleSchema& nulls_block_schema,
    PointerVector<const BoundMultiSourceProjector>* transformation_projectors,
    Cursor* input,
    BufferAllocator* allocator)
    : BasicCursor(transformed_schema, input),
      nulls_block_(nulls_block_schema, allocator),
      transformation_projectors_(transformation_projectors),
      next_projector_(transformation_projectors_->end()),
      child_view_(NULL),
      projection_result_(schema()),
      view_iterator_(schema()) {}

FailureOrOwned<const BoundMultiSourceProjector>
HybridGroupTransformCursor::SelectColumnGroup(
    const SingleSourceProjector& group_by_columns,
    const PointerVector<const SingleSourceProjector>& column_group_projectors,
    const vector<const TupleSchema*>& schemas,
    int selected_group) {
  CHECK(selected_group >= -1 &&
        selected_group < static_cast<int>(column_group_projectors.size()));
  CHECK_EQ(2, schemas.size());
  CompoundMultiSourceProjector projector;
  // Take group_by_columns columns from the input data view (input 0).
  projector.add(0, group_by_columns.Clone());
  for (int i = 0; i < column_group_projectors.size(); ++i) {
    // Only the selected_group is taken from the input data view (input 0).
    // The rest are taken from nulls_block_ (input 1).
    projector.add(i == selected_group ? 0 : 1,
                  column_group_projectors[i]->Clone());
  }
  FailureOrOwned<const BoundMultiSourceProjector> bound_projector(
      projector.Bind(schemas));
  PROPAGATE_ON_FAILURE(bound_projector);
  return bound_projector;
}

// TODO(user): This could be simplified if Table was extended with methods for
// adding rows filled with NULLs.
FailureOrVoid HybridGroupTransformCursor::EnsureNullsBlockCapacity(
    rowcount_t row_count) {
  const rowcount_t old_capacity = nulls_block_.row_capacity();
  if (old_capacity >= row_count) {
    return Success();
  }
  if (!nulls_block_.Reallocate(row_count)) {
    THROW(new Exception(
        ERROR_MEMORY_EXCEEDED,
        StrCat("Couldn't reallocate nulls_block_ to ",
               row_count, " rows.")));
  }
  for (int col = 0; col < nulls_block_.column_count(); ++col) {
    OwnedColumn* column = nulls_block_.mutable_column(col);
    CHECK_NOTNULL(column->mutable_is_null());
    bit_pointer::FillWithTrue(
        column->mutable_is_null_plus_offset(old_capacity),
        nulls_block_.row_capacity() - old_capacity);
  }
  return Success();
}

FailureOrOwned<BoundExpression> TypedZero(
    DataType type,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  FailureOrOwned<BoundExpression> zero(
      BoundConstUInt64(0, allocator, max_row_count));
  PROPAGATE_ON_FAILURE(zero);
  if (type != UINT64) {
    FailureOrOwned<BoundExpression> cast(
        BoundCastTo(type, zero.release(), allocator, max_row_count));
    PROPAGATE_ON_FAILURE(cast);
    return cast;
  } else {
    return zero;
  }
}

}  // namespace

FailureOrOwned<Cursor> BoundHybridGroupTransform(
    const SingleSourceProjector* group_by_columns,
    const util::gtl::PointerVector<const SingleSourceProjector>&
        column_group_projectors,
    BufferAllocator* allocator,
    Cursor* child) {
  std::unique_ptr<Cursor> child_owner(child);
  std::unique_ptr<const SingleSourceProjector> group_by_columns_owned(
      group_by_columns);
  FailureOrOwned<Cursor> transform_cursor(
      HybridGroupTransformCursor::Create(*group_by_columns_owned,
                                         column_group_projectors,
                                         allocator,
                                         child_owner.release()));
  PROPAGATE_ON_FAILURE(transform_cursor);
  return transform_cursor;
}

FailureOrOwned<Cursor> MakeSelectedColumnsNotNullable(
    const SingleSourceProjector* selection_projector,
    BufferAllocator* allocator,
    Cursor* input) {
  const rowcount_t max_row_count = Cursor::kDefaultRowCount;
  std::unique_ptr<const SingleSourceProjector> selection_projector_owned(
      selection_projector);
  std::unique_ptr<Cursor> input_owner(input);
  FailureOrOwned<const BoundSingleSourceProjector> bound_selection_projector(
      selection_projector_owned->Bind(input_owner->schema()));
  PROPAGATE_ON_FAILURE(bound_selection_projector);
  // We take apart the cursor column by column and put it together with
  // BoundRenameCompoundExpression. name[i] defines an alias for
  // expression_list.GetAt(i).
  vector<string> names;
  std::unique_ptr<BoundExpressionList> expression_list(new BoundExpressionList);
  for (int i = 0; i < input_owner->schema().attribute_count(); ++i) {
    const Attribute& attribute = input_owner->schema().attribute(i);
    std::unique_ptr<BoundExpression> column_expression;
    {
      FailureOrOwned<BoundExpression> projection_expression(
          BoundAttributeAt(input_owner->schema(), i));
      PROPAGATE_ON_FAILURE(projection_expression);
      column_expression.reset(projection_expression.release());
    }
    if (bound_selection_projector->IsAttributeProjected(i) &&
        attribute.nullability() == NULLABLE) {
      // Column is NULLABLE an is selected by bound_selection_projector -
      // wrapping it in IFNULL(..., 0).
      FailureOrOwned<BoundExpression> zero(
          TypedZero(attribute.type(), allocator, max_row_count));
      PROPAGATE_ON_FAILURE(zero);
      FailureOrOwned<BoundExpression> ifnull(
          BoundIfNull(column_expression.release(),
                      zero.release(),
                      allocator,
                      max_row_count));
      PROPAGATE_ON_FAILURE(ifnull);
      column_expression.reset(ifnull.release());
    }
    expression_list->add(column_expression.release());
    names.push_back(attribute.name());
  }
  FailureOrOwned<BoundExpression> compound_expression(
      BoundRenameCompoundExpression(names, expression_list.release()));
  // Build the Compute cursor.
  PROPAGATE_ON_FAILURE(compound_expression);
  FailureOrOwned<BoundExpressionTree> expression_tree(
      CreateBoundExpressionTree(compound_expression.release(),
                                allocator,
                                max_row_count));
  PROPAGATE_ON_FAILURE(expression_tree);
  FailureOrOwned<Cursor> compute_cursor(
      BoundCompute(expression_tree.release(),
                   allocator,
                   max_row_count,
                   input_owner.release()));
  PROPAGATE_ON_FAILURE(compute_cursor);
  return compute_cursor;
}

FailureOrOwned<Cursor> ExtendByConstantColumn(
    const string& new_column_name,
    BufferAllocator* allocator,
    Cursor* input) {
  std::unique_ptr<Cursor> input_owner(input);
  const rowcount_t max_row_count = Cursor::kDefaultRowCount;
  vector<string> names;
  std::unique_ptr<BoundExpressionList> expression_list(new BoundExpressionList);
  // Add all original columns.
  for (int i = 0; i < input_owner->schema().attribute_count(); ++i) {
    FailureOrOwned<BoundExpression> projection_expression(
        BoundAttributeAt(input_owner->schema(), i));
    PROPAGATE_ON_FAILURE(projection_expression);
    expression_list->add(projection_expression.release());
    names.push_back(input_owner->schema().attribute(i).name());
  }
  // Add the new column.
  FailureOrOwned<BoundExpression> bound_zero(
      BoundConstInt32(0, allocator, max_row_count));
  PROPAGATE_ON_FAILURE(bound_zero);
  expression_list->add(bound_zero.release());
  names.push_back(new_column_name);
  FailureOrOwned<BoundExpression> compound_expression(
      BoundRenameCompoundExpression(names, expression_list.release()));
  // Build the Compute cursor.
  PROPAGATE_ON_FAILURE(compound_expression);
  FailureOrOwned<BoundExpressionTree> expression_tree(
      CreateBoundExpressionTree(compound_expression.release(),
                                allocator,
                                max_row_count));
  PROPAGATE_ON_FAILURE(expression_tree);
  FailureOrOwned<Cursor> compute_cursor(
      BoundCompute(expression_tree.release(),
                   allocator,
                   max_row_count,
                   input_owner.release()));
  PROPAGATE_ON_FAILURE(compute_cursor);
  return compute_cursor;
}

}  // namespace supersonic
