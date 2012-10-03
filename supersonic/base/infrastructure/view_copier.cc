// Copyright 2010 Google Inc.  All Rights Reserved
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
// Family of ViewCopier classes.

#include "supersonic/base/infrastructure/view_copier.h"

#include <stddef.h>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"

namespace supersonic {

ViewCopier::ViewCopier(
    const TupleSchema& input_schema,
    const TupleSchema& output_schema,
    const RowSelectorType row_selector_type,
    bool deep_copy)
    : projector_(NULL) {
  DCHECK(input_schema.EqualByType(output_schema));
  CreateColumnCopiers(input_schema, output_schema,
                      row_selector_type, deep_copy);
}

ViewCopier::ViewCopier(
    const BoundSingleSourceProjector* projector,
    const RowSelectorType row_selector_type,
    bool deep_copy)
    : projector_(projector) {
  CreateColumnCopiers(
      projector->result_schema(), projector->result_schema(),
      row_selector_type, deep_copy);
}

void ViewCopier::CreateColumnCopiers(
    const TupleSchema& input_schema,
    const TupleSchema& output_schema,
    const RowSelectorType row_selector_type,
    bool deep_copy) {
  for (int i = 0; i < input_schema.attribute_count(); i++) {
    column_copiers_.push_back(ResolveCopyColumnFunction(
        input_schema.attribute(i).type(),
        output_schema.attribute(i).nullability(),
        row_selector_type,
        deep_copy));
  }
}

rowcount_t ViewCopier::Copy(
    const rowcount_t row_count,
    const View& input_view,
    const rowid_t* input_row_ids,
    const rowcount_t output_offset,
    Block* output_block) const {
  DCHECK(output_block != NULL) << "Missing output for view copy";
  const TupleSchema& output_schema = projector_ ?
      projector_->result_schema() : input_view.schema();
  DCHECK(output_schema.EqualByType(output_block->schema()));
  rowcount_t rows_copied = row_count;
  for (int i = 0; i < column_copiers_.size(); i++) {
    // Doesn't call Project to avoid creating a view in every call.
    const Column& input_column = input_view.column(
        projector_ ? projector_->source_attribute_position(i) : i);
    // May decrease rows_copied; perhaps even to zero.
    rows_copied = column_copiers_[i](
        rows_copied, input_column, input_row_ids, output_offset,
        output_block->mutable_column(i));
  }
  return rows_copied;
}

MultiViewCopier::MultiViewCopier(
    const BoundMultiSourceProjector* projector,
    const RowSelectorType row_selector_type,
    bool deep_copy)
    : projector_(projector) {
  for (int i = 0; i < projector_->result_schema().attribute_count(); i++) {
    const Attribute& attribute = projector_->result_schema().attribute(i);
    column_copiers_.push_back(ResolveCopyColumnFunction(
        attribute.type(),
        attribute.nullability(),
        row_selector_type,
        deep_copy));
  }
}

rowcount_t MultiViewCopier::Copy(
    const rowcount_t row_count,
    const vector<const View*>& input_views,
    const rowid_t* input_row_ids,
    const rowcount_t output_offset,
    Block* output_block) const {
  DCHECK(projector_->result_schema().EqualByType(output_block->schema()));
  rowcount_t rows_copied = row_count;
  for (int i = 0; i < projector_->result_schema().attribute_count(); i++) {
    // Doesn't call Project to avoid creating a view in every call.
    const Column& input_column =
        input_views[projector_->source_index(i)]->
        column(projector_->source_attribute_position(i));
    // May decrease row_count; perhaps even to zero.
    rows_copied = column_copiers_[i](
        rows_copied, input_column, input_row_ids, output_offset,
        output_block->mutable_column(i));
  }
  return rows_copied;
}

}  // namespace supersonic
