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

BaseViewCopier::BaseViewCopier(const TupleSchema& input_schema,
                               const TupleSchema& output_schema,
                               const BoundSingleSourceProjector* projector,
                               const RowSelectorType row_selector_type,
                               bool deep_copy)
    : projector_(projector) {
  CreateColumnCopiers(input_schema, output_schema, row_selector_type,
                      deep_copy);
  source_schema_ =
      (projector != NULL) ? projector->source_schema() : input_schema;
  result_schema_ = output_schema;
}

ViewCopier::ViewCopier(const TupleSchema& schema, bool deep_copy)
    : BaseViewCopier(schema, schema, NULL, NO_SELECTOR, deep_copy) {}

ViewCopier::ViewCopier(const BoundSingleSourceProjector* projector,
                       bool deep_copy)
    : BaseViewCopier(projector->result_schema(), projector->result_schema(),
                     projector, NO_SELECTOR, deep_copy) {}

SelectiveViewCopier::SelectiveViewCopier(
    const TupleSchema& schema,
    bool deep_copy)
    : BaseViewCopier(schema, schema, NULL, INPUT_SELECTOR, deep_copy) {}

SelectiveViewCopier::SelectiveViewCopier(
    const TupleSchema& source_schema,
    const TupleSchema& result_schema,
    bool deep_copy)
    : BaseViewCopier(source_schema, result_schema, NULL, INPUT_SELECTOR,
                     deep_copy) {}

SelectiveViewCopier::SelectiveViewCopier(
    const BoundSingleSourceProjector* projector,
    bool deep_copy)
    : BaseViewCopier(projector->result_schema(), projector->result_schema(),
                     projector, INPUT_SELECTOR, deep_copy) {}

void BaseViewCopier::CreateColumnCopiers(
    const TupleSchema& input_schema,
    const TupleSchema& output_schema,
    const RowSelectorType row_selector_type,
    bool deep_copy) {
  for (int i = 0; i < output_schema.attribute_count(); i++) {
    column_copiers_.push_back(ResolveCopyColumnFunction(
        output_schema.attribute(i).type(),
        input_schema.attribute(i).nullability(),
        output_schema.attribute(i).nullability(),
        row_selector_type,
        deep_copy));
  }
}

rowcount_t BaseViewCopier::Copy(
    const rowcount_t row_count,
    const View& input_view,
    const rowid_t* input_row_ids,
    const rowcount_t output_offset,
    Block* output_block) const {
  DCHECK(TupleSchema::AreEqual(source_schema_, input_view.schema(), false))
      << "Expected: " << source_schema_.GetHumanReadableSpecification() << ", "
      << "Got: " << input_view.schema().GetHumanReadableSpecification();
  DCHECK(output_block != NULL) << "Missing output for view copy";
  DCHECK(TupleSchema::AreEqual(result_schema_, output_block->schema(), false))
      << "Expected: " << result_schema_.GetHumanReadableSpecification() << ", "
      << "Got: " << output_block->schema().GetHumanReadableSpecification();
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

rowcount_t ViewCopier::Copy(
    const rowcount_t row_count,
    const View& input_view,
    const rowcount_t output_offset,
    Block* output_block) const {
  return BaseViewCopier::Copy(row_count, input_view, NULL, output_offset,
                              output_block);
}

rowcount_t SelectiveViewCopier::Copy(
    const rowcount_t row_count,
    const View& input_view,
    const rowid_t* input_row_ids,
    const rowcount_t output_offset,
    Block* output_block) const {
  return BaseViewCopier::Copy(row_count, input_view, input_row_ids,
                              output_offset, output_block);
}

}  // namespace supersonic
