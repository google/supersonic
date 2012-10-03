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
// Family of classes for repetitive copying of fixed-schema view(s) in a single
// call. Supports copying of projected columns only.

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_VIEW_COPIER_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_VIEW_COPIER_H_

#include <vector>
using std::vector;

#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

class Block;
class BoundMultiSourceProjector;
class BoundSingleSourceProjector;
class TupleSchema;
class View;

// Copies a single source view into a destination block. Source view columns
// subject to copy can be optionally selected with a projector.
class ViewCopier {
 public:
  // Variant that copies all columns from a given schema.
  ViewCopier(
      const TupleSchema& input_schema, const TupleSchema& output_schema,
      const RowSelectorType row_selector_type, bool deep_copy);

  // Variant that only copies projected columns; input and output schemas
  // are given by the bound projector, which must outlive this instance
  // of ViewCopier.
  ViewCopier(
      const BoundSingleSourceProjector* projector,
      const RowSelectorType row_selector_type,
      bool deep_copy);

  // Returns the number of rows successfully copied. Can be less than row_count
  // if memory allocation for variable-length types fails.
  rowcount_t Copy(
      const rowcount_t row_count,
      const View& input_view,
      const rowid_t* input_row_ids,
      const rowcount_t output_offset,
      Block* output_block) const;

 private:
  void CreateColumnCopiers(
      const TupleSchema& input_schema,
      const TupleSchema& output_schema,
      const RowSelectorType row_selector_type,
      bool deep_copy);

  const BoundSingleSourceProjector* projector_;  // optional projector.
  vector<ColumnCopier> column_copiers_;
};

// Copies multiple source views into a destination block. A projector must
// be supplied to indicate the mapping from many sources to a single
// destination.
class MultiViewCopier {
 public:
  // Input and output schemas are given by the bound projector, which must
  // outlive this instance of ViewMultiCopier
  MultiViewCopier(
      const BoundMultiSourceProjector* projector,
      const RowSelectorType row_selector_type,
      bool deep_copy);

  // Returns the number of rows successfully copied. Can be less than row_count
  // if memory allocation for variable-length types fails.
  rowcount_t Copy(
      const rowcount_t row_count,
      const vector<const View*>& input_views,
      const rowid_t* input_row_ids,
      const rowcount_t output_offset,
      Block* output_block) const;

 private:
  const BoundMultiSourceProjector* projector_;
  vector<ColumnCopier> column_copiers_;
};

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_VIEW_COPIER_H_
