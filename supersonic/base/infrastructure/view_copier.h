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
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

class Block;
class BoundMultiSourceProjector;
class BoundSingleSourceProjector;
class TupleSchema;
class View;


//
// All known uses in test code:
//  1. In BlockBuilder::Build, to make the copy of the block data. Uses
//     NO_SELECTOR, no projector, and deep copy. Does not alter schema.
//  2. In OperationTesting::DeepCopierCursor, to implement the test cursor
//     wrapper that always deep-copies. Uses NO_SELECTOR, no projector, and
//     deep copy. Does not alter the schema.
//  3. In RepeatingBlock, to create 'big' blocks by repeating some input data
//     multiple times. Uses NO_SELECTOR, no projector, and deep copy. Does not
//     alter the schema.

// Don't use directly.
class BaseViewCopier {
 protected:
  BaseViewCopier(const TupleSchema& source_schema,
                 const TupleSchema& result_schema,
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
  void CreateColumnCopiers(const TupleSchema& source_schema,
                           const TupleSchema& result_schema,
                           const RowSelectorType row_selector_type,
                           bool deep_copy);

  TupleSchema source_schema_;
  TupleSchema result_schema_;
  const BoundSingleSourceProjector* projector_;  // optional projector.
  vector<ColumnCopier> column_copiers_;
};

// Copies a single source view into a destination block. Source view columns
// subject to copy can be optionally selected with a projector.
//
// All known uses of this class in non-test code:
//  1. In BufferedSplitter, to make copies of buffered streams. Uses deep copy.
//     Does not alter the schema.
//  2. In Bigtable LookupJoin, to copy the columns from the left side into
//     the join result. Uses deep copy.
//  3. In Table, to append subsequent views to the table. Uses deep copy.
//
class ViewCopier : public BaseViewCopier {
 public:
  // Copies all columns from a given schema.
  ViewCopier(const TupleSchema& schema, bool deep_copy);

  // Variant that only copies projected columns; input and output schemas
  // are given by the bound projector, which must outlive this instance.
  // Apparently, not currently used.
  ViewCopier(const BoundSingleSourceProjector* projector, bool deep_copy);

  // Returns the number of rows successfully copied. Can be less than row_count
  // if memory allocation for variable-length types fails.
  rowcount_t Copy(
      const rowcount_t row_count,
      const View& input_view,
      const rowcount_t output_offset,
      Block* output_block) const;
};

// To make copies with a selection vector.
//
// All known uses of this class in non-test code:
//  1. In Filter. Used to compact the results of the filter into a result block.
//     Uses projector, and both deep and shallow copy variants (depending on
//     whether the input cursor is advanced or not).
//  2. In ForeignFilter (which is a special purpose semin-join cursor; perhaps
//     not needed). Uses to flatten out the left side of the semi-join. Uses
//     projector, and shallow copy.
//  3. In HashJoin. Used (1) to copy the left-hand-side columns for matched
//     rows into the join result. Uses (projector, shallow copy). (2) to copy
//     matched rows from the index (w/ no projector, and shallow copy;
//     downgrades from NOT_NULLABLE to NULLABLE if the join is LEFT_OUTER).
//  4. In RowidMergeJoin, to flatten out the left side of the join. Uses
//     projector, and shallow copy.
//  5. In BigtableLookup, to copy the lookup results into the result cursor.
//     Uses no projector, and shallow copy. Does not alter schema. Quite
//     possibly an overkill, as it could be replaced by a row-wise interface,
//     and the RPC costs of the BT lookup far outweight the benefits of
//     columnar processing.
//  6. In Bigtable LookupJoin, to copy results of the lookup to their matching
//     rows (uses projector, and shallow copy; alters nullability).
//  7. In ViewCursorWithSelector, to make data copies as it traverses selected
//     rows from the input view. Uses no projector, and shallow copy. Does not
//     alter the schema.
//
class SelectiveViewCopier : public BaseViewCopier {
 public:
  // Copies all columns, without changing the schema.
  SelectiveViewCopier(const TupleSchema& schema, bool deep_copy);

  // Copies all columns from a given source schema to the given result schema.
  // The schemas must match by type, but may differ by nullability. For any
  // NOT_NULLABLE column in the result_schema that has a NULLABLE counterpart
  // in the source schema, the caller must ensure that the input_row_ids vector
  // never refers to any rows with NULL values (or bad things will happen).
  SelectiveViewCopier(
      const TupleSchema& source_schema,
      const TupleSchema& result_schema,
      bool deep_copy);

  // Variant that only copies projected columns; input and output schemas
  // are given by the bound projector, which must outlive this instance.
  SelectiveViewCopier(
      const BoundSingleSourceProjector* projector,
      bool deep_copy);

  // Returns the number of rows successfully copied. Can be less than row_count
  // if memory allocation for variable-length types fails.
  rowcount_t Copy(
      const rowcount_t row_count,
      const View& input_view,
      const rowid_t* input_row_ids,
      const rowcount_t output_offset,
      Block* output_block) const;
};

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_VIEW_COPIER_H_
