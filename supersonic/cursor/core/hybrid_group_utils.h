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

#ifndef SUPERSONIC_CURSOR_CORE_HYBRID_GROUP_UTILS_H_
#define SUPERSONIC_CURSOR_CORE_HYBRID_GROUP_UTILS_H_

#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/utils/pointer_vector.h"

namespace supersonic {

// Input transformation for hybrid group. The transformation is defined by the
// original group key and N projectors each defining one of N (output) column
// groups. The input data is copied N times, once for each column group. These
// are not exact copies of the input data:
// 1) columns may be reordered, omitted, duplicated or renamed (consistently
// within the whole output),
// 2) some data is replaced with NULLs.
//
// Each column group gets its data intact only in "its copy" of the data. In all
// other copies the column's data is replaced with NULLs. Example: if
// group_by_columns selects "K" and there are column groups "a", "b" and
// "c,a->a2", then transforming input:
//
// K a b c d
// 1 2 3 4 5
// 6 7 8 9 0
//
// will produce (_ means NULL):
//
// K a b c a2
// 1 2 _ _ _
// 6 7 _ _ _
// 1 _ 3 _ _
// 6 _ 8 _ _
// 1 _ _ 4 2
// 6 _ _ 9 7
//
// This transformation would be used when grouping on K, with DISTINCT
// aggregations on "a" and "b", non-DISTINCT aggregations on "c" and "a", and no
// aggregations on "d". For the initial aggregation columns "a" and "b" would be
// added to the key (so the key would be "K,a,b"). This allows to eliminate
// duplicates within those columns before final grouping.  Non-DISTINCT
// aggregated columns - "c" and "a2" (renamed to allow processing independently
// from its DISTINCT use) - are put in the same column group. They are not part
// of the extended pregroup key.
//
// There are no guarantees about output data order. The transformation is cheap
// in the sense that no actual copying of the input data is performed.
// Everything is achieved by performing various projections on input blocks and
// the internal nulls_block_. This cursor only allocates memory to grow its
// internal nulls_block_ up to the biggest used max_row_count.
//
// Takes ownership of group_by_columns and child.
FailureOrOwned<Cursor> BoundHybridGroupTransform(
    const SingleSourceProjector* group_by_columns,
    const util::gtl::PointerVector<const SingleSourceProjector>&
        column_group_projectors,
    BufferAllocator* allocator,
    Cursor* child);

// Debug options for BoundHybridGroupAggregate.
class HybridGroupDebugOptions {
 public:
  HybridGroupDebugOptions()
      : return_transformed_input_(false) {}

  bool return_transformed_input() const {
    return return_transformed_input_;
  }

  // Makes BoundHybridGroupAggregate exit early returning the transformed input,
  // without performing further stages.
  HybridGroupDebugOptions* set_return_transformed_input(bool value) {
    return_transformed_input_ = value;
    return this;
  }

 private:
  bool return_transformed_input_;
  DISALLOW_COPY_AND_ASSIGN(HybridGroupDebugOptions);
};

// Make selected (by the projector) columns not nullable by wrapping them in a
// IFNULL(.., 0) expression. The columns should have numeric data type. The
// projector is used only for selecting which columns should be made
// not nullable. The original input schema is preserved with exception of
// nullability.
// Takes ownership of projector and input.
FailureOrOwned<Cursor> MakeSelectedColumnsNotNullable(
    const SingleSourceProjector* selection_projector,
    BufferAllocator* allocator,
    Cursor* input);

// Extend the schema by an INT32 column filled with 0-es. This is used by hybrid
// GROUP BY to properly compute COUNT(*) in face of rows being replicated N
// times by BoundHybridGroupTransform. COUNT(*) is replaced with
// COUNT(new_column_name).
// Takes ownership of input.
FailureOrOwned<Cursor> ExtendByConstantColumn(
    const string& new_column_name,
    BufferAllocator* allocator,
    Cursor* input);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_HYBRID_GROUP_UTILS_H_
