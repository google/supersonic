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
// Family of CopyColumn functions.

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_COPY_COLUMN_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_COPY_COLUMN_H_

#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class Column;
class OwnedColumn;

// Public interface.

enum RowSelectorType {
  // Row selector is not used.
  NO_SELECTOR = 0,
  // Row selector is used to match input rows, output is sequential.
  INPUT_SELECTOR = 1
};

// Prototype of a function that copies (a part of) input Column into
// a selected region of output OwnedColumn. Output column must be allocated.
// selected_row_ids is a sequence of row_count ids that select a subset of
// input rows to copy from. It must be present if the selector type is
// INPUT_SELECTOR, and must be absent if the selector type is NO_SELECTOR.
// The destination_offset indicates where sequential writes should start.
// Returns the number rows successfully copied. It can be less than row_count
// iff the data being copied is of variable-length type and the destination
// arena can't accommodate a copy of a variable-length data buffer.
typedef rowcount_t (*ColumnCopier)(
    const rowcount_t row_count,
    const Column& source,
    const rowid_t* const selected_row_ids,
    const rowcount_t destination_offset,
    OwnedColumn* const destination);

// Returns the specialization of a copy column function appropriate for the
// arguments given. row_selector_type should be different from NO_SELECTOR iff
// the function is intended to be used with a non-NULL selector_row_ids vector.
// If deep_copy is false, values from variable-length typed columns (STRING and
// BINARY) are not be copied but instead only references (StringPiece) are
// created. Use carefully, as in this case values from input column must outlive
// the output column.
ColumnCopier ResolveCopyColumnFunction(
    const DataType type,
    const Nullability input_nullability,
    const Nullability output_nullability,
    const RowSelectorType row_selector_type,
    bool deep_copy);

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_COPY_COLUMN_H_
