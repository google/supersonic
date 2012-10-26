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

#include "supersonic/testing/view_comparator.h"

#include <stddef.h>

#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {


FailureOrVoid Compare(const View& left, const View& right) {
  // TODO(user): There have to be schema comparison somewhere out there
  // already use it here.
  // CompareSchemas(left.schema(), right.schema());
  if (left.row_count() != right.row_count()) {
    THROW(new Exception(
        ERROR_TOO_FEW_ROWS,
        StrCat("Different number of rows. Left: ", left.row_count(),
               ", Right: ", right.row_count())));
  }
  size_t row_count = left.row_count();
  size_t column_count = left.schema().attribute_count();
  for (int column_index = 0; column_index < column_count; column_index++) {
    const Column& left_c = left.column(column_index);
    const Column& right_c = right.column(column_index);
    EqualityComparator comparator = GetEqualsComparator(
        left.schema().attribute(column_index).type(),
        right.schema().attribute(column_index).type(),
        !left.schema().attribute(column_index).is_nullable(),
        !right.schema().attribute(column_index).is_nullable());
    for (int row_i = 0; row_i < row_count; row_i++) {
      VariantConstPointer left_value =
          (left_c.is_null() != NULL && left_c.is_null()[row_i])
              ? NULL
              : left_c.data_plus_offset(row_i);
      VariantConstPointer right_value =
          (right_c.is_null() != NULL && right_c.is_null()[row_i])
              ? NULL
              : right_c.data_plus_offset(row_i);
      if (!comparator(left_value, right_value)) {
        THROW(new Exception(
            ERROR_UNKNOWN_ERROR,
            StringPrintf("Values not equal. Column: %d, Row: %d",
                         column_index, row_i)));
      }
    }
  }
  return Success();
}

FailureOrVoid Compare(const Block& left, const Block& right) {
  return Compare(left.view(), right.view());
}

FailureOrVoid Compare(const View& view, const Block& block) {
  return Compare(view, block.view());
}

FailureOrVoid Compare(const Block& block, const View& view) {
  return Compare(block.view(), view);
}

}  // namespace supersonic

