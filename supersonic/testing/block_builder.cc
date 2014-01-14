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

#include "supersonic/testing/block_builder.h"

#include <stddef.h>

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/view_copier.h"

namespace supersonic {

namespace {

TupleSchema GetSchemaWithOptimizedNullabilityWithSomeForcedNullable(
    const View& view, const vector<bool>& is_forced_null) {
  TupleSchema schema;
  for (size_t i = 0; i < view.schema().attribute_count(); ++i) {
    const Attribute& attribute = view.schema().attribute(i);
    Nullability nullability = attribute.nullability();
    if (nullability == NULLABLE) {
      bool_const_ptr is_null = view.column(i).is_null();
      nullability = NOT_NULLABLE;  // Temporarily.
      if (is_null != NULL) {
        for (size_t j = 0; j < view.row_count(); ++j) {
          if (*is_null) {
            nullability = NULLABLE;  // Indeed.
            break;
          }
          ++is_null;
        }
      }
    }
    if (is_forced_null[i]) {
      nullability = NULLABLE;
    }
    schema.add_attribute(
        Attribute(attribute.name(), attribute.type(), nullability));
  }
  return schema;
}

}  // namespace

namespace internal {

Block* CloneView(const View& view) {
  std::unique_ptr<Block> copy(
      new Block(view.schema(), HeapBufferAllocator::Get()));
  CHECK(copy->Reallocate(view.row_count()));
  const ViewCopier block_copier(view.schema(), true);
  CHECK_EQ(view.row_count(),
           block_copier.Copy(view.row_count(), view, 0, copy.get()));
  return copy.release();
}

Block* CloneViewAndOptimizeNullability(
    const View& view,
    const vector<bool>& is_column_forced_nullable) {
  // TODO(user): we might need some more systematic way to remove
  // nullability; perhaps an expression.
  TupleSchema result_schema =
      GetSchemaWithOptimizedNullabilityWithSomeForcedNullable(
          view, is_column_forced_nullable);
  View shadow(result_schema);
  // Copy view to the shadow, but override the original nullability (true for
  // all columns) with the actual nullability, as we want stored in copy.
  for (int i = 0; i < shadow.column_count(); ++i) {
    Column* column = shadow.mutable_column(i);
    if (result_schema.attribute(i).is_nullable()) {
      column->ResetFrom(view.column(i));
    } else {
      column->Reset(view.column(i).data(), bool_ptr(NULL));
    }
  }
  shadow.set_row_count(view.row_count());
  return CloneView(shadow);
}

}  // namespace internal

}  // namespace supersonic
