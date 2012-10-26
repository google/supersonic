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

Block* CloneViewAndOptimizeNullabilityWithSomeForcedNullable(
    const View& view,
    const vector<bool>& is_column_forced_nullable) {
  View source(view);
  TupleSchema result_schema =
      GetSchemaWithOptimizedNullabilityWithSomeForcedNullable(
          source, is_column_forced_nullable);
  scoped_ptr<Block> copy(
      new Block(result_schema, HeapBufferAllocator::Get()));
  CHECK(copy->Reallocate(source.row_count()));
  // Shadow the original nullability (true for all columns) with the actual
  // nullability, as stored in copy.
  for (int i = 0; i < source.column_count(); ++i) {
    if (!result_schema.attribute(i).is_nullable()) {
      source.mutable_column(i)->Reset(source.column(i).data(), bool_ptr(NULL));
    }
  }

  // TODO(user): we might need some more systematic way to remove
  // nullability; perhaps an expression.
  const ViewCopier block_copier(
      copy->schema(), copy->schema(), NO_SELECTOR, true);
  CHECK_EQ(source.row_count(),
           block_copier.Copy(source.row_count(), source, NULL, 0,
                             copy.get()));
  return copy.release();
}

Block* CloneViewAndOptimizeNullability(const View& view) {
  return CloneViewAndOptimizeNullabilityWithSomeForcedNullable(
      view, vector<bool>(view.schema().attribute_count(), false));
}

}  // namespace internal

}  // namespace supersonic
