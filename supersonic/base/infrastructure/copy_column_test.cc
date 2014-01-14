// Copyright 2010 Google Inc. All Rights Reserved.
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

#include "supersonic/base/infrastructure/copy_column.h"

#include <stddef.h>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/row.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "gtest/gtest.h"

namespace supersonic {

class CopyColumnTest : public testing::Test {
 public:
  void SetUpColumnPair(DataType type, Nullability input, Nullability output) {
    CreateColumnPair(type, input, output);
    FillInputColumn();
  }

  void CopyInputColumnToOutputColumn(
      rowcount_t row_count,
      rowcount_t output_offset,
      const rowid_t* input_row_ids,
      RowSelectorType row_selector_type,
      bool deep_copy) {
    ASSERT_TRUE(input_row_ids != NULL || row_selector_type == NO_SELECTOR);
    ColumnCopier copy_column_func = ResolveCopyColumnFunction(
        schema_.attribute(0).type(),
        schema_.attribute(1).nullability(),
        schema_.attribute(1).nullability(),
        row_selector_type,
        deep_copy);
    const rowcount_t result = copy_column_func(
        row_count, input_->column(0), input_row_ids,
        output_offset, block_->mutable_column(1));
    ASSERT_EQ(row_count, result);
  }

  TupleSchema schema_;
  scoped_ptr<Block> block_;
  scoped_ptr<View> input_;
  scoped_ptr<View> output_;

 private:
  void CreateColumnPair(DataType type,
                        Nullability input_nullability,
                        Nullability output_nullability) {
    TupleSchema input_schema(
        TupleSchema::Singleton("input", type, input_nullability));
    TupleSchema output_schema(
        TupleSchema::Singleton("output", type, output_nullability));

    schema_.add_attribute(input_schema.attribute(0));
    schema_.add_attribute(output_schema.attribute(0));

    block_.reset(new Block(schema_, HeapBufferAllocator::Get()));
    block_->Reallocate(Cursor::kDefaultRowCount);

    input_.reset(new View(input_schema));
    input_->mutable_column(0)->Reset(block_->column(0).data(),
                                     block_->column(0).is_null());
    input_->set_row_count(Cursor::kDefaultRowCount);

    // A view over the output column.
    output_.reset(new View(output_schema));
    output_->mutable_column(0)->ResetFrom(block_->column(1));
    output_->set_row_count(Cursor::kDefaultRowCount);
  }

  void FillInputColumn() {
    switch (schema_.attribute(0).type()) {
      case INT64: {
        // Sets column values to 0, 1, 2, ..., row_capacity-1
        int64* p = block_->mutable_column(0)->mutable_typed_data<INT64>();
        for (rowcount_t i = 0; i < block_->row_capacity(); i++, p++)
          *p = i;
        break;
      }
      case DOUBLE: {
        // Sets column values to 1, 1/2, 1/3, ..., 1/row_capacity
        double* p = block_->mutable_column(0)->mutable_typed_data<DOUBLE>();
        for (rowcount_t i = 0; i < block_->row_capacity(); i++, p++)
          *p = 1 / (i + 1);
        break;
      }
      case STRING: {
        // Sets column values to
        // "00000000", "00000001", ..., atoi(row_capacity-1)
        StringPiece* p = block_->mutable_column(0)->
            mutable_typed_data<STRING>();
        Arena* arena = block_->mutable_column(0)->arena();
        for (rowcount_t i = 0; i < block_->row_capacity(); i++, p++) {
          string s = StringPrintf("%08" GG_LL_FORMAT "d", i);
          const char* location = arena->AddStringPieceContent(s);
          ASSERT_TRUE(location != NULL);
          p->set(location, s.length());
        }
        break;
      }
      default:
        LOG(FATAL) << "Unsupported DataType in test: "
                   << schema_.attribute(0).type();
    }
  }
};

TEST_F(CopyColumnTest,
       BasicTestInputIsNotNullableOutputIsNotNullable) {
  SetUpColumnPair(INT64, NOT_NULLABLE, NOT_NULLABLE);
  CopyInputColumnToOutputColumn(block_->row_capacity(), 0,
                                NULL, NO_SELECTOR, true);
  EXPECT_VIEWS_EQUAL(*input_, *output_);
}

TEST_F(CopyColumnTest,
       BasicTestInputIsNotNullableOutputIsNullable) {
  SetUpColumnPair(INT64, NOT_NULLABLE, NULLABLE);
  // Set output is_null column to non-0s to verify it's overwritten with 0s.
  bit_pointer::FillWithTrue(block_->mutable_column(1)->mutable_is_null(),
                            block_->row_capacity());
  CopyInputColumnToOutputColumn(block_->row_capacity(), 0,
                                NULL, NO_SELECTOR, true);
  EXPECT_VIEWS_EQUAL(*input_, *output_);
}

TEST_F(CopyColumnTest,
       BasicTestInputIsNotNullableOutputIsNullableWithOffset) {
  SetUpColumnPair(INT64, NOT_NULLABLE, NULLABLE);
  // Set output is_null column to non-0s to verify it's overwritten with 0s.
  bit_pointer::FillWithTrue(block_->mutable_column(1)->mutable_is_null(),
                            block_->row_capacity());
  CopyInputColumnToOutputColumn(block_->row_capacity() - 4, 4,
                                NULL, NO_SELECTOR, true);
  for (int i = 0; i < block_->row_capacity() - 4; ++i) {
    EXPECT_EQ(block_->view().column(0).typed_data<INT64>()[i],
              block_->view().column(1).typed_data<INT64>()[i+4]);
    EXPECT_EQ(false, block_->view().column(1).is_null()[i+4]);
  }
}

TEST_F(CopyColumnTest,
       InputSelectorTest) {
  SetUpColumnPair(INT64, NOT_NULLABLE, NOT_NULLABLE);
  const rowcount_t num_row_ids = block_->row_capacity() / 5;
  scoped_ptr<rowid_t[]> row_ids(new rowid_t[num_row_ids]);
  for (rowid_t i = 0; i < num_row_ids; ++i) {
    row_ids[i] = i * 5;
  }
  CopyInputColumnToOutputColumn(
      num_row_ids, 0, row_ids.get(), INPUT_SELECTOR, true);
  for (rowid_t i = 0; i < num_row_ids; ++i) {
    EXPECT_ROWS_EQUAL(Row(*input_, row_ids[i]), Row(*output_, i))
        << " i = " << i;
  }
}

TEST_F(CopyColumnTest,
       ShallowCopy) {
  SetUpColumnPair(STRING, NOT_NULLABLE, NOT_NULLABLE);
  CopyInputColumnToOutputColumn(block_->row_capacity(), 0,
                                NULL, NO_SELECTOR, false);
  EXPECT_VIEWS_EQUAL(*input_, *output_);
  for (int i = 0; i < input_->row_count(); ++i) {
    // Also check if StringPieces are pointing to the same string.
    StringPiece input_str = input_->column(0).typed_data<STRING>()[i];
    StringPiece output_str = output_->column(0).typed_data<STRING>()[i];
    EXPECT_EQ(input_str.data(), output_str.data());
    // Just in case.
    EXPECT_EQ(input_str.length(), output_str.length());
  }
}

TEST_F(CopyColumnTest, LimitedMemoryShouldCausePartialSuccess) {
  SetUpColumnPair(STRING, NOT_NULLABLE, NOT_NULLABLE);
  ColumnCopier copy_column_func = ResolveCopyColumnFunction(
      schema_.attribute(0).type(),
      schema_.attribute(1).nullability(),
      schema_.attribute(1).nullability(),
      NO_SELECTOR,
      true);
  MemoryLimit memory_limit(block_->row_capacity() * sizeof(StringPiece) + 512);
  Block result_block(input_->schema(), &memory_limit);
  ASSERT_TRUE(result_block.Reallocate(block_->row_capacity()));
  EXPECT_EQ(512, memory_limit.GetQuota() - memory_limit.GetUsage());
  const rowcount_t result = copy_column_func(block_->row_capacity(),
                                             input_->column(0), NULL, 0,
                                             result_block.  mutable_column(0));
  EXPECT_EQ(64, result);
}

// TODO(user): More tests to follow.

}  // namespace supersonic
