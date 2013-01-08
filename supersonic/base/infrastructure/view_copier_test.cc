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

#include "supersonic/base/infrastructure/view_copier.h"

#include <stddef.h>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparable_view.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/row.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

class ViewCopierTest : public testing::Test {
 public:
  void SetUp() {
    // BlockBuilder should set 3rd column as not nullable.
    input_.reset(
        BlockBuilder<INT64, STRING, STRING>().
        AddRow(1,  "a", "b").
        AddRow(__, "a", "b").
        AddRow(1,   __, "b").
        AddRow(__,  __, "b").
        Build());

    TupleSchema output_schema;
    output_schema.add_attribute(Attribute("c1", INT64, NULLABLE));
    output_schema.add_attribute(Attribute("c2", STRING, NULLABLE));
    output_schema.add_attribute(Attribute("c3", STRING, NOT_NULLABLE));
    output_.reset(new Block(output_schema, HeapBufferAllocator::Get()));
    output_->Reallocate(4);

    single_source_projector_.reset(
        new BoundSingleSourceProjector(input_->schema()));
    single_source_projector_->Add(0);
    single_source_projector_->Add(2);

    projected_output_.reset(
        new Block(single_source_projector_->result_schema(),
                  HeapBufferAllocator::Get()));
    projected_output_->Reallocate(4);
  }

  scoped_ptr<Block> input_, output_, projected_output_;
  scoped_ptr<BoundSingleSourceProjector> single_source_projector_;
};

TEST_F(ViewCopierTest,
       ViewCopierSimpleCopy) {
  ViewCopier view_copier(input_->schema(), true);
  EXPECT_EQ(4, view_copier.Copy(4, input_->view(), 0, output_.get()));
  EXPECT_VIEWS_EQUAL(input_->view(), output_->view());
  EXPECT_TRUE(VariableSizeColumnIsACopy(input_->column(1),
                                        output_->column(1), 4));
  EXPECT_TRUE(VariableSizeColumnIsACopy(input_->column(2),
                                        output_->column(2), 4));
}

TEST_F(ViewCopierTest,
       ViewCopierCopySubsetOfRowsIntoOffset) {
  ViewCopier view_copier(input_->schema(), true);
  EXPECT_EQ(3, view_copier.Copy(3, input_->view(), 1, output_.get()));
  View expected(input_->view(), 0, 3);
  View observed(output_->view(), 1, 3);
  EXPECT_VIEWS_EQUAL(expected, observed);
  EXPECT_NE(Row(input_->view(), 0), Row(output_->view(), 0));
}

TEST_F(ViewCopierTest,
       ViewCopierCopyUsingSelectionVector) {
  SelectiveViewCopier view_copier(input_->schema(), true);
  rowid_t selection_vector[2] = { 1, 3 };
  EXPECT_EQ(2, view_copier.Copy(
      2, input_->view(), selection_vector, 2, output_.get()));
  EXPECT_EQ(Row(input_->view(), 1), Row(output_->view(), 2));
  EXPECT_EQ(Row(input_->view(), 3), Row(output_->view(), 3));
  EXPECT_NE(Row(input_->view(), 0), Row(output_->view(), 0));
  EXPECT_NE(Row(input_->view(), 1), Row(output_->view(), 1));
}

TEST_F(ViewCopierTest,
       ViewCopierCopyAlongProjection) {
  ViewCopier view_copier(single_source_projector_.get(), true);
  EXPECT_EQ(4, view_copier.Copy(
      4, input_->view(), 0, projected_output_.get()));
  // Set up a view over input block's 1st and 3rd column.
  View projected_input_view(projected_output_->schema());
  projected_input_view.mutable_column(0)->ResetFrom(input_->column(0));
  projected_input_view.mutable_column(1)->ResetFrom(input_->column(2));
  projected_input_view.set_row_count(4);
  EXPECT_VIEWS_EQUAL(projected_input_view, projected_output_->view());
  EXPECT_TRUE(VariableSizeColumnIsACopy(input_->column(2),
                                        projected_output_->column(1), 4));
}

}  // namespace supersonic
