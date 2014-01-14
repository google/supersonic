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

#include "supersonic/cursor/infrastructure/row_copier.h"

#include <memory>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/infrastructure/row.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparable_view.h"
#include "supersonic/testing/comparators.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

Block* CreateOutputBlock(size_t row_count) {
  // 3rd output column explicitly set as nullable != 3rd input column.
  TupleSchema output_schema;
  output_schema.add_attribute(Attribute("c1", INT64, NULLABLE));
  output_schema.add_attribute(Attribute("c2", STRING, NULLABLE));
  output_schema.add_attribute(Attribute("c3", STRING, NULLABLE));
  std::unique_ptr<Block> result(
      new Block(output_schema, HeapBufferAllocator::Get()));
  CHECK(result->Reallocate(row_count));
  return result.release();
}

class RowCopierTest : public testing::Test {};

TEST_F(RowCopierTest, RowCopierSimpleCopy) {
  std::unique_ptr<Block> input(BlockBuilder<INT64, STRING, STRING>()
                                   .AddRow(0, "a", "b")
                                   .AddRow(1, "c", "d")
                                   .AddRow(2, "e", "f")
                                   .Build());
  std::unique_ptr<Block> output(CreateOutputBlock(10));
  RowCopier<
      DirectRowSourceReader<RowSourceAdapter>,
      DirectRowSourceWriter<RowSinkAdapter> > copier(output->schema(), true);
  DirectRowSourceReader<RowSourceAdapter> reader;
  DirectRowSourceWriter<RowSinkAdapter> writer;
  RowSourceAdapter source(input->view(), 2);
  RowSinkAdapter sink(output.get(), 7);
  EXPECT_EQ(true, copier.Copy(reader, source, writer, &sink));
  View expected(input->view(), 2, 1);
  View observed(output->view(), 7, 1);
  EXPECT_VIEWS_EQUAL(expected, observed);
  EXPECT_TRUE(VariableSizeColumnIsACopy(expected.column(1),
                                        observed.column(1), 1));
  EXPECT_TRUE(VariableSizeColumnIsACopy(expected.column(2),
                                        observed.column(2), 1));
}

TEST_F(RowCopierTest, ViewCopierCopyAlongProjection) {
  std::unique_ptr<Block> input(BlockBuilder<INT64, STRING, STRING>()
                                   .AddRow(0, "a", "b")
                                   .AddRow(1, "c", "d")
                                   .AddRow(2, "e", "f")
                                   .Build());
  BoundSingleSourceProjector projector(input->schema());
  projector.Add(0);
  projector.Add(2);
  Block output(projector.result_schema(), HeapBufferAllocator::Get());
  CHECK(output.Reallocate(10));

  RowCopierWithProjector<
      DirectRowSourceReader<RowSourceAdapter>,
      DirectRowSourceWriter<RowSinkAdapter> > copier(&projector, true);
  DirectRowSourceReader<RowSourceAdapter> reader;
  DirectRowSourceWriter<RowSinkAdapter> writer;
  RowSourceAdapter source(input->view(), 2);
  RowSinkAdapter sink(&output, 7);
  EXPECT_EQ(true, copier.Copy(reader, source, writer, &sink));
  View projected_input(output.schema());
  projector.Project(input->view(), &projected_input);
  projected_input.set_row_count(3);
  View expected(projected_input, 2, 1);
  View observed(output.view(), 7, 1);

  EXPECT_VIEWS_EQUAL(expected, observed);
  EXPECT_TRUE(VariableSizeColumnIsACopy(expected.column(1),
                                        observed.column(1), 1));
}

TEST_F(RowCopierTest, MultiViewCopierCopyAlongProjection) {
  std::unique_ptr<Block> input(BlockBuilder<INT64, STRING, STRING>()
                                   .AddRow(0, "a", "b")
                                   .AddRow(1, "c", "d")
                                   .AddRow(2, "e", "f")
                                   .Build());
  // Use the same source twice, project it's 1rd and 3st column, taking one
  // from each 'copy' of the source.
  BoundMultiSourceProjector projector(
      util::gtl::Container(&input->schema(), &input->schema()).
      As<vector<const TupleSchema*> >());
  projector.Add(0, 0);
  projector.Add(1, 2);
  Block output(projector.result_schema(), HeapBufferAllocator::Get());
  CHECK(output.Reallocate(10));

  MultiRowCopier<
      DirectRowSourceReader<RowSourceAdapter>,
      DirectRowSourceWriter<RowSinkAdapter> > copier(&projector, true);
  DirectRowSourceReader<RowSourceAdapter> reader;
  DirectRowSourceWriter<RowSinkAdapter> writer;
  RowSourceAdapter source(input->view(), 2);
  RowSinkAdapter sink(&output, 7);
  EXPECT_EQ(true, copier.Copy(
      util::gtl::Container(&reader, &reader).
          As<vector<const DirectRowSourceReader<RowSourceAdapter>*> >(),
      util::gtl::Container(&source, &source).
          As<vector<const RowSourceAdapter*> >(),
      writer, &sink));

  // Set up a view over input block's 3rd and 2nd column.
  View projected_input(output.schema());
  projected_input.mutable_column(0)->ResetFrom(input->column(0));
  projected_input.mutable_column(1)->ResetFrom(input->column(2));
  projected_input.set_row_count(3);
  View expected(projected_input, 2, 1);
  View observed(output.view(), 7, 1);
  EXPECT_VIEWS_EQUAL(expected, observed);
  EXPECT_TRUE(VariableSizeColumnIsACopy(expected.column(1),
                                        observed.column(1), 1));
}

}  // namespace supersonic
