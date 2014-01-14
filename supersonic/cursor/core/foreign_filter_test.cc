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

#include "supersonic/cursor/core/foreign_filter.h"

#include <memory>

#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class ForeignFilterTest : public testing::Test {
 protected:
  virtual void CreateSampleData() {
    sample_filter_builder_.AddRow(1)
                          .AddRow(3)
                          .AddRow(5)
                          .AddRow(7)
                          .AddRow(9);
    sample_input_builder_.AddRow(1,  "A")
                         .AddRow(1,  "B")
                         .AddRow(3,  "C")
                         .AddRow(3,  "D")
                         .AddRow(5,  "E")
                         .AddRow(5,  "F")
                         .AddRow(7,  "G")
                         .AddRow(9,  "H");
    sample_output_builder_.AddRow(0,  "A")
                          .AddRow(0,  "B")
                          .AddRow(1,  "C")
                          .AddRow(1,  "D")
                          .AddRow(2,  "E")
                          .AddRow(2,  "F")
                          .AddRow(3,  "G")
                          .AddRow(4,  "H");
  }

  TestDataBuilder<kRowidDatatype> sample_filter_builder_;
  TestDataBuilder<kRowidDatatype, STRING> sample_input_builder_;
  TestDataBuilder<kRowidDatatype, STRING> sample_output_builder_;
};

TEST_F(ForeignFilterTest, IdentityFilterIsPassThru) {
  OperationTest test;
  TestDataBuilder<kRowidDatatype, STRING> input_builder;
  input_builder.AddRow(5,  "A")
               .AddRow(7,  "B")
               .AddRow(7,  "C")
               .AddRow(7,  "D")
               .AddRow(9,  "E")
               .AddRow(10, "F")
               .AddRow(10, "G")
               .AddRow(11, "H")
               .AddRow(12, "I")
               .AddRow(15, "J");

  test.AddInput(TestDataBuilder<kRowidDatatype>()
                .AddRow(0)
                .AddRow(1)
                .AddRow(2)
                .AddRow(3)
                .AddRow(4)
                .AddRow(5)
                .AddRow(6)
                .AddRow(7)
                .AddRow(8)
                .AddRow(9)
                .AddRow(10)
                .AddRow(11)
                .AddRow(12)
                .AddRow(13)
                .AddRow(14)
                .AddRow(15)
                .AddRow(16)
                .Build());
  test.AddInput(input_builder.Build());
  test.SetExpectedResult(input_builder.Build());
  test.Execute(
      ForeignFilter(
          ProjectAttributeAt(0),
          ProjectAttributeAt(0),
          test.input_at(0),
          test.input_at(1)));
}

TEST_F(ForeignFilterTest, IdentityFilterIsPassThru2) {
  OperationTest test;
  test.AddInput(TestDataBuilder<kRowidDatatype>()
                .AddRow(7)
                .Build());
  test.AddInput(TestDataBuilder<kRowidDatatype, STRING>()
                .AddRow(7,  "A")
                .AddRow(7,  "B")
                .AddRow(7,  "C")
                .AddRow(7,  "D")
                .AddRow(7,  "E")
                .AddRow(7,  "F")
                .AddRow(7,  "G")
                .AddRow(7,  "H")
                .Build());
  test.SetExpectedResult(TestDataBuilder<kRowidDatatype, STRING>()
                         .AddRow(0,  "A")
                         .AddRow(0,  "B")
                         .AddRow(0,  "C")
                         .AddRow(0,  "D")
                         .AddRow(0,  "E")
                         .AddRow(0,  "F")
                         .AddRow(0,  "G")
                         .AddRow(0,  "H")
                         .Build());
  test.Execute(
      ForeignFilter(
          ProjectAttributeAt(0),
          ProjectAttributeAt(0),
          test.input_at(0),
          test.input_at(1)));
}

TEST_F(ForeignFilterTest, AlternatingEmpty) {
  OperationTest test;
  test.AddInput(TestDataBuilder<kRowidDatatype>()
                .AddRow(0)
                .AddRow(2)
                .AddRow(4)
                .AddRow(6)
                .AddRow(8)
                .Build());
  test.AddInput(TestDataBuilder<kRowidDatatype, STRING>()
                .AddRow(1,  "A")
                .AddRow(1,  "B")
                .AddRow(3,  "C")
                .AddRow(3,  "D")
                .AddRow(5,  "E")
                .AddRow(5,  "F")
                .AddRow(7,  "G")
                .AddRow(7,  "H")
                .Build());
  test.SetExpectedResult(TestDataBuilder<kRowidDatatype, STRING>()
                         .Build());
  test.Execute(
      ForeignFilter(
          ProjectAttributeAt(0),
          ProjectAttributeAt(0),
          test.input_at(0),
          test.input_at(1)));
}

TEST_F(ForeignFilterTest, AlternatingMatching) {
  OperationTest test;
  CreateSampleData();
  test.AddInput(sample_filter_builder_.Build());
  test.AddInput(sample_input_builder_.Build());
  test.SetExpectedResult(sample_output_builder_.Build());
  test.Execute(
      ForeignFilter(
          ProjectAttributeAt(0),
          ProjectAttributeAt(0),
          test.input_at(0),
          test.input_at(1)));
}

TEST_F(ForeignFilterTest, AlternatingMatchingWithSpyTransform) {
  CreateSampleData();
  Cursor* filter = sample_filter_builder_.BuildCursor();
  Cursor* input = sample_input_builder_.BuildCursor();

  std::unique_ptr<Cursor> foreign(BoundForeignFilter(0, 0, filter, input));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  foreign->ApplyToChildren(spy_transformer.get());
  foreign.reset(spy_transformer->Transform(foreign.release()));

  std::unique_ptr<Cursor> expected_result(sample_output_builder_.BuildCursor());
  EXPECT_CURSORS_EQUAL(expected_result.release(), foreign.release());
}

TEST_F(ForeignFilterTest, EmptyFilter) {
  OperationTest test;
  test.AddInput(TestDataBuilder<kRowidDatatype>()
                .Build());
  test.AddInput(TestDataBuilder<kRowidDatatype, STRING>()
                .AddRow(1,  "A")
                .AddRow(1,  "B")
                .AddRow(3,  "C")
                .AddRow(3,  "D")
                .AddRow(5,  "E")
                .AddRow(5,  "F")
                .AddRow(7,  "G")
                .AddRow(7,  "H")
                .Build());
  test.SetExpectedResult(TestDataBuilder<kRowidDatatype, STRING>()
                         .Build());
  test.Execute(
      ForeignFilter(
          ProjectAttributeAt(0),
          ProjectAttributeAt(0),
          test.input_at(0),
          test.input_at(1)));
}

TEST_F(ForeignFilterTest, EmptyInput) {
  OperationTest test;
  test.AddInput(TestDataBuilder<kRowidDatatype>()
                .AddRow(0)
                .AddRow(2)
                .AddRow(4)
                .AddRow(6)
                .AddRow(8)
                .Build());
  test.AddInput(TestDataBuilder<kRowidDatatype, STRING>()
                .Build());
  test.SetExpectedResult(TestDataBuilder<kRowidDatatype, STRING>()
                         .Build());
  test.Execute(
      ForeignFilter(
          ProjectAttributeAt(0),
          ProjectAttributeAt(0),
          test.input_at(0),
          test.input_at(1)));
}

TEST_F(ForeignFilterTest, Projectors) {
  OperationTest test;
  test.AddInput(TestDataBuilder<STRING, kRowidDatatype>()
                .AddRow("foo", 1)
                .AddRow("foo", 3)
                .Build());
  test.AddInput(TestDataBuilder<STRING, DOUBLE, kRowidDatatype, STRING>()
                .AddRow("bar", 0.0, 1,  "B")
                .AddRow("bar", 0.0, 3,  "C")
                .AddRow("bar", 0.0, 3,  "D")
                .AddRow("bar", 0.0, 5,  "E")
                .AddRow("bar", 0.0, 5,  "F")
                .Build());
  test.SetExpectedResult(
      TestDataBuilder<STRING, DOUBLE, kRowidDatatype, STRING>()
      .AddRow("bar", 0.0, 0,  "B")
      .AddRow("bar", 0.0, 1,  "C")
      .AddRow("bar", 0.0, 1,  "D")
      .Build());
  test.Execute(
      ForeignFilter(
          ProjectAttributeAt(1),
          ProjectAttributeAt(2),
          test.input_at(0),
          test.input_at(1)));
}

TEST_F(ForeignFilterTest, TransformTest) {
  // Cursors built from empty builders.
  Cursor* filter = sample_filter_builder_.BuildCursor();
  Cursor* input = sample_input_builder_.BuildCursor();

  std::unique_ptr<Cursor> foreign(BoundForeignFilter(0, 0, filter, input));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  foreign->ApplyToChildren(spy_transformer.get());

  ASSERT_EQ(2, spy_transformer->GetHistoryLength());
  EXPECT_EQ(filter, spy_transformer->GetEntryAt(0)->original());
  EXPECT_EQ(input, spy_transformer->GetEntryAt(1)->original());
}

}  // namespace supersonic
