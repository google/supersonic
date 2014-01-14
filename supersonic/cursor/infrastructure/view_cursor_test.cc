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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/cursor/infrastructure/view_cursor.h"

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/comparators.h"
#include "gtest/gtest.h"

namespace supersonic {

namespace {

class ViewCursorTest : public ::testing::Test {
 protected:
  ViewCursorTest()
      : block_(new Block(input_schema(),
                         HeapBufferAllocator::Get())) {
    block_->Reallocate(2);
    block_->mutable_column(0)->mutable_typed_data<INT32>()[0] = 1;
    block_->mutable_column(0)->mutable_typed_data<INT32>()[1] = 2;

    block_->mutable_column(1)->mutable_typed_data<INT64>()[0] = 3LL;
    block_->mutable_column(1)->mutable_typed_data<INT64>()[1] = 3LL;

    selection_vector[0] = 0;
    selection_vector[1] = 1;
    selection_vector[2] = 0;
    selection_vector[3] = 1;
  }

  View input_view() { return block_->view(); }

  Cursor* view_cursor() { return CreateCursorOverView(input_view()); }

  Cursor* selection_cursor(rowcount_t buffer_size) {
    return SucceedOrDie(CreateCursorOverViewWithSelection(
        input_view(),
        4,
        &selection_vector[0],
        HeapBufferAllocator::Get(),
        buffer_size));
  }

  static TupleSchema input_schema() {
    TupleSchema schema;
    schema.add_attribute(Attribute("col1", INT32, NOT_NULLABLE));
    schema.add_attribute(Attribute("col2", INT64, NULLABLE));
    return schema;
  }

 private:
  int64 selection_vector[4];
  std::unique_ptr<Block> block_;
};

TEST_F(ViewCursorTest, ViewCursorHasCorrectSchema) {
  std::unique_ptr<Cursor> cursor(view_cursor());
  EXPECT_TUPLE_SCHEMAS_EQUAL(input_schema(), cursor->schema());
}

TEST_F(ViewCursorTest, ViewCursorDescription) {
  string name = "(prefix)";
  std::unique_ptr<Cursor> cursor(view_cursor());
  cursor->AppendDebugDescription(&name);
  EXPECT_EQ(name, string("(prefix)ViewCursor(col1: INT32 NOT NULL, "
                         "col2: INT64)"));
}

// Not using the testing fixture for Cursors, because the fixture uses tables
// and CursorOverView.
TEST_F(ViewCursorTest, ViewCursorNext) {
  std::unique_ptr<Cursor> cursor(view_cursor());
  ResultView view = cursor->Next(20);
  EXPECT_TRUE(view.has_data());
  EXPECT_TUPLE_SCHEMAS_EQUAL(input_schema(), view.view().schema());
  for (rowid_t row = 0; row < 2; ++row) {
    EXPECT_EQ(input_view().column(0).typed_data<INT32>()[row],
              view.view().column(0).typed_data<INT32>()[row]);
  }
  for (rowid_t row = 0; row < 2; ++row) {
    EXPECT_EQ(input_view().column(1).typed_data<INT64>()[row],
              view.view().column(1).typed_data<INT64>()[row]);
  }
  view = cursor->Next(20);
  EXPECT_TRUE(view.is_eos());
}

TEST_F(ViewCursorTest, SelectionViewCursorHasCorrectSchema) {
  std::unique_ptr<Cursor> cursor(selection_cursor(10));
  EXPECT_TUPLE_SCHEMAS_EQUAL(input_schema(), cursor->schema());
}

TEST_F(ViewCursorTest, SelectionViewCursorDescription) {
  string name = "(prefix)";
  std::unique_ptr<Cursor> cursor(selection_cursor(1));
  cursor->AppendDebugDescription(&name);
  EXPECT_EQ(name, string("(prefix)ViewCursorWithSelectionVector("
                         "col1: INT32 NOT NULL, "
                         "col2: INT64)"));
}

TEST_F(ViewCursorTest, SelectionViewCursorNext) {
  std::unique_ptr<Cursor> cursor(selection_cursor(3));
  ResultView view = cursor->Next(20);
  EXPECT_TRUE(view.has_data());
  EXPECT_EQ(3, view.view().row_count());
  EXPECT_TUPLE_SCHEMAS_EQUAL(input_schema(), view.view().schema());
  // Check contents.
  EXPECT_EQ(1, view.view().column(0).typed_data<INT32>()[0]);
  EXPECT_EQ(2, view.view().column(0).typed_data<INT32>()[1]);
  EXPECT_EQ(1, view.view().column(0).typed_data<INT32>()[2]);

  // Second call.
  view = cursor->Next(2);
  EXPECT_TRUE(view.has_data());
  EXPECT_EQ(1, view.view().row_count());
  EXPECT_EQ(2, view.view().column(0).typed_data<INT32>()[0]);

  // Third (EOS) call.
  view = cursor->Next(1);
  EXPECT_TRUE(view.is_eos());
}

}  // namespace

}  // namespace supersonic
