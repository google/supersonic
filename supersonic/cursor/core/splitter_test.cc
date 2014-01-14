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

#include "supersonic/cursor/core/splitter.h"

#include <memory>

#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"
#include "supersonic/utils/pointer_vector.h"

namespace supersonic {

namespace {

class BarrierSplitterTest : public testing::Test {};

class BarrierSplitterSpyTest : public testing::TestWithParam<bool> {};

TEST_F(BarrierSplitterTest, UnclaimedSplitterDoesntLeak) {
  BarrierSplitter splitter(TestDataBuilder<INT32, STRING>().BuildCursor());
}

class SingleSourceBufferedSplitter : public BasicOperation {
 public:
  explicit SingleSourceBufferedSplitter(Operation* child) :
      BasicOperation(child) {}
  virtual FailureOrOwned<Cursor> CreateCursor() const {
    return Success(
        (new BufferedSplitter(
            SucceedOrDie(child()->CreateCursor()),
            HeapBufferAllocator::Get(),
            /* max row count = */ 5))->AddReader());
  }
};

// Use BufferedSplitter to create copies of the input cursor.
// This is an evil operation to test BufferedSplitter.
class CursorToOperation : public BasicOperation {
 public:
  CursorToOperation(Cursor* child,
                    int copies_count,
                    rowcount_t max_row_count) :
        BasicOperation() {
    CHECK_GT(copies_count, 0);
    SplitterInterface* splitter = new BufferedSplitter(
        child, HeapBufferAllocator::Get(), max_row_count);
    for (int i = 0; i < copies_count; ++i) {
      cursors_.push_back(splitter->AddReader());
    }
  }
  virtual FailureOrOwned<Cursor> CreateCursor() const {
    DVLOG(7) << "Asking to create a cursor...";
    CHECK(!cursors_.empty());
    Cursor* cursor = cursors_.back().release();
    cursors_.pop_back();
    return Success(cursor);
  }
 private:
  mutable util::gtl::PointerVector<Cursor> cursors_;
};

class SingleSourceBarrierSplitter : public BasicOperation {
 public:
  explicit SingleSourceBarrierSplitter(Operation* child) :
      BasicOperation(child) {}
  virtual FailureOrOwned<Cursor> CreateCursor() const {
    return Success(
        (new BarrierSplitter(
            SucceedOrDie(child()->CreateCursor())))->AddReader());
  }
};

TEST_F(BarrierSplitterTest, SingleReaderSplitterNeverBlocks) {
  OperationTest test;
  TestDataBuilder<INT32> builder;
  builder.AddRow(1)
         .AddRow(2)
         .AddRow(3)
         .AddRow(4)
         .AddRow(5)
         .AddRow(6)
         .AddRow(7)
         .AddRow(8)
         .AddRow(9)
         .AddRow(10);
  test.SetInput(builder.Build());
  test.SetExpectedResult(builder.Build());
  test.Execute(new SingleSourceBarrierSplitter(test.input()));
}

TEST_F(BarrierSplitterTest, DualSplitterAlternates) {
  TestDataBuilder<INT32> builder;
  builder.AddRow(1)
         .AddRow(2)
         .AddRow(3)
         .AddRow(4)
         .AddRow(5)
         .AddRow(6)
         .AddRow(7)
         .AddRow(8)
         .AddRow(9);
  SplitterInterface* splitter(new BarrierSplitter(
      CreateViewLimiter(3, builder.BuildCursor())));
  CursorIterator reader_1(splitter->AddReader());
  CursorIterator reader_2(splitter->AddReader());
  ASSERT_TRUE(reader_1.Next(1, true));
  EXPECT_EQ(1, reader_1.view().row_count());
  EXPECT_EQ(1, reader_1.view().column(0).typed_data<INT32>()[0]);
  ASSERT_TRUE(reader_2.Next(2, true));
  EXPECT_EQ(2, reader_2.view().row_count());
  EXPECT_EQ(2, reader_2.view().column(0).typed_data<INT32>()[1]);
  ASSERT_TRUE(reader_1.Next(3, true));
  EXPECT_EQ(2, reader_1.view().row_count());
  EXPECT_EQ(2, reader_1.view().column(0).typed_data<INT32>()[0]);
  ASSERT_FALSE(reader_1.Next(2, true));
  EXPECT_TRUE(reader_1.is_waiting_on_barrier());
  ASSERT_TRUE(reader_2.Next(2, true));
  EXPECT_EQ(1, reader_2.view().row_count());
  EXPECT_EQ(3, reader_2.view().column(0).typed_data<INT32>()[0]);
  ASSERT_TRUE(reader_2.Next(2, true));
  EXPECT_EQ(4, reader_2.view().column(0).typed_data<INT32>()[0]);
  ASSERT_TRUE(reader_1.Next(3, true));
  EXPECT_EQ(3, reader_1.view().row_count());
  EXPECT_EQ(4, reader_1.view().column(0).typed_data<INT32>()[0]);
  ASSERT_TRUE(reader_2.Next(3, true));
  EXPECT_EQ(1, reader_2.view().row_count());
  EXPECT_EQ(6, reader_2.view().column(0).typed_data<INT32>()[0]);

  ASSERT_FALSE(reader_2.Next(3, true));
  EXPECT_TRUE(reader_2.is_waiting_on_barrier());
  ASSERT_TRUE(reader_1.Next(2, true));
  EXPECT_EQ(2, reader_1.view().row_count());
  EXPECT_EQ(7, reader_1.view().column(0).typed_data<INT32>()[0]);
  ASSERT_TRUE(reader_2.Next(3, true));
  EXPECT_EQ(3, reader_2.view().row_count());
  EXPECT_EQ(7, reader_2.view().column(0).typed_data<INT32>()[0]);
  ASSERT_TRUE(reader_1.Next(2, true));
  EXPECT_EQ(1, reader_1.view().row_count());
  EXPECT_EQ(9, reader_1.view().column(0).typed_data<INT32>()[0]);
  ASSERT_FALSE(reader_2.Next(2, true));
  EXPECT_TRUE(reader_2.is_waiting_on_barrier());
  ASSERT_FALSE(reader_1.Next(2, true));
  EXPECT_TRUE(reader_1.is_eos());
  ASSERT_FALSE(reader_2.Next(2, true));
  EXPECT_TRUE(reader_2.is_eos());
}

INSTANTIATE_TEST_CASE_P(SpyUse, BarrierSplitterSpyTest, testing::Bool());

TEST_P(BarrierSplitterSpyTest, LateJoin) {
  TestDataBuilder<INT32> builder;
  builder.AddRow(1)
         .AddRow(2)
         .AddRow(3)
         .AddRow(4)
         .AddRow(5)
         .AddRow(6)
         .AddRow(7)
         .AddRow(8)
         .AddRow(9);
  Cursor* input = CreateViewLimiter(3, builder.BuildCursor());

  SplitterInterface* splitter(new BarrierSplitter(input));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());

  Cursor* reader_cursor1 = splitter->AddReader();
  Cursor* reader_cursor_spy1 = NULL;
  if (GetParam()) {
    reader_cursor1->ApplyToChildren(spy_transformer.get());
    reader_cursor_spy1 = spy_transformer->Transform(reader_cursor1);
  }

  CursorIterator reader_1(GetParam() ? reader_cursor_spy1 : reader_cursor1);
  ASSERT_TRUE(reader_1.Next(2, true));
  EXPECT_EQ(2, reader_1.view().row_count());

  Cursor* reader_cursor2 = splitter->AddReader();
  Cursor* reader_cursor_spy2 = NULL;
  if (GetParam()) {
    reader_cursor2->ApplyToChildren(spy_transformer.get());
    reader_cursor_spy2 = spy_transformer->Transform(reader_cursor2);
  }

  CursorIterator reader_2(GetParam() ? reader_cursor_spy2 : reader_cursor2);
  ASSERT_FALSE(reader_2.Next(1, true));
  EXPECT_TRUE(reader_2.is_waiting_on_barrier());
  ASSERT_TRUE(reader_1.Next(2, true));
  EXPECT_EQ(1, reader_1.view().row_count());
  ASSERT_TRUE(reader_1.Next(3, true));
  EXPECT_EQ(3, reader_1.view().row_count());
  ASSERT_TRUE(reader_2.Next(3, true));
  EXPECT_EQ(3, reader_2.view().row_count());
  EXPECT_EQ(4, reader_2.view().column(0).typed_data<INT32>()[0]);

  if (GetParam()) {
    // There should be three stored cursors; they are (in the order of store
    // time) input, reader_cursor1 and reader_cursor2. Despite the Transform()
    // call on reader_cursor2 input will not be stored twice, as the function
    // implementation for readers will only propagate to children if the
    // reader it is called by is the owner of the splitter.
    ASSERT_EQ(3, spy_transformer->GetHistoryLength());
    EXPECT_EQ(input, spy_transformer->GetEntryAt(0)->original());
    EXPECT_EQ(reader_cursor1, spy_transformer->GetEntryAt(1)->original());
    EXPECT_EQ(reader_cursor2, spy_transformer->GetEntryAt(2)->original());
  }
}

TEST_F(BarrierSplitterTest, EarlyDeparture) {
  TestDataBuilder<INT32> builder;
  builder.AddRow(1)
         .AddRow(2)
         .AddRow(3)
         .AddRow(4)
         .AddRow(5)
         .AddRow(6)
         .AddRow(7)
         .AddRow(8)
         .AddRow(9);
  SplitterInterface* splitter(new BarrierSplitter(
      CreateViewLimiter(3, builder.BuildCursor())));
  CursorIterator reader_1(splitter->AddReader());
  CursorIterator reader_2(splitter->AddReader());
  CursorIterator reader_3(splitter->AddReader());
  CursorIterator reader_4(splitter->AddReader());
  reader_3.Terminate();
  ASSERT_TRUE(reader_1.Next(3, true));
  EXPECT_EQ(3, reader_1.view().row_count());
  reader_4.Terminate();
  ASSERT_FALSE(reader_1.Next(1, true));
  EXPECT_TRUE(reader_1.is_waiting_on_barrier());
  ASSERT_TRUE(reader_2.Next(3, true));
  EXPECT_EQ(3, reader_2.view().row_count());
  ASSERT_TRUE(reader_2.Next(3, true));
  EXPECT_EQ(3, reader_2.view().row_count());
  EXPECT_EQ(4, reader_2.view().column(0).typed_data<INT32>()[0]);
  reader_1.Terminate();
  ASSERT_TRUE(reader_2.Next(3, true));
  EXPECT_EQ(3, reader_2.view().row_count());
  EXPECT_EQ(7, reader_2.view().column(0).typed_data<INT32>()[0]);
  ASSERT_FALSE(reader_2.Next(3, true));
  EXPECT_TRUE(reader_2.is_eos());
}

class BufferedSplitterTest : public ::testing::TestWithParam<rowcount_t> {};

rowcount_t number_of_rows[] = {0, 1, 5, 30};
size_t number_of_rows_count = arraysize(number_of_rows);

rowcount_t max_row_counts[] = {1, 5, 100};
size_t number_of_max_row_counts = arraysize(max_row_counts);

INSTANTIATE_TEST_CASE_P(
    InstantiationName,
    BufferedSplitterTest,
    ::testing::ValuesIn(number_of_rows, number_of_rows + number_of_rows_count));

// TODO(user): Instead of iterating over the number of rows and max row count
// as above, consider splitting them into multiple tests.

TEST_P(BufferedSplitterTest, SingleReaderSplitterNeverBlocks) {
  OperationTest test;
  TestDataBuilder<INT32> builder;
  for (rowcount_t j = 0; j < GetParam(); ++j) {
    builder.AddRow(j);
  }
  test.SetInput(builder.Build());
  test.SetExpectedResult(builder.Build());
  test.Execute(new SingleSourceBufferedSplitter(test.input()));
}

TEST_P(BufferedSplitterTest, SplitterCopiesCursorTest) {
  for (size_t j = 0; j < number_of_max_row_counts; ++j) {
    OperationTest test;
    TestDataBuilder<INT32> builder;
    for (rowcount_t k = 0; k < GetParam(); ++k) {
      builder.AddRow(k);
    }
    test.SetExpectedResult(builder.Build());
    test.Execute(new CursorToOperation(builder.BuildCursor(),
                                       /* copies count = */ 100,
                                       max_row_counts[j]));
  }
}

TEST_P(BufferedSplitterTest, SplitterCopiesCursorStringDataTest) {
  for (size_t j = 0; j < number_of_max_row_counts; ++j) {
    OperationTest test;
    TestDataBuilder<STRING> builder;
    for (rowcount_t k = 0; k < GetParam(); ++k) {
      switch (k%3) {
        case 0:
          builder.AddRow("irvan");
          break;
        case 1:
          builder.AddRow("is");
          break;
        case 2:
          builder.AddRow("awesome!");
          break;
      }
    }
    test.SetExpectedResult(builder.Build());
    test.Execute(new CursorToOperation(builder.BuildCursor(),
                                       /* copies count = */ 100,
                                       max_row_counts[j]));
  }
}

size_t readers[] = {2, 3, 10};
size_t number_of_readers = arraysize(readers);

TEST_P(BufferedSplitterTest, AlternatingCallTest) {
  for (size_t j = 0; j < number_of_max_row_counts; ++j) {
    for (size_t k = 0; k < number_of_readers; ++k) {
      TestDataBuilder<INT32> builder;
      for (rowcount_t l = 0; l < GetParam(); ++l) {
        builder.AddRow(l);
      }
      SplitterInterface* splitter = new BufferedSplitter(
          builder.BuildCursor(),
          HeapBufferAllocator::Get(),
          max_row_counts[j]);
      util::gtl::PointerVector<CursorIterator> iterators;
      for (size_t l = 0; l < readers[k]; ++l) {
        iterators.push_back(new CursorIterator(splitter->AddReader()));
      }
      for (size_t l = 0; l < GetParam(); ++l) {
        for (size_t m = 0; m < readers[k]; ++m) {
          ASSERT_TRUE(iterators[m]->Next(1, /* limit cursor input = */ true));
          EXPECT_EQ(iterators[m]->view().row_count(), 1);
          EXPECT_EQ(*(iterators[m]->view().column(0).typed_data<INT32>()), l);
        }
      }
      for (size_t m = 0; m < readers[k]; ++m) {
        ASSERT_FALSE(iterators[m]->Next(1, /* limit cursor input = */ true));
        ASSERT_TRUE(iterators[m]->is_eos());
      }
    }
  }
}

}  // namespace

}  // namespace supersonic
