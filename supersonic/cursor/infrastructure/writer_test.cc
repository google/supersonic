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

#include "supersonic/cursor/infrastructure/writer.h"

#include <memory>

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/compute.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/expression/core/arithmetic_expressions.h"
#include "supersonic/expression/core/projecting_expressions.h"
#include "supersonic/expression/infrastructure/terminal_expressions.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class WriterTest : public testing::Test {};
class WriterSpyTest : public testing::TestWithParam<bool> {};

// Prototype of a function that consumes an input cursor and writes some
// data from it into the specified table. For testing various Write variants.
// Takes ownership of the child cursor. Does NOT take ownership of the
// table.
typedef void (*WriterFn)(Cursor* child, Table* table);

TEST_F(WriterTest, WriteAll) {
  TestDataBuilder<INT32, STRING> builder;
  builder.AddRow(4, "foo")
         .AddRow(6, "bar")
         .AddRow(1, "baz")
         .AddRow(7, "car")
         .AddRow(2, "cat");
  int block_sizes[] = { 1, 2, 4, 5, 10, 100 };
  for (int i = 0; i < arraysize(block_sizes); ++i) {
    std::unique_ptr<TestData> input(builder.Build());
    Writer writer(CreateViewLimiter(block_sizes[i],
                                    SucceedOrDie(input->CreateCursor())));
    Table table(writer.schema(), HeapBufferAllocator::Get());
    TableSink sink(&table);
    FailureOr<rowcount_t> result = writer.WriteAll(&sink);
    sink.Finalize();
    EXPECT_TRUE(result.is_success());
    EXPECT_TRUE(writer.is_eos());
    EXPECT_EQ(5, result.get());
    EXPECT_VIEWS_EQUAL(input->view(), table.view());
  }
}

TEST_F(WriterTest, WritePartial) {
  TestDataBuilder<INT32, STRING> builder;
  builder.AddRow(4, "foo")
         .AddRow(6, "bar")
         .AddRow(1, "baz")
         .AddRow(7, "car")
         .AddRow(2, "cat");
  std::unique_ptr<TestData> input(builder.Build());
  Writer writer(SucceedOrDie(input->CreateCursor()));
  Table table1(writer.schema(), HeapBufferAllocator::Get());
  Table table2(writer.schema(), HeapBufferAllocator::Get());
  TableSink sink1(&table1);
  TableSink sink2(&table2);
  FailureOr<rowcount_t> result1 = writer.Write(&sink1, 2);
  EXPECT_TRUE(result1.is_success());
  EXPECT_FALSE(writer.is_eos());
  EXPECT_EQ(2, result1.get());
  sink1.Finalize();
  FailureOr<rowcount_t> result2 = writer.Write(&sink2, 3);
  EXPECT_TRUE(result2.is_success());
  EXPECT_FALSE(writer.is_eos());
  EXPECT_EQ(3, result2.get());
  result2 = writer.Write(&sink2, 1);
  EXPECT_TRUE(result2.is_success());
  EXPECT_TRUE(writer.is_eos());
  EXPECT_EQ(0, result2.get());
  sink2.Finalize();
  EXPECT_VIEWS_EQUAL(View(input->view(), 0, 2), table1.view());
  EXPECT_VIEWS_EQUAL(View(input->view(), 2, 3), table2.view());
}

TEST_F(WriterTest, WriteFailure) {
  TestDataBuilder<INT32> builder;
  builder.AddRow(4)
         .AddRow(6)
         .AddRow(4)
         .AddRow(0)  // Division by 0 occurs here.
         .AddRow(2);
  std::unique_ptr<Operation> compute(
      Compute(DivideSignaling(ConstInt32(5), AttributeAt(0)), builder.Build()));

  Writer writer(SucceedOrDie(compute->CreateCursor()));

  Table table1(writer.schema(), HeapBufferAllocator::Get());
  Table table2(writer.schema(), HeapBufferAllocator::Get());
  TableSink sink1(&table1);
  TableSink sink2(&table2);

  // Even if we use max_row_count parameter lower then 4, the writer might
  // return failure because of compute trying to evaluate as much data as
  // available in its buffer (max_row_capacity).
  FailureOr<rowcount_t> result1 = writer.Write(&sink1, 4);
  EXPECT_TRUE(result1.is_failure());
  EXPECT_FALSE(writer.is_eos());
  EXPECT_FALSE(writer.is_waiting_on_barrier());

  // We do here a secondary check to ensure that the writer didn't lost
  // ownership of the exception and will return it second time.
  FailureOr<rowcount_t> result2 = writer.Write(&sink2, 1);
  EXPECT_TRUE(result2.is_failure());
  EXPECT_FALSE(writer.is_eos());
  EXPECT_FALSE(writer.is_waiting_on_barrier());
}

INSTANTIATE_TEST_CASE_P(SpyUse, WriterSpyTest, testing::Bool());

TEST_P(WriterSpyTest, WritePartialToConstrainedSinks) {
  TestDataBuilder<INT32> builder;
  builder.AddRow(4).AddRow(6).AddRow(1).AddRow(7).AddRow(2);
  std::unique_ptr<TestData> input(builder.Build());
  Writer writer(CreateViewLimiter(2, SucceedOrDie(input->CreateCursor())));

  if (GetParam()) {
    std::unique_ptr<CursorTransformer> spy_transform(PrintingSpyTransformer());
    writer.ApplyToIterator(spy_transform.get());
  }

  MemoryLimit limit1(12);
  MemoryLimit limit2(24);  // 2 ints + 4 ints during reallocation
  Table table1(writer.schema(), &limit1);
  Table table2(writer.schema(), &limit2);
  TableSink sink1(&table1);
  TableSink sink2(&table2);
  FailureOr<rowcount_t> result1 = writer.WriteAll(&sink1);
  EXPECT_TRUE(result1.is_success());
  EXPECT_FALSE(writer.is_eos());
  EXPECT_EQ(2, result1.get());
  EXPECT_EQ(8, limit1.GetUsage());
  sink1.Finalize();
  FailureOr<rowcount_t> result2 = writer.WriteAll(&sink2);
  EXPECT_TRUE(result2.is_success());
  EXPECT_TRUE(writer.is_eos());
  EXPECT_EQ(3, result2.get());
  // We expect the table to double its capacity (from 2 to 4).
  EXPECT_EQ(16, limit2.GetUsage());
  sink2.Finalize();
  EXPECT_VIEWS_EQUAL(View(input->view(), 0, 2), table1.view());
  EXPECT_VIEWS_EQUAL(View(input->view(), 2, 3), table2.view());
}

}  // namespace supersonic
