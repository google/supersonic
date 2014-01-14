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

#include "supersonic/cursor/infrastructure/table.h"

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

// TODO(onufry): Refactor this. There are two problems here. First, the testing
// should actually concern only the table, the ViewCursor is tested in its own
// test file. Second, it's a bad idea to use TestDataBuilder in the testing of
// Table, as TestDataBuilder actually uses Table to represent it's data.

TEST(TableCursorTest, SimpleData) {
  Table table(TupleSchema::Merge(
      TupleSchema::Singleton("col0", INT32, NULLABLE),
      TupleSchema::Singleton("col1", STRING, NULLABLE)),
              HeapBufferAllocator::Get());
  TableRowWriter writer(&table);
  writer.AddRow().Int32(1).String("a")
        .AddRow().Int32(3).String("b")
        .AddRow().Null().Null();
  ASSERT_TRUE(writer.success());
  std::unique_ptr<Cursor> table_cursor(SucceedOrDie(table.CreateCursor()));

  std::unique_ptr<Cursor> expected_output(TestDataBuilder<INT32, STRING>()
                                              .AddRow(1, "a")
                                              .AddRow(3, "b")
                                              .AddRow(__, __)
                                              .BuildCursor());
  EXPECT_TUPLE_SCHEMAS_EQUAL(
      expected_output->schema(),
      table_cursor->schema());
  EXPECT_CURSORS_EQUAL(expected_output.release(), table_cursor.release());
}

TEST(TableCursorTest, Empty) {
  Table table(TupleSchema::Singleton("col0", INT32, NOT_NULLABLE),
              HeapBufferAllocator::Get());

  std::unique_ptr<Cursor> table_cursor(SucceedOrDie(table.CreateCursor()));

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32>().BuildCursor());
  EXPECT_TUPLE_SCHEMAS_EQUAL(
      expected_output->schema(),
      table_cursor->schema());
  EXPECT_CURSORS_EQUAL(expected_output.release(), table_cursor.release());
}

TEST(TableCursorTest, Large) {
  Table table(TupleSchema::Singleton("col0", INT32, NOT_NULLABLE),
              HeapBufferAllocator::Get());
  TestDataBuilder<INT32> expected_builder;
  TableRowWriter row_writer(&table);
  for (int i = 0; i < 10000; i++) {
    row_writer.AddRow().Int32(i);
    expected_builder.AddRow(i);
  }
  std::unique_ptr<Cursor> table_cursor(SucceedOrDie(table.CreateCursor()));

  std::unique_ptr<Cursor> expected_output(expected_builder.BuildCursor());
  EXPECT_TUPLE_SCHEMAS_EQUAL(
      expected_output->schema(),
      table_cursor->schema());
  EXPECT_CURSORS_EQUAL(expected_output.release(), table_cursor.release());
}

TEST(TableCursorTest, LimitedMemory) {
  MemoryLimit allocator_with_quota(0);
  Table table(TupleSchema::Singleton("a", INT32, NULLABLE),
              &allocator_with_quota);
  TableRowWriter row_writer(&table);
  row_writer.AddRow();
  EXPECT_FALSE(row_writer.success());
}

TEST(TableTest, NewTableIsEmpty) {
  Table table(TupleSchema::Singleton("a", INT32, NULLABLE),
              HeapBufferAllocator::Get());
  EXPECT_EQ(0, table.view().row_count());
  FailureOrOwned<Cursor> cursor_result(table.CreateCursor());
  ASSERT_TRUE(cursor_result.is_success());
  std::unique_ptr<Cursor> cursor(cursor_result.release());
  EXPECT_TRUE(cursor->Next(100).is_eos());
}

TEST(TableTest, ClearedTableIsEmpty) {
  Table table(BlockBuilder<INT32>().AddRow(5).Build());
  EXPECT_EQ(1, table.view().row_count());
  table.Clear();
  EXPECT_EQ(0, table.view().row_count());
}

TEST(TableTest, ClearedTableCanReuseCapacity) {
  std::unique_ptr<Block> block(BlockBuilder<INT32>().AddRow(5).Build());
  Table table(block->schema(), HeapBufferAllocator::Get());
  table.ReserveRowCapacity(1);
  table.AppendView(block->view());
  EXPECT_EQ(1, table.view().row_count());
  EXPECT_EQ(1, table.row_capacity());
  table.Clear();
  EXPECT_EQ(0, table.view().row_count());
  EXPECT_EQ(1, table.row_capacity());
  table.AppendView(block->view());
  EXPECT_EQ(1, table.view().row_count());
  EXPECT_EQ(1, table.row_capacity());
}

TEST(TableTest, TableRespectsSoftQuota) {
  // This property is used by hybrid-sort, for example.
  StaticQuota<false> soft_quota(1, false);
  std::unique_ptr<BufferAllocator> allocator_with_soft_quota(
      new MediatingBufferAllocator(HeapBufferAllocator::Get(), &soft_quota));
  std::unique_ptr<Block> block(BlockBuilder<INT32>().AddRow(5).Build());
  Table table(block->schema(), allocator_with_soft_quota.get());
  // Table may allow some number of rows even when soft quota is 0, but it
  // should stop growing at some point. At the time of writing the limit is
  // Cursor::kDefaultRowCount.
  for (int i = 0; i < 5000 && table.AppendView(block->view()); ++i) {}
  EXPECT_EQ(0, table.AppendView(block->view()));
}

TEST(TableTest, SetRowCapacitySetsCapacity) {
  Table table(BlockBuilder<INT32>().AddRow(5).Build());
  EXPECT_EQ(1, table.row_capacity());
  table.Clear();
  EXPECT_EQ(1, table.row_capacity());
  ASSERT_TRUE(table.SetRowCapacity(10));
  EXPECT_EQ(10, table.row_capacity());
  ASSERT_TRUE(table.SetRowCapacity(0));
  EXPECT_EQ(0, table.row_capacity());
}

TEST(TableTest, ReserveRowCapacityEnsuresCapacity) {
  Table table(BlockBuilder<INT32>().AddRow(5).Build());
  EXPECT_EQ(1, table.row_capacity());
  table.Clear();
  EXPECT_EQ(1, table.row_capacity());
  ASSERT_TRUE(table.ReserveRowCapacity(0));
  EXPECT_EQ(1, table.row_capacity());
  ASSERT_TRUE(table.ReserveRowCapacity(0));
  EXPECT_EQ(1, table.row_capacity());
  ASSERT_TRUE(table.SetRowCapacity(0));
  EXPECT_EQ(0, table.row_capacity());
}

TEST(TableTest, MaterializationMaterializes) {
  Table table(BlockBuilder<INT32>().AddRow(5).AddRow(6).Build());
  FailureOrOwned<Table> result =
      MaterializeTable(HeapBufferAllocator::Get(),
                       SucceedOrDie(table.CreateCursor()));
  EXPECT_TRUE(result.is_success());
  EXPECT_CURSORS_EQUAL(SucceedOrDie(table.CreateCursor()),
                       SucceedOrDie(result->CreateCursor()));
}

TEST(TableTest, MaterializationOOMsWhenTight) {
  MemoryLimit limit(4);
  Table table(BlockBuilder<INT32>().AddRow(5).AddRow(6).Build());
  FailureOrOwned<Table> result =
      MaterializeTable(&limit, SucceedOrDie(table.CreateCursor()));
  ASSERT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_MEMORY_EXCEEDED, result.exception().return_code());
}

}  // namespace supersonic
