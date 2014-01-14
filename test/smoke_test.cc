// Copyright 2012 Google Inc. All Rights Reserved.
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
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)
//
// Simple smoke test to check if compilation and linking have succeeded.
// TODO(tkaftal): This will be replaced with more a elaborate testing suite,
// for now it serves the purpose of identifying linker issues.

#include <iostream>
#include <memory>

#include "gtest/gtest.h"
#include "supersonic/utils/integral_types.h"
#include "supersonic/supersonic.h"

using supersonic::TupleSchema;
using supersonic::Attribute;
using supersonic::MemoryLimit;
using supersonic::Block;
using supersonic::INT32;
using supersonic::NULLABLE;
using supersonic::DATETIME;
using supersonic::NOT_NULLABLE;
using supersonic::STRING;
using supersonic::Expression;
using supersonic::NamedAttribute;
using supersonic::Plus;
using supersonic::Table;
using supersonic::TableRowWriter;
using supersonic::Operation;
using supersonic::Compute;
using supersonic::HeapBufferAllocator;
using supersonic::FailureOrOwned;
using supersonic::Cursor;
using supersonic::ResultView;
using supersonic::View;
using supersonic::RandInt32;
using supersonic::ConstDateTimeFromMicrosecondsSinceEpoch;
using supersonic::Column;

TEST(SmokeTestSuite, BlockTest) {
  TupleSchema schema;
  schema.add_attribute(Attribute("foo", INT32, NULLABLE));
  schema.add_attribute(Attribute("BAR", STRING, NOT_NULLABLE));
  MemoryLimit limit(0);
  Block block(schema, &limit);
  EXPECT_EQ(0, block.row_capacity());
  EXPECT_EQ(2, block.column_count());
  EXPECT_FALSE(block.column(0).data().is_null());
  EXPECT_FALSE(block.column(1).data().is_null());
  EXPECT_TRUE(block.is_null(0) != NULL);
  EXPECT_TRUE(block.is_null(1) == NULL);
}

TEST(SmokeTestSuite, MemTest) {
  Table table(TupleSchema::Merge(
      TupleSchema::Singleton("col0", INT32, NULLABLE),
      TupleSchema::Singleton("col1", STRING, NULLABLE)),
              HeapBufferAllocator::Get());
  TableRowWriter writer(&table);
  writer.AddRow().Int32(1).String("a")
        .AddRow().Int32(3).String("b")
        .AddRow().Null().Null();
  ASSERT_TRUE(writer.success());
}

TEST(SmokeTestSuite, ExpressionTest) {
  std::unique_ptr<const Expression> plus(
      Plus(NamedAttribute("a"), NamedAttribute("b")));
  std::unique_ptr<Table> table(new Table(
      TupleSchema::Merge(TupleSchema::Singleton("a", INT32, NULLABLE),
                         TupleSchema::Singleton("b", INT32, NULLABLE)),
      HeapBufferAllocator::Get()));
  TableRowWriter writer(table.get());
  writer.AddRow().Int32(1).Int32(2)
        .AddRow().Int32(3).Int32(5)
        .CheckSuccess();

  std::unique_ptr<Operation> computation(
      Compute(plus.release(), table.release()));
  computation->SetBufferAllocator(HeapBufferAllocator::Get(), false);

  FailureOrOwned<Cursor> cursor = computation->CreateCursor();
  ASSERT_TRUE(cursor.is_success());
  ResultView output = cursor.get()->Next(2);
  ASSERT_TRUE(output.has_data());
  View view = SucceedOrDie(output);
  const int* p = view.column(0).typed_data<INT32>();
  EXPECT_EQ(3, p[0]);
  EXPECT_EQ(8, p[1]);
}

TEST(SmokeTestSuite, RandExprTest) {
  std::unique_ptr<Table> table(
      new Table(TupleSchema::Singleton("a", INT32, NULLABLE),
                HeapBufferAllocator::Get()));
  TableRowWriter writer(table.get());
  writer.AddRow().Int32(1).CheckSuccess();

  std::unique_ptr<Operation> computation(Compute(RandInt32(), table.release()));
  computation->SetBufferAllocator(HeapBufferAllocator::Get(), false);

  FailureOrOwned<Cursor> cursor = computation->CreateCursor();
  ASSERT_TRUE(cursor.is_success());
  ResultView output = cursor.get()->Next(1);
  ASSERT_TRUE(output.has_data());
  View view = SucceedOrDie(output);
  const int* p = view.column(0).typed_data<INT32>();
  std::cout << "Generated random number: " << p[0] << std::endl;
}

TEST(SmokeTestSuite, DateTimeTest) {
  int32 useconds = 1000121012;
  std::unique_ptr<const Expression> date(
      ConstDateTimeFromMicrosecondsSinceEpoch(useconds));
  std::unique_ptr<Table> table(
      new Table(TupleSchema::Singleton("a", INT32, NULLABLE),
                HeapBufferAllocator::Get()));
  TableRowWriter writer(table.get());
  writer.AddRow().Int32(1).CheckSuccess();

  std::unique_ptr<Operation> computation(
      Compute(date.release(), table.release()));
  computation->SetBufferAllocator(HeapBufferAllocator::Get(), false);

  FailureOrOwned<Cursor> cursor = computation->CreateCursor();
  ASSERT_TRUE(cursor.is_success());
  ResultView output = cursor.get()->Next(1);
  ASSERT_TRUE(output.has_data());
  View view = SucceedOrDie(output);
  const int64* p = view.column(0).typed_data<DATETIME>();
  EXPECT_EQ(useconds, p[0]);
}
