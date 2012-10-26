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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/base/infrastructure/double_buffered_block.h"

#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/proto/supersonic.pb.h"
#include "gtest/gtest.h"

namespace supersonic {

class DoubleBufferedBlockTest : public ::testing::Test {
 protected:
  // Remove all of the functions below that you do not need.
  // See http://goto/gunitfaq#CtorVsSetUp for when to use SetUp/TearDown.

  DoubleBufferedBlockTest() {
    schema.add_attribute(Attribute("foo", INT32, NULLABLE));
    schema.add_attribute(Attribute("BAR", STRING, NOT_NULLABLE));
  }

  TupleSchema schema;
};

TEST_F(DoubleBufferedBlockTest, Reallocate) {
  DoubleBufferedBlock supply(HeapBufferAllocator::Get(), schema);
  ASSERT_TRUE(supply.Reallocate(10LL));
}

TEST_F(DoubleBufferedBlockTest, GetBlockGivesValidBlock) {
  DoubleBufferedBlock supply(HeapBufferAllocator::Get(), schema);
  supply.Reallocate(10LL);
  Block* block = supply.get_block();
  EXPECT_FALSE(block->column(0).data().is_null());
  EXPECT_EQ(10, block->row_capacity());
}

TEST_F(DoubleBufferedBlockTest, GetBlockGivesSameBlock) {
  DoubleBufferedBlock supply(HeapBufferAllocator::Get(), schema);
  supply.Reallocate(10LL);
  Block* block = supply.get_block();
  EXPECT_TRUE(supply.get_block() == block);
}

TEST_F(DoubleBufferedBlockTest, SecondBlockIsValid) {
  DoubleBufferedBlock supply(HeapBufferAllocator::Get(), schema);
  supply.Reallocate(10LL);
  Block* block = supply.get_block();
  EXPECT_FALSE(block->column(0).data().is_null());
  EXPECT_EQ(10, block->row_capacity());
  block = supply.switch_block();
  EXPECT_FALSE(block->column(0).data().is_null());
  EXPECT_EQ(10, block->row_capacity());
}

TEST_F(DoubleBufferedBlockTest, ThirdBlockEqualsFirstBlock) {
  DoubleBufferedBlock supply(HeapBufferAllocator::Get(), schema);
  supply.Reallocate(10LL);
  Block* block = supply.switch_block();
  EXPECT_FALSE(block == supply.switch_block());
  EXPECT_TRUE(block == supply.switch_block());
  EXPECT_FALSE(supply.get_block()->column(0).data().is_null());
  EXPECT_EQ(10, supply.get_block()->row_capacity());
}

TEST_F(DoubleBufferedBlockTest, CanChangeAllocation) {
  DoubleBufferedBlock supply(HeapBufferAllocator::Get(), schema);
  supply.Reallocate(10LL);
  Block* block = supply.get_block();
  EXPECT_EQ(10, block->row_capacity());
  EXPECT_TRUE(supply.Reallocate(20LL));
  block = supply.get_block();
  EXPECT_EQ(20, block->row_capacity());
  block = supply.switch_block();
  EXPECT_EQ(20, block->row_capacity());
}

}  // namespace supersonic
