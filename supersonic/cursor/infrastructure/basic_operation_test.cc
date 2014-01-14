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

#include "supersonic/cursor/infrastructure/basic_operation.h"

#include <memory>

#include "gtest/gtest.h"

namespace supersonic {

class BasicOperationForTest : public BasicOperation {
 public:
  BasicOperationForTest() {}
  BasicOperationForTest(Operation* child1, Operation* child2)
     : BasicOperation(child1, child2) {}

  using BasicOperation::SetBufferAllocator;
  using BasicOperation::buffer_allocator;
};

TEST(BasicOperationTest, SetBufferAllocatorTest) {
  BufferAllocator* default_allocator(
      HeapBufferAllocator::Get());
  MemoryLimit allocator1;
  MemoryLimit allocator2;
  BasicOperationForTest* operation_left(new BasicOperationForTest);
  BasicOperationForTest* operation_right_left(new BasicOperationForTest);
  BasicOperationForTest* operation_right_right(new BasicOperationForTest);
  BasicOperationForTest* operation_right(new BasicOperationForTest(
      operation_right_left, operation_right_right));
  std::unique_ptr<BasicOperationForTest> operation(
      new BasicOperationForTest(operation_left, operation_right));
  EXPECT_TRUE(default_allocator == operation->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_left->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right_left->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right_right->buffer_allocator());
  operation->SetBufferAllocator(&allocator1, true);
  EXPECT_TRUE(&allocator1 == operation->buffer_allocator());
  EXPECT_TRUE(&allocator1 == operation_left->buffer_allocator());
  EXPECT_TRUE(&allocator1 == operation_right->buffer_allocator());
  EXPECT_TRUE(&allocator1 == operation_right_left->buffer_allocator());
  EXPECT_TRUE(&allocator1 == operation_right_right->buffer_allocator());
  operation->SetBufferAllocator(&allocator2, false);
  EXPECT_TRUE(&allocator2 == operation->buffer_allocator());
  EXPECT_TRUE(&allocator1 == operation_left->buffer_allocator());
  EXPECT_TRUE(&allocator1 == operation_right->buffer_allocator());
  EXPECT_TRUE(&allocator1 == operation_right_left->buffer_allocator());
  EXPECT_TRUE(&allocator1 == operation_right_right->buffer_allocator());
  operation->SetBufferAllocator(NULL, true);
  EXPECT_TRUE(default_allocator == operation->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_left->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right_left->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right_right->buffer_allocator());
  operation->SetBufferAllocator(&allocator1, false);
  EXPECT_TRUE(&allocator1 == operation->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_left->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right_left->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right_right->buffer_allocator());
  operation_right->SetBufferAllocator(default_allocator, false);
  operation->SetBufferAllocatorWhereUnset(&allocator2, true);
  EXPECT_TRUE(&allocator1 == operation->buffer_allocator());
  EXPECT_TRUE(&allocator2 == operation_left->buffer_allocator());
  EXPECT_TRUE(default_allocator == operation_right->buffer_allocator());
  EXPECT_TRUE(&allocator2 == operation_right_left->buffer_allocator());
  EXPECT_TRUE(&allocator2 == operation_right_right->buffer_allocator());
}

}  // namespace supersonic
