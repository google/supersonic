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

#include "supersonic/expression/vector/vector_logic.h"

#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/memory/memory.h"
#include "gtest/gtest.h"

namespace supersonic {
namespace {

void FillEveryNth(bit_pointer::bit_ptr selector, size_t size, int n) {
  for (int i = 0; i < size; ++i) selector[i] = (i % n == 0);
}

void FillEveryNth(bool* tab, size_t size, int n) {
  for (int i = 0; i < size; ++i) tab[i] = (i % n == 0);
}

void Prepare(bool* left, bool* right, size_t size) {
  FillEveryNth(left, size, 3);
  FillEveryNth(right, size, 5);
}

void Prepare(bit_pointer::bit_array* array,
             bit_pointer::bit_ptr* ptr,
             size_t size) {
  array->Reallocate(size, HeapBufferAllocator::Get());
  *ptr = bit_pointer::bit_ptr(array->mutable_data());
}

TEST(VectorLogicTest, Or) {
  const int kSize = 200;
  bool left[kSize], right[kSize], result[kSize];
  Prepare(left, right, kSize);

  vector_logic::Or(left, right, kSize, result);
  for (int i = 0; i < kSize; ++i)
    EXPECT_EQ(i % 3 == 0 || i % 5 == 0, result[i]);
}

TEST(VectorLogicTest, And) {
  const int kSize = 150;
  bool left[kSize], right[kSize], result[kSize];
  Prepare(left, right, kSize);

  vector_logic::And(left, right, kSize, result);
  for (int i = 0; i < kSize; ++i) EXPECT_EQ(i % 15 == 0, result[i]);
}

TEST(VectorLogicTest, AndNot) {
  const int kSize = 170;
  bool left[kSize], right[kSize], result[kSize];
  Prepare(left, right, kSize);

  vector_logic::AndNot(left, right, kSize, result);
  for (int i = 0; i < kSize; ++i)
    EXPECT_EQ(i % 3 != 0 && i % 5 == 0, result[i]);
}

TEST(VectorLogicTest, Not) {
  const int kSize = 120;
  bool left[kSize], right[kSize], result[kSize];
  Prepare(left, right, kSize);

  vector_logic::Not(left, kSize, result);
  for (int i = 0; i < kSize; ++i)
    EXPECT_EQ(i % 3 != 0, result[i]);
}

TEST(VectorLogicTest, BitOr) {
  const int kSize = 203;
  bit_pointer::bit_array left_array, right_array, result_array;
  bit_pointer::bit_ptr left, right, result;

  Prepare(&left_array, &left, kSize);
  FillEveryNth(left, kSize, 3);
  Prepare(&right_array, &right, kSize);
  FillEveryNth(right, kSize, 5);
  Prepare(&result_array, &result, kSize);

  vector_logic::Or(left, right, kSize, result);
  for (int i = 0; i < kSize; ++i)
    EXPECT_EQ(i % 3 == 0 || i % 5 == 0, result[i]);
}

TEST(VectorLogicTest, BitAnd) {
  const int kSize = 171;
  bit_pointer::bit_array left_array, right_array, result_array;
  bit_pointer::bit_ptr left, right, result;

  Prepare(&left_array, &left, kSize);
  FillEveryNth(left, kSize, 3);
  Prepare(&right_array, &right, kSize);
  FillEveryNth(right, kSize, 5);
  Prepare(&result_array, &result, kSize);

  vector_logic::And(left, right, kSize, result);
  for (int i = 0; i < kSize; ++i) EXPECT_EQ(i % 15 == 0, result[i]);
}

TEST(VectorLogicTest, BitAndNot) {
  const int kSize = 123;
  bit_pointer::bit_array left_array, right_array, result_array;
  bit_pointer::bit_ptr left, right, result;

  Prepare(&left_array, &left, kSize);
  FillEveryNth(left, kSize, 3);
  Prepare(&right_array, &right, kSize);
  FillEveryNth(right, kSize, 5);
  Prepare(&result_array, &result, kSize);

  vector_logic::AndNot(left, right, kSize, result);
  for (int i = 0; i < kSize; ++i)
    EXPECT_EQ(i % 3 != 0 && i % 5 == 0, result[i]);
}

TEST(VectorLogicTest, BitNot) {
  const int kSize = 129;
  bit_pointer::bit_array in_array, result_array;
  bit_pointer::bit_ptr in, result;

  Prepare(&in_array, &in, kSize);
  FillEveryNth(in, kSize, 7);
  Prepare(&result_array, &result, kSize);

  vector_logic::Not(in, kSize, result);
  for (int i = 0; i < kSize; ++i)
    EXPECT_EQ(i % 7 != 0, result[i]);
}

}  // namespace
}  // namespace supersonic
