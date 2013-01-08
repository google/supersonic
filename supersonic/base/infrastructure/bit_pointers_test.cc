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

#include "supersonic/base/infrastructure/bit_pointers.h"

#include "gtest/gtest.h"

// TODO(onufry): Add a parametrized test version that could test many array
// sizes/shifts at once.

namespace supersonic {
namespace bit_pointer {
namespace {

TEST(BitPointersTest, Normalization) {
  char data[100];
  for (int i = 0; i < 100; ++i) data[i] = i;
  for (int i = 0; i < 100; ++i) data[i] = bit_pointer::normalize(data[i]);
  for (int i = 0; i < 100; ++i) EXPECT_EQ((i == 0) ? 0 : 1, data[i]);
}

TEST(BitPointersTest, Accessors) {
  uint32 data[2];
  bit_ptr ptr(data);
  EXPECT_EQ(data, ptr.data());
  EXPECT_EQ(0, ptr.shift());

  bit_const_ptr cptr(data);
  EXPECT_EQ(data, cptr.data());
  EXPECT_EQ(0, cptr.shift());
}

TEST(BitPointersTest, ShiftCreator) {
  uint32 data[2];
  bit_ptr ptr(data, 3);
  EXPECT_EQ(data, ptr.data());
  EXPECT_EQ(3, ptr.shift());

  bit_const_ptr cptr(data, 3);
  EXPECT_EQ(data, cptr.data());
  EXPECT_EQ(3, cptr.shift());
}

TEST(BitPointersTest, Equality) {
  uint32 data[2];
  bit_ptr ptr1(data);
  bit_ptr ptr2(data);
  bit_ptr ptr3(&data[1]);
  EXPECT_EQ(ptr1, ptr2);
  EXPECT_FALSE(ptr1 != ptr2);
  EXPECT_FALSE(ptr1 == ptr3);
  EXPECT_TRUE(ptr1 != ptr3);
  EXPECT_FALSE(ptr2 == ptr3);

  ptr1 = bit_ptr(data, 3);
  ptr2 = bit_ptr(data, 5);
  ptr3 = bit_ptr(data, 3);
  EXPECT_EQ(ptr1, ptr3);
  EXPECT_FALSE(ptr1 == ptr2);
  EXPECT_FALSE(ptr2 == ptr3);

  bit_const_ptr cptr1(data);
  bit_const_ptr cptr2(data);
  bit_const_ptr cptr3(&data[1]);
  EXPECT_EQ(cptr1, cptr2);
  EXPECT_FALSE(cptr1 != cptr2);
  EXPECT_FALSE(cptr1 == cptr3);
  EXPECT_TRUE(cptr1 != cptr3);
  EXPECT_FALSE(cptr2 == cptr3);

  cptr1 = bit_const_ptr(data, 3);
  cptr2 = bit_const_ptr(data, 5);
  cptr3 = bit_const_ptr(data, 3);
  EXPECT_EQ(cptr1, ptr3);
  EXPECT_FALSE(cptr1 == cptr2);
  EXPECT_FALSE(cptr2 == cptr3);

  EXPECT_EQ(ptr1, cptr1);
  EXPECT_EQ(cptr1, ptr1);

  EXPECT_FALSE(ptr1 == data);
  EXPECT_FALSE(cptr1 == data);
  ptr1 = bit_ptr(data);
  EXPECT_TRUE(ptr1 == data);
  ptr1 = bit_ptr();
  EXPECT_TRUE(ptr1 == NULL);
  EXPECT_FALSE(ptr1 != NULL);
  EXPECT_FALSE(ptr2 == NULL);
  EXPECT_TRUE(ptr2 != NULL);
}

TEST(BitPointersTest, NullPointer) {
  bit_ptr ptr;
  EXPECT_EQ(ptr, bit_ptr(NULL));
  EXPECT_EQ(NULL, ptr.data());

  bit_const_ptr cptr;
  EXPECT_EQ(cptr, bit_const_ptr(NULL));
  EXPECT_EQ(NULL, cptr.data());
}

TEST(BitPointersTest, Copy) {
  uint32 data[2];
  bit_ptr ptr(data, 5);
  bit_ptr ptr2(ptr);
  EXPECT_EQ(ptr, ptr2);

  bit_const_ptr cptr(data, 5);
  bit_const_ptr cptr2(cptr);
  EXPECT_EQ(cptr, cptr2);

  bit_const_ptr cptr3(ptr);
  EXPECT_EQ(cptr, cptr3);
}

TEST(BitPointersTest, Incrementation) {
  uint32 data[2];
  bit_ptr ptr(data);
  EXPECT_EQ(bit_ptr(data, 1), ++ptr);
  for (int i = 0; i < 31; ++i) ++ptr;
  EXPECT_EQ(bit_ptr(&data[1]), ptr);

  bit_const_ptr cptr(data);
  EXPECT_EQ(bit_const_ptr(data, 1), ++cptr);
  for (int i = 0; i < 31; ++i) ++cptr;
  EXPECT_EQ(bit_const_ptr(&data[1]), cptr);
}

TEST(BitPointersTest, IterationByPointer) {
  uint32 data[3];
  int loop_length = 0;
  // The bit_ptr(&data[3]) is the equivalent of an "end()" of a STL container -
  // is is the first out of bounds index.
  for (bit_ptr ptr(data); ptr != bit_ptr(&data[3]); ++ptr) {
    loop_length += 1;
  }
  EXPECT_EQ(96, loop_length);
}

TEST(BitPointersTest, Decrementation) {
  uint32 data[2];
  bit_ptr ptr(&data[1]);
  EXPECT_EQ(bit_ptr(data, 31), --ptr);
  for (int i = 0; i < 31; ++i) --ptr;
  EXPECT_EQ(bit_ptr(data), ptr);

  bit_const_ptr cptr(&data[1]);
  EXPECT_EQ(bit_const_ptr(data, 31), --cptr);
  for (int i = 0; i < 31; ++i) --cptr;
  EXPECT_EQ(bit_const_ptr(data), cptr);
}

TEST(BitPointersTest, Addition) {
  uint32 data[2];
  bit_ptr ptr(data);
  EXPECT_EQ(bit_ptr(data, 5), ptr += 5);
  EXPECT_EQ(bit_ptr(data, 10), ptr += 5);
  EXPECT_EQ(bit_ptr(&data[1], 8), ptr += 30);
  EXPECT_EQ(bit_ptr(&data[1], 18), ptr + 10);

  bit_const_ptr cptr(data);
  EXPECT_EQ(bit_const_ptr(data, 5), cptr += 5);
  EXPECT_EQ(bit_const_ptr(data, 10), cptr += 5);
  EXPECT_EQ(bit_const_ptr(&data[1], 8), cptr += 30);
  EXPECT_EQ(bit_const_ptr(&data[1], 18), cptr += 10);
}

TEST(BitPointersTest, Assignment) {
  uint32 data[2];
  bit_ptr ptr(data);
  ptr += 7;
  bit_ptr ptr2 = ptr;
  ptr2 += 7;
  EXPECT_EQ(7, ptr.shift());
  EXPECT_EQ(14, ptr2.shift());

  bit_const_ptr cptr(data);
  cptr += 7;
  bit_const_ptr cptr2 = cptr;
  cptr2 += 7;
  EXPECT_EQ(7, cptr.shift());
  EXPECT_EQ(14, cptr2.shift());
}

TEST(BitPointersTest, BitAssignment) {
  uint32 data[1] = {0};
  bit_ptr ptr(data);

  ptr[2] |= true;
  EXPECT_EQ(true, ptr[2]);
  ptr[3] |= false;
  EXPECT_EQ(false, ptr[3]);
  ptr[2] |= false;
  EXPECT_EQ(true, ptr[2]);
  ptr[2] |= true;
  EXPECT_EQ(true, ptr[2]);

  EXPECT_EQ(4, data[0]);

  ptr[2] &= true;
  EXPECT_EQ(true, ptr[2]);
  ptr[2] &= false;
  EXPECT_EQ(false, ptr[2]);
  ptr[2] &= false;
  EXPECT_EQ(false, ptr[2]);
  ptr[2] &= true;
  EXPECT_EQ(false, ptr[2]);

  EXPECT_EQ(0, data[0]);
}

TEST(BitPointersTest, DereferenceAndRead) {
  uint32 data[2];
  data[0] = 7;
  data[1] = 3;
  bit_ptr ptr(data);
  for (int i = 0; i < 64; ++i) {
    EXPECT_EQ(i < 3 || (i > 31 && i < 34), *ptr);
    ++ptr;
  }

  bit_const_ptr cptr(data);
  for (int i = 0; i < 64; ++i) {
    EXPECT_EQ(i < 3 || (i > 31 && i < 34), *cptr);
    ++cptr;
  }
}

TEST(BitPointersTest, DereferenceAndWrite) {
  uint32 data[2];
  data[0] = 0;
  data[1] = ~0;
  bit_ptr ptr(data);
  for (int i = 0; i < 64; ++i) {
    *ptr = (i % 2 == 0);
    ++ptr;
  }
  EXPECT_EQ(1431655765, data[0]);
  EXPECT_EQ(1431655765, data[1]);
}

TEST(BitPointersTest, ArrayAllocate) {
  bit_array array;
  EXPECT_TRUE(array.mutable_data().is_null());

  EXPECT_TRUE(array.Reallocate(10, HeapBufferAllocator::Get()));
  EXPECT_TRUE(array.mutable_data().is_aligned());
  EXPECT_FALSE(array.mutable_data().is_null());
}

TEST(BitPointersTest, ReallocatePreservesData) {
  bit_array array;
  EXPECT_TRUE(array.Reallocate(10, HeapBufferAllocator::Get()));

  bit_ptr ptr = array.mutable_data();
  for (int i = 0; i < 10; ++i) {
    *ptr = (((i >> 2) ^ (i & 1)) == 1);
    ++ptr;
  }

  EXPECT_TRUE(array.Reallocate(20, HeapBufferAllocator::Get()));
  ptr = array.mutable_data();
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(((i >> 2) ^ (i & 1)) == 1, *ptr);
    ++ptr;
  }
}

TEST(BitPointersTest, BitArrayTryReallocate) {
  MemoryLimit limit(20);
  bit_array array;

  // Remember, the reallocated size is given in bits, not bytes, while the
  // memory limit is in bytes.
  EXPECT_TRUE(array.TryReallocate(40, &limit).is_success());
  EXPECT_FALSE(array.TryReallocate(400, &limit).is_success());
}

TEST(BitPointersTest, BitArrayReallocateFailurePreservesData) {
  MemoryLimit limit(20);
  bit_array array;
  ASSERT_TRUE(array.TryReallocate(10, &limit).is_success());

  bit_pointer::FillWithTrue(array.mutable_data(), 10);
  ASSERT_FALSE(array.TryReallocate(200, &limit).is_success());
  EXPECT_EQ(10, bit_pointer::PopCount(array.mutable_data(), 10));
}



// Expects the pointer to be aligned, also fills the four first bits with some
// data. Expects the const pointer also to be aligned, and to read the data
// the non-const pointer writes.
void ExpectAlignedPointer(bit_ptr ptr, bit_const_ptr const_ptr) {
  int64 int_ptr = reinterpret_cast<int64>(ptr.data());
  EXPECT_EQ(0, int_ptr & 15) << int_ptr;  // Expect to be 16-byte aligned.
  int_ptr = reinterpret_cast<int64>(ptr.data());
  EXPECT_EQ(0, int_ptr & 15) << int_ptr;
  ptr[3] = true;
  EXPECT_EQ(true, const_ptr[3]);
  ptr[4] = false;
  EXPECT_EQ(false, const_ptr[4]);
  ptr[5] = true;
  EXPECT_EQ(true, const_ptr[5]);
  ptr[3] = false;
  EXPECT_EQ(false, const_ptr[3]);
}

template<size_t size>
void test_array() {
  static_bit_array<size> array1;
  // This serves to break the alignment between the two arrays.
  char dummy = 'a';
  static_bit_array<size> array2;

  ExpectAlignedPointer(array1.mutable_data(), array1.const_data());
  ExpectAlignedPointer(array2.mutable_data(), array2.const_data());
  EXPECT_EQ('a', dummy);  // The compiler protests if we don't use the value.
}

TEST(BitPointersTest, StaticArray) {
  test_array<10>();
  test_array<1024>();
}

TEST(BitPointersTest, Subscript) {
  uint32 data[2];
  data[0] = 0;
  data[1] = ~0;
  bit_ptr ptr(data);
  for (int i = 0; i < 64; ++i) ptr[i] = !ptr[i];
  EXPECT_EQ(~0, data[0]);
  EXPECT_EQ(0, data[1]);
}

TEST(BitPointersTest, PopCount) {
  uint32 data[3];
  data[0] = data[1] = 0;
  data[2] = ~0;
  bit_ptr ptr(data);

  EXPECT_EQ(0, bit_pointer::PopCount(ptr, 64));
  EXPECT_EQ(0, bit_pointer::PopCount(ptr, 63));
  ptr[4] = true;
  ptr[37] = true;
  ptr[63] = true;
  EXPECT_EQ(3, bit_pointer::PopCount(ptr, 64));
  EXPECT_EQ(2, bit_pointer::PopCount(ptr, 63));
  EXPECT_EQ(2, bit_pointer::PopCount(ptr, 38));
}

TEST(BitPointersTest, FillFromBitPointerAligned) {
  uint32 source_data[3];
  uint32 dest_data[3];
  bit_ptr source(source_data);
  bit_ptr dest(dest_data);

  // Some random filling.
  for (int i = 0; i < 96; ++i) source[i] = ((i ^ ((i + 5) >> 3)) & 1) == 0;
  bit_pointer::FillFrom(dest, source, 96);
  for (int i = 0; i < 3; ++i) EXPECT_EQ(source_data[i], dest_data[i]);
}

TEST(BitPointersTest, FillFromBitPointerSingleByte) {
  uint32 source_data[1] = {0};
  uint32 dest_data[1] = {0};
  bit_ptr source(source_data);
  bit_ptr dest(dest_data);

  source[2] = source[5] = true;
  dest[0] = dest[2] = dest[4] = dest[7] = true;
  dest += 2;
  source += 2;
  bit_pointer::FillFrom(dest, source, 4);

  source = bit_ptr(source_data);
  source[0] = source[7] = true;
  EXPECT_EQ(source_data[0], dest_data[0]);
}

TEST(BitPointersTest, FillFromBitPointerEquiAligned) {
  uint32 source_data[4] = {0, 123456789, 0, 0};
  uint32 dest_data[4] = {0, 0, 0, 123124};

  bit_ptr source(source_data);
  bit_ptr dest(dest_data);

  dest[1] = dest[90] = true;
  source[4] = source[80] = true;
  dest += 2;
  source += 2;
  bit_pointer::FillFrom(dest, source, 80);

  source = bit_ptr(source_data);
  source[1] = source[90] = true;
  source_data[3] = 123124;

  for (int i = 0; i < 4; ++i) EXPECT_EQ(source_data[i], dest_data[i]);
}

TEST(BitPointersTest, FillFromBitPointerUnalignedDest) {
  uint32 source_data[4] = {111, 1111, 111111, 1007};
  uint32 dest_data[4] = {3, 0, 0, 1007};
  uint32 expected_data[4] = {447, 4444, 444444, 1006};

  bit_ptr source(source_data);
  bit_ptr dest(dest_data);
  dest += 2;
  bit_pointer::FillFrom(dest, source, 95);

  for (int i = 0; i < 4; ++i) EXPECT_EQ(expected_data[i], dest_data[i]);
}

TEST(BitPointersTest, FillFromBitPointerUnalignedBoth) {
  uint32 source_data[3] = {49, (1 << 7) | (1 << 15), (1 << 7) | (1 << 15)};
  uint32 dest_data[3] = {26, 2314, (1 << 16)};
  uint32 expected_data[3] = {14, (1 << 5) | (1 << 13), (1 << 5) | (1 << 16)};

  bit_ptr source(source_data);
  bit_ptr dest(dest_data);
  dest += 2;
  source += 4;
  bit_pointer::FillFrom(dest, source, 68);

  for (int i = 0; i < 3; ++i) EXPECT_EQ(expected_data[i], dest_data[i]);
}

// Regression test for a previous bug (bad loop initialization).
TEST(BitPointersTest, FillFromBitPointerUnalignedSingleByte) {
  uint32 source_data[1] = {0};
  uint32 dest_data[1] = {~0U};

  bit_ptr source(source_data);
  ++source;
  bit_ptr dest(dest_data);

  bit_pointer::FillFrom(dest, source, 2);
  EXPECT_EQ(~3U, dest_data[0]);
}

TEST(BitPointersTest, FillFromBooleanAligned) {
  uint32 data[3] = {13245678, 87654321, 13579864};
  bool source[80];
  bit_ptr ptr(data);

  // Some random filling.
  for (int i = 0; i < 80; ++i) source[i] = ((i ^ ((i + 3) >> 2)) & 1) == 0;
  bit_pointer::FillFrom(ptr, source, 80);
  for (int i = 0; i < 80; ++i) EXPECT_EQ(source[i], ptr[i]) << i;
}

TEST(BitPointersTest, FillFromBooleanUnaligned) {
  uint32 data[3] = {2, 0, 1 << 30};
  bool source[85];
  bit_ptr ptr(data);

  // Some random filling.
  for (int i = 0; i < 85; ++i) source[i] = ((i ^ ((i + 3) >> 2)) & 1) == 0;
  bit_pointer::FillFrom(ptr + 7, source, 85);
  for (int i = 0; i < 7; ++i) EXPECT_EQ((i == 1), ptr[i]);
  for (int i = 7; i < 7 + 85; ++i) EXPECT_EQ(source[i-7], ptr[i]);
  for (int i = 7 + 85; i < 96; ++i) EXPECT_EQ((i == 94), ptr[i]);
}

TEST(BitPointersTest, FillFromBitPtrToBooleanAligned) {
  uint32 data[3] = {0, 0, 0};
  bool dest[96];
  bit_ptr ptr(data);

  // Some random filling.
  for (int i = 0; i < 96; ++i) ptr[i] = ((i ^ ((i + 3) >> 2)) & 1) == 0;
  bit_pointer::FillFrom(dest, ptr, 96);
  for (int i = 0; i < 96; ++i) EXPECT_EQ(ptr[i], dest[i]);
}

TEST(BitPointersTest, FillFromBitPtrToBooleanUnaligned) {
  uint32 data[3] = {0, 0, 0};
  bool dest[96];
  dest[0] = dest[94] = false;
  dest[1] = dest[95] = true;
  bit_ptr ptr(data);

  // Some random filling.
  for (int i = 0; i < 96; ++i) ptr[i] = ((i ^ ((i + 3) >> 2)) & 1) == 0;
  bit_pointer::FillFrom(&dest[2], ptr + 2, 92);
  // No overwriting of data.
  EXPECT_EQ(false, dest[0]);
  EXPECT_EQ(true, dest[1]);
  EXPECT_EQ(false, dest[94]);
  EXPECT_EQ(true, dest[95]);
  for (int i = 2; i < 94; ++i) EXPECT_EQ(ptr[i], dest[i]);
}

TEST(BitPointersTest, FillFromBooleanRegressionTest) {
  // Code from column_validity_checkers_test.cc.
  static_bit_array<10> array;
  bool nulls[6] = { true, true, false, false, false, false };
  bit_ptr null_ptr(array.mutable_data());
  bit_pointer::FillFrom(null_ptr, nulls, 6);
  for (int i = 0; i < 6; ++i) EXPECT_EQ(nulls[i], null_ptr[i]);
}

#if 0
// These two tests rely on undefined behavior, which will cause a test failure
// when compiling with LLVM/Clang.
TEST(BitPointersTest, FillFromBooleanRegressionTest2) {
  // Didn't work for "evil" (i.e. not 0/1) boolean variables.
  char data[9] = { 17, 15, 16, 118, 0, 43, 65, 91, 222 };
  static_bit_array<10> array;
  bit_ptr ptr(array.mutable_data());
  bit_pointer::FillFrom(ptr, reinterpret_cast<bool*>(data), 9);
  for (int i = 0; i < 9; ++i) EXPECT_EQ(i != 4, ptr[i]);
}

TEST(BitPointersTest, FillBooleanFromBooleanSafe) {
  char data[12] = { 17, 15, 16, 118, 0, 43, 0, 64, 65, 222, 0, 1 };
  char expected[12] = { 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 0 };
  char output[12];
  output[10] = true;
  output[11] = false;
  bit_pointer::SafeFillFrom(reinterpret_cast<bool*>(output),
                            reinterpret_cast<bool*>(data),
                            10);
  for (int i = 0; i < 12; ++i) EXPECT_EQ(expected[i], output[i]);
}

// This test relies on undefined behavior, and crosstoolv15
// makes it fail due to valid transformation of invalid code. See also
TEST(BitPointersTest, AssignEvil) {
  char data[9] = { 17, 15, 16, 118, 0, 43, 65, 91, 222 };
  bool* bool_data = reinterpret_cast<bool*>(data);
  static_bit_array<10> array;
  bit_ptr ptr(array.mutable_data());
  bit_pointer::FillWithFalse(ptr, 9);
  ptr[2] = bool_data[1];
  for (int i = 0; i < 9; ++i) EXPECT_EQ(i == 2, ptr[i]);
  ptr[2] = false;

  ptr[2] |= bool_data[1];
  for (int i = 0; i < 9; ++i) EXPECT_EQ(i == 2, ptr[i]);

  ptr[4] = true;
  ptr[5] = true;

  ptr[2] &= bool_data[3];
  for (int i = 0; i < 9; ++i) EXPECT_EQ(i == 2 || i == 4 || i == 5, ptr[i]);
}
#endif

TEST(BitPointersTest, FillWithTrue) {
  uint32 data[2] = {0, 0};
  bit_ptr ptr(data);

  bit_pointer::FillWithTrue(ptr, 40);
  EXPECT_EQ(~0, data[0]);
  EXPECT_EQ((1 << 8) - 1, data[1]);
}

TEST(BitPointersTest, FillWithTrueUnaligned) {
  uint32 data[4] = {0, 0, 0, 0};
  bit_ptr ptr(data);
  ptr += 19;
  bit_pointer::FillWithTrue(ptr, 25);

  bit_ptr check(data);
  for (int i = 0; i < 70; ++i) {
    EXPECT_EQ((i >= 19 && i < 19 + 25), *check) << "Failure at " << i;
    ++check;
  }
}

TEST(BitPointersTest, FillWithTrueInSingleByte) {
  uint32 data = 0;
  bit_ptr ptr(&data);
  ptr += 10;
  bit_pointer::FillWithTrue(ptr, 3);

  bit_ptr check(&data);
  for (int i = 0; i < 32; ++i) {
    EXPECT_EQ((i >= 10 && i < 10 + 3), *check);
    ++check;
  }
}

TEST(BitPointersTest, FillWithFalse) {
  uint32 data[3] = {~0U, ~0U, ~0U};
  bit_ptr ptr(data);

  bit_pointer::FillWithFalse(ptr, 37);
  EXPECT_EQ(0, data[0]);
  EXPECT_EQ(~0 << 5, data[1]);
  EXPECT_EQ(~0, data[2]);
}

TEST(BitPointersTest, FillWithFalseUnaligned) {
  uint32 data[4] = {~0U, ~0U, ~0U, ~0U};
  bit_ptr ptr(data);
  ptr += 35;
  bit_pointer::FillWithFalse(ptr, 47);

  bit_ptr check(data);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(!(i >= 35 && i < 35 + 47), *check) << "Failure at " << i;
    ++check;
  }
}

TEST(BitPointersTest, FillWithFalseInSingleByte) {
  uint32 data[1] = {~0U};
  bit_ptr ptr(data);
  ptr += 1;
  bit_pointer::FillWithFalse(ptr, 6);

  bit_ptr check(data);
  for (int i = 0; i < 32; ++i) {
    EXPECT_EQ(!(i >= 1 && i < 1 + 6), *check) << "Failure at " << i;
    ++check;
  }
}

TEST(BitPointersTest, BoolViewCreation) {
  BoolView view(2);

  EXPECT_EQ(bool_ptr(NULL), view.column(0));
  EXPECT_EQ(bool_ptr(NULL), view.column(1));

  EXPECT_EQ(2, view.column_count());
  EXPECT_EQ(0, view.row_count());
}

TEST(BitPointersTest, BoolViewReset) {
  bool_array data;
  ASSERT_TRUE(data.Reallocate(10, HeapBufferAllocator::Get()));
  bit_pointer::FillWithFalse(data.mutable_data(), 10);

  BoolView view(2);

  view.ResetColumn(0, data.mutable_data());
  view.ResetColumn(1, data.mutable_data());
  view.set_row_count(10);

  EXPECT_EQ(data.mutable_data(), view.column(0));
  EXPECT_EQ(data.mutable_data(), view.column(1));

  EXPECT_EQ(false, view.column(1)[0]);
  view.column(0)[0] = true;
  EXPECT_EQ(true, view.column(1)[0]);
}

TEST(BitPointersTest, BoolBlockCreate) {
  BoolBlock block(3, HeapBufferAllocator::Get());

  EXPECT_EQ(3, block.column_count());
  EXPECT_EQ(0, block.row_capacity());
}

TEST(BitPointersTest, BoolBlockReallocate) {
  BoolBlock block(2, HeapBufferAllocator::Get());
  ASSERT_TRUE(block.TryReallocate(4).is_success());

  EXPECT_EQ(4, block.row_capacity());
  EXPECT_EQ(2, block.column_count());

  bit_pointer::FillWithFalse(block.view().column(0), 4);
  bit_pointer::FillWithTrue(block.view().column(1), 4);
  EXPECT_EQ(0, bit_pointer::PopCount(block.view().column(0), 4));
  EXPECT_EQ(4, bit_pointer::PopCount(block.view().column(1), 4));
}

TEST(BitPointersTest, BoolBlockReallocateFailurePreservesData) {
  // We don't use a sharp memory limit, because this has to work for both
  // implementations of bool_array, which have different memory requirements.
  MemoryLimit limit(20);
  BoolBlock block(1, &limit);
  ASSERT_TRUE(block.TryReallocate(10).is_success());

  bit_pointer::FillWithTrue(block.view().column(0), 10);
  ASSERT_FALSE(block.TryReallocate(200).is_success());
  EXPECT_EQ(10, bit_pointer::PopCount(block.view().column(0), 10));
}

TEST(BitPointerTest, BoolBlockReallocatePreservesData) {
  BoolBlock block(2, HeapBufferAllocator::Get());
  ASSERT_TRUE(block.TryReallocate(10).is_success());

  bit_pointer::FillWithTrue(block.view().column(1), 10);
  block.view().column(1)[2] = false;
  block.view().column(1)[5] = false;
  block.view().column(1)[9] = false;
  EXPECT_EQ(7, bit_pointer::PopCount(block.view().column(1), 10));
  ASSERT_TRUE(block.TryReallocate(5).is_success());
  EXPECT_EQ(4, bit_pointer::PopCount(block.view().column(1), 5));
}

TEST(BitPointersTest, BoolViewSingleColumnConstructor) {
  BoolBlock block(1, HeapBufferAllocator::Get());
  ASSERT_TRUE(block.TryReallocate(10).is_success());

  BoolView view_copy(block.view().column(0));
  view_copy.column(0)[0] = false;
  view_copy.column(0)[1] = true;
  EXPECT_EQ(false, block.view().column(0)[0]);
  EXPECT_EQ(true, block.view().column(0)[1]);
  block.view().column(0)[0] = true;
  EXPECT_EQ(true, view_copy.column(0)[0]);
}

}  // namespace
}  // namespace bit_pointer
}  // namespace supersonic
