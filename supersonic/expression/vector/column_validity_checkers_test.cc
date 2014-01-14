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

#include "supersonic/expression/vector/column_validity_checkers.h"

#include <sys/types.h>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "gtest/gtest.h"

namespace supersonic {

// -------------------    Failers    ----------------------

// IsNonPositiveFailer.
TEST(ColumnValidityCheckersTest, IsNonPositiveFailerSucceeds) {
  // We'll always be using the same size of the bit_array to avoid template
  // code bloat.
  small_bool_array array;
  bool nulls[4] = {true, true, false, false};
  bit_pointer::FillFrom(array.mutable_data(), nulls, 4);
  int32 data[4] = {-1, 1, 2, 1};
  failers::IsNonPositiveFailer failer;
  EXPECT_FALSE(failer(data, array.const_data(), 4));
}

TEST(ColumnValidityCheckersTest, IsNonPositiveFailerFails) {
  small_bool_array array;
  bool nulls[5] = {true, true, false, false, false};
  bit_pointer::FillFrom(array.mutable_data(), nulls, 5);
  int32 data[5] = {1, 0, 1, 0, -1};
  failers::IsNonPositiveFailer failer;
  EXPECT_EQ(1, failer(data, array.const_data(), 4));
}

TEST(ColumnValidityCheckersTest, IsNonPositiveFailerNotNull) {
  int32 data[5] = {1, 0, 1, 0, -1};
  failers::IsNonPositiveFailer failer;
  EXPECT_EQ(3, failer(data, bool_const_ptr(NULL), 5));
}

// IsNegativeFailer.
TEST(ColumnValidityCheckersTest, IsNegativeFailerSucceeds) {
  small_bool_array array;
  bool nulls[5] = {true, true, false, false, false};
  bit_pointer::FillFrom(array.mutable_data(), nulls, 5);
  double data[5] = {-1., 1., 0., 1., -2.};
  failers::IsNegativeFailer failer;
  EXPECT_FALSE(failer(data, array.const_data(), 4));
}

TEST(ColumnValidityCheckersTest, IsNegativeFailerFails) {
  small_bool_array array;
  bool nulls[6] = {true, true, false, false, false, false};
  bit_pointer::FillFrom(array.mutable_data(), nulls, 6);
  double data[6] = {1., 0., 1., -1., 0., -2.};
  failers::IsNegativeFailer failer;
  EXPECT_EQ(2, failer(data, array.const_data(), 6));
}

TEST(ColumnValidityCheckersTest, IsNegativeFailerNotNull) {
  int32 data[4] = {-1, 0, 1, 2};
  failers::IsNegativeFailer failer;
  EXPECT_EQ(1, failer(data, bool_ptr(NULL), 4));
}

// SecondColumnZeroFailer.
TEST(ColumnValidityCheckersTest, SecondColumnZeroFailerSucceeds) {
  int64 left[6] = {2LL, 0LL, -2LL, 2LL, 0LL, -2LL};
  small_bool_array right_array;
  bool right_nulls[6] = {true, false, false, false, true, false};
  bit_pointer::FillFrom(right_array.mutable_data(), right_nulls, 6);
  uint32 right[6] = {4, 3, 2, 1, 0, 0};
  failers::SecondColumnZeroFailer failer;
  EXPECT_EQ(0, failer(left, bool_ptr(NULL), right,
                      right_array.mutable_data(), 5));
}

TEST(ColumnValidityCheckersTest, SecondColumnZeroFailerFails) {
  uint64 left[6] = {2LL, 0LL, 2LL, 0LL, 2LL, 0LL};
  small_bool_array left_array;
  bool left_nulls[6] = {true, true, true, true, true, true};
  bit_pointer::FillFrom(left_array.mutable_data(), left_nulls, 6);
  small_bool_array right_array;
  bool right_nulls[6] = {true, false, false, true, false, false};
  bit_pointer::FillFrom(right_array.mutable_data(), right_nulls, 6);
  int64 right_data[6] = {2LL, 1LL, 0LL, 0LL, 0LL, 0LL};
  failers::SecondColumnZeroFailer failer;
  EXPECT_EQ(2, failer(left, left_array.const_data(), right_data,
                      right_array.const_data(), 5));
}

TEST(ColumnValidityCheckersTest, SecondColumnZeroFailerNotNull) {
  int32 left[5] = {0, 1, 2, 3, 4};
  int32 right[5] = {1, 0, 0, 0, 0};
  failers::SecondColumnZeroFailer failer;
  EXPECT_EQ(3, failer(left, bool_ptr(NULL), right, bool_ptr(NULL), 4));
}

// SecondColumnNegativeFailer.
TEST(ColumnValidityCheckersTest, SecondColumnNegativeFailerSucceeds) {
  small_bool_array left_array;
  bool left_nulls[6] = {false, false, false, false, false};
  bit_pointer::FillFrom(left_array.mutable_data(), left_nulls, 6);
  double left[6] = {-1., -1., -1., -1., -1., -1.};
  small_bool_array right_array;
  bool right_nulls[6] = {true, false, false, false, true, false};
  bit_pointer::FillFrom(right_array.mutable_data(), right_nulls, 6);
  double right[6] = {0., 1., 0., 2., -1., -1.};
  failers::SecondColumnNegativeFailer failer;
  EXPECT_EQ(0, failer(left, left_array.const_data(),
                      right, right_array.const_data(), 5));
}

TEST(ColumnValidityCheckersTest, SecondColumnNegativeFailerFails) {
  small_bool_array left_array;
  bool left_nulls[6] = {true, true, true, true, true, true};
  bit_pointer::FillFrom(left_array.mutable_data(), left_nulls, 6);
  string left[6] = {"", "", "", "", "", ""};
  small_bool_array right_array;
  bool right_nulls[6] = {true, false, false, false, true, false};
  bit_pointer::FillFrom(right_array.mutable_data(), right_nulls, 6);
  int32 right[6] = {0, 1, 0, -1, -2, -3};
  failers::SecondColumnNegativeFailer failer;
  EXPECT_EQ(1, failer(left, left_array.const_data(),
                      right, right_array.const_data(), 5));
}

TEST(ColumnValidityCheckersTest, SecondColumnNegativeFailerNotNull) {
  int32 data[4] = {-1, -2, -3, -4};
  failers::SecondColumnNegativeFailer failer;
  EXPECT_EQ(3, failer(data, bool_ptr(NULL), data, bool_ptr(NULL), 3));
}

TEST(ColumnValidityCheckersTest, SecondColumnNegativeFailerUint) {
  uint32 data[4] = {static_cast<uint>(-1), static_cast<uint>(-2), 0, 1};
  failers::SecondColumnNegativeFailer failer;
  EXPECT_EQ(0, failer(data, bool_ptr(NULL), data, bool_ptr(NULL), 4));
}

// First column negative, second non integer failer.
TEST(ColumnValidityCheckersTest,
     FirstColumnNegativeAndSecondNonIntegerFailer) {
  small_bool_array left_array;
  bool left_nulls[8] =  {false, false, false, true,  false, true, false, false};
  bit_pointer::FillFrom(left_array.mutable_data(), left_nulls, 8);
  double left[8] =      {1.,    0.,    -1.,   -1.,   -1.,  -1,    -1,    -1};
  small_bool_array right_array;
  bool right_nulls[8] = {false, false, false, false, true,  true, false, false};
  bit_pointer::FillFrom(right_array.mutable_data(), right_nulls, 8);
  double right[8] =     {0.5,   0.001, 0.001, 0.5,   0.5,   0.5,  -1,    0.5};

  int failures[8] =     {0,     0,     1,     1,     1,     1,    1,     2};
  failers::FirstColumnNegativeAndSecondNonIntegerFailer failer;
  for (int i = 0; i < 8; ++i)
    EXPECT_EQ(failures[i], failer(left, left_array.const_data(),
                                  right, right_array.const_data(), i+1));
}

// -------------------   Nullers    ------------------

// IsNonPositiveNuller.
TEST(ColumnValidityCheckersTest, IsNonPositiveNuller) {
  small_bool_array array;
  bool nulls[6] =    {true, true, false, false, false, false};
  bool_ptr null_ptr(array.mutable_data());
  bit_pointer::FillFrom(null_ptr, nulls, 6);
  int64 data[6] =    {1LL,  -1LL, 1LL,   0LL,   -1LL,  -2LL};
  bool expected[6] = {true, true, false, true,  true,  false};

  nullers::IsNonPositiveNuller nuller;
  nuller(data, null_ptr, 5);
  for (int i = 0; i < 6; ++i) EXPECT_EQ(expected[i], null_ptr[i]) << i;
}

// IsNegativeNuller.
TEST(ColumnValidityCheckersTest, IsNegativeNuller) {
  small_bool_array array;
  bool nulls[6] =    {true, true, false, false, false, false};
  bool_ptr null_ptr(array.mutable_data());
  bit_pointer::FillFrom(null_ptr, nulls, 6);
  int64 data[6] =    {1LL,  -1LL,   1LL,   0LL,  -1LL, -2LL};
  bool expected[6] = {true, true, false, false,  true, false};

  nullers::IsNegativeNuller nuller;
  nuller(data, null_ptr, 5);
  for (int i = 0; i < 6; ++i) EXPECT_EQ(expected[i], null_ptr[i]) << i;
}

// SecondColumnZeroNuller.
TEST(ColumnValidityCheckersTest, SecondColumnZeroNuller) {
  small_bool_array array;
  bool nulls[6] =    {true, true, false, false, true, false};
  bool_ptr null_ptr(array.mutable_data());
  bit_pointer::FillFrom(null_ptr, nulls, 6);
  string left[5] =   {"",     "",    "",    "",   ""};
  int32 right[6] =   {0,       1,     0,     1,    1,     0};
  bool expected[6] = {true, true,  true, false, true, false};

  nullers::SecondColumnZeroNuller nuller;
  nuller(left, right, null_ptr, 5);
  for (int i = 0; i < 6; ++i) EXPECT_EQ(expected[i], null_ptr[i]) << i;
}

// SecondColumnNegativeNuller.
TEST(ColumnValidityCheckersTest, SecondColumnNegativeNuller) {
  small_bool_array array;
  bool nulls[6] =    {true, false, false, false, true, false};
  bool_ptr null_ptr(array.mutable_data());
  bit_pointer::FillFrom(null_ptr, nulls, 6);
  int left[5] =         {1,     2,     3,    -1,  -1};
  double right[6] =    {2.,    1.,    0.,   -1.,  -2.,   -3.};
  bool expected[6] = {true, false, false,  true, true, false};

  nullers::SecondColumnNegativeNuller nuller;
  nuller(left, right, null_ptr, 5);
  for (int i = 0; i < 6; ++i) EXPECT_EQ(expected[i], null_ptr[i]) << i;
}

// First column negative, second non integer failer.
TEST(ColumnValidityCheckersTest,
     FirstColumnNegativeAndSecondNonIntegerNuller) {
  small_bool_array array;
  bool nulls[6] =    {true, true, false, false, false, false};
  bool_ptr null_ptr(array.mutable_data());
  bit_pointer::FillFrom(null_ptr, nulls, 6);
  for (int i = 0; i < 6; ++i) EXPECT_EQ(nulls[i], null_ptr[i]) << i << "!";
  double left[6] =   {-1,     -1,    -1,    -1,     0,     1};
  double right[6] =  {1,     0.4,   0.2,     0,   0.2,   0.2};
  bool expected[6] = {true, true,  true, false, false, false};

  nullers::FirstColumnNegativeAndSecondNonIntegerNuller nuller;
  nuller(left, right, null_ptr, 6);
  for (int i = 0; i < 6; ++i) EXPECT_EQ(expected[i], null_ptr[i]) << i;
}

}  // namespace supersonic
