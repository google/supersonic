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

#include "supersonic/base/infrastructure/aggregation_operators.h"

#include "supersonic/utils/integral_types.h"
#include "gtest/gtest.h"

namespace supersonic {
namespace aggregations {
namespace {

TEST(AggregationOperatorsTest, TrivialAssignment) {
  AssignmentOperator<INT32, INT32> assigner(HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  int32 result = 0;
  EXPECT_TRUE(assigner(1, &result, &buffer));
  EXPECT_EQ(1, result);
}

TEST(AggregationOperatorsTest, CrossTypeAssignment) {
  AssignmentOperator<INT32, INT64> assigner(HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  int64 result = 1LL;
  EXPECT_TRUE(assigner(1, &result, &buffer));
  EXPECT_EQ(1, result);
}

TEST(AggregationOperatorsTest, StringAssignmentTrivial) {
  AssignmentOperator<STRING, STRING> assigner(HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  StringPiece result;
  EXPECT_TRUE(assigner("One", &result, &buffer));
  EXPECT_EQ("One", result);
}

TEST(AggregationOperatorsTest, DeepCopyDeepCopies) {
  AssignmentOperator<STRING, STRING, true> assigner(HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  StringPiece result;
  char data[4] = "One";
  EXPECT_TRUE(assigner(data, &result, &buffer));
  EXPECT_EQ(data, result);
  data[2] = 'g';
  EXPECT_EQ("One", result);
}

TEST(AggregationOperatorsTest, ShallowCopyShallowCopies) {
  AssignmentOperator<STRING, STRING, false> assigner(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  StringPiece result;
  char data[4] = "One";
  EXPECT_TRUE(assigner(data, &result, &buffer));
  EXPECT_EQ(data, result);
  data[2] = 'g';
  EXPECT_EQ("Ong", result);
}

TEST(AggregationOperatorsTest, StringAssignmentDeepCopies) {
  AssignmentOperator<STRING, STRING> assigner(HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;
  char tab[5] = "One";

  StringPiece result;
  EXPECT_TRUE(assigner(tab, &result, &buffer));
  EXPECT_EQ("One", result);
  tab[1] = 'M';
  tab[2] = 'G';
  EXPECT_EQ("One", result);  // result does not point to original data.
}

TEST(AggregationOperatorsTest, StringAssignmentSeveralAssignments) {
  AssignmentOperator<STRING, STRING> assigner(HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  StringPiece result;
  EXPECT_TRUE(assigner("One", &result, &buffer));
  EXPECT_EQ("One", result);
  EXPECT_TRUE(assigner("Two", &result, &buffer));
  EXPECT_EQ("Two", result);
  EXPECT_TRUE(assigner("Three", &result, &buffer));
  EXPECT_EQ("Three", result);
}

TEST(AggregationOperatorsTest, StringAssignmentDoesNotWasteMemory) {
  std::unique_ptr<BufferAllocator> allocator(new MemoryLimit(12));
  AssignmentOperator<STRING, STRING> assigner(allocator.get());
  std::unique_ptr<Buffer> buffer;

  StringPiece result;
  char tab[11] = "ABCDEFGHIJ";
  // We check that the old memory is not kept, but released back to the
  // allocator.
  for (int i = 0; i < 9; ++i) {
    EXPECT_TRUE(assigner(StringPiece(tab, i), &result, &buffer));
    EXPECT_EQ(StringPiece(tab, i), result);
  }
}

TEST(AggregationOperatorsTest, StringAssignmentRespectsMemoryLimit) {
  std::unique_ptr<BufferAllocator> allocator(new MemoryLimit(10));
  AssignmentOperator<STRING, STRING> assigner(allocator.get());
  std::unique_ptr<Buffer> buffer;

  StringPiece result;
  EXPECT_TRUE(assigner("Short", &result, &buffer));
  EXPECT_FALSE(assigner("Very, very long", &result, &buffer));
}

TEST(AggregationOperatorsTest, StringAssignmentSurvivesBufferChange) {
  AssignmentOperator<STRING, STRING> assigner(HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer_one;
  std::unique_ptr<Buffer> buffer_two;

  StringPiece result;
  EXPECT_TRUE(assigner("One", &result, &buffer_one));
  EXPECT_EQ("One", result);
  EXPECT_TRUE(assigner("Two", &result, &buffer_two));
  EXPECT_EQ("Two", result);
  EXPECT_TRUE(assigner("Three", &result, &buffer_one));
  EXPECT_EQ("Three", result);
}

TEST(AggregationOperatorsTest, IntToStringAssignment) {
  AssignmentOperator<INT32, STRING> assigner(HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  StringPiece result;
  EXPECT_TRUE(assigner(2011, &result, &buffer));
  EXPECT_EQ("2011", result);
}

TEST(AggregationOperatorsTest, Sum) {
  AggregationOperator<SUM, UINT32, INT64> aggregator(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  int64 result = -12LL;
  EXPECT_TRUE(aggregator(1, &result, &buffer));
  EXPECT_EQ(-11LL, result);
  EXPECT_TRUE(aggregator(30, &result, &buffer));
  EXPECT_EQ(19LL, result);
}

TEST(AggregationOperatorsTest, Max) {
  AggregationOperator<MAX, FLOAT, FLOAT> aggregator(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  float result = 0.;
  EXPECT_TRUE(aggregator(1., &result, &buffer));
  EXPECT_EQ(1., result);
  EXPECT_TRUE(aggregator(0.5, &result, &buffer));
  EXPECT_EQ(1., result);
  EXPECT_TRUE(aggregator(-1. / 0., &result, &buffer));  // -inf.
  EXPECT_EQ(1., result);
  EXPECT_TRUE(aggregator(1. / 0., &result, &buffer));  // +inf.
  // TODO(onufry): Add tests for NaN support, once NaN supported.
}

TEST(AggregationOperatorsTest, MaxCopyDepth) {
  AggregationOperator<MAX, STRING, STRING, true> deep_maxer(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;
  AggregationOperator<MAX, STRING, STRING, false> shallow_maxer(
      HeapBufferAllocator::Get());

  char data1[4] = "One";
  char data2[4] = "Two";
  StringPiece deep_result(data1);
  StringPiece shallow_result(data1);
  EXPECT_TRUE(deep_maxer(data2, &deep_result, &buffer));
  EXPECT_TRUE(shallow_maxer(data2, &shallow_result, &buffer));
  data2[2] = 't';
  EXPECT_EQ("Two", deep_result);
  EXPECT_EQ("Twt", shallow_result);
}

// This is a regression test for a bug that caused each of the arguments of the
// max to be cast to the type of the other one.
TEST(AggregationOperatorsTest, MaxNoCastArguments) {
  AggregationOperator<MAX, INT32, UINT32> aggregator(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  uint32 result = 0;
  EXPECT_TRUE(aggregator(1, &result, &buffer));
  EXPECT_EQ(1, result);
  EXPECT_TRUE(aggregator(-1, &result, &buffer));
  EXPECT_EQ(1, result);  // This is the line that failed.
}

TEST(AggregationOperatorsTest, MinForStrings) {
  AggregationOperator<MIN, STRING, STRING> aggregator(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  char tab[8] = "weasel";
  StringPiece result(tab);
  EXPECT_TRUE(aggregator("zebra", &result, &buffer));
  EXPECT_EQ("weasel", result);
  EXPECT_TRUE(aggregator("gnu", &result, &buffer));
  EXPECT_EQ("gnu", result);
  EXPECT_TRUE(aggregator("hippopotamus", &result, &buffer));
  EXPECT_EQ("gnu", result);
  EXPECT_TRUE(aggregator("antelope (a big one)", &result, &buffer));
  EXPECT_EQ("antelope (a big one)", result);
}

TEST(AggregationOperatorsTest, MinCopyDepth) {
  AggregationOperator<MIN, STRING, STRING, true> deep_miner(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;
  AggregationOperator<MIN, STRING, STRING, false> shallow_miner(
      HeapBufferAllocator::Get());

  char data1[4] = "One";
  char data2[4] = "Two";
  StringPiece deep_result(data2);
  StringPiece shallow_result(data2);
  EXPECT_TRUE(deep_miner(data1, &deep_result, &buffer));
  EXPECT_TRUE(shallow_miner(data1, &shallow_result, &buffer));
  data1[2] = 't';
  EXPECT_EQ("One", deep_result);
  EXPECT_EQ("Ont", shallow_result);
}

TEST(AggregationOperatorsTest, ConcatStrings) {
  AggregationOperator<CONCAT, STRING, STRING> aggregator(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  StringPiece result("");
  EXPECT_TRUE(aggregator("G", &result, &buffer));
  EXPECT_EQ(",G", result);
  EXPECT_TRUE(aggregator("az", &result, &buffer));
  EXPECT_EQ(",G,az", result);
  EXPECT_TRUE(aggregator("elle", &result, &buffer));
  EXPECT_EQ(",G,az,elle", result);
  EXPECT_TRUE(aggregator(" is a kind of an antelope", &result, &buffer));
  EXPECT_EQ(",G,az,elle, is a kind of an antelope", result);
}

TEST(AggregationOperatorsTest, ConcatInts) {
  AggregationOperator<CONCAT, INT32, STRING> aggregator(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  StringPiece result("");
  EXPECT_TRUE(aggregator(-7, &result, &buffer));
  EXPECT_EQ(",-7", result);
  EXPECT_TRUE(aggregator(0, &result, &buffer));
  EXPECT_EQ(",-7,0", result);
}

TEST(AggregationOperatorsTest, First) {
  AggregationOperator<FIRST, INT32, INT32> aggregator(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  int32 result = 7;
  EXPECT_TRUE(aggregator(1, &result, &buffer));
  EXPECT_EQ(7, result);
  EXPECT_TRUE(aggregator(9, &result, &buffer));
  EXPECT_EQ(7, result);
}

TEST(AggregationOperatorsTest, Last) {
  AggregationOperator<LAST, INT32, INT32> aggregator(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;

  int32 result = 7;
  EXPECT_TRUE(aggregator(1, &result, &buffer));
  EXPECT_EQ(1, result);
  EXPECT_TRUE(aggregator(9, &result, &buffer));
  EXPECT_EQ(9, result);
}

TEST(AggregationOperatorsTest, LastCopyDepth) {
  AggregationOperator<LAST, STRING, STRING, true> deep_laster(
      HeapBufferAllocator::Get());
  std::unique_ptr<Buffer> buffer;
  AggregationOperator<LAST, STRING, STRING, false> shallow_laster(
      HeapBufferAllocator::Get());

  char data1[4] = "One";
  char data2[4] = "Two";
  StringPiece deep_result(data1);
  StringPiece shallow_result(data1);
  EXPECT_TRUE(deep_laster(data2, &deep_result, &buffer));
  EXPECT_TRUE(shallow_laster(data2, &shallow_result, &buffer));
  data2[2] = 't';
  EXPECT_EQ("Two", deep_result);
  EXPECT_EQ("Twt", shallow_result);
}

}  // namespace
}  // namespace aggregations
}  // namespace supersonic
