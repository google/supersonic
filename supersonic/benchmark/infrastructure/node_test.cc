// Copyright 2012 Google Inc.  All Rights Reserved
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

#include "supersonic/benchmark/infrastructure/node.h"

#include <memory>

#include "supersonic/benchmark/infrastructure/cursor_statistics_mock.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

namespace supersonic {

namespace {

class BenchmarkTreeNodeTest : public testing::Test {
 protected:
  // Does not take ownership of the argument.
  virtual MockCursorStatistics* ExpectOneCall(
      MockCursorStatistics* mock_stats) {
    // Expect exactly one call to GatherData() - tree should only be traversed
    // once.
    EXPECT_CALL(*mock_stats, GatherData());
    return mock_stats;
  }
};

TEST_F(BenchmarkTreeNodeTest, ChildrenTest) {
  // Create benchmark nodes.
  std::unique_ptr<BenchmarkTreeNode> root(new BenchmarkTreeNode(NULL));

  std::unique_ptr<BenchmarkTreeNode> child_1(new BenchmarkTreeNode(NULL));
  BenchmarkTreeNode* child_1_ptr = child_1.get();

  std::unique_ptr<BenchmarkTreeNode> child_2(new BenchmarkTreeNode(NULL));
  BenchmarkTreeNode* child_2_ptr = child_2.get();

  std::unique_ptr<BenchmarkTreeNode> child_1_1(new BenchmarkTreeNode(NULL));
  BenchmarkTreeNode* child_1_1_ptr = child_1_1.get();

  std::unique_ptr<BenchmarkTreeNode> child_1_2(new BenchmarkTreeNode(NULL));
  BenchmarkTreeNode* child_1_2_ptr = child_1_2.get();

  std::unique_ptr<BenchmarkTreeNode> child_2_1(new BenchmarkTreeNode(NULL));
  BenchmarkTreeNode* child_2_1_ptr = child_2_1.get();

  // Attach children, relinquish scoped_ptr's ownership.
  root->AddChild(child_1.release());
  root->AddChild(child_2.release());

  child_1_ptr->AddChild(child_1_1.release());
  child_1_ptr->AddChild(child_1_2.release());

  child_2_ptr->AddChild(child_2_1.release());

  // Length tests.
  ASSERT_EQ(2, root->GetChildren().size());
  ASSERT_EQ(2, root->GetChildren()[0]->GetChildren().size());
  ASSERT_EQ(1, root->GetChildren()[1]->GetChildren().size());

  // Node tests.
  EXPECT_EQ(child_1_ptr, root->GetChildren()[0].get());
  EXPECT_EQ(child_2_ptr, root->GetChildren()[1].get());

  EXPECT_EQ(child_1_1_ptr, root->GetChildren()[0]->GetChildren()[0].get());
  EXPECT_EQ(child_1_2_ptr, root->GetChildren()[0]->GetChildren()[1].get());
  EXPECT_EQ(child_2_1_ptr, root->GetChildren()[1]->GetChildren()[0].get());
}

TEST_F(BenchmarkTreeNodeTest, GatherDataTest) {
  CursorWithBenchmarkListener dummy;

  std::unique_ptr<CursorStatistics> stats1(
      ExpectOneCall(new MockCursorStatistics(&dummy)));
  CursorStatistics* stats1_ptr = stats1.get();

  std::unique_ptr<CursorStatistics> stats1_1(
      ExpectOneCall(new MockCursorStatistics(&dummy)));
  CursorStatistics* stats1_1_ptr = stats1_1.get();

  std::unique_ptr<CursorStatistics> stats1_1_1(
      ExpectOneCall(new MockCursorStatistics(&dummy)));
  CursorStatistics* stats1_1_1_ptr = stats1_1_1.get();

  std::unique_ptr<CursorStatistics> stats1_2(
      ExpectOneCall(new MockCursorStatistics(&dummy)));
  CursorStatistics* stats1_2_ptr = stats1_2.get();

  std::unique_ptr<BenchmarkTreeNode> node1(
      new BenchmarkTreeNode(stats1.release()));
  std::unique_ptr<BenchmarkTreeNode> node1_1(
      new BenchmarkTreeNode(stats1_1.release()));
  std::unique_ptr<BenchmarkTreeNode> node1_1_1(
      new BenchmarkTreeNode(stats1_1_1.release()));
  std::unique_ptr<BenchmarkTreeNode> node1_2(
      new BenchmarkTreeNode(stats1_2.release()));

  // Check statistics.
  EXPECT_EQ(stats1_ptr, &node1->GetStats());
  EXPECT_EQ(stats1_1_ptr, &node1_1->GetStats());
  EXPECT_EQ(stats1_1_1_ptr, &node1_1_1->GetStats());
  EXPECT_EQ(stats1_2_ptr, &node1_2->GetStats());

  // Attach children.
  node1_1->AddChild(node1_1_1.release());
  node1->AddChild(node1_1.release());
  node1->AddChild(node1_2.release());

  // Run data gathering.
  node1->GatherAllData();
}

}  // namespace

}  // namespace supersonic

