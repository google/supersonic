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

#include "supersonic/benchmark/manager/benchmark_manager.h"

#include <memory>

#include "supersonic/benchmark/infrastructure/cursor_statistics_mock.h"
#include "supersonic/cursor/base/cursor_mock.h"

#include "supersonic/utils/file_util.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

namespace supersonic {

namespace {

using testing::HasSubstr;
using testing::InvokeWithoutArgs;
using testing::NiceMock;
using testing::Return;
using testing::ReturnRef;
using testing::_;


class MockBenchmarkNode : public BenchmarkTreeNode {
 public:
  explicit MockBenchmarkNode(CursorStatistics* cursor_statistics)
      : BenchmarkTreeNode(cursor_statistics) {}

  MOCK_METHOD0(GatherAllData, void());
};

class BenchmarkManagerTest : public testing::Test {
 protected:
  // Caller takes ownership of the Cursor.
  MockCursor* CreateMockCursor() {
    // Using NiceMock for cursor as BenchmarkManager only acts on the cursor
    // through the tree builder.
    std::unique_ptr<MockCursor> cursor(new NiceMock<MockCursor>);
    ON_CALL(*cursor, schema()).WillByDefault(ReturnRef(schema_));
    ON_CALL(*cursor, GetCursorId()).WillByDefault(Return(VIEW));
    return cursor.release();
  }

  // Caller takes ownership of the result node.
  BenchmarkTreeNode* CreateMockNodeWithExpectation() {
    std::unique_ptr<CursorStatistics> stats(new MockCursorStatistics(&dummy_));
    std::unique_ptr<MockBenchmarkNode> node(
        new MockBenchmarkNode(stats.release()));
    EXPECT_CALL(*node, GatherAllData());
    return node.release();
  }

  TupleSchema schema_;
  CursorWithBenchmarkListener dummy_;
};

TEST_F(BenchmarkManagerTest, TypicalUseCaseTest) {
  rowcount_t block_size = -1;
  std::unique_ptr<MockCursor> cursor(CreateMockCursor());

  // Return EOS on next call, hence there should only be one call to Next()
  // with the argument being the specified block size.
  ON_CALL(*cursor, Next(_)).WillByDefault(
      InvokeWithoutArgs(&ResultView::EOS));
  EXPECT_CALL(*cursor, Next(_)).Times(0);
  EXPECT_CALL(*cursor, Next(block_size));
  GraphVisualisationOptions options(DOT_STRING);
  string dot_code = PerformBenchmark("ManagerTest",
                                     cursor.release(),
                                     block_size,
                                     options);
  EXPECT_THAT(dot_code, HasSubstr("ManagerTest"));
}

TEST_F(BenchmarkManagerTest, SetUpTest) {
  std::unique_ptr<Cursor> cursor(CreateMockCursor());
  std::unique_ptr<BenchmarkDataWrapper> data_wrapper(
      SetUpBenchmarkForCursor(cursor.release()));
  std::unique_ptr<Cursor> transformed_cursor(data_wrapper->release_cursor());

  EXPECT_TRUE(transformed_cursor.get() != NULL);
  ASSERT_TRUE(data_wrapper->node() != NULL);

  const BenchmarkData& data =
      data_wrapper->node()->GetStats().GetBenchmarkData();
  EXPECT_EQ("VIEW", data.cursor_name());
  EXPECT_EQ(BenchmarkData::BENCHMARKED, data.cursor_type());
}

class BenchmarkManagerWithOutputTest
    : public BenchmarkManagerTest,
      public testing::WithParamInterface<Destination> {
 protected:
  virtual GraphVisualisationOptions CreateOptions(
      Destination destination) {
    GraphVisualisationOptions options(destination);
    if (destination == DOT_FILE) {
      options.file_name = TempFile::TempFilename(NULL);
    }

    return options;
  }
};

INSTANTIATE_TEST_CASE_P(Destination,
                        BenchmarkManagerWithOutputTest,
                        testing::Values(DOT_STRING, DOT_FILE));

TEST_P(BenchmarkManagerWithOutputTest, CreateStringGraphTest) {
  std::unique_ptr<BenchmarkTreeNode> node(CreateMockNodeWithExpectation());
  CreateGraph("CreateGraphTest",
              node.get(),
              CreateOptions(GetParam()));
}

}  // namespace

}  // namespace supersonic
