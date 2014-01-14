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
//
// The file contains tests for the RemoteDOTDrawer. The tests also produce links
// to remotely generated DOT diagrams - see test logs for more information.

#include "supersonic/benchmark/dot/dot_drawer.h"
#include <memory>

#include "supersonic/benchmark/proto/benchmark.pb.h"
#include "supersonic/benchmark/infrastructure/benchmark_listener_mock.h"
#include "supersonic/benchmark/infrastructure/cursor_statistics.h"
#include "supersonic/benchmark/infrastructure/node.h"
#include "supersonic/cursor/base/cursor.h"

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/pointer_vector.h"

namespace supersonic {

namespace {

using testing::HasSubstr;

// Fake cursor statistics object which allows for direct benchmark data
// specification.
class FakeCursorStatistics : public CursorStatistics {
 public:
  // Does not take ownership of the passed CursorWithBenchmarkListener object.
  FakeCursorStatistics(
      CursorWithBenchmarkListener* listener)
      : CursorStatistics(
          listener,
          NULL,  // No relative stats - feature not tested.
          BenchmarkData::BENCHMARKED) {}  // Any value - feature not tested.

  // This function is used to hijack the benchmark data stored within the cursor
  // statistics object and replace it with the argument. This approach is used
  // instead of a more typical getter function mock, as the getter is
  // non-virtual, and it would require significant production code adaptation
  // to keep it that way and still be able to mock it.
  FakeCursorStatistics* SetFakeData(const BenchmarkData& data) {
    benchmark_data_ = data;
    return this;
  }

  // Fake method stub.
  void GatherData() {
    LOG(FATAL) << "Method should not have been called!";
  }
};

class DOTDrawerTest : public testing::Test {
 protected:
  // Caller takes ownership of the constructed node.
  BenchmarkTreeNode* GetNodeForData(const BenchmarkData& data) {
    entries_.push_back(
        new CursorWithBenchmarkListener(NULL, new MockBenchmarkListener));

    std::unique_ptr<CursorStatistics> stats(
        (new FakeCursorStatistics(entries_.back().get()))->SetFakeData(data));
    return new BenchmarkTreeNode(stats.release());
  }

  void SetCommonBenchmarkData(BenchmarkData* data,
                              const string& name,
                              BenchmarkData::CursorType type,
                              int64 total_subtree_time,
                              int64 processing_time,
                              double relative_time,
                              int64 rows_processed,
                              double throughput,
                              int64 next_calls) {
    data->set_cursor_name(name);
    data->set_cursor_type(type);
    data->set_total_subtree_time(total_subtree_time);
    data->set_processing_time(processing_time);
    data->set_relative_time(relative_time);
    data->set_rows_processed(rows_processed);
    data->set_throughput(throughput);
    data->set_next_calls(next_calls);
  }

  util::gtl::PointerVector<CursorWithBenchmarkListener> entries_;
};

TEST_F(DOTDrawerTest, TinyTest) {

  string output_dot;
  std::unique_ptr<DOTDrawer> string_drawer(
      new DOTDrawer(CreateStringOutputWriter(&output_dot), "DrawerTest"));

  BenchmarkData data1;
  BenchmarkData data2;

  data1.set_cursor_name("ParentCursor");
  data2.set_cursor_name("ChildCursor");

  data1.set_total_subtree_time(1500);
  data2.set_total_subtree_time(800);

  data1.set_processing_time(1000);
  data2.set_processing_time(800);

  data1.set_relative_time(100.0);
  data2.set_relative_time(80.0);

  data2.set_rows_processed(350);
  data2.set_return_rate(45.0);

  std::unique_ptr<BenchmarkTreeNode> node1(GetNodeForData(data1));
  std::unique_ptr<BenchmarkTreeNode> node2(GetNodeForData(data2));
  node1->AddChild(node2.release());


  string_drawer->DrawDOT(*node1);

  EXPECT_THAT(output_dot, HasSubstr("Total time: 1.50 ms"));
  EXPECT_THAT(output_dot, HasSubstr("GlobalStats"));

  EXPECT_THAT(output_dot, HasSubstr("ParentCursor0->Root"));
  EXPECT_THAT(output_dot, HasSubstr("ChildCursor1->ParentCursor0"));
}

TEST_F(DOTDrawerTest, RunMediumTreeExample) {

  string output_dot;
  std::unique_ptr<DOTDrawer> string_drawer(
      new DOTDrawer(CreateStringOutputWriter(&output_dot), "DrawerTest"));

  BenchmarkData data1;
  SetCommonBenchmarkData(&data1,
                         "PassAllCursor",
                         BenchmarkData::BENCHMARKED,
                         /* total subtree time */ 2000,
                         /* processing time */ 500,
                         /* relative time */ 25.0,
                         /* rows processed */ 140,
                         /* throughput */ 0.28,
                         /* Next() calls */ 1);

  BenchmarkData data1_1;
  SetCommonBenchmarkData(&data1_1,
                         "JoinCursor",
                         BenchmarkData::BENCHMARKED,
                         /* total subtree time */ 500,
                         /* processing time */ 200,
                         /* relative time */ 10.0,
                         /* rows processed */ 40,
                         /* throughput */ 0.2,
                         /* Next() calls */ 2);
  data1_1.set_index_set_up_time(50);
  data1_1.set_matching_time(100);

  BenchmarkData data1_1_1;
  SetCommonBenchmarkData(&data1_1_1,
                         "LeafCursor",
                         BenchmarkData::BENCHMARKED,
                         /* total subtree time */ 200,
                         /* processing time */ 200,
                         /* relative time */ 10.0,
                         /* rows processed */ 50,
                         /* throughput */ 0.25,
                         /* Next() calls */ 1);

  BenchmarkData data1_1_2;
  data1_1_2.set_cursor_name("TransparentCursor");
  data1_1_2.set_cursor_type(BenchmarkData::NOT_BENCHMARKED);

  BenchmarkData data1_2;
  SetCommonBenchmarkData(&data1_2,
                         "PreprocessCursor",
                         BenchmarkData::BENCHMARKED,
                         /* total subtree time */ 1000,
                         /* processing time */ 500,
                         /* relative time */ 25.0,
                         /* rows processed */ 100,
                         /* throughput */ 0.2,
                         /* Next() calls */ 1);
  data1_2.set_preprocessing_time(400);

  BenchmarkData data1_2_1;
  SetCommonBenchmarkData(&data1_2_1,
                         "LeafCursor",
                         BenchmarkData::BENCHMARKED,
                         /* total subtree time */ 500,
                         /* processing time */ 500,
                         /* relative time */ 25.0,
                         /* rows processed */ 100,
                         /* throughput */ 0.2,
                         /* Next() calls */ 1);

  BenchmarkData data1_1_2_1;
  data1_1_2_1.set_cursor_name("UnknownCursor");
  data1_1_2_1.set_cursor_type(BenchmarkData::UNRECOGNISED);

  std::unique_ptr<BenchmarkTreeNode> node1(GetNodeForData(data1));
  std::unique_ptr<BenchmarkTreeNode> node1_1(GetNodeForData(data1_1));
  std::unique_ptr<BenchmarkTreeNode> node1_2(GetNodeForData(data1_2));
  std::unique_ptr<BenchmarkTreeNode> node1_1_1(GetNodeForData(data1_1_1));
  std::unique_ptr<BenchmarkTreeNode> node1_1_2(GetNodeForData(data1_1_2));
  std::unique_ptr<BenchmarkTreeNode> node1_2_1(GetNodeForData(data1_2_1));
  std::unique_ptr<BenchmarkTreeNode> node1_1_2_1(GetNodeForData(data1_1_2_1));

  node1_1_2->AddChild(node1_1_2_1.release());

  node1_1->AddChild(node1_1_1.release());
  node1_1->AddChild(node1_1_2.release());

  node1_2->AddChild(node1_2_1.release());

  node1->AddChild(node1_1.release());
  node1->AddChild(node1_2.release());


  string_drawer->DrawDOT(*node1);

  EXPECT_THAT(output_dot, HasSubstr("Total time: 2.00 ms"));
  EXPECT_THAT(output_dot, HasSubstr("GlobalStats"));

  EXPECT_THAT(output_dot, HasSubstr("PassAllCursor0->Root"));
  EXPECT_THAT(output_dot, HasSubstr("JoinCursor1->PassAllCursor0"));
  EXPECT_THAT(output_dot, HasSubstr("PreprocessCursor5->PassAllCursor0"));
  EXPECT_THAT(output_dot, HasSubstr("LeafCursor2->JoinCursor1"));
  EXPECT_THAT(output_dot, HasSubstr("UnknownCursor4->JoinCursor1"));
  EXPECT_THAT(output_dot, HasSubstr("LeafCursor6->PreprocessCursor5"));

  // node1_1_2 is not benchmarked, so the following should not be present
  // in the diagram.
  EXPECT_THAT(output_dot, Not(HasSubstr("TransparentCursor")));
}

TEST_F(DOTDrawerTest, MultipleUseFail) {
  string output_dot;
  std::unique_ptr<DOTDrawer> string_drawer(
      new DOTDrawer(CreateStringOutputWriter(&output_dot), "DrawerTest"));

  BenchmarkData data;
  std::unique_ptr<BenchmarkTreeNode> node(GetNodeForData(data));

  string_drawer->DrawDOT(*node);
  EXPECT_DEATH(string_drawer->DrawDOT(*node),
               "DOTDrawer object is for single use only.");
}

}  // namespace

}  // namespace supersonic
