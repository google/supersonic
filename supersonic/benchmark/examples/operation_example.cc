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
// File containing several examples of operation benchmarks.

#include <memory>

#include "supersonic/benchmark/examples/common_utils.h"
#include "supersonic/cursor/core/merge_union_all.h"
#include "supersonic/supersonic.h"
#include "supersonic/testing/block_builder.h"

#include "supersonic/utils/file_util.h"

#include "supersonic/utils/container_literal.h"
#include "supersonic/utils/random.h"

#include <gflags/gflags.h>
DEFINE_string(output_directory, "", "Directory to which the output files will"
    " be written.");


namespace supersonic {

namespace {

using util::gtl::Container;

const size_t kInputRowCount = 1000000;
const size_t kGroupNum = 50;

Operation* CreateGroup() {
  MTRandom random(0);
  BlockBuilder<STRING, INT32> builder;
  for (int64 i = 0; i < kInputRowCount; ++i) {
    builder.AddRow(StringPrintf("test_string_%lld", i % kGroupNum),
                   random.Rand32());
  }

  std::unique_ptr<Operation> group(GroupAggregate(
      ProjectAttributeAt(0),
      (new AggregationSpecification)->AddAggregation(MAX, "col1", "col1_maxes"),
      NULL, new Table(builder.Build())));
  return group.release();
}

Operation* CreateCompute() {
  MTRandom random(0);
  BlockBuilder<INT32, INT64, DOUBLE> builder;
  for (int64 i = 0; i < kInputRowCount; ++i) {
    builder.AddRow(random.Rand32(), random.Rand64(), random.RandDouble());
  }

  return Compute(Multiply(AttributeAt(0),
                          Plus(Sin(AttributeAt(2)),
                               Exp(AttributeAt(1)))),
                 new Table(builder.Build()));
}

SortOrder* CreateExampleSortOrder() {
  return (new SortOrder)
      ->add(ProjectAttributeAt(0), ASCENDING)
      ->add(ProjectAttributeAt(1), DESCENDING);
}

Operation* CreateSort(size_t input_row_count) {
  MTRandom random(0);
  BlockBuilder<INT32, STRING> builder;
  for (int64 i = 0; i < input_row_count; ++i) {
    builder.AddRow(random.Rand32(), StringPrintf("test_string_%lld", i));
  }

  return Sort(
      CreateExampleSortOrder(),
      NULL,
      std::numeric_limits<size_t>::max(),
      new Table(builder.Build()));
}

Operation* CreateMergeUnion() {
  return MergeUnionAll(CreateExampleSortOrder(),
                       Container(CreateSort(kInputRowCount),
                                 CreateSort(2 * kInputRowCount)));
}

Operation* CreateHashJoin() {
  std::unique_ptr<Operation> lhs(CreateSort(kInputRowCount));
  std::unique_ptr<Operation> rhs(CreateGroup());

  std::unique_ptr<CompoundMultiSourceProjector> projector(
      new CompoundMultiSourceProjector());
  projector->add(0, ProjectAllAttributes("L."));
  projector->add(1, ProjectAllAttributes("R."));

  return new HashJoinOperation(LEFT_OUTER,
                               ProjectAttributeAt(1),
                               ProjectAttributeAt(0),
                               projector.release(),
                               UNIQUE,
                               lhs.release(),
                               rhs.release());
}

Operation* SimpleTreeExample() {
  MTRandom random(0);
  // col0, col1  , col2  , col3, col4
  // name, salary, intern, age , boss_name
  BlockBuilder<STRING, INT32, BOOL, INT32, STRING> builder;
  for (int64 i = 0; i < kInputRowCount; ++i) {
    builder.AddRow(StringPrintf("Name%lld", i),
                   (random.Rand16() % 80) * 100,
                   random.Rand16() % 1000 == 0 && i > kGroupNum,
                   (random.Rand16() % 60) + 20,
                   StringPrintf("Name%lld", i % kGroupNum));
  }

  std::unique_ptr<Operation> named_columns(Project(
      ProjectRename(Container("name", "salary", "intern", "age", "boss_name"),
                    ProjectAllAttributes()),
      new Table(builder.Build())));

  std::unique_ptr<Operation> filter1(Filter(NamedAttribute("intern"),
                                            ProjectAllAttributes(),
                                            named_columns.release()));
  std::unique_ptr<Operation> compute(Compute(
      (new CompoundExpression)
          ->Add(NamedAttribute("name"))
          ->Add(NamedAttribute("intern"))
          ->Add(NamedAttribute("boss_name"))
          ->AddAs("ratio",
                  Divide(NamedAttribute("salary"), NamedAttribute("age"))),
      filter1.release()));

  std::unique_ptr<Operation> group(GroupAggregate(
      ProjectNamedAttribute("boss_name"),
      (new AggregationSpecification)->AddAggregation(MAX, "ratio", "max_ratio"),
      NULL, compute.release()));

  // Let every fourth pass.
  std::unique_ptr<Operation> filter2(
      Filter(Equal(ConstInt32(0), Modulus(Sequence(), ConstInt32(4))),
             ProjectAllAttributes(), new Table(builder.Build())));

  std::unique_ptr<CompoundMultiSourceProjector> projector(
      new CompoundMultiSourceProjector());
  projector->add(0, ProjectAllAttributes("L."));
  projector->add(1, ProjectAllAttributes("R."));

  return new HashJoinOperation(INNER,
                               ProjectAttributeAt(0),
                               ProjectNamedAttribute("boss_name"),
                               projector.release(),
                               UNIQUE,
                               filter2.release(),
                               group.release());
}

typedef vector<Operation*>::iterator operation_iterator;

void Run() {
  vector<Operation*> operations(Container(
      CreateGroup(),
      CreateSort(kInputRowCount),
      CreateCompute(),
      CreateMergeUnion(),
      CreateHashJoin(),
      SimpleTreeExample()));

  GraphVisualisationOptions options(DOT_FILE);
  for (int64 i = 0; i < operations.size(); ++i) {
    options.file_name = File::JoinPath(
        FLAGS_output_directory, StrCat("benchmark_", i, ".dot"));


    // Automatically disposes of the operations after having drawn DOT graphs.
    BenchmarkOperation(
        operations[i],
        "Operation Benchmark",
        options,
        /* 16KB (optimised for cache size) */ 16 * Cursor::kDefaultRowCount,
        /* log result? */ false);
  }
}

}  // namespace

}  // namespace supersonic

int main(int argc, char *argv[]) {
  supersonic::SupersonicInit(&argc, &argv);
  supersonic::Run();
}
